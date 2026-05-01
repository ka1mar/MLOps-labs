package com.foodfacts.datamart

import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, isnan, avg, coalesce, lit}
import org.apache.spark.sql.types.NumericType
import org.apache.spark.internal.Logging
import java.sql.DriverManager

class DataMart(
    spark: SparkSession,
    maxMissing: Double,
    minUnique: Double
) extends Logging {
  private val jdbcUrl = sys.env.getOrElse("MYSQL_URL", "jdbc:mysql://mysql:3306/foodfacts")
  private val jdbcUser = sys.env.getOrElse("MYSQL_USER", "user")
  private val jdbcPassword = sys.env.getOrElse("MYSQL_PASSWORD", "password")

  // Test database connection
  private def testConnection(): Unit = {
    logInfo("TESTING MYSQL CONNECTION...")
    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
      val conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
      val valid = conn.isValid(5)
      conn.close()
      if (valid) logInfo("MYSQL CONNECTION SUCCESS") 
      else throw new Exception("Connection invalid")
    } catch {
      case e: Exception =>
        logError(s"MYSQL CONNECTION FAILED: ${e.getMessage}")
        throw e
    }
  }

  def readProcessedData(table: String): DataFrame = {
    testConnection()
    
    logInfo(s"READING RAW TABLE: $table")
    val rawDF = try {
      spark.read
        .format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", jdbcUrl)
        .option("dbtable", table)
        .option("user", jdbcUser)
        .option("password", jdbcPassword)
        .load()
    } catch {
      case e: Exception =>
        logError(s"JDBC READ FAILED: ${e.getMessage}")
        throw e
    }
    
    logInfo(s"RAW TABLE LOADED (${rawDF.count()} rows)")
    logInfo("STARTING PREPROCESSING")
    val processedDF = preprocessData(rawDF)
    logInfo(s"PREPROCESSING COMPLETE (${processedDF.count()} rows)")
    processedDF.createOrReplaceTempView(s"${table}_processed")
    processedDF
  }

  private def preprocessData(df: DataFrame): DataFrame = {
    logInfo("FILTERING NUMERIC COLUMNS")
    val numericCols = df.schema.fields
      .filter(_.dataType.isInstanceOf[NumericType])
      .map(_.name)
    
    logInfo(s"FOUND ${numericCols.size} NUMERIC COLUMNS")
    
    if (numericCols.isEmpty) {
      logWarning("NO NUMERIC COLUMNS FOUND")
      return df
    }
    
    logInfo("CALCULATING TOTAL ROWS")
    val total = df.count().toDouble
    logInfo(s"TOTAL ROWS: $total")
    
    logInfo(s"FILTERING COLUMNS (maxMissing=$maxMissing, minUnique=$minUnique)")
    val filteredCols = numericCols.filter { colName =>
      logInfo(s"PROCESSING COLUMN: $colName")
      try {
        logInfo(s"Counting missing values: $colName")
        val missing = df.filter(col(colName).isNull || isnan(col(colName))).count()
        val missingRatio = missing / total
        
        logInfo(s"Counting unique values: $colName")
        val unique = df.select(colName).distinct().count()
        val uniqueRatio = unique / total
        
        val passed = (missingRatio < maxMissing) && (uniqueRatio >= minUnique)
        
        if (passed) logInfo(s"COLUMN PASSED: $colName (missing=$missingRatio, unique=$uniqueRatio)") 
        else logWarning(s"COLUMN REJECTED: $colName (missing=$missingRatio, unique=$uniqueRatio)")
        
        passed
      } catch {
        case e: Exception =>
          logError(s"COLUMN PROCESSING FAILED: $colName - ${e.getMessage}")
          false
      }
    }
    
    logInfo(s"SELECTED ${filteredCols.size} VALID COLUMNS: [${filteredCols.mkString(", ")}]")
    
    try {
      logInfo("COMPUTING MEAN VALUES FOR IMPUTATION")
      val meanValues = df.select(filteredCols.map(c => avg(c).alias(c)): _*).first()
      
      logInfo("STARTING IMPUTATION")
      val imputedDF = filteredCols.foldLeft(df) { (currentDF, colName) =>
        currentDF.withColumn(colName, 
          coalesce(col(colName), lit(meanValues.getAs[Double](colName))))
      }
      logInfo("IMPUTATION COMPLETE")
      
      imputedDF.select(filteredCols.map(col): _*)
    } catch {
      case e: Exception =>
        logError(s"IMPUTATION FAILED: ${e.getMessage}")
        throw e
    }
  }
  
  def writeResults(df: DataFrame, table: String): Unit = {
    logInfo(s"SAVING RESULTS TO TABLE: $table")
    try {
      val cleanedDF = postprocessData(df)
      cleanedDF.write
        .format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", jdbcUrl)
        .option("dbtable", table)
        .option("user", jdbcUser)
        .option("password", jdbcPassword)
        .mode(SaveMode.Append)
        .save()
      logInfo("SAVE COMPLETED SUCCESSFULLY")
    } catch {
      case e: Exception =>
        logError(s"SAVE FAILED: ${e.getMessage}")
        throw e
    }
  }

  private def postprocessData(df: DataFrame): DataFrame = {
    df.withColumn("prediction", col("prediction").cast("float"))
  }
}



