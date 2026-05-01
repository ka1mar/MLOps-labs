package com.foodfacts.datamart

import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, isnan, avg, coalesce}
import org.apache.spark.sql.types.NumericType
import org.apache.spark.internal.Logging

class DataMart(
    spark: SparkSession,
    maxMissing: Double,
    minUnique: Double
) extends Logging {
  private val jdbcUrl = sys.env.getOrElse("MYSQL_URL", "jdbc:mysql://mysql:3306/foodfacts")
  private val jdbcUser = sys.env.getOrElse("MYSQL_USER", "user")
  private val jdbcPassword = sys.env.getOrElse("MYSQL_PASSWORD", "password")

  def readProcessedData(table: String): DataFrame = {
    val rawDF = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", jdbcUrl)
      .option("dbtable", table)
      .option("user", jdbcUser)
      .option("password", jdbcPassword)
      .load()
    
    val processedDF = preprocessData(rawDF)
    processedDF.createOrReplaceTempView(s"${table}_processed")
    processedDF
  }

  def writeResults(df: DataFrame, table: String): Unit = {
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
  }

  private def preprocessData(df: DataFrame): DataFrame = {
    val numericCols = df.schema.fields
      .filter(_.dataType.isInstanceOf[NumericType])
      .map(_.name)
    
    if (numericCols.isEmpty) {
      logWarning("No numeric columns found in the dataset")
      return df
    }
    
    val total = df.count().toDouble
    logInfo(s"Total rows: $total")
    logInfo(s"Filtering columns with maxMissing=$maxMissing, minUnique=$minUnique")
    
    val filteredCols = numericCols.filter { colName =>
      val missing = df.filter(col(colName).isNull || isnan(col(colName))).count()
      val missingRatio = missing / total
      val unique = df.select(colName).distinct().count()
      val uniqueRatio = unique / total
      
      val passed = (missingRatio < maxMissing) && (uniqueRatio >= minUnique)
      
      if (passed) {
        logInfo(
          s"Column '$colName' PASSED: " +
          s"missing=${missingRatio.formatted("%.2f")}, " +
          s"unique=${uniqueRatio.formatted("%.2f")}"
        )
      } else {
        val reasons = new StringBuilder
        if (missingRatio >= maxMissing) reasons.append(s"missing=${missingRatio.formatted("%.2f")} >= $maxMissing")
        if (uniqueRatio < minUnique) {
          if (reasons.nonEmpty) reasons.append("; ")
          reasons.append(s"unique=${uniqueRatio.formatted("%.2f")} < $minUnique")
        }
        logWarning(s"Column '$colName' REJECTED: $reasons")
      }
      
      passed
    }
    
    logInfo(s"Selected ${filteredCols.size} of ${numericCols.size} columns: [${filteredCols.mkString(", ")}]")
    
    val imputedDF = filteredCols.foldLeft(df) { (currentDF, colName) =>
      currentDF.withColumn(colName, 
        coalesce(col(colName), avg(colName).over()))
    }
    
    imputedDF.select(filteredCols.map(col): _*)
  }

  private def postprocessData(df: DataFrame): DataFrame = {
    df.withColumn("prediction", col("prediction").cast("float"))
  }
}
