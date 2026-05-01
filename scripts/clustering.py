from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col, count, when, isnan
from pyspark.sql.types import NumericType
import numpy as np
import os
import logging
import argparse
import traceback
import time


class AutoClusteringPipeline:   
   def __init__(self, input_table, output_table, output_path, max_missing=0.3, min_unique=0.3):
       self.logger = logging.getLogger(self.__class__.__name__)
       self.input_table = input_table
       self.output_table = output_table
       self.output_path = output_path
       self.max_missing = max_missing
       self.min_unique = min_unique
       self.spark = self._init_spark()
       self.data_mart = self._init_data_mart()
       self.numeric_columns = []
       self.logger.setLevel(logging.INFO)


   def _init_spark(self):
        self.logger.info("INITIALIZING SPARK SESSION")
        spark = SparkSession.builder \
            .config("spark.executor.cores", os.getenv('SPARK_EXECUTOR_CORES', '1'))  \
            .config("spark.driver.memory", os.getenv('SPARK_DRIVER_MEMORY', '1g')) \
            .config("spark.executor.memory", os.getenv('SPARK_EXECUTOR_MEMORY', '1g')) \
            .config("spark.default.parallelism", os.getenv('SPARK_DEFAULT_PARALLELISM', '4')) \
            .config("spark.sql.shuffle.partitions", os.getenv('SPARK_SQL_SHUFFLE_PARTITIONS', '4')) \
            .config("spark.jars", "/app/jars/mysql-connector-java-8.0.33.jar") \
            .config("spark.driver.extraClassPath", "/app/jars/mysql-connector-java-8.0.33.jar") \
            .appName("AutoClustering") \
            .getOrCreate()
        
        self.logger.info("SPARK SESSION CREATED")
        return spark


   def _init_data_mart(self):
       self.logger.info("INITIALIZING DATA MART")
       try:
           DataMart = self.spark._jvm.com.foodfacts.datamart.DataMart
           dm = DataMart(
               self.spark._jsparkSession,
               float(self.max_missing),
               float(self.min_unique)
           )
           self.logger.info("DATA MART INITIALIZED")
           return dm
       except Exception as e:
           self.logger.error(f"DATA MART INIT FAILED: {str(e)}")
           raise


   def load_and_preprocess(self):
       try:
           self.logger.info("STARTING DATA MART PROCESSING")
           self.logger.info(f"Calling data_mart.readProcessedData({self.input_table})")
           start_time = time.time()
           self.data_mart.readProcessedData(self.input_table)
           self.logger.info(f"DATA PROCESSING COMPLETED IN {time.time() - start_time:.2f}s")
           
           self.logger.info(f"Reading processed table: {self.input_table}_processed")
           df = self.spark.sql(f"SELECT * FROM {self.input_table}_processed")
           self.logger.info(f"Loaded processed DataFrame, row count: {df.count()}")
           
           self.numeric_columns = df.columns
           self.logger.info(f"Detected {len(self.numeric_columns)} numeric columns")
           self.logger.info(f"Column names: {', '.join(self.numeric_columns)[:200]}...")
           return df
       except Exception as e:
           self.logger.error(f"LOAD ERROR: {str(e)}")
           self.logger.error(traceback.format_exc())
           raise


   def feature_engineering(self, df):
       try:
           self.logger.info("STARTING FEATURE ENGINEERING")
           self.logger.info(f"Assembling {len(self.numeric_columns)} features")
           
           assembler = VectorAssembler(
               inputCols=self.numeric_columns,
               outputCol="raw_features"
           )
           self.logger.info("Transforming with VectorAssembler")
           assembled_df = assembler.transform(df)
           self.logger.info("VECTOR ASSEMBLY COMPLETE")
           
           scaler = StandardScaler(
               inputCol="raw_features",
               outputCol="features",
               withStd=True,
               withMean=True
           )
           self.logger.info("Fitting scaler model")
           scaler_model = scaler.fit(assembled_df)
           self.logger.info("SCALER MODEL FITTED")
           
           self.logger.info("Applying feature scaling")
           scaled_df = scaler_model.transform(assembled_df)
           self.logger.info("FEATURE SCALING COMPLETE")
           
           return scaled_df
       except Exception as e:
           self.logger.error(f"FEATURE ENGINEERING ERROR: {str(e)}")
           self.logger.error(traceback.format_exc())
           raise


   def train(self, df, k=11):
       try:
           self.logger.info(f"STARTING KMEANS TRAINING (k={k})")
           start_time = time.time()
           model = KMeans(featuresCol="features", k=k, seed=42).fit(df)
           self.logger.info(f"TRAINING COMPLETED IN {time.time() - start_time:.2f}s")
           return model
       except Exception as e:
           self.logger.error(f"TRAINING ERROR: {str(e)}")
           self.logger.error(traceback.format_exc())
           raise


   def save_results(self, model, df):
       try:
           self.logger.info("TRANSFORMING DATAFRAME WITH MODEL")
           results = model.transform(df).select("prediction", *self.numeric_columns)
           
           self.logger.info("SAVING RESULTS TO DATAMART")
           self.data_mart.writeResults(results._jdf, self.output_table)
           
           self.logger.info(f"SAVING MODEL TO {self.output_path}/model")
           model.write().overwrite().save(f"{self.output_path}/model")
       except Exception as e:
           self.logger.error(f"SAVE ERROR: {str(e)}")
           self.logger.error(traceback.format_exc())
           raise


   def run(self):
       try:
           df = self.load_and_preprocess()
           processed_df = self.feature_engineering(df)
           model = self.train(processed_df)
           self.save_results(model, processed_df)
           self.logger.info("CLUSTERING SUCCESSFULLY COMPLETED!")
       except Exception as e:
           self.logger.error(f"RUN ERROR: {str(e)}", exc_info=True)
           raise
       finally:
           self.logger.info("STOPPING SPARK SESSION")
           self.spark.stop()
           self.logger.info("SPARK SESSION STOPPED")


class AutoClusteringConfig:
   def __init__(self):
       self.parser = argparse.ArgumentParser(description="Автоматическая кластеризация")
       self._setup_arguments()
  
   def _setup_arguments(self):
       self.parser.add_argument("--input_table", required=True,
                              help="Имя таблицы в MySQL для чтения данных")
       self.parser.add_argument("--output_table", required=True,
                              help="Имя таблицы в MySQL для сохранения результатов")
       self.parser.add_argument("-o", "--output", required=True,
                              help="Директория для сохранения весов модели")
       self.parser.add_argument("--max_missing", type=float, default=0.3,
                              help="Максимальная доля пропущенных значений (по умолчанию 0.3)")
       self.parser.add_argument("--min_unique", type=float, default=0.3,
                              help="Минимальное количество уникальных значений (по умолчанию 0.3)")
       self.parser.add_argument("-v", "--verbose", action="store_true",
                              help="Включить детальное логирование")


def main():
   logging.basicConfig(
       level=logging.INFO,
       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
       datefmt='%Y-%m-%d %H:%M:%S'
   )
  
   config = AutoClusteringConfig()
   args = config.parser.parse_args()
  
   if args.verbose:
       logging.getLogger().setLevel(logging.DEBUG)
  
   pipeline = AutoClusteringPipeline(
       input_table=args.input_table,
       output_table=args.output_table,
       output_path=args.output,
       max_missing=args.max_missing,
       min_unique=args.min_unique
   )
  
   pipeline.run()


if __name__ == "__main__":
   main()



