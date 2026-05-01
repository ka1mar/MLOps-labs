import argparse
import logging
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('WordCount')

class WordCounter:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("DockerWordCount") \
            .master("spark://spark-master:7077") \
            .getOrCreate()
            
        self.word_counts = None

    def process(self, input_path, output_path=None):
        try:
            # Чтение и обработка данных
            df = self.spark.read.text(input_path)
            words = df.selectExpr("explode(split(value, ' ')) as word")
            self.word_counts = words.groupBy("word").count().orderBy("count", ascending=False)
            
            # Вывод результатов
            logger.info("Результаты подсчета:")
            self.word_counts.show()
            
            # Сохранение результатов
            if output_path:
                self.word_counts.write.mode("overwrite").csv(output_path, header=True)
                logger.info(f"Результаты сохранены в: {output_path}")
                
        except Exception as e:
            logger.error(f"Ошибка: {str(e)}", exc_info=True)
            raise
        finally:
            self.spark.stop()
            logger.info("Spark сессия остановлена")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", required=True)
    parser.add_argument("-o", "--output")
    args = parser.parse_args()
    
    wc = WordCounter()
    wc.process(args.input, args.output)