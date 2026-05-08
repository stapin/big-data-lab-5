import os
import time
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id
from config import GreenplumConfig

class DataSeeder:
    """Класс для скачивания сырых данных и их первичной загрузки в Greenplum."""

    def __init__(self):
        self.jar_path = os.path.abspath("postgresql-42.5.4.jar")
        self.download_jdbc_driver()

        self.spark = SparkSession.builder \
            .appName("Greenplum_Seeder") \
            .master("local[*]") \
            .config("spark.jars", self.jar_path) \
            .config("spark.driver.extraClassPath", self.jar_path) \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("ERROR")
        self.file_path = "openfoodfacts.csv.gz"
        self.url = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"

    def download_jdbc_driver(self):
        if not os.path.exists(self.jar_path):
            print("[*] Скачивание PostgreSQL JDBC драйвера...")
            urllib.request.urlretrieve("https://jdbc.postgresql.org/download/postgresql-42.5.4.jar", self.jar_path)
            print("[*] Драйвер скачан.")

    def download_data(self):
        if not os.path.exists(self.file_path):
            print(f"[*] Скачивание данных с {self.url}...")
            urllib.request.urlretrieve(self.url, self.file_path)
            print("[*] Данные скачаны.")
        else:
            print("[*] Файл с данными уже существует.")

    def process_and_load(self):
        print("[*] Чтение данных в Spark...")
        raw_df = self.spark.read.csv(self.file_path, sep='\t', header=True)

        target_columns = [
            "product_name", "energy-kcal_100g", "proteins_100g", 
            "fat_100g", "carbohydrates_100g"
        ]
        df = raw_df.select(*target_columns).dropna()

        for c in target_columns[1:]:
            df = df.withColumn(c, col(c).cast("float"))
        
        # Уникальный ID для ключа дистрибуции Greenplum
        df = df.withColumn("id", monotonically_increasing_id())

        df_sample = df.sample(fraction=0.05, seed=42)
        print(f"[*] Запись данных ({df_sample.count()} строк) в Greenplum...")
        
        # Реализация механизма Retry (Ожидание готовности базы)
        max_retries = 5
        for attempt in range(max_retries):
            try:
                df_sample.write \
                    .mode("overwrite") \
                    .option("createTableOptions", "DISTRIBUTED BY (id)") \
                    .jdbc(url=GreenplumConfig.JDBC_URL,
                          table=GreenplumConfig.RAW_TABLE,
                          properties=GreenplumConfig.PROPERTIES)
                
                print(f"[*] Данные УСПЕШНО загружены в таблицу {GreenplumConfig.RAW_TABLE}!")
                break
            except Exception as e:
                print(f"[!] Попытка {attempt + 1}/{max_retries} провалилась. База еще инициализируется...")
                if attempt < max_retries - 1:
                    print("Ожидание 15 секунд перед повтором...")
                    time.sleep(15)
                else:
                    print(f"Ошибка загрузки: {str(e)}")

    def stop(self):
        self.spark.stop()

if __name__ == "__main__":
    seeder = DataSeeder()
    seeder.download_data()
    seeder.process_and_load()
    seeder.stop()