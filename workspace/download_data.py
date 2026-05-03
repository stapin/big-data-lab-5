import os
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id
from config import GreenplumConfig

class DataSeeder:
    """Класс для скачивания сырых данных и их первичной загрузки в Greenplum."""

    def __init__(self):
        # Используем абсолютный путь для JDBC драйвера
        self.jar_path = os.path.abspath("postgresql-42.5.4.jar")
        self.download_jdbc_driver()

        # Инициализируем Spark без конфликтов
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
        """Прямое скачивание драйвера БД."""
        if not os.path.exists(self.jar_path):
            print("[*] Скачивание PostgreSQL JDBC драйвера...")
            url = "https://jdbc.postgresql.org/download/postgresql-42.5.4.jar"
            urllib.request.urlretrieve(url, self.jar_path)
            print("[*] Драйвер успешно скачан.")

    def download_data(self):
        """Скачивание дампа данных."""
        if not os.path.exists(self.file_path):
            print(f"[*] Скачивание данных с {self.url}...")
            # Используем urllib вместо wget для надежности в Python-контейнере
            urllib.request.urlretrieve(self.url, self.file_path)
            print("[*] Данные скачаны.")
        else:
            print("[*] Файл с данными уже существует, загрузка пропущена.")

    def process_and_load(self):
        """Чтение файла, базовая очистка и запись в Greenplum."""
        print("[*] Чтение данных в Spark...")
        raw_df = self.spark.read.csv(self.file_path, sep='\t', header=True)

        target_columns = [
            "product_name", "energy-kcal_100g", "proteins_100g", 
            "fat_100g", "carbohydrates_100g"
        ]
        df = raw_df.select(*target_columns).dropna()

        # Приводим типы
        for c in target_columns[1:]:
            df = df.withColumn(c, col(c).cast("float"))
        
        # Добавляем уникальный ID (необходимо для ключа распределения Greenplum)
        df = df.withColumn("id", monotonically_increasing_id())

        # Берем 5% данных для быстрой работы
        df_sample = df.sample(fraction=0.05, seed=42)
        
        print(f"[*] Запись данных ({df_sample.count()} строк) в Greenplum...")
        
        # Запись через JDBC
        df_sample.write \
            .mode("overwrite") \
            .option("createTableOptions", "DISTRIBUTED BY (id)") \
            .jdbc(url=GreenplumConfig.JDBC_URL,
                  table=GreenplumConfig.RAW_TABLE,
                  properties=GreenplumConfig.PROPERTIES)
        
        print(f"[*] Данные успешно загружены в таблицу {GreenplumConfig.RAW_TABLE}!")

    def stop(self):
        self.spark.stop()

if __name__ == "__main__":
    seeder = DataSeeder()
    seeder.download_data()
    seeder.process_and_load()
    seeder.stop()