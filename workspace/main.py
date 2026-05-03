from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from config import GreenplumConfig

class KMeansETLPipeline:
    """ETL пайплайн для кластеризации данных из Greenplum."""

    def __init__(self):
        print("[*] Инициализация SparkSession...")
        self.spark = SparkSession.builder \
            .appName("Greenplum_ETL_KMeans") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4") \
            .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        self.df = None
        self.clustered_df = None

    def extract(self):
        """Выгрузка данных из источника (Greenplum)."""
        print(f"[*] Выгрузка (EXTRACT) из таблицы {GreenplumConfig.RAW_TABLE}...")
        self.df = self.spark.read.jdbc(
            url=GreenplumConfig.JDBC_URL,
            table=GreenplumConfig.RAW_TABLE,
            properties=GreenplumConfig.PROPERTIES
        ).cache()
        print(f"[*] Успешно выгружено {self.df.count()} строк.")

    def transform(self, k_clusters=5):
        """Очистка, векторизация, масштабирование и применение K-Means."""
        print("[*] Трансформация (TRANSFORM): Подготовка признаков и запуск ML модели...")
        
        feature_cols = ["energy-kcal_100g", "proteins_100g", "fat_100g", "carbohydrates_100g"]
        
        # 1. Векторизация
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df_vectorized = assembler.transform(self.df.dropna(subset=feature_cols))

        # 2. Масштабирование
        scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
        scaler_model = scaler.fit(df_vectorized)
        df_scaled = scaler_model.transform(df_vectorized)

        # 3. K-Means кластеризация
        kmeans = KMeans(featuresCol="scaledFeatures", predictionCol="cluster_id", k=k_clusters, seed=42)
        model = kmeans.fit(df_scaled)
        
        # 4. Формирование финального датафрейма для отправки в базу
        # Нам нужны только ID продукта, название и номер кластера (нормализация данных)
        self.clustered_df = model.transform(df_scaled).select("id", "product_name", "cluster_id")
        print("[*] Кластеризация успешно завершена. Пример результатов:")
        self.clustered_df.show(5)

    def load(self):
        """Загрузка результатов модели обратно в источник (Greenplum)."""
        print(f"[*] Загрузка (LOAD) результатов в таблицу {GreenplumConfig.CLUSTERED_TABLE}...")
        
        # Снова используем DISTRIBUTED BY для корректной балансировки в Greenplum
        self.clustered_df.write \
            .mode("overwrite") \
            .option("createTableOptions", "DISTRIBUTED BY (id)") \
            .jdbc(url=GreenplumConfig.JDBC_URL,
                  table=GreenplumConfig.CLUSTERED_TABLE,
                  properties=GreenplumConfig.PROPERTIES)
                  
        print("[*] Результаты модели успешно загружены в Greenplum!")

    def run(self):
        """Оркестратор запуска."""
        try:
            self.extract()
            self.transform(k_clusters=5)
            self.load()
        except Exception as e:
            print(f"[!] Ошибка во время выполнения пайплайна: {e}")
        finally:
            self.spark.stop()
            print("[*] ETL Пайплайн завершил работу.")

if __name__ == "__main__":
    pipeline = KMeansETLPipeline()
    pipeline.run()