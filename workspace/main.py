from config import GreenplumConfig
from spark_manager import SparkManager
from model import FoodClusteringModel

class ETLPipeline:
    """Главный оркестратор: Выгрузка из БД -> Обучение модели -> Загрузка в БД."""
    
    def __init__(self):
        # Инициализируем наши изолированные компоненты
        self.spark_manager = SparkManager()
        self.spark = self.spark_manager.get_session()
        self.ml_model = FoodClusteringModel(k_clusters=5)

    def extract_data(self):
        """Этап 1: Extract (Выгрузка данных из Greenplum)."""
        print(f"[*] Extract: Чтение таблицы {GreenplumConfig.RAW_TABLE} из Greenplum...")
        df = self.spark.read.jdbc(
            url=GreenplumConfig.JDBC_URL,
            table=GreenplumConfig.RAW_TABLE,
            properties=GreenplumConfig.PROPERTIES
        )
        # Кэшируем выгруженные данные в RAM, так как K-Means итеративный
        df.cache()
        print(f"[*] Выгружено строк: {df.count()}")
        return df

    def transform_and_model(self, df):
        """Этап 2: Transform (Запуск пайплайна машинного обучения)."""
        # Вся сложная математика теперь скрыта внутри класса модели
        result_df = self.ml_model.fit_predict(df)
        return result_df

    def load_data(self, df):
        """Этап 3: Load (Загрузка результатов обратно в Greenplum)."""
        print(f"[*] Load: Запись результатов в таблицу {GreenplumConfig.CLUSTERED_TABLE}...")
        df.write \
            .mode("overwrite") \
            .option("createTableOptions", "DISTRIBUTED BY (id)") \
            .jdbc(
                url=GreenplumConfig.JDBC_URL,
                table=GreenplumConfig.CLUSTERED_TABLE,
                properties=GreenplumConfig.PROPERTIES
            )
        print("[*] ETL процесс успешно завершен!")

    def run(self):
        """Точка входа пайплайна."""
        try:
            # Строгая последовательность E -> T -> L
            raw_data = self.extract_data()
            clustered_data = self.transform_and_model(raw_data)
            self.load_data(clustered_data)
        except Exception as e:
            print(f"[!] Ошибка во время ETL процесса: {e}")
        finally:
            # Гарантированное освобождение ресурсов
            self.spark_manager.stop()

if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run()