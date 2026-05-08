from pyspark.sql import SparkSession
from config import SparkConfig

class SparkManager:
    """Класс, инкапсулирующий логику инициализации и настройки SparkSession."""
    
    def __init__(self):
        self._spark = None

    def get_session(self) -> SparkSession:
        """Создает (или возвращает существующую) сконфигурированную сессию."""
        if self._spark is None:
            print("[*] Инициализация SparkSession с кастомными параметрами...")
            builder = SparkSession.builder \
                .appName(SparkConfig.APP_NAME) \
                .master(SparkConfig.MASTER)
            
            # Применяем все параметры тюнинга из конфигурации
            for key, value in SparkConfig.SETTINGS.items():
                builder = builder.config(key, value)
                
            self._spark = builder.getOrCreate()
            
            # Оставляем только важные логи, чтобы не засорять консоль
            self._spark.sparkContext.setLogLevel("ERROR")
            print("[*] SparkSession успешно создана.")
            
        return self._spark

    def stop(self):
        """Корректное завершение работы кластера."""
        if self._spark:
            self._spark.stop()
            print("[*] SparkSession остановлена.")