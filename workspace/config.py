import os

class GreenplumConfig:
    """Конфигурация подключения к источнику данных (Greenplum)."""
    HOST = "localhost"
    PORT = "5432"
    DB_NAME = "postgres" 
    USER = "gpadmin"
    PASSWORD = "pivotal" 
    
    JDBC_URL = f"jdbc:postgresql://{HOST}:{PORT}/{DB_NAME}?sslmode=disable&stringtype=unspecified"
    
    PROPERTIES = {
        "user": USER,
        "password": PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    
    RAW_TABLE = "products_raw"
    CLUSTERED_TABLE = "products_clustered"

class SparkConfig:
    """Конфигурация среды вычислений Apache Spark."""
    APP_NAME = "Greenplum_ML_Pipeline"
    MASTER = "local[*]" # Использовать все доступные ядра CPU
    
    # Путь к драйверу для работы с Greenplum
    JDBC_DRIVER_PATH = os.path.abspath("postgresql-42.5.4.jar")
    
    # Тюнинг параметров кластера
    SETTINGS = {
        # Память
        "spark.driver.memory": "2g",       # Память управляющего узла
        "spark.executor.memory": "4g",     # Память рабочих узлов (важно для K-Means)
        "spark.memory.fraction": "0.8",    # Отдаем 80% памяти под кэш и вычисления
        
        # Партиционирование (Оптимизация)
        "spark.sql.shuffle.partitions": "10", # Снижаем с 200 (дефолт) до 10 для локальной работы
        "spark.default.parallelism": "10",    # Базовый параллелизм
        
        # Настройки драйвера БД
        "spark.jars": JDBC_DRIVER_PATH,
        "spark.driver.extraClassPath": JDBC_DRIVER_PATH
    }