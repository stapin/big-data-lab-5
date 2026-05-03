class GreenplumConfig:
    """Класс для хранения конфигурации подключения к Greenplum."""
    
    # ВАЖНО: Так как контейнеры делят сеть, мы стучимся на localhost!
    HOST = "localhost"
    PORT = "5432"
    
    # Дефолтная база и учетные данные для образов на базе pivotal/projectairws
    DB_NAME = "postgres" 
    USER = "gpadmin"
    PASSWORD = "pivotal" 
    
    # URL для JDBC подключения (Обязательно sslmode=disable)
    JDBC_URL = f"jdbc:postgresql://{HOST}:{PORT}/{DB_NAME}?sslmode=disable&stringtype=unspecified"
    
    # Свойства подключения для Spark
    PROPERTIES = {
        "user": USER,
        "password": PASSWORD,
        "driver": "org.postgresql.Driver"
    }
    
    # Таблицы
    RAW_TABLE = "products_raw"
    CLUSTERED_TABLE = "products_clustered"