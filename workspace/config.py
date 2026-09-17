import os
from dotenv import load_dotenv


load_dotenv()

class SparkConfig:
    APP_NAME = "Greenplum_ML_Pipeline"
    MASTER = "local[*]"
    
    SETTINGS = {
        "spark.driver.memory": "2g",
        "spark.executor.memory": "4g",
        "spark.memory.fraction": "0.8",
        "spark.sql.shuffle.partitions": "10",
        "spark.default.parallelism": "10",
    }