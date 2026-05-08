import logging
from config import GreenplumConfig
from spark_manager import SparkManager
from model import FoodClusteringModel

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ETL_Orchestrator")

class ETLPipeline:
    
    def __init__(self):
        self.spark_manager = SparkManager()
        self.spark = self.spark_manager.get_session()
        self.ml_model = FoodClusteringModel(k_clusters=5)

    def extract_data(self):
        logger.info(f"Extract stage: Reading table '{GreenplumConfig.RAW_TABLE}' from Greenplum")
        df = self.spark.read.jdbc(
            url=GreenplumConfig.JDBC_URL,
            table=GreenplumConfig.RAW_TABLE,
            properties=GreenplumConfig.PROPERTIES
        )
        
        df.cache()
        row_count = df.count()
        logger.info(f"Extract stage completed. Rows extracted: {row_count}")
        return df

    def transform_and_model(self, df):
        logger.info("Transform stage: Starting machine learning pipeline")
        result_df = self.ml_model.fit_predict(df)
        return result_df

    def load_data(self, df):
        logger.info(f"Load stage: Writing results to table '{GreenplumConfig.CLUSTERED_TABLE}'")
        df.write \
            .mode("overwrite") \
            .option("createTableOptions", "DISTRIBUTED BY (id)") \
            .jdbc(
                url=GreenplumConfig.JDBC_URL,
                table=GreenplumConfig.CLUSTERED_TABLE,
                properties=GreenplumConfig.PROPERTIES
            )
        logger.info("Load stage completed successfully")

    def run(self):
        logger.info("Starting ETL pipeline execution")
        try:
            raw_data = self.extract_data()
            clustered_data = self.transform_and_model(raw_data)
            self.load_data(clustered_data)
            logger.info("ETL pipeline successfully finished")
        except Exception as e:
            logger.error(f"Critical error occurred during ETL pipeline execution: {e}")
        finally:
            self.spark_manager.stop()

if __name__ == "__main__":
    pipeline = ETLPipeline()
    pipeline.run()