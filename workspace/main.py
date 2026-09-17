import logging
from pyspark.sql.functions import col, monotonically_increasing_id
from spark_manager import SparkManager, SparkProfile
from model import FoodClusteringModel

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Lab1_Pipeline")

class ClusteringPipeline:
    """Pipeline: Read CSV -> Preprocess -> Train ML -> Save Results."""
    
    def __init__(self):
        self.spark_manager = SparkManager()
        self.spark = self.spark_manager.get_session(SparkProfile.ML_PIPELINE)
        self.ml_model = FoodClusteringModel(k_clusters=5)
        self.file_path = "openfoodfacts.csv.gz"
        self.output_path = "clustering_results"

    def load_and_preprocess_data(self):
        """Reads the local CSV file and samples the data."""
        logger.info(f"Reading raw data from {self.file_path}")
        raw_df = self.spark.read.csv(self.file_path, sep='\t', header=True)

        target_columns = [
            "product_name", "energy-kcal_100g", "proteins_100g", 
            "fat_100g", "carbohydrates_100g"
        ]
        df = raw_df.select(*target_columns).dropna()

        for c in target_columns[1:]:
            df = df.withColumn(c, col(c).cast("float"))
            
        df = df.withColumn("id", monotonically_increasing_id())

        # Take a 5% sample to accommodate local system resources
        df_sample = df.sample(fraction=0.05, seed=42)
        df_sample.cache()
        
        logger.info(f"Data loading completed. Sampled rows for ML: {df_sample.count()}")
        return df_sample

    def run_modeling(self, df):
        """Executes the machine learning clustering."""
        logger.info("Starting machine learning pipeline")
        result_df = self.ml_model.fit_predict(df)
        return result_df

    def save_results(self, df):
        """Saves the output to a local directory."""
        logger.info(f"Saving clustering results to directory: {self.output_path}")
        df.write \
            .mode("overwrite") \
            .csv(self.output_path, header=True)
        logger.info("Results successfully saved")

    def run(self):
        """Entry point for the execution."""
        logger.info("Starting Lab 1 Execution Pipeline")
        try:
            data = self.load_and_preprocess_data()
            clustered_data = self.run_modeling(data)
            self.save_results(clustered_data)
            logger.info("Lab 1 pipeline successfully finished")
        except Exception as e:
            logger.error(f"Critical error occurred: {e}")
        finally:
            self.spark_manager.stop()

if __name__ == "__main__":
    pipeline = ClusteringPipeline()
    pipeline.run()