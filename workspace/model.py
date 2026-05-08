import logging
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

class FoodClusteringModel:
    def __init__(self, k_clusters: int = 5, seed: int = 42):
        self.k_clusters = k_clusters
        self.seed = seed
        self.feature_cols = ["energy-kcal_100g", "proteins_100g", "fat_100g", "carbohydrates_100g"]
        
    def _preprocess(self, df: DataFrame) -> DataFrame:
        logger.info("Preprocessing data: Applying VectorAssembler and StandardScaler")
        
        assembler = VectorAssembler(inputCols=self.feature_cols, outputCol="raw_features")
        df_assembled = assembler.transform(df)
        
        scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=False)
        scaler_model = scaler.fit(df_assembled)
        df_scaled = scaler_model.transform(df_assembled)
        
        return df_scaled

    def fit_predict(self, df: DataFrame) -> DataFrame:
        df_prepared = self._preprocess(df)
        
        logger.info(f"Training K-Means model with k={self.k_clusters}")
        kmeans = KMeans(featuresCol="features", predictionCol="cluster_label", 
                        k=self.k_clusters, seed=self.seed)
        
        model = kmeans.fit(df_prepared)
        predictions = model.transform(df_prepared)
        
        logger.info("Evaluating clustering quality using Silhouette Score")
        evaluator = ClusteringEvaluator(
            predictionCol="cluster_label", 
            featuresCol="features",
            metricName="silhouette", 
            distanceMeasure="squaredEuclidean"
        )
        silhouette_score = evaluator.evaluate(predictions)
        
        logger.info(f"Model evaluation completed. Silhouette Score: {silhouette_score:.4f}")
        logger.info("Clustering pipeline completed successfully")
        
        return predictions.select("id", "product_name", "cluster_label")