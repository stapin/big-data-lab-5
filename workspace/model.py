from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.sql import DataFrame

class FoodClusteringModel:
    """Класс, инкапсулирующий логику машинного обучения (K-Means)."""
    
    def __init__(self, k_clusters: int = 5, seed: int = 42):
        self.k_clusters = k_clusters
        self.seed = seed
        self.feature_cols = ["energy-kcal_100g", "proteins_100g", "fat_100g", "carbohydrates_100g"]
        
    def _preprocess(self, df: DataFrame) -> DataFrame:
        """Внутренний метод: Подготовка признаков (векторизация и масштабирование)."""
        print("[*] Model: Предобработка (VectorAssembler + StandardScaler)...")
        
        # 1. Сборка колонок в один вектор
        assembler = VectorAssembler(inputCols=self.feature_cols, outputCol="raw_features")
        df_assembled = assembler.transform(df)
        
        # 2. Масштабирование признаков (критично для K-Means)
        scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=False)
        scaler_model = scaler.fit(df_assembled)
        df_scaled = scaler_model.transform(df_assembled)
        
        return df_scaled

    def fit_predict(self, df: DataFrame) -> DataFrame:
        """Обучает модель и возвращает датафрейм с предсказаниями."""
        # Подготавливаем данные
        df_prepared = self._preprocess(df)
        
        # Инициализируем и обучаем алгоритм
        print(f"[*] Model: Обучение K-Means (k={self.k_clusters})...")
        kmeans = KMeans(featuresCol="features", predictionCol="cluster_label", 
                        k=self.k_clusters, seed=self.seed)
        
        model = kmeans.fit(df_prepared)
        
        # Получаем предсказания
        predictions = model.transform(df_prepared)
        
        # Возвращаем только необходимые для БД колонки
        print("[*] Model: Кластеризация завершена.")
        return predictions.select("id", "product_name", "cluster_label")