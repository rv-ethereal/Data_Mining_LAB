from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import *
import numpy as np

class KMeansClustering:
    def __init__(self, spark_session):
        self.spark = spark_session
        
    def fit_kmeans(self, feature_df, k=4):
        print(f"Running K-Means with {k} clusters...")
        
        kmeans = KMeans(
            k=k,
            featuresCol='scaled_features',
            predictionCol='kmeans_cluster',
            seed=42
        )
        
        model = kmeans.fit(feature_df)
        predictions = model.transform(feature_df)
        
        print("K-Means completed!")
        predictions.groupBy("kmeans_cluster").count().orderBy("kmeans_cluster").show()
        
        return predictions
