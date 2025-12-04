from pyspark.sql.functions import *
import numpy as np
from sklearn.cluster import DBSCAN
import pandas as pd

class DBSCANClustering:
    def __init__(self, spark_session):
        self.spark = spark_session
        
    def fit_dbscan(self, feature_df, eps=0.5, min_samples=10):
        print("Running DBSCAN...")
        
        features_pd = feature_df.select("scaled_features").toPandas()
        features_array = np.array([row['scaled_features'].toArray() for _, row in features_pd.iterrows()])
        
        dbscan = DBSCAN(eps=eps, min_samples=min_samples)
        cluster_labels = dbscan.fit_predict(features_array)
        
        labels_df = self.spark.createDataFrame(
            [(int(i), int(label)) for i, label in enumerate(cluster_labels)],
            ["index", "dbscan_cluster"]
        )
        
        feature_df_indexed = feature_df.rdd.zipWithIndex().map(
            lambda x: (x[1],) + tuple(x[0])
        ).toDF(["index"] + feature_df.columns)
        
        result_df = feature_df_indexed.join(labels_df, "index").drop("index")
        
        print("DBSCAN completed!")
        result_df.groupBy("dbscan_cluster").count().orderBy("dbscan_cluster").show()
        
        return result_df
