from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler

class SparkDataLake:
    def __init__(self):
        self.spark = None
        self.df = None
        
    def initialize_spark(self):
        self.spark = SparkSession.builder \
            .appName("MallCustomerClustering") \
            .getOrCreate()
        print("Spark started!")
        return self.spark
    
    def load_data(self, file_path):
        schema = StructType([
            StructField("CustomerID", StringType(), True),
            StructField("Gender", StringType(), True),
            StructField("Age", IntegerType(), True),
            StructField("AnnualIncome", IntegerType(), True),
            StructField("SpendingScore", IntegerType(), True)
        ])
        
        self.df = self.spark.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(file_path)
            
        print(f"Loaded {self.df.count()} records")
        return self.df
    
    def get_feature_data(self):
        feature_cols = ["Age", "AnnualIncome", "SpendingScore"]
        
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        feature_df = assembler.transform(self.df)
        
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True
        )
        scaler_model = scaler.fit(feature_df)
        scaled_df = scaler_model.transform(feature_df)
        
        return scaled_df, feature_cols
