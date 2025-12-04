import os
import sys
from datetime import datetime

sys.path.append('/app/spark')
sys.path.append('/app/clustering')
sys.path.append('/app/visualization')
sys.path.append('/app/data')

from data.generate_data import save_data
from spark.spark_setup import SparkDataLake
from clustering.kmeans_clustering import KMeansClustering
from clustering.dbscan_clustering import DBSCANClustering
from visualization.visualize_results import ResultVisualizer

def main():
    print("=" * 50)
    print("MALL CUSTOMER CLUSTERING")
    print("=" * 50)
    
    start_time = datetime.now()
    
    # Step 1: Data Generation
    print("\n📊 STEP 1: Creating customer data...")
    data_file = "data/mall_customers.csv"
    if not os.path.exists(data_file):
        save_data()
    
    # Step 2: Spark Setup
    print("\n⚡ STEP 2: Starting Spark...")
    data_lake = SparkDataLake()
    spark_session = data_lake.initialize_spark()
    df = data_lake.load_data(data_file)
    feature_df, feature_cols = data_lake.get_feature_data()
    
    # Step 3: K-Means
    print("\n🎯 STEP 3: Running K-Means...")
    kmeans = KMeansClustering(spark_session)
    kmeans_result = kmeans.fit_kmeans(feature_df, k=4)
    
    # Step 4: DBSCAN
    print("\n🔍 STEP 4: Running DBSCAN...")
    dbscan = DBSCANClustering(spark_session)
    dbscan_result = dbscan.fit_dbscan(feature_df)
    
    # Step 5: Combine Results
    print("\n🔄 STEP 5: Combining results...")
    final_result = kmeans_result.join(
        dbscan_result.select("CustomerID", "dbscan_cluster"), 
        "CustomerID"
    )
    
    # Step 6: Visualization
    print("\n📈 STEP 6: Creating visualizations...")
    visualizer = ResultVisualizer(spark_session)
    os.makedirs("results", exist_ok=True)
    visualizer.create_comparison_plot(final_result, 'results/clustering_comparison.png')
    
    # Step 7: Save Results
    print("\n💾 STEP 7: Saving results...")
    final_result_pandas = final_result.select(
        "CustomerID", "Gender", "Age", "AnnualIncome", "SpendingScore",
        "kmeans_cluster", "dbscan_cluster"
    ).toPandas()
    final_result_pandas.to_csv("results/final_clustered_customers.csv", index=False)
    
    # Summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n✅ PROJECT COMPLETED!")
    print(f"⏱️  Total time: {duration}")
    print("📁 Results saved in 'results/' folder")
    print("🌐 Spark UI: http://localhost:8080")
    
    # Keep running to view Spark UI
    try:
        import time
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nShutting down...")
        spark_session.stop()

if __name__ == "__main__":
    main()
