import matplotlib.pyplot as plt
import pandas as pd
from sklearn.decomposition import PCA

class ResultVisualizer:
    def __init__(self, spark_session):
        self.spark = spark_session
        
    def create_comparison_plot(self, clustered_df, save_path=None):
        print("Creating visualization...")
        
        viz_df = clustered_df.select(
            "CustomerID", "Age", "AnnualIncome", "SpendingScore", 
            "kmeans_cluster", "dbscan_cluster"
        ).toPandas()
        
        features = viz_df[['Age', 'AnnualIncome', 'SpendingScore']].values
        pca = PCA(n_components=2, random_state=42)
        features_2d = pca.fit_transform(features)
        
        viz_df['PCA1'] = features_2d[:, 0]
        viz_df['PCA2'] = features_2d[:, 1]
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        scatter1 = ax1.scatter(viz_df['PCA1'], viz_df['PCA2'], c=viz_df['kmeans_cluster'], cmap='viridis', alpha=0.6, s=10)
        ax1.set_title('K-Means Clustering')
        ax1.set_xlabel('PCA Component 1')
        ax1.set_ylabel('PCA Component 2')
        plt.colorbar(scatter1, ax=ax1)
        
        scatter2 = ax2.scatter(viz_df['PCA1'], viz_df['PCA2'], c=viz_df['dbscan_cluster'], cmap='viridis', alpha=0.6, s=10)
        ax2.set_title('DBSCAN Clustering')
        ax2.set_xlabel('PCA Component 1')
        ax2.set_ylabel('PCA Component 2')
        plt.colorbar(scatter2, ax=ax2)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"Plot saved to {save_path}")
        
        plt.show()
        
        return fig
