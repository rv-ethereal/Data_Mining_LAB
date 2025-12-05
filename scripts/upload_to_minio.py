"""
Upload sample data to MinIO
"""

from minio import Minio
import os
from dotenv import load_dotenv

load_dotenv()

# MinIO client
minio_user = os.getenv("MINIO_ROOT_USER", "admin")
minio_password = os.getenv("MINIO_ROOT_PASSWORD", "minio123")

client = Minio(
    "localhost:9000",
    access_key=minio_user,
    secret_key=minio_password,
    secure=False
)

# Data directory (go up one level from scripts/ to project root, then to data/sample)
project_root = os.path.dirname(os.path.dirname(__file__))
data_dir = os.path.join(project_root, "data", "sample")

# Files to upload
files = ['customers.csv', 'products.csv', 'orders.csv', 'order_items.csv']

print("Uploading data to MinIO...")
for filename in files:
    filepath = os.path.join(data_dir, filename)
    if os.path.exists(filepath):
        client.fput_object("raw-data", filename, filepath)
        size = os.path.getsize(filepath)
        print(f"[OK] Uploaded {filename} ({size:,} bytes)")
    else:
        print(f"[ERROR] File not found: {filepath}")

print("\n[SUCCESS] All files uploaded to MinIO!")
