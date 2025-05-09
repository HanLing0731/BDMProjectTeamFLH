from minio import Minio
from PIL import Image
from pyspark.sql import SparkSession
import io
import os

def validate_and_upload_photos():
    spark = SparkSession.builder \
        .appName("Validate and Upload Photos") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    df = spark.read.format("delta").load("/data/delta/profile_photos")
    photo_rows = df.select("photo_id", "student_id", "photo_bytes").collect()

    minio_client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    if not minio_client.bucket_exists("trusted-photos"):
        minio_client.make_bucket("trusted-photos")

    for row in photo_rows:
        try:
            photo_id = row["photo_id"]
            student_id = row["student_id"]
            photo_data = row["photo_bytes"]
            file_name = f"{student_id}/{photo_id}.jpg"

            # Validate image from binary bytes
            img = Image.open(io.BytesIO(photo_data))
            img.verify()  # will raise exception if invalid

            # Re-upload to trusted zone
            minio_client.put_object(
                "trusted-photos",
                f"validated/{file_name}",
                io.BytesIO(photo_data),
                length=len(photo_data),
                content_type="image/jpeg"
            )
            print(f"Uploaded: {file_name}")
        except Exception as e:
            print(f"Invalid photo {row['photo_id']} from student {row['student_id']}: {str(e)}")

    spark.stop()

if __name__ == "__main__":
    validate_and_upload_photos()
