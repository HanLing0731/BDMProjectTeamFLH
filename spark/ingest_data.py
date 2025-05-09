import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from delta import configure_spark_with_delta_pip
from pyspark.sql.types import *
from datetime import datetime
import kagglehub

# Configuration
DELTA_LAKE_PATH = os.getenv("DELTA_LAKE_PATH", "/data/delta")
LOG_DATA_PATH = os.getenv("LOG_DATA_PATH", "/data/log_data")
PHOTO_DIR = os.getenv("PHOTO_DIR", "/data/unstructured/profile_photos")
DELTA_ENRICHED_PATH = os.path.join(DELTA_LAKE_PATH, "streaming_enriched_recommendations")

# Batch datasets to process
LOGDATASETS = [
    "clean_df_10_2.5.csv", "clean_df_10_5.0.csv", "clean_df_10_8.5.csv",
    "clean_df_25_2.5.csv", "clean_df_25_5.0.csv", "clean_df_25_8.5.csv",
    "clean_df_33_2.5.csv", "clean_df_33_5.0.csv", "clean_df_33_8.5.csv",
    "clean_df_50_2.5.csv", "clean_df_50_5.0.csv", "clean_df_50_8.5.csv"
]

def init_spark():
    builder = (
        SparkSession.builder.appName("DataIngestionPipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore")
        
    )
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def define_log_schema():
    """Return schema for log data"""
    return StructType([
        StructField("UID", StringType(), True),
        StructField("COURSE", StringType(), True),
        StructField("BIN_TARGET", StringType(), True),
        StructField("ACCOMPLISH_MANDATORY", FloatType(), True),
        StructField("ACCOMPLISH_MANDATORY_GRADE", FloatType(), True),
        StructField("ACCOMPLISH_MANDATORY_PCT_GRADED", FloatType(), True),
        StructField("ACCOMPLISH_MANDATORY_PERCENTILE_GRADE", FloatType(), True),
        StructField("ACCOMPLISH_OPTIONAL", FloatType(), True),
        StructField("ACCOMPLISH_OPTIONAL_GRADE", FloatType(), True),
        StructField("ACCOMPLISH_OPTIONAL_PCT_GRADED", FloatType(), True),
        StructField("ACCOMPLISH_OPTIONAL_PERCENTILE_GRADE", FloatType(), True),
        StructField("COURSE_VIEW_PCT", FloatType(), True),
        StructField("COURSE_VIEW_TIME_1", FloatType(), True),
        StructField("COURSE_VIEW_TIME_2", FloatType(), True),
        StructField("COURSE_VIEW_TIME_3", FloatType(), True),
        StructField("COURSE_VIEW_TIME_4", FloatType(), True),
        StructField("COURSE_VIEW_TIME_5", FloatType(), True),
        StructField("COURSE_VIEW_TIME_PCT", FloatType(), True),
        StructField("RESOURCE_VIEW_PCT", FloatType(), True),
        StructField("RESOURCE_VIEW_TIME_1", FloatType(), True),
        StructField("RESOURCE_VIEW_TIME_2", FloatType(), True),
        StructField("RESOURCE_VIEW_TIME_3", FloatType(), True),
        StructField("RESOURCE_VIEW_TIME_4", FloatType(), True),
        StructField("RESOURCE_VIEW_TIME_5", FloatType(), True),
        StructField("RESOURCE_VIEW_TIME_PCT", FloatType(), True),
        StructField("RESOURCE_VIEW_UNIQUE_PCT", FloatType(), True),
        StructField("URL_VIEW_PCT", FloatType(), True),
        StructField("URL_VIEW_TIME_1", FloatType(), True),
        StructField("URL_VIEW_TIME_2", FloatType(), True),
        StructField("URL_VIEW_TIME_3", FloatType(), True),
        StructField("URL_VIEW_TIME_4", FloatType(), True),
        StructField("URL_VIEW_TIME_5", FloatType(), True),
        StructField("URL_VIEW_TIME_PCT", FloatType(), True),
        StructField("URL_VIEW_UNIQUE_PCT", FloatType(), True),
        StructField("ASSIGN_VIEW_PCT", FloatType(), True),
        StructField("ASSIGN_VIEW_TIME_1", FloatType(), True),
        StructField("ASSIGN_VIEW_TIME_2", FloatType(), True),
        StructField("ASSIGN_VIEW_TIME_3", FloatType(), True),
        StructField("ASSIGN_VIEW_TIME_PCT", FloatType(), True),
        StructField("ASSIGN_VIEW_UNIQUE_PCT", FloatType(), True),
        StructField("QUIZ_VIEW_PCT", FloatType(), True),
        StructField("QUIZ_VIEW_TIME_1", FloatType(), True),
        StructField("QUIZ_VIEW_TIME_2", FloatType(), True),
        StructField("QUIZ_VIEW_TIME_3", FloatType(), True),
        StructField("QUIZ_VIEW_TIME_PCT", FloatType(), True),
        StructField("QUIZ_VIEW_UNIQUE_PCT", FloatType(), True),
        StructField("ASSIGN_SUBMIT_PCT", FloatType(), True),
        StructField("ASSIGN_SUBMIT_TIME_1", FloatType(), True),
        StructField("ASSIGN_SUBMIT_TIME_2", FloatType(), True),
        StructField("ASSIGN_SUBMIT_TIME_3", FloatType(), True),
        StructField("ASSIGN_SUBMIT_TIME_PCT", FloatType(), True),
        StructField("ASSIGN_SUBMIT_UNIQUE_PCT", FloatType(), True),
        StructField("QUIZ_ATTEMPT_PCT", FloatType(), True),
        StructField("QUIZ_ATTEMPT_TIME_1", FloatType(), True),
        StructField("QUIZ_ATTEMPT_TIME_2", FloatType(), True),
        StructField("QUIZ_ATTEMPT_TIME_3", FloatType(), True),
        StructField("QUIZ_ATTEMPT_TIME_PCT", FloatType(), True),
        StructField("QUIZ_ATTEMPT_UNIQUE_PCT", FloatType(), True),
        StructField("QUIZ_CLOSE_ATTEMPT_PCT", FloatType(), True),
        StructField("QUIZ_CLOSE_ATTEMPT_TIME_1", FloatType(), True),
        StructField("QUIZ_CLOSE_ATTEMPT_TIME_2", FloatType(), True),
        StructField("QUIZ_CLOSE_ATTEMPT_TIME_3", FloatType(), True),
        StructField("QUIZ_CLOSE_ATTEMPT_TIME_PCT", FloatType(), True),
        StructField("QUIZ_CLOSE_ATTEMPT_UNIQUE_PCT", FloatType(), True),
        StructField("FORUM_VIEW_FORUM_PCT", FloatType(), True),
        StructField("FORUM_VIEW_DISCUSSION_PCT", FloatType(), True)
        ])


def download_and_organize_photos(spark):
    """Download from Kaggle and organize photos by student ID"""
    os.makedirs(PHOTO_DIR, exist_ok=True)
    
    if not os.listdir(PHOTO_DIR):
        print("Downloading photos from Kaggle...")
        try:
            import shutil
            dataset_dir = kagglehub.dataset_download("osmankagankurnaz/human-profile-photos-dataset")
            
            # Copy instead of move files
            for root, _, files in os.walk(dataset_dir):
                for file in files:
                    if file.lower().endswith(('.jpg', '.jpeg', '.png')):
                        shutil.copy2(
                            os.path.join(root, file),
                            os.path.join(PHOTO_DIR, file)
                        )

            # Organize by student ID
            students_df = spark.read.csv("/data/structured/students.csv", header=True)
            student_ids = [row.id_student for row in students_df.select("id_student").collect()]
            
            for i, filename in enumerate(os.listdir(PHOTO_DIR)):
                if filename.lower().endswith(('.jpg', '.jpeg', '.png')):
                    student_id = student_ids[i % len(student_ids)]
                    student_dir = os.path.join(PHOTO_DIR, str(student_id))
                    os.makedirs(student_dir, exist_ok=True)
                    shutil.move(  # Now moving within same filesystem
                        os.path.join(PHOTO_DIR, filename),
                        os.path.join(student_dir, f"{student_id}_{filename}")
                    )
                    
            print("Photos organized successfully")
        except Exception as e:
            raise RuntimeError(f"Photo download failed: {str(e)}")
        
def process_photos_to_delta(spark):
    """Convert organized photos to Delta table"""
    photo_schema = StructType([
        StructField("photo_id", StringType()),
        StructField("student_id", StringType()),
        StructField("photo_bytes", BinaryType()),
        StructField("ingestion_time", TimestampType())
    ])
    
    photos = []
    for student_id in os.listdir(PHOTO_DIR):
        student_path = os.path.join(PHOTO_DIR, student_id)
        if os.path.isdir(student_path):
            for photo_file in os.listdir(student_path):
                if photo_file.lower().endswith(('.png', '.jpg', '.jpeg')):
                    try:
                        with open(os.path.join(student_path, photo_file), 'rb') as f:
                            photos.append((
                                os.path.splitext(photo_file)[0],  # photo_id
                                student_id,                      # student_id
                                f.read(),                       # binary data
                                datetime.now()                  # timestamp
                            ))
                    except Exception as e:
                        print(f"Skipping {photo_file}: {str(e)}")

    if photos:
        df = spark.createDataFrame(photos, schema=photo_schema)
        df.write.format("delta") \
            .mode("overwrite") \
            .save(f"{DELTA_LAKE_PATH}/profile_photos")
        print(f"Processed {len(photos)} photos for {len(set(p[1] for p in photos))} students")

def process_batch_logs(spark):
    """Process all batch log files"""
    schema = define_log_schema()
    
    for log_file in LOGDATASETS:
        try:
            file_path = f"{LOG_DATA_PATH}/{log_file}"
            if not os.path.exists(file_path):
                print(f"File not found: {file_path}")
                continue

            df = spark.read \
                .option("header", "true") \
                .option("delimiter", ";") \
                .schema(schema) \
                .csv(file_path)
            
            df.withColumn("ingestion_time", current_timestamp()) \
              .write.format("delta") \
              .mode("append") \
              .save(f"{DELTA_LAKE_PATH}/batch_logs")
            print(f"Processed {log_file}")
        except Exception as e:
            print(f"Error processing {log_file}: {str(e)}")

from pyspark.sql.functions import col, when, count, avg

def enrich_with_recommendations(df):
    """Add recommendation based on engagement metrics"""
    return df.withColumn(
        "recommendation",
        when(col("COURSE_VIEW_PCT") < 30, "Watch intro videos & core lectures")
        .when(col("RESOURCE_VIEW_PCT") < 20, "Review reading materials")
        .otherwise("Keep up the good work!")
    )

def start_log_streaming(spark):
    """Run streaming for a fixed duration (e.g., 3 seconds), enrich data, and store"""

    # Define the stream schema
    stream_df = spark.readStream.schema(define_log_schema()) \
        .option("maxFilesPerTrigger", 1) \
        .csv(f"{LOG_DATA_PATH}/*.csv")

    # Enrich with real-time recommendation
    enriched_df = enrich_with_recommendations(stream_df)

    # Write enriched stream (with recommendation) to Delta Lake
    enriched_query = enriched_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{DELTA_LAKE_PATH}/_checkpoints/streaming_enriched") \
        .trigger(processingTime="1 second") \
        .start(f"{DELTA_LAKE_PATH}/streaming_enriched_recommendations")

    # Let the enriched query run for 3 seconds
    enriched_query.awaitTermination(3)

    # Stop the enriched query after processing
    enriched_query.stop()

    return {
        "status": "success",
        "message": "Processed 3 seconds of streaming data with enrichment"
    }


def run_post_analysis(spark):
    # Read enriched Delta table
    df = spark.read.format("delta").load(DELTA_ENRICHED_PATH)

    print("=== Sample of Enriched Logs ===")
    df.show(10, truncate=False)

    print("=== Recommendation Counts ===")
    df.groupBy("recommendation").count().show()

    print("=== Average Engagement Metrics ===")
    df.select(
        avg(col("COURSE_VIEW_PCT")).alias("avg_course_view_pct"),
        avg(col("RESOURCE_VIEW_PCT")).alias("avg_resource_view_pct")
    ).show()

    # Optional: save summary as CSV
    summary_df = df.groupBy("recommendation").agg(
        count("*").alias("count")
    )
    summary_df.coalesce(1).write.mode("overwrite").option("header", True).csv("output/recommendation_summary")

    spark.stop()


def main(mode):
    """Main entry point for all processing modes"""
    spark = init_spark()
    
    try:
        if mode == "photos":
            download_and_organize_photos(spark)
            process_photos_to_delta(spark)
        elif mode == "batch":
            process_batch_logs(spark)
        elif mode == "streaming":
            result = start_log_streaming(spark)
            run_post_analysis(spark)
            print(result)
    finally:
        if mode != "streaming":  # Don't stop Spark for streaming
            spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["photos", "batch", "streaming"], required=True)
    args = parser.parse_args()
    
    try:
        main(args.mode)
    except Exception as e:
        print(f"Fatal error in {args.mode} mode: {str(e)}")
        raise
