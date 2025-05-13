from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower, trim, to_date, expr
import duckdb
import os

def transform_table(df: DataFrame, table_name: str) -> DataFrame:
    if table_name == "students":
        df = df.dropDuplicates(["id_student"])
        df = df.withColumn("age_band", trim(col("age_band")))
        df = df.withColumn("gender", lower(col("gender")))
        df = df.withColumn("CGPA", col("CGPA").cast("float"))
        df = df.withColumn("studied_credits", col("studied_credits").cast("int"))
        df = df.filter(col("CGPA").isNotNull() & (col("CGPA") >= 0.0) & (col("CGPA") <= 4.0))
    
    elif table_name == "courses":
        df = df.dropDuplicates(["code_module", "code_presentation"])
        df = df.withColumn("module_presentation_length", col("module_presentation_length").cast("int"))

    elif table_name == "students_mental_health_survey":
        df = df.dropDuplicates(["id_student"])
        for col_name in ["Stress_Level", "Depression_Score", "Anxiety_Score"]:
            df = df.withColumn(col_name, col(col_name).cast("int"))
            df = df.filter(col(col_name).between(0, 5))
        df = df.withColumn("Residence_Type", lower(trim(col("Residence_Type"))))

    elif table_name == "healthcare_dataset_updated":
        df = df.dropDuplicates(["Date of Admission", "Doctor", "Room Number"])
        df = df.withColumn("Date of Admission", to_date(col("Date of Admission"), "yyyy-MM-dd"))
        df = df.withColumn("Discharge Date", to_date(col("Discharge Date"), "yyyy-MM-dd"))
        df = df.withColumn("Billing Amount", col("Billing Amount").cast("float"))
        df = df.filter(col("Billing Amount").isNotNull() & (col("Billing Amount") >= 0.0))

    elif table_name == "assessments":
        df = df.dropDuplicates(["code_module", "code_presentation", "id_assessment"])
        df = df.withColumn("weight", col("weight").cast("int"))
        df = df.withColumn("date", col("date").cast("int"))
        df = df.filter(col("weight").between(0, 100))

    elif table_name == "studentAssessments":
        df = df.dropDuplicates(["id_assessment", "id_student"])
        df = df.withColumn("score", col("score").cast("float"))
        df = df.withColumn("date_submitted", col("date_submitted").cast("int"))
        df = df.filter((col("score") >= 0.0) & (col("score") <= 100.0))

    
    elif table_name == "studentRegistration":
        df = df.dropDuplicates(["code_module", "code_presentation", "id_student"])
        df = df.withColumn(
            "date_registration",
            expr("date_add(to_date('2022-01-01'), CAST(date_registration AS INT))")
        )
        df = df.withColumn(
            "date_unregistration",
            expr("date_add(to_date('2022-01-01'), CAST(date_unregistration AS INT))")
        )

    elif table_name == "student_course":
        df = df.dropDuplicates(["code_module", "code_presentation", "id_student"])
        df = df.withColumn("num_of_prev_attempts", col("num_of_prev_attempts").cast("int"))
        df = df.withColumn("final_result", lower(trim(col("final_result"))))

    elif table_name == "vle":
        df = df.dropDuplicates(["id_site", "code_module", "code_presentation"])
        df = df.withColumnRenamed("id_site", "id_student")
        df = (
            df
            .withColumn("activity_type", lower(trim(col("activity_type"))))
            .withColumn("week_from", col("week_from").cast("int"))
            .withColumn("week_to",   col("week_to").cast("int"))
        )

    return df

def load_to_duckdb(delta_path: str, duckdb_path: str):
    spark = SparkSession.builder \
        .appName("Delta → DuckDB") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # ensure parent dir exists
    os.makedirs(os.path.dirname(duckdb_path), exist_ok=True)
    con = duckdb.connect(duckdb_path)

    for table in os.listdir(delta_path):
        table_path = os.path.join(delta_path, table)
        if not os.path.isdir(table_path) or table.startswith("_"):
            continue

        print(f"→ reading Delta table `{table}`")
        df = spark.read.format("delta").load(table_path)
        df = transform_table(df, table)

        # write out to a per-table Parquet folder
        parquet_tmp = f"/tmp/parquet_{table}"
        df.write.mode("overwrite").parquet(parquet_tmp)

        # ingest into DuckDB in one shot
        con.execute(f"""
            CREATE OR REPLACE TABLE {table} AS
            SELECT * FROM parquet_scan('{parquet_tmp}/*.parquet');
        """)
        print(f"✔ loaded `{table}`")

    con.close()
    spark.stop()

if __name__ == "__main__":
    import os

    DELTA_PATH  = os.environ['DELTA_PATH']    # "/data/delta"
    DUCKDB_PATH = os.environ['DUCKDB_PATH']   # "/data/trusted_zone.duckdb"
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    load_to_duckdb(DELTA_PATH, DUCKDB_PATH)
