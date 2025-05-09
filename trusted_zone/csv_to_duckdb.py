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
        # Convert relative date fields to actual dates
        df = df.withColumn("date_registration", expr(f"date_add(to_date('2013-01-01'), date_registration)"))
        df = df.withColumn("date_unregistration", expr(f"date_add(to_date('2013-01-01'), date_unregistration)"))

    elif table_name == "student_course":
        df = df.dropDuplicates(["code_module", "code_presentation", "id_student"])
        df = df.withColumn("num_of_prev_attempts", col("num_of_prev_attempts").cast("int"))
        df = df.withColumn("final_result", lower(trim(col("final_result"))))

    elif table_name == "vle":
        df = df.dropDuplicates(["id_student", "code_module", "code_presentation"])
        df = df.withColumn("description", lower(trim(col("description"))))

    return df

def load_to_duckdb(delta_path: str, duckdb_path: str):
    spark = SparkSession.builder \
        .appName("Delta to DuckDB Loader") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    duckdb_conn = duckdb.connect(duckdb_path)
    delta_tables = [
       d for d in os.listdir(delta_path)
        if (
            os.path.isdir(os.path.join(delta_path, d))
            and not d.startswith("_")
        )   ]
    for table in delta_tables:
        print(f"Transforming and loading {table} into DuckDB...")

        df = spark.read.format("delta").load(os.path.join(delta_path, table))
        for col_name, dtype in df.dtypes:
            if dtype in ("date", "timestamp"):
               df = df.withColumn(col_name, col(col_name).cast("string"))

        pandas_df = df.toPandas()

        duckdb_conn.register('temp_df', pandas_df)
        duckdb_conn.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM temp_df")

    spark.stop()
    duckdb_conn.close()


if __name__ == "__main__":
    import os

    DELTA_PATH  = os.environ['DELTA_PATH']    # "/data/delta"
    DUCKDB_PATH = os.environ['DUCKDB_PATH']   # "/data/trusted_zone.duckdb"
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    load_to_duckdb(DELTA_PATH, DUCKDB_PATH)
