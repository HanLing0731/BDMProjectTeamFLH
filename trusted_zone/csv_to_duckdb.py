from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower, trim, to_date, expr
from great_expectations.dataset import SparkDFDataset
import duckdb
import os
import json
from datetime import datetime

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

def validate_table(df: DataFrame, table_name: str) -> dict:
    """Perform comprehensive data quality validation"""
    try:
        ge_df = SparkDFDataset(df)
    except Exception as e:
        return {
            "table_name": table_name,
            "timestamp": datetime.utcnow().isoformat(),
            "initial_row_count": df.count(),
            "final_row_count": df.count(),
            "error": f"Validation initialization failed: {str(e)}",
            "validations": {},
            "success_rate": 0.0
        }

    metrics = {
        "table_name": table_name,
        "timestamp": datetime.utcnow().isoformat(),
        "initial_row_count": df.count(),
        "validations": {
            # Basic validations for all tables
            "table_not_empty": ge_df.expect_table_row_count_to_be_between(min_value=1).success,
            "no_duplicate_rows": ge_df.expect_compound_columns_to_be_unique(list(df.columns)).success,
            "schema_consistent": ge_df.expect_table_columns_to_match_ordered_list(list(df.columns)).success
        }
    }

    # Common column validations
    for col_name in df.columns:
        if "id" in col_name.lower():
            metrics["validations"][f"{col_name}_not_null"] = ge_df.expect_column_values_to_not_be_null(col_name).success
            metrics["validations"][f"{col_name}_unique"] = ge_df.expect_column_values_to_be_unique(col_name).success

    # Table-specific validations
    validation_rules = {
        "students": [
            ("expect_column_values_to_be_between", {"column": "CGPA", "min_value": 0.0, "max_value": 4.0}),
            ("expect_column_values_to_be_in_set", {"column": "gender", "value_set": ["male", "female", "other"]}),
            ("expect_column_values_to_be_between", {"column": "studied_credits", "min_value": 0})
        ],
        "courses": [
            ("expect_compound_columns_to_be_unique", {"column_list": ["code_module", "code_presentation"]}),
            ("expect_column_values_to_be_between", {"column": "module_presentation_length", "min_value": 1})
        ],
        "students_mental_health_survey": [
            ("expect_column_values_to_be_between", {"column": "Stress_Level", "min_value": 0, "max_value": 5}),
            ("expect_column_values_to_be_in_set", {"column": "Residence_Type", "value_set": ["urban", "rural"]})
        ],
        "healthcare_dataset_updated": [
            ("expect_column_values_to_be_between", {"column": "Billing Amount", "min_value": 0}),
            ("expect_column_pair_values_A_to_be_greater_than_B", {
                "column_A": "Discharge Date", 
                "column_B": "Date of Admission"
            })
        ],
        "assessments": [
            ("expect_column_values_to_be_between", {"column": "weight", "min_value": 0, "max_value": 100}),
            ("expect_column_values_to_be_between", {"column": "date", "min_value": 1, "max_value": 365})
        ],
        "studentAssessments": [
            ("expect_column_values_to_be_between", {"column": "score", "min_value": 0, "max_value": 100}),
            ("expect_column_values_to_be_between", {"column": "date_submitted", "min_value": 1, "max_value": 365})
        ],
        "studentRegistration": [
            ("expect_column_pair_values_A_to_be_greater_than_B", {
                "column_A": "date_unregistration", 
                "column_B": "date_registration"
            })
        ],
        "student_course": [
            ("expect_column_values_to_be_in_set", {
                "column": "final_result", 
                "value_set": ["pass", "fail", "withdrawn"]
            })
        ],
        "vle": [
            ("expect_column_values_to_be_between", {"column": "week_from", "min_value": 1, "max_value": 52}),
            ("expect_column_values_to_be_between", {"column": "week_to", "min_value": 1, "max_value": 52})
        ]
    }

    for test, params in validation_rules.get(table_name, []):
        try:
            result = getattr(ge_df, test)(**params)
            key = f"{test}_{params.get('column', 'compound')}"
            metrics["validations"][key] = result["success"]
        except Exception as e:
            metrics["validations"][f"{test}_error"] = str(e)

    metrics["final_row_count"] = df.count()
    successful = sum(1 for v in metrics["validations"].values() if v is True)
    total = len(metrics["validations"])
    metrics["success_rate"] = (successful / total * 100) if total > 0 else 100.0
    
    return metrics

def load_to_duckdb(delta_path: str, duckdb_path: str):
    spark = (
        SparkSession.builder
        .appName("Delta→DuckDB")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        # enable the Delta Lake SQL extension
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # redirect the default catalog to Delta
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    os.makedirs(os.path.dirname(duckdb_path), exist_ok=True)
    con = duckdb.connect(duckdb_path)

    con.execute("""
    CREATE TABLE IF NOT EXISTS validation_results (
        table_name STRING,
        timestamp TIMESTAMP,
        initial_row_count INTEGER,
        final_row_count INTEGER,
        success_rate FLOAT,
        validations JSON,
        report_generated BOOLEAN DEFAULT FALSE
    )
    """)

    for table in os.listdir(delta_path):
        table_path = os.path.join(delta_path, table)
        if not os.path.isdir(table_path) or table.startswith("_"):
            continue

        print(f"→ Processing table `{table}`")
        df = spark.read.format("delta").load(table_path)
        
        try:
            df = transform_table(df, table)
            validation_metrics = validate_table(df, table)
            
            # FIXED: Include all 7 columns in INSERT statement
            con.execute("""
            INSERT INTO validation_results 
            (table_name, timestamp, initial_row_count, final_row_count, success_rate, validations, report_generated)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                validation_metrics["table_name"],
                validation_metrics["timestamp"],
                validation_metrics["initial_row_count"],
                validation_metrics["final_row_count"],
                validation_metrics["success_rate"],
                json.dumps(validation_metrics["validations"]),
                False  # report_generated
            ])

            parquet_tmp = f"/tmp/parquet_{table}"
            df.write.mode("overwrite").parquet(parquet_tmp)
            con.execute(f"""
            CREATE OR REPLACE TABLE {table} AS
            SELECT * FROM parquet_scan('{parquet_tmp}/*.parquet')
            """)
            
            print(f"✔ Loaded `{table}` with {validation_metrics['success_rate']:.2f}% validation success")
        except Exception as e:
            print(f"❌ Failed processing `{table}`: {str(e)}")

    con.close()
    spark.stop()



def generate_validation_report(duckdb_path: str, output_dir: str = "/data/reports"):
    os.makedirs(output_dir, exist_ok=True)
    con = duckdb.connect(duckdb_path)
    
    # FIXED: Proper HTML template with escaped curly braces
    html_template = """<!DOCTYPE html>
<html>
<head>
    <title>Data Quality Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px }}
        table {{ border-collapse: collapse; width: 100% }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left }}
        tr:nth-child(even) {{ background-color: #f2f2f2 }}
        .pass {{ color: green }}
        .fail {{ color: red }}
    </style>
</head>
<body>
    <h1>Data Quality Validation Report</h1>
    <p>Generated at: {report_time}</p>
    <table>
        <tr>
            <th>Table</th>
            <th>Timestamp</th>
            <th>Initial Rows</th>
            <th>Final Rows</th>
            <th>Success Rate</th>
            <th>Validations</th>
        </tr>
"""

    # Format the template with current time
    html_content = html_template.format(
        report_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

    results = con.sql("""
    SELECT 
        table_name,
        timestamp,
        initial_row_count,
        final_row_count,
        success_rate,
        validations 
    FROM validation_results 
    ORDER BY success_rate ASC, timestamp DESC
    """).fetchall()

    for row in results:
        validations = json.loads(row[5])
        validation_details = "\n".join(
            f"{k}: {'✅' if v is True else '❌' if v is False else '⚠️ ' + str(v)}"
            for k, v in validations.items()
        )
        
        html_content += f"""
        <tr>
            <td>{row[0]}</td>
            <td>{row[1]}</td>
            <td>{row[2]}</td>
            <td>{row[3]}</td>
            <td class="{'pass' if row[4] >= 90 else 'fail'}">{row[4]:.2f}%</td>
            <td><pre>{validation_details}</pre></td>
        </tr>
        """
    
    html_content += """
    </table>
</body>
</html>
"""
    
    report_path = os.path.join(output_dir, "validation_report.html")
    with open(report_path, "w") as f:
        f.write(html_content)
    
    con.execute("UPDATE validation_results SET report_generated = TRUE")
    con.close()
    
    return report_path

if __name__ == "__main__":
    DELTA_PATH = os.environ.get('DELTA_PATH', '/data/delta')
    DUCKDB_PATH = os.environ.get('DUCKDB_PATH', '/data/trusted_zone.duckdb')
    
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    load_to_duckdb(DELTA_PATH, DUCKDB_PATH)
    
    # Generate report in /data/reports
    report_path = generate_validation_report(DUCKDB_PATH, "/data/reports")
    print(f"Validation report generated at: {report_path}")
