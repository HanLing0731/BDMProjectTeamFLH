import os
import duckdb
import pandas as pd
import numpy as np
from sklearn.impute           import SimpleImputer
from sklearn.preprocessing    import StandardScaler
from sklearn.cluster          import KMeans

# 1) Load exploitation‐zone DuckDB
duckdb_path = os.environ.get("DUCKDB_PATH", "/data/exploitation_zone.duckdb")
con = duckdb.connect(duckdb_path)

# 2) Pull down the signals table
df = con.execute("SELECT * FROM student_struggle_signals").fetchdf()
con.close()

# 3) Preprocess for clustering
feature_cols = [
    'CGPA',
    'Stress_Level',
    'Depression_Score',
    'Anxiety_Score',
    'billing_amount',
    'num_assessments',
    'avg_score',
    'dropouts'
]

# Impute any missing feature values with the column mean
imputer = SimpleImputer(strategy="mean")
X = imputer.fit_transform(df[feature_cols])

# Standardize
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# 4) KMeans clustering
kmeans = KMeans(n_clusters=3, random_state=42, n_init="auto")
labels = kmeans.fit_predict(X_scaled)
df['struggle_cluster'] = labels

# Map cluster → severity
cluster_means = kmeans.cluster_centers_.mean(axis=1)
order = np.argsort(cluster_means)  # lowest‐mean cluster first
severity_map = {
    order[0]: "Low",
    order[1]: "Medium",
    order[2]: "High"
}
df['struggle_level'] = df['struggle_cluster'].map(severity_map)

# 5) Impute again or fill any remaining NaNs in the raw columns
df['Stress_Level']     = df['Stress_Level'].fillna(0)
df['Anxiety_Score']    = df['Anxiety_Score'].fillna(0)
df['billing_amount']   = df['billing_amount'].fillna(0)
df['avg_score']        = df['avg_score'].fillna(df['avg_score'].mean())
df['dropouts']         = df['dropouts'].fillna(0)

# 6) Classify type of struggler
def classify_type(row):
    if row['CGPA'] < 2.0 or row['avg_score'] < 50:
        return "Academic Struggler"
    elif row['billing_amount'] > 1000:
        return "Financial Concerns"
    elif row['Stress_Level'] > 7 and row['Anxiety_Score'] > 6:
        return "Engagement Issues"
    elif row['dropouts'] > 0:
        return "Time Management"
    else:
        return "General Risk"

df['struggler_type'] = df.apply(classify_type, axis=1)

# 7) Extract just the predictions
predictions_df = df[['id_student', 'struggle_level', 'struggler_type']]

# 8) Write back into DuckDB
con = duckdb.connect(duckdb_path)
con.register('predictions_df', predictions_df)
con.execute("""
    CREATE OR REPLACE TABLE student_risk_predictions AS
    SELECT * FROM predictions_df;
""")
con.close()

print("✅ Prediction pipeline completed and saved to DuckDB.")
