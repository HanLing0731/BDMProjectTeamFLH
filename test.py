import duckdb

# connect to your exploitation DB
con = duckdb.connect("data/exploitation_zone.duckdb")

# pull the whole table into a Pandas DataFrame
df = con.execute("SELECT * FROM student_risk_predictions").fetchdf()

# inspect
print(df.head(20))

con.close()
