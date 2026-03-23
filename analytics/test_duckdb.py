import duckdb

con = duckdb.connect()

query = """
SELECT *
FROM read_parquet('gold_v2/pipeline_metrics_per_minute/**/*.parquet')
ORDER BY window_start DESC
LIMIT 10
"""

df = con.execute(query).df()
print(df)