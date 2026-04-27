import sys
from pathlib import Path
import duckdb

def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python analytics/run_query.py <sql_file>")
    
    sql_file = Path(sys.argv[1])
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    
    query = sql_file.read_text()
    con = duckdb.connect()
    df = con.execute(query).df()

    print(df.to_string(index=False))

if __name__ == "__main__":
    main()

# /Users/omkarsawant/realtime-data-pipeline/analytics/sql