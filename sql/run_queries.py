import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path("data/affordability.db")
SQL_PATH = Path(__file__).parent / "queries.sql"

conn = sqlite3.connect(DB_PATH)

with open(SQL_PATH, "r") as f:
    sql = f.read()

queries = [q.strip() for q in sql.split(";") if q.strip()]

for i, query in enumerate(queries):
    try:
        df = pd.read_sql_query(query, conn)
        print(f"\nQuery {i+1} — {len(df)} rows")
        print(df.head(10).to_string())
    except Exception as e:
        print(f"\nQuery {i+1} failed: {e}")

conn.close()