# load_to_db.py
import pandas as pd
import sqlite3
from pathlib import Path

DB_PATH = Path("data/affordability.db")
PROCESSED_DIR = Path("data/processed")

conn = sqlite3.connect(DB_PATH)

# Load each CSV directly into the database as-is
pd.read_csv(PROCESSED_DIR / "national_trends.csv").to_sql("national_trends", conn, if_exists="replace", index=False)
print("national_trends loaded")

pd.read_csv(PROCESSED_DIR / "county_affordability.csv").to_sql("county_affordability", conn, if_exists="replace", index=False)
print("county_affordability loaded")

pd.read_csv(PROCESSED_DIR / "hud_fmr_state.csv").to_sql("hud_fmr_state", conn, if_exists="replace", index=False)
print("hud_fmr_state loaded")

conn.commit()
conn.close()
print("Done")