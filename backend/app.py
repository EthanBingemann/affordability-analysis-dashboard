# backend/app.py
import sqlite3
import pandas as pd
from flask import Flask, render_template, jsonify
from pathlib import Path

app = Flask(
    __name__,
    template_folder="../frontend/templates",
    static_folder="../frontend/static"
)

DB_PATH = Path("data/affordability.db")
FORECAST_PATH = Path("data/processed/forecast_affordability.csv")

def get_conn():
    return sqlite3.connect(DB_PATH)


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/map")
def api_map():
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT
            fips,
            NAME                        AS county_name,
            year,
            rent_to_income_ratio,
            Median_Gross_Rent           AS median_rent,
            Median_HH_Income            AS median_income,
            pct_severely_cost_burdened
        FROM county_affordability
        WHERE rent_to_income_ratio IS NOT NULL
    """, conn)
    conn.close()
    df["fips"] = df["fips"].astype(str).str.zfill(5)
    return jsonify(df.to_dict(orient="records"))


@app.route("/api/top25")
def api_top25():
    conn = get_conn()
    df = pd.read_sql_query("""
        SELECT
            NAME                        AS county_name,
            ROUND(AVG(Median_Gross_Rent), 0)         AS avg_rent,
            ROUND(AVG(Median_HH_Income), 0)          AS avg_income,
            ROUND(AVG(rent_to_income_ratio), 1)      AS avg_ratio
        FROM county_affordability
        WHERE rent_to_income_ratio IS NOT NULL
        GROUP BY fips
        ORDER BY avg_rent DESC
    """, conn)
    conn.close()
    most = df.head(25).to_dict(orient="records")
    least = df.tail(25).to_dict(orient="records")
    return jsonify({"most": most, "least": least})


@app.route("/api/states")
def api_states():
    conn = get_conn()
    df = pd.read_sql_query("""
        WITH state_lookup AS (
            SELECT '01' AS fips, 'AL' AS code, 'Alabama' AS name UNION ALL
            SELECT '02','AK','Alaska' UNION ALL SELECT '04','AZ','Arizona' UNION ALL
            SELECT '05','AR','Arkansas' UNION ALL SELECT '06','CA','California' UNION ALL
            SELECT '08','CO','Colorado' UNION ALL SELECT '09','CT','Connecticut' UNION ALL
            SELECT '10','DE','Delaware' UNION ALL SELECT '11','DC','District of Columbia' UNION ALL
            SELECT '12','FL','Florida' UNION ALL SELECT '13','GA','Georgia' UNION ALL
            SELECT '15','HI','Hawaii' UNION ALL SELECT '16','ID','Idaho' UNION ALL
            SELECT '17','IL','Illinois' UNION ALL SELECT '18','IN','Indiana' UNION ALL
            SELECT '19','IA','Iowa' UNION ALL SELECT '20','KS','Kansas' UNION ALL
            SELECT '21','KY','Kentucky' UNION ALL SELECT '22','LA','Louisiana' UNION ALL
            SELECT '23','ME','Maine' UNION ALL SELECT '24','MD','Maryland' UNION ALL
            SELECT '25','MA','Massachusetts' UNION ALL SELECT '26','MI','Michigan' UNION ALL
            SELECT '27','MN','Minnesota' UNION ALL SELECT '28','MS','Mississippi' UNION ALL
            SELECT '29','MO','Missouri' UNION ALL SELECT '30','MT','Montana' UNION ALL
            SELECT '31','NE','Nebraska' UNION ALL SELECT '32','NV','Nevada' UNION ALL
            SELECT '33','NH','New Hampshire' UNION ALL SELECT '34','NJ','New Jersey' UNION ALL
            SELECT '35','NM','New Mexico' UNION ALL SELECT '36','NY','New York' UNION ALL
            SELECT '37','NC','North Carolina' UNION ALL SELECT '38','ND','North Dakota' UNION ALL
            SELECT '39','OH','Ohio' UNION ALL SELECT '40','OK','Oklahoma' UNION ALL
            SELECT '41','OR','Oregon' UNION ALL SELECT '42','PA','Pennsylvania' UNION ALL
            SELECT '44','RI','Rhode Island' UNION ALL SELECT '45','SC','South Carolina' UNION ALL
            SELECT '46','SD','South Dakota' UNION ALL SELECT '47','TN','Tennessee' UNION ALL
            SELECT '48','TX','Texas' UNION ALL SELECT '49','UT','Utah' UNION ALL
            SELECT '50','VT','Vermont' UNION ALL SELECT '51','VA','Virginia' UNION ALL
            SELECT '53','WA','Washington' UNION ALL SELECT '54','WV','West Virginia' UNION ALL
            SELECT '55','WI','Wisconsin' UNION ALL SELECT '56','WY','Wyoming'
        )
        SELECT
            s.name                                      AS state_name,
            s.code                                      AS state_code,
            ROUND(AVG(c.Median_Gross_Rent), 0)          AS avg_rent,
            ROUND(AVG(c.Median_HH_Income), 0)           AS avg_income,
            ROUND(AVG(c.rent_to_income_ratio), 1)       AS avg_ratio,
            ROUND(AVG(c.pct_severely_cost_burdened), 1) AS avg_cost_burdened
        FROM county_affordability c
        LEFT JOIN state_lookup s ON SUBSTR(c.fips, 1, 2) = s.fips
        WHERE s.name IS NOT NULL
        GROUP BY s.fips
        ORDER BY avg_ratio DESC
    """, conn)
    conn.close()
    return jsonify(df.to_dict(orient="records"))


@app.route("/api/forecast")
def api_forecast():
    df = pd.read_csv(FORECAST_PATH)
    return jsonify(df.to_dict(orient="records"))


if __name__ == "__main__":
    app.run(debug=True)