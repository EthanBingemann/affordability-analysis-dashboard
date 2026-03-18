# forecast.py
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.linear_model import LinearRegression

DB_PATH = Path("data/affordability.db")
FORECAST_YEARS = [2024, 2025, 2026, 2027, 2028]

conn = sqlite3.connect(DB_PATH)
df = pd.read_sql_query("""
    SELECT
        c.fips,
        c.NAME                      AS county_name,
        c.year,
        c.rent_to_income_ratio,
        c.Median_Gross_Rent         AS median_rent,
        c.Median_HH_Income          AS median_income,
        c.Total_Population          AS population,
        n.CPI_All_Items             AS cpi,
        n.CPI_Rent_Primary          AS cpi_rent
    FROM county_affordability c
    LEFT JOIN national_trends n ON c.year = n.year
    WHERE c.rent_to_income_ratio IS NOT NULL
    ORDER BY c.fips, c.year
""", conn)
conn.close()

df["covid"] = df["year"].isin([2020, 2021]).astype(int)
df["log_population"] = np.log(df["population"].replace(0, np.nan))

model_vars = ["rent_to_income_ratio", "cpi", "cpi_rent", "log_population", "covid"]
df_model = df.dropna(subset=model_vars).copy()

# Filter out very small counties
df_model = df_model[df_model["population"] > 10000]

county_trends = []
for fips, group in df_model.groupby("fips"):
    if len(group) < 4:
        continue
    group = group.sort_values("year")

    X = group["year"].values.reshape(-1, 1)
    y = group["rent_to_income_ratio"].values
    reg = LinearRegression().fit(X, y)

    # Skip poor fitting models
    if reg.score(X, y) < 0.5:
        continue

    # Cap slope to prevent unrealistic extrapolation
    slope = max(min(reg.coef_[0], 0.5), -0.5)

    latest = group.iloc[-1]
    county_trends.append({
        "fips":           fips,
        "county_name":    latest["county_name"],
        "slope":          slope,
        "intercept":      reg.intercept_,
        "latest_year":    int(latest["year"]),
        "latest_ratio":   latest["rent_to_income_ratio"],
        "latest_rent":    latest["median_rent"],
        "latest_income":  latest["median_income"],
        "log_population": latest["log_population"],
        "r2":             reg.score(X, y),
    })

trends_df = pd.DataFrame(county_trends)
print(f"Trained models for {len(trends_df):,} counties")

forecast_rows = []
for _, row in trends_df.iterrows():
    for yr in FORECAST_YEARS:
        years_ahead = yr - row["latest_year"]
        # Project change from last known value, not from intercept
        predicted = row["latest_ratio"] + (row["slope"] * years_ahead)
        # Only clamp extremes
        predicted = max(min(predicted, 65), 5)
        forecast_rows.append({
            "fips":            row["fips"],
            "county_name":     row["county_name"],
            "year":            yr,
            "predicted_ratio": round(predicted, 2),
            "trend_per_year":  round(row["slope"], 4),
            "latest_ratio":    round(row["latest_ratio"], 2),
            "latest_rent":     round(row["latest_rent"], 0),
            "latest_income":   round(row["latest_income"], 0),
        })

forecast_df = pd.DataFrame(forecast_rows)

print("\n" + "=" * 55)
print("25 CHEAPEST COUNTIES BY FORECAST YEAR")
print("=" * 55)

for yr in FORECAST_YEARS:
    yr_df = forecast_df[forecast_df["year"] == yr].sort_values("predicted_ratio")
    print(f"\n--- {yr} ---")
    print(yr_df.head(25)[["county_name", "predicted_ratio", "latest_rent", "latest_income"]].to_string(index=False))

forecast_df.to_csv("data/processed/forecast_affordability.csv", index=False)
print("\nForecast saved to data/processed/forecast_affordability.csv")

trends_df.sort_values("slope").head(25).to_csv("data/processed/most_improving_counties.csv", index=False)
print("Most improving counties saved to data/processed/most_improving_counties.csv")