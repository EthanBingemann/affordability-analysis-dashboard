# analysis.py
import sqlite3
import pandas as pd
import numpy as np
import statsmodels.formula.api as smf
from pathlib import Path

DB_PATH = Path("data/affordability.db")

#load dataset for regression
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql_query("""
    SELECT
        c.fips,
        c.NAME              AS county_name,
        c.year,
        c.rent_to_income_ratio,
        c.Median_Gross_Rent         AS median_rent,
        c.Median_HH_Income          AS median_income,
        c.Total_Population          AS population,
        c.poverty_rate,
        c.pct_severely_cost_burdened,
        n.CPI_All_Items             AS cpi,
        n.CPI_Rent_Primary          AS cpi_rent
    FROM county_affordability c
    LEFT JOIN national_trends n ON c.year = n.year
    WHERE c.rent_to_income_ratio IS NOT NULL
    ORDER BY c.fips, c.year
""", conn)
conn.close()


# COVID dummy
df["covid"] = df["year"].isin([2020, 2021]).astype(int)

# Log population to reduce skew
df["log_population"] = np.log(df["population"].replace(0, np.nan))

# Drop rows with any nulls in the variables we need
model_vars = [
    "rent_to_income_ratio",
    "cpi",
    "cpi_rent",
    "poverty_rate",
    "log_population",
    "covid"
]
df_model = df.dropna(subset=model_vars).copy()
print(f"Regression dataset: {len(df_model):,} rows")

#Ordinary least squares model
formula = (
    "rent_to_income_ratio ~ "
    "cpi + "
    "cpi_rent + "
    "log_population + "
    "covid"
)

model = smf.ols(formula, data=df_model).fit()
print("\n" + "=" * 55)
print("OLS Regression Results")
print("=" * 55)
print(model.summary())


# Save coefficients to CSV
coef_df = pd.DataFrame({
    "variable":    model.params.index,
    "coefficient": model.params.values,
    "p_value":     model.pvalues.values,
    "significant": model.pvalues.values < 0.05
})
coef_df.to_csv("data/processed/regression_coefficients.csv", index=False)
print("\nCoefficients saved to data/processed/regression_coefficients.csv")

# Save predictions vs actuals
df_model["predicted"] = model.fittedvalues
df_model["residual"] = model.resid
df_model[["fips", "county_name", "year", "rent_to_income_ratio", "predicted", "residual"]]\
    .to_csv("data/processed/regression_predictions.csv", index=False)
print("Predictions saved to data/processed/regression_predictions.csv")