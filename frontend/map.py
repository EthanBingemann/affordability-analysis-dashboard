# frontend/map.py
import sqlite3
import pandas as pd
import plotly.express as px
from pathlib import Path

DB_PATH = Path("data/affordability.db")

# Pull data
conn = sqlite3.connect(DB_PATH)
df = pd.read_sql_query("""
    SELECT
        fips,
        NAME                    AS county_name,
        year,
        rent_to_income_ratio,
        Median_Gross_Rent       AS median_rent,
        Median_HH_Income        AS median_income,
        pct_severely_cost_burdened
    FROM county_affordability
    ORDER BY fips, year
""", conn)
conn.close()

# FIPS must be a zero-padded string
df["fips"] = df["fips"].astype(str).str.zfill(5)

# Build map
fig = px.choropleth(
    df,
    geojson="https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json",
    locations="fips",
    color="rent_to_income_ratio",
    color_continuous_scale=[
    [0.0,  "#006400"],   # dark green
    [0.2,  "#38a800"],   # medium green
    [0.4,  "#ffff00"],   # yellow
    [0.6,  "#ff6600"],   # orange
    [0.8,  "#cc0000"],   # red
    [1.0,  "#4a0000"],   # very dark red
],
range_color=[15, 40],    
    scope="usa",
    hover_name="county_name",
    hover_data={
        "fips":                     False,
        "median_rent":              True,
        "median_income":            True,
        "pct_severely_cost_burdened": True,
        "rent_to_income_ratio":     True,
    },
    animation_frame="year",              # this creates the year slider
    title="County-Level Rent to Income Ratio by Year",
    labels={"rent_to_income_ratio": "Rent to Income %"}
)

fig.update_layout(
    coloraxis_colorbar=dict(title="Rent to<br>Income %"),
    margin=dict(l=0, r=0, t=40, b=0),
)

# Save as HTML
output = Path("frontend/templates/map.html")
fig.write_html(output)
print(f"Map saved to {output}")