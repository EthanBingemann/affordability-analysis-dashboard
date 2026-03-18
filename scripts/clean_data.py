# clean_data.py
import pandas as pd
import numpy as np
from pathlib import Path

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


#bls
def clean_bls() -> pd.DataFrame:
    df = pd.read_csv(RAW_DIR / "bls_series.csv")

    # Keep only annual averages (period == "M13")
    df = df[df["period"] == "M13"].copy()

    # Pivot so each series becomes a column
    df = df.pivot_table(
        index="year",
        columns="series_name",
        values="value",
        aggfunc="first"
    ).reset_index()

    df.columns.name = None
    df["geo"] = "national"
    df["fips"] = "00000"
    print(f"  BLS cleaned: {len(df)} rows")
    return df

#fred
def clean_fred() -> pd.DataFrame:
    df = pd.read_csv(RAW_DIR / "fred_macro.csv", parse_dates=["date"])
    df["year"] = df["date"].dt.year

    # Annual mean for each series
    df = df.groupby(["year", "series_name"])["value"].mean().reset_index()

    # Pivot so each series becomes a column
    df = df.pivot_table(
        index="year",
        columns="series_name",
        values="value",
        aggfunc="first"
    ).reset_index()

    df.columns.name = None
    df["geo"] = "national"
    df["fips"] = "00000"
    print(f"  FRED cleaned: {len(df)} rows")
    return df


#acs
def clean_acs() -> pd.DataFrame:
    df = pd.read_csv(
        RAW_DIR / "census_acs5_county.csv",
        dtype={"fips": str, "state": str, "county": str}
    )

    # Ensure FIPS is always 5 digits
    df["fips"] = df["fips"].str.zfill(5)

    # Drop rows with no FIPS or population
    df = df.dropna(subset=["fips", "Total_Population"])
    df = df[df["Total_Population"] > 0]

    # Compute derived affordability metrics
    df["pct_severely_cost_burdened"] = (
        df["Renters_Over50pct_Income_On_Rent"] / df["Renter_Occupied_Units"] * 100
    ).round(2)

    df["poverty_rate"] = (
        df["Population_Below_Poverty"] / df["Total_Population"] * 100
    ).round(2)

    df["renter_share"] = (
        df["Renter_Occupied_Units"] / (df["Owner_Occupied_Units"] + df["Renter_Occupied_Units"]) * 100
    ).round(2)

    # Annual rent estimate from monthly
    df["annual_rent"] = df["Median_Gross_Rent"] * 12

    # Rent to income ratio
    df["rent_to_income_ratio"] = (
        df["annual_rent"] / df["Median_HH_Income"] * 100
    ).round(2)

    df["geo"] = "county"
    print(f"  ACS cleaned: {len(df)} rows, {df['fips'].nunique()} unique counties")
    return df


#hud
def clean_hud() -> pd.DataFrame:
    df = pd.read_csv(RAW_DIR / "hud_fair_market_rents.csv", dtype={"fips": str})

    # Standardize column names
    df.columns = df.columns.str.lower().str.strip()

    # Keep only relevant columns
    keep = ["state_code", "year", "fmr_0br", "fmr_1br", "fmr_2br", "fmr_3br", "fmr_4br"]
    df = df[[c for c in keep if c in df.columns]]

    # Coerce FMR columns to numeric
    for col in ["fmr_0br", "fmr_1br", "fmr_2br", "fmr_3br", "fmr_4br"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows where all FMR values are null
    fmr_cols = [c for c in ["fmr_0br", "fmr_1br", "fmr_2br", "fmr_3br", "fmr_4br"] if c in df.columns]
    df = df.dropna(subset=fmr_cols, how="all")

    # Aggregate to state level
    df = df.groupby(["state_code", "year"])[fmr_cols].mean().round(0).reset_index()

    print(f"  HUD cleaned: {len(df)} rows, {df['state_code'].nunique()} states")
    return df


#aggregate table
def build_national_table(bls: pd.DataFrame, fred: pd.DataFrame) -> pd.DataFrame:
    """Merge BLS and FRED on year — both are national level."""
    merged = pd.merge(
        bls, fred,
        on=["year", "geo", "fips"],
        how="outer",
        suffixes=("_bls", "_fred")
    )
    merged = merged.sort_values("year").reset_index(drop=True)
    print(f"  National table: {len(merged)} rows, {len(merged.columns)} columns")
    return merged


def build_county_table(acs: pd.DataFrame) -> pd.DataFrame:
    """ACS is the county table — already complete."""
    acs = acs.sort_values(["fips", "year"]).reset_index(drop=True)
    print(f"  County table: {len(acs)} rows, {acs['fips'].nunique()} unique counties")
    return acs

# Main
def main():
    print("=" * 55)
    print("Cleaning and merging affordability data")
    print("=" * 55)

    print("\n[1/4] Cleaning BLS …")
    bls = clean_bls()

    print("\n[2/4] Cleaning FRED …")
    fred = clean_fred()

    print("\n[3/4] Cleaning ACS …")
    acs = clean_acs()

    print("\n[4/4] Cleaning HUD …")
    hud = clean_hud()

    print("\n[5/5] Building master tables …")
    national = build_national_table(bls, fred)
    county = build_county_table(acs)

    # Save outputs
    national.to_csv(PROCESSED_DIR / "national_trends.csv", index=False)
    county.to_csv(PROCESSED_DIR / "county_affordability.csv", index=False)
    hud.to_csv(PROCESSED_DIR / "hud_fmr_state.csv", index=False)

    print(f"\n✓ Saved:")
    print(f"  → processed/national_trends.csv        ({len(national)} rows)")
    print(f"  → processed/county_affordability.csv   ({len(county)} rows)")
    print(f"  → processed/hud_fmr_state.csv          ({len(hud)} rows)")


if __name__ == "__main__":
    main()