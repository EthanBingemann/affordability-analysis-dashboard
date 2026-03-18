# fetch_data.py
import os
import requests
import pandas as pd
import time
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).parent.parent / ".env"
print(f"Looking for .env at: {env_path}")
print(f"File exists: {env_path.exists()}")

load_dotenv(env_path)
print(f"BLS key: {os.getenv('BLS_API_KEY')}")
print(f"Census key: {os.getenv('CENSUS_API_KEY')}")
print(f"HUD key: {os.getenv('HUD_API_KEY')}")


BLS_API_KEY = os.getenv("BLS_API_KEY")
CENSUS_API_KEY = os.getenv("CENSUS_API_KEY")
HUD_API_KEY = os.getenv("HUD_API_KEY")

RAW_DATA_DIR = Path("data/raw")
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

CURRENT_YEAR = datetime.now().year
START_YEAR = CURRENT_YEAR - 10

#BLS data
BLS_SERIES = {
    # Cost of living
    "CUUR0000SA0":   "CPI_All_Items",
    "CUUR0000SAH1":  "CPI_Shelter",
    "CUUR0000SEHA":  "CPI_Rent_Primary",
    "CUUR0000SAF1":  "CPI_Food",
    "CUUR0000SA0E":  "CPI_Energy",

    # Wages & employment
    "CES0500000003": "Avg_Hourly_Earnings",
    "CES0500000011": "Avg_Weekly_Earnings",
    "LNS14000000":   "Unemployment_Rate",
}

def fetch_bls_series() -> pd.DataFrame:
    payload = {
        "seriesid": list(BLS_SERIES.keys()),
        "startyear": str(START_YEAR),
        "endyear": str(CURRENT_YEAR),
        "registrationkey": BLS_API_KEY,
        "annualaverage": True,
        "calculations": True,  # includes year-over-year % changes
    }
    resp = requests.post(
        "https://api.bls.gov/publicAPI/v2/timeseries/data/",
        json=payload, timeout=30
    )
    resp.raise_for_status()
    result = resp.json()

    rows = []
    for series in result["Results"]["series"]:
        sid = series["seriesID"]
        for obs in series["data"]:
            rows.append({
                "series_id":       sid,
                "series_name":     BLS_SERIES.get(sid, sid),
                "year":            int(obs["year"]),
                "period":          obs["period"],
                "period_name":     obs["periodName"],
                "value":           float(obs["value"]) if obs["value"] != "-" else None,
                "pct_change_1yr":  (
                    float(obs["calculations"]["pct_changes"]["1"])
                    if obs.get("calculations") and obs["calculations"].get("pct_changes", {}).get("1")
                    else None
                ),
            })
    return pd.DataFrame(rows)

#Census data
ACS5_VARS = {
    # Income & wages
    "B19013_001E": "Median_HH_Income",
    "B19301_001E": "Per_Capita_Income",
    "B20004_001E": "Median_Earnings_Full_Time",

    # Housing costs
    "B25064_001E": "Median_Gross_Rent",
    "B25077_001E": "Median_Home_Value",
    "B25088_001E": "Median_Monthly_Owner_Costs_Mortgage",

    # Rent burden (the core affordability metric)
    "B25071_001E": "Median_Rent_Pct_Income",
    "B25070_010E": "Renters_Over50pct_Income_On_Rent",  # severely cost-burdened

    # Context
    "B01003_001E": "Total_Population",
    "B25003_002E": "Owner_Occupied_Units",
    "B25003_003E": "Renter_Occupied_Units",
    "B17001_002E": "Population_Below_Poverty",
}

ACS5_YEARS = list(range(max(2013, START_YEAR), min(CURRENT_YEAR - 1, 2023) + 1))

def fetch_acs5_county(year: int) -> pd.DataFrame:
    var_list = ",".join(["NAME"] + list(ACS5_VARS.keys()))
    url = (
        f"https://api.census.gov/data/{year}/acs/acs5"
        f"?get={var_list}&for=county:*&key={CENSUS_API_KEY}"
    )
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    raw = resp.json()

    df = pd.DataFrame(raw[1:], columns=raw[0])
    df.rename(columns=ACS5_VARS, inplace=True)
    df["year"] = year
    df["fips"] = df["state"] + df["county"]

    for col in ACS5_VARS.values():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].replace(-666666666, pd.NA)

    return df


#HUD data
def fetch_hud_fmr() -> pd.DataFrame:
    """Fetch FMR data via HUD's official API for all states/counties."""
    if not HUD_API_KEY:
        print("  HUD API key missing — skipping")
        return pd.DataFrame()

    headers = {"Authorization": f"Bearer {HUD_API_KEY}"}
    frames = []

    # Get state list once
    try:
        resp = requests.get(
            "https://www.huduser.gov/hudapi/public/fmr/listStates",
            headers=headers, timeout=30
        )
        resp.raise_for_status()
        states = resp.json()
        # Filter to only actual US states + DC (exclude territories)
        states = [s for s in states if s["category"] == "State"]
        print(f"  Found {len(states)} states")
    except Exception as e:
        print(f"  HUD API — could not fetch state list: {e}")
        return pd.DataFrame()

    for year in range(max(2017, START_YEAR), CURRENT_YEAR):
        year_rows = []
        for state in states:
            state_code = state["state_code"]
            try:
                # Fetch FMR data for entire state at once
                fmr_resp = requests.get(
                    f"https://www.huduser.gov/hudapi/public/fmr/statedata/{state_code}",
                    params={"year": year},
                    headers=headers, timeout=30
                )
                if fmr_resp.status_code != 200:
                    continue

                fmr_raw = fmr_resp.json()
                data = fmr_raw.get("data", {})
                counties = data.get("metroareas", []) + data.get("counties", [])

                for county in counties:
                    year_rows.append({
                        "fips":        county.get("fips_code", ""),
                        "county_name": county.get("areaname", ""),
                        "state_code":  state_code,
                        "year":        year,
                        "fmr_0br":     county.get("Efficiency"),
                        "fmr_1br":     county.get("One-Bedroom"),
                        "fmr_2br":     county.get("Two-Bedroom"),
                        "fmr_3br":     county.get("Three-Bedroom"),
                        "fmr_4br":     county.get("Four-Bedroom"),
                    })
                time.sleep(0.2)
            except Exception as e:
                print(f"    {state_code} {year}: skipped ({e})")
                continue

        print(f"  HUD FMR {year}: {len(year_rows):,} areas")
        frames.append(pd.DataFrame(year_rows))
        time.sleep(0.3)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


#FRED data
FRED_SERIES = {
    "MORTGAGE30US":  "30yr_Fixed_Mortgage_Rate",
    "MSPUS":         "Median_Home_Sale_Price",
    "RHORUSQ156N":   "Homeownership_Rate",
    "PSAVERT":       "Personal_Savings_Rate",
    "MEPAINUSA672N": "Real_Median_Personal_Income",
}

def fetch_fred_series(series_id: str, series_name: str) -> pd.DataFrame:
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    try:
        df = pd.read_csv(url)
        df.columns = ["date", "value"]
        df["date"] = pd.to_datetime(df["date"])
        df["series_id"] = series_id
        df["series_name"] = series_name
        df = df[df["date"].dt.year >= START_YEAR]
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        print(f"  FRED {series_id}: {len(df)} obs")
        return df
    except Exception as e:
        print(f"  FRED {series_id}: skipped ({e})")
        return pd.DataFrame()


#Main
def main():
    print("=" * 55)
    print(f"Fetching affordability data ({START_YEAR}–{CURRENT_YEAR})")
    print("=" * 55)

    # Verify API keys loaded correctly before making any requests
    print(f"\nBLS key loaded:    {'YES' if BLS_API_KEY else 'NO — check .env'}")
    print(f"Census key loaded: {'YES' if CENSUS_API_KEY else 'NO — check .env'}")
    if not BLS_API_KEY or not CENSUS_API_KEY:
        raise SystemExit("Fix missing API keys before continuing.")

    # 1. BLS
    print("\n[1/4] BLS — CPI & wages …")
    bls_df = fetch_bls_series()
    bls_df.to_csv(RAW_DATA_DIR / "bls_series.csv", index=False)
    print(f"  Saved {len(bls_df):,} rows → bls_series.csv")

    # 2. Census ACS-5
    print(f"\n[2/4] Census ACS-5 county data ({len(ACS5_YEARS)} years) …")
    acs_frames = []
    for yr in ACS5_YEARS:
        try:
            df = fetch_acs5_county(yr)
            acs_frames.append(df)
            print(f"  ACS {yr}: {len(df):,} counties")
            time.sleep(0.3)
        except Exception as e:
            print(f"  ACS {yr}: failed ({e})")
    if acs_frames:
        acs_all = pd.concat(acs_frames, ignore_index=True)
        acs_all.to_csv(RAW_DATA_DIR / "census_acs5_county.csv", index=False)
        print(f"  Saved {len(acs_all):,} rows → census_acs5_county.csv")

    # 3. HUD FMR
    print("\n[3/4] HUD Fair Market Rents …")
    fmr_df = fetch_hud_fmr()
    if not fmr_df.empty:
        fmr_df.to_csv(RAW_DATA_DIR / "hud_fair_market_rents.csv", index=False)
        print(f"  Saved {len(fmr_df):,} rows → hud_fair_market_rents.csv")

    # 4. FRED
    print("\n[4/4] FRED macro indicators …")
    fred_frames = [fetch_fred_series(sid, name) for sid, name in FRED_SERIES.items()]
    fred_valid = [f for f in fred_frames if not f.empty]
    if fred_valid:
        fred_all = pd.concat(fred_valid, ignore_index=True)
        fred_all.to_csv(RAW_DATA_DIR / "fred_macro.csv", index=False)
        print(f"  Saved {len(fred_all):,} rows → fred_macro.csv")
    else:
        print("  No FRED data retrieved — skipping")

    print("\n✓ Done. Files saved to data/raw/")

if __name__ == "__main__":
    main()