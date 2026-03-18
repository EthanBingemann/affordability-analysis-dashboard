-- schema.sql
-- Tables: national_trends, county_affordability, hud_fmr_state
-- All joined via year, and county linked to state via state_fips
-- REFERENCE TABLE: states
-- Links county (ACS) to state (HUD) data

CREATE TABLE IF NOT EXISTS states (
    state_code      TEXT PRIMARY KEY,       --ex. 'AL'
    state_fips      TEXT UNIQUE NOT NULL,   -- ex. '01'
    state_name      TEXT NOT NULL           -- ex. 'Alabama'
);


-- table 1: national_trends
-- One row per year — BLS + FRED national indicators
CREATE TABLE IF NOT EXISTS national_trends (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    year                        INTEGER NOT NULL UNIQUE,

    -- CPI / Inflation (BLS)
    cpi_all_items               REAL,
    cpi_shelter                 REAL,
    cpi_rent_primary            REAL,
    cpi_food                    REAL,
    cpi_energy                  REAL,

    -- Wages & Employment (BLS)
    avg_hourly_earnings         REAL,
    avg_weekly_earnings         REAL,
    unemployment_rate           REAL,

    -- Housing Market (FRED)
    mortgage_rate_30yr          REAL,
    median_home_sale_price      REAL,
    homeownership_rate          REAL,

    -- Income & Savings (FRED)
    real_median_personal_income REAL,
    personal_savings_rate       REAL
);

-- table 2: county_affordability
-- One row per county per year — ACS data
-- Links to states via state_fips
-- Links to national_trends via year
-- Links to hud_fmr_state via state_code + year
CREATE TABLE IF NOT EXISTS county_affordability (
    id                              INTEGER PRIMARY KEY AUTOINCREMENT,
    fips                            TEXT NOT NULL,      -- 5-digit county FIPS
    state_fips                      TEXT NOT NULL,      -- first 2 digits of FIPS
    year                            INTEGER NOT NULL,

    -- specific county
    county_name                     TEXT,

    -- Income
    median_hh_income                REAL,
    per_capita_income               REAL,
    median_earnings_full_time       REAL,

    -- Housing costs
    median_gross_rent               REAL,
    annual_rent                     REAL,
    median_home_value               REAL,
    median_monthly_owner_costs      REAL,

    -- Affordability metrics
    median_rent_pct_income          REAL,   -- from ACS directly
    rent_to_income_ratio            REAL,   -- computed in clean_data.py
    pct_severely_cost_burdened      REAL,   -- renters >50% income on rent

    -- Demographics
    total_population                INTEGER,
    poverty_rate                    REAL,
    renter_share                    REAL,
    owner_occupied_units            INTEGER,
    renter_occupied_units           INTEGER,

    -- Constraints
    UNIQUE (fips, year),
    FOREIGN KEY (year)       REFERENCES national_trends (year),
    FOREIGN KEY (state_fips) REFERENCES states (state_fips)
);


-- table 3: hud_fmr_state
-- One row per state per year — HUD Fair Market Rents
-- Links to states via state_code
-- Links to national_trends via year
-- Links to county_affordability via state_fips + year
CREATE TABLE IF NOT EXISTS hud_fmr_state (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    state_code  TEXT NOT NULL,      -- e.g. 'AL'
    year        INTEGER NOT NULL,

    -- Fair Market Rents by bedroom size
    fmr_0br     REAL,   -- studio
    fmr_1br     REAL,
    fmr_2br     REAL,   -- standard benchmark for affordability calculations
    fmr_3br     REAL,
    fmr_4br     REAL,

    UNIQUE (state_code, year),
    FOREIGN KEY (year)       REFERENCES national_trends (year),
    FOREIGN KEY (state_code) REFERENCES states (state_code)
);



-- County lookups by state
CREATE INDEX IF NOT EXISTS idx_county_state_fips ON county_affordability (state_fips);

-- Time series queries
CREATE INDEX IF NOT EXISTS idx_county_year      ON county_affordability (year);
CREATE INDEX IF NOT EXISTS idx_hud_year         ON hud_fmr_state (year);

-- Cross-reference county to HUD via state + year
CREATE INDEX IF NOT EXISTS idx_county_state_year ON county_affordability (state_fips, year);
CREATE INDEX IF NOT EXISTS idx_hud_state_year     ON hud_fmr_state (state_code, year);