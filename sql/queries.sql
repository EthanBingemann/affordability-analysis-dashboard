-- queries.sql

-- Most expensive counties
SELECT
    NAME                                        AS county_name,
    ROUND(AVG(Median_Gross_Rent), 0)            AS avg_rent,
    ROUND(AVG(Median_HH_Income), 0)             AS avg_income,
    ROUND(AVG(rent_to_income_ratio), 1)         AS avg_rent_to_income
FROM county_affordability
GROUP BY fips
ORDER BY avg_rent DESC
LIMIT 25;


-- Least expensive counties
SELECT
    NAME                                        AS county_name,
    ROUND(AVG(Median_Gross_Rent), 0)            AS avg_rent,
    ROUND(AVG(Median_HH_Income), 0)             AS avg_income,
    ROUND(AVG(rent_to_income_ratio), 1)         AS avg_rent_to_income
FROM county_affordability
GROUP BY fips
ORDER BY avg_rent ASC
LIMIT 25;


-- Percent of renters over 50% wealth in housing
SELECT
    NAME                                                AS county_name,
    ROUND(AVG(pct_severely_cost_burdened), 1)          AS avg_pct_cost_burdened,
    ROUND(AVG(Median_Gross_Rent), 0)                   AS avg_rent,
    ROUND(AVG(Median_HH_Income), 0)                    AS avg_income
FROM county_affordability
GROUP BY fips
ORDER BY avg_pct_cost_burdened DESC
LIMIT 25;


-- Rent to income ratio
SELECT
    NAME                                        AS county_name,
    year,
    ROUND(Median_Gross_Rent, 0)                 AS rent,
    ROUND(Median_HH_Income, 0)                  AS income,
    ROUND(rent_to_income_ratio, 1)              AS rent_to_income
FROM county_affordability
ORDER BY NAME, year;


-- National level CPI trends over time
SELECT
    year,
    ROUND(CPI_All_Items, 2)                     AS cpi,
    ROUND(CPI_Rent_Primary, 2)                  AS cpi_rent,
    ROUND(CPI_Shelter, 2)                       AS cpi_shelter,
    ROUND(Real_Median_Personal_Income, 0)       AS real_median_income,
    -- Year over year CPI growth %
    ROUND(
        (CPI_All_Items - LAG(CPI_All_Items) OVER (ORDER BY year))
        / LAG(CPI_All_Items) OVER (ORDER BY year) * 100, 2
    ) AS cpi_growth_pct,
    -- Year over year rent CPI growth %
    ROUND(
        (CPI_Rent_Primary - LAG(CPI_Rent_Primary) OVER (ORDER BY year))
        / LAG(CPI_Rent_Primary) OVER (ORDER BY year) * 100, 2
    ) AS rent_cpi_growth_pct
FROM national_trends
ORDER BY year;


-- State-level affordability
-- State-level affordability
WITH state_lookup AS (
    SELECT '01' AS fips, 'AL' AS code, 'Alabama' AS name UNION ALL
    SELECT '02', 'AK', 'Alaska' UNION ALL
    SELECT '04', 'AZ', 'Arizona' UNION ALL
    SELECT '05', 'AR', 'Arkansas' UNION ALL
    SELECT '06', 'CA', 'California' UNION ALL
    SELECT '08', 'CO', 'Colorado' UNION ALL
    SELECT '09', 'CT', 'Connecticut' UNION ALL
    SELECT '10', 'DE', 'Delaware' UNION ALL
    SELECT '11', 'DC', 'District of Columbia' UNION ALL
    SELECT '12', 'FL', 'Florida' UNION ALL
    SELECT '13', 'GA', 'Georgia' UNION ALL
    SELECT '15', 'HI', 'Hawaii' UNION ALL
    SELECT '16', 'ID', 'Idaho' UNION ALL
    SELECT '17', 'IL', 'Illinois' UNION ALL
    SELECT '18', 'IN', 'Indiana' UNION ALL
    SELECT '19', 'IA', 'Iowa' UNION ALL
    SELECT '20', 'KS', 'Kansas' UNION ALL
    SELECT '21', 'KY', 'Kentucky' UNION ALL
    SELECT '22', 'LA', 'Louisiana' UNION ALL
    SELECT '23', 'ME', 'Maine' UNION ALL
    SELECT '24', 'MD', 'Maryland' UNION ALL
    SELECT '25', 'MA', 'Massachusetts' UNION ALL
    SELECT '26', 'MI', 'Michigan' UNION ALL
    SELECT '27', 'MN', 'Minnesota' UNION ALL
    SELECT '28', 'MS', 'Mississippi' UNION ALL
    SELECT '29', 'MO', 'Missouri' UNION ALL
    SELECT '30', 'MT', 'Montana' UNION ALL
    SELECT '31', 'NE', 'Nebraska' UNION ALL
    SELECT '32', 'NV', 'Nevada' UNION ALL
    SELECT '33', 'NH', 'New Hampshire' UNION ALL
    SELECT '34', 'NJ', 'New Jersey' UNION ALL
    SELECT '35', 'NM', 'New Mexico' UNION ALL
    SELECT '36', 'NY', 'New York' UNION ALL
    SELECT '37', 'NC', 'North Carolina' UNION ALL
    SELECT '38', 'ND', 'North Dakota' UNION ALL
    SELECT '39', 'OH', 'Ohio' UNION ALL
    SELECT '40', 'OK', 'Oklahoma' UNION ALL
    SELECT '41', 'OR', 'Oregon' UNION ALL
    SELECT '42', 'PA', 'Pennsylvania' UNION ALL
    SELECT '44', 'RI', 'Rhode Island' UNION ALL
    SELECT '45', 'SC', 'South Carolina' UNION ALL
    SELECT '46', 'SD', 'South Dakota' UNION ALL
    SELECT '47', 'TN', 'Tennessee' UNION ALL
    SELECT '48', 'TX', 'Texas' UNION ALL
    SELECT '49', 'UT', 'Utah' UNION ALL
    SELECT '50', 'VT', 'Vermont' UNION ALL
    SELECT '51', 'VA', 'Virginia' UNION ALL
    SELECT '53', 'WA', 'Washington' UNION ALL
    SELECT '54', 'WV', 'West Virginia' UNION ALL
    SELECT '55', 'WI', 'Wisconsin' UNION ALL
    SELECT '56', 'WY', 'Wyoming' UNION ALL
    SELECT '72', 'PR', 'Puerto Rico'
)
SELECT
    s.name                                      AS state_name,
    s.code                                      AS state_code,
    ROUND(AVG(c.Median_Gross_Rent), 0)          AS avg_rent,
    ROUND(AVG(c.Median_HH_Income), 0)           AS avg_income,
    ROUND(AVG(c.rent_to_income_ratio), 1)       AS avg_rent_to_income,
    ROUND(AVG(c.pct_severely_cost_burdened), 1) AS avg_pct_cost_burdened,
    ROUND(AVG(h.fmr_1br), 0)                   AS hud_fmr_1br,
    ROUND(AVG(h.fmr_2br), 0)                   AS hud_fmr_2br
FROM county_affordability c
LEFT JOIN state_lookup s ON SUBSTR(c.fips, 1, 2) = s.fips
LEFT JOIN hud_fmr_state h ON s.code = h.state_code
GROUP BY s.fips
ORDER BY s.name;