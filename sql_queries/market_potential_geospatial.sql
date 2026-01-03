-- Market Saturation Analysis: The combination of the CBS population and the number of supermarkets (Supply vs Demand):
WITH CityPopulation AS (
  -- Demand Side Data Preparation:
  -- Retrieves population data to represent potential market size.
  -- Uses TRIM to remove whitespace and ensure accurate matching during the JOIN operation.
  SELECT
    TRIM(city_name) AS city_name,
    total_population
  FROM `bqproj-435911.Final_Project_2025.Lamas_israel_2025`
  WHERE total_population > 0 -- Excludes invalid entries with zero population
),

CitySupermarkets AS (
  -- Supply Side Data Preparation:
  -- Aggregates the total number of supermarkets per city.
  -- Groups by city name to create a distinct count of retail entities.
  SELECT
    TRIM(city) AS city_name,
    COUNT(*) AS supermarket_count
  FROM `bqproj-435911.Final_Project_2025.supermarkets_il`
  GROUP BY city_name
)

SELECT
  P.city_name,
  
  -- Independent Variable (X-Axis):
  -- Represents the total population size of the city.
  P.total_population,

  -- Dependent Variable (Y-Axis):
  -- Represents the existing number of supermarkets in the city.
  S.supermarket_count,

  -- Derived Metric (Market Saturation Indicator):
  -- Calculates the ratio of residents per supermarket.
  -- A higher value suggests an "underserved" market (potential for expansion),
  -- while a lower value suggests a "saturated" market.
  ROUND(P.total_population / S.supermarket_count, 0) AS people_per_supermarket
  
FROM CityPopulation P
JOIN CitySupermarkets S
  ON P.city_name = S.city_name -- Merges Demand and Supply datasets

-- Data Filtering:
-- Filters out small settlements (population < 10,000) to reduce noise in the scatter plot
-- and focus the analysis on relevant urban centers.
WHERE P.total_population > 10000;

---------------------------------------------------------

-- Yavne Market Potential:
  /* =================================================================
PART 1: Market Potential Analysis (Total Addressable Market)
Objective: Estimate the number of potential households (Buying Units) 
in Yavne based on CBS (Lamas) population data.
=================================================================
*/

SELECT 
    city_name, 
    total_population,
    -- Methodology: Converting Total Population to Households.
    -- Assumption: Average household size in Israel is approx. 3.3 persons.
    -- Rationale: Supermarkets target households (Buying Units), not individual consumers.
    ROUND(total_population / 3.3) as estimated_households
FROM `bqproj-435911.Final_Project_2025.Lamas_israel_2025`
WHERE TRIM(city_name) = 'יבנה';


/* =================================================================
PART 2: Competitive Landscape Analysis (Market Saturation)
Objective: Determine competition density by calculating the 
ratio of residents per supermarket branch.
=================================================================
*/

SELECT 
    city, 
    COUNT(*) as number_of_competitors,
    -- Metric: Residents per Supermarket.
    -- Calculation: Total Population (60,156) divided by Number of Competitors.
    -- Note: Population count is derived from the previous CBS query.
    ROUND(60156 / COUNT(*)) as residents_per_supermarket
FROM `bqproj-435911.Final_Project_2025.supermarkets_il`
WHERE TRIM(city) = 'יבנה'
GROUP BY city;


ORDER BY total_population;
