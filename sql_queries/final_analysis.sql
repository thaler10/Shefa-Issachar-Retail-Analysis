-- Part 1: Data Show
-------------------------------------------------------------------------------------------------

-- 1.1
/*
QUERY: Daily Visitor Traffic Analysis
Objective: Analyze the distribution of unique visitors across days of the week to identify peak and off-peak operational days.
*/

SELECT
  -- Formats the timestamp to display the full English name of the day (e.g., 'Sunday', 'Monday')
  FORMAT_DATE('%A', timestamp) AS day_name,
  
  -- Counts distinct devices to determine the actual number of unique visitors per day, eliminating duplicate pings
  COUNT(DISTINCT device_id) AS unique_visitors

FROM `bqproj-435911.Final_Project_2025.geolocation`

GROUP BY 
  day_name, 
  -- Extracting the numeric day index (1=Sunday, 7=Saturday) is crucial for correct chronological sorting
  EXTRACT(DAYOFWEEK FROM timestamp)

ORDER BY 
  -- Sorts the output chronologically from Sunday to Saturday rather than alphabetically
  EXTRACT(DAYOFWEEK FROM timestamp);
-- 1.2
-- Payment Method Distribution:
SELECT
  payment_method,
  COUNT(*) AS total,  -- Count the number of transactions for each payment method
  COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS percentage  -- Calculate the percentage of total transactions
FROM `bqproj-435911.Final_Project_2025.log_sales`
GROUP BY payment_method;
-- 1.3
-- The 10 Most Profitable Days:

SELECT
  -- Extracts the date portion from the timestamp to aggregate sales on a daily basis
  DATE(timestamp) as sale_date,

  -- Calculates the total revenue for each specific day
  SUM(total) as daily_revenue

FROM `bqproj-435911.Final_Project_2025.log_sales`

GROUP BY 
  sale_date

ORDER BY 
  -- Sorts the results in descending order to bring the highest revenue days to the top
  daily_revenue DESC

LIMIT 10;
-------------------------------------------------------------------------------------------------
-- 2.1

SELECT
    -- Numeric Sort Key:
    -- Uses the FLOOR function to bucket dwell times into 5-minute intervals.
    -- This key is essential for ordering the X-axis chronologically (5, 10, 15) rather than alphabetically.
    FLOOR(dwell_minutes / 5) * 5 AS sort_key,

    -- X-Axis Label:
    -- Concatenates strings to create a clear visual range for the graph (e.g., "0-5 min").
    CONCAT(
        CAST(FLOOR(dwell_minutes / 5) * 5 AS STRING), 
        '-', 
        CAST((FLOOR(dwell_minutes / 5) * 5) + 5 AS STRING), 
        ' min'
    ) AS time_range,

    -- Dependent Variable (Y-Axis):
    -- Calculates the average transaction total for each time bucket, rounded to 2 decimal places.
    ROUND(AVG(total), 2) AS average_purchase_amount

FROM `bqproj-435911.Final_Project_2025.log_sales`

WHERE 
    -- Data Cleaning & Filtering:
    dwell_minutes IS NOT NULL        -- Ensures valid dwell time exists
    AND customer_id IS NOT NULL      -- Focuses on identified app users for accurate time tracking
    AND dwell_minutes > 0            -- Removes data noise (zero duration)
    AND dwell_minutes <= 120         -- Filters outliers (duration > 2 hours) to maintain analysis relevance

GROUP BY 1, 2
ORDER BY 1 ASC; -- Sorts results by the numeric key to ensure correct sequence

-- 2.2

--Traffic Heatmap:

SELECT
  -- Temporal Dimensions (X-Axis):
  -- Extracts the full name of the weekday (e.g., "Sunday", "Monday") for clear visualization.
  FORMAT_DATE('%A', timestamp) AS day_name,

  -- Sorting Helper:
  -- Extracts the numeric day of the week (1=Sunday, 7=Saturday).
  -- This column is crucial for ordering the graph chronologically rather than alphabetically.
  EXTRACT(DAYOFWEEK FROM timestamp) AS day_sort,

  -- Time Dimension:
  -- Extracts the specific hour of the day (0-23) to analyze hourly traffic patterns.
  EXTRACT(HOUR FROM timestamp) AS hour_of_day,

  -- Key Metric: Average Visitors per Hour
  -- Logic: Calculates the average traffic for a specific hour on a specific day of the week.
  -- Formula: Total unique visitors observed in this time slot / Number of times this specific day occurred in the dataset.
  -- This normalization ensures the metric represents a "typical" day rather than a sum total.
  ROUND(COUNT(DISTINCT device_id) / COUNT(DISTINCT DATE(timestamp)), 1) AS avg_visitors

FROM `bqproj-435911.Final_Project_2025.geolocation`
GROUP BY day_name, day_sort, hour_of_day
ORDER BY day_sort, hour_of_day;
-- 2.3

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
WHERE P.total_population > 10000 

ORDER BY total_population;

-------------------------------------------------------------------------------------------------

-- Part 2 - Metrics:
-------------------------------------------------------------------------------------------------
-- 1. 

WITH classified_customers AS (
  SELECT
    -- Unique Identifier Construction:
    -- Creates a robust unified ID to handle both app users and anonymous purchasers.
    -- Priority 1: 'device_id' from geolocation (standard app users).
    -- Priority 2: 'sale_id' is used as a fallback for 'no_phone' customers.
    -- Crucial: Using 'sale_id' prevents all anonymous transactions from collapsing into a single NULL entity.
    COALESCE(g.device_id, s.customer_id, s.sale_id) AS final_user_id,

    -- Customer Segmentation Logic:
    -- Determines the customer type based on their presence in the geolocation table.
    -- If 'device_id' is NULL, the record exists only in sales logs, identifying a 'no_phone' customer.
    -- Otherwise, retains the behavioral classification (repeat/one-time/not_paying) from the geolocation data.
    CASE
      WHEN g.device_id IS NULL THEN 'no_phone'
      ELSE g.role
    END as customer_type

  FROM `bqproj-435911.Final_Project_2025.log_sales` as s
  
  -- Data Merging Strategy:
  -- Uses a FULL JOIN to ensure comprehensive coverage:
  -- 1. Captures non-paying visitors (exist only in geolocation).
  -- 2. Captures 'no_phone' purchasers (exist only in sales logs).
  FULL JOIN `bqproj-435911.Final_Project_2025.geolocation` as g
    ON s.customer_id = g.device_id

  WHERE
    -- Filtering Irrelevant Entities:
    -- Excludes internal staff roles while explicitly retaining the 'no_phone' segment
    -- (rows where device_id is NULL) to ensure total revenue capture.
    g.role IN ('repeat_customer', 'one_time_customer', 'not_paying')
    OR g.device_id IS NULL
)

SELECT
  -- De-duplication:
  -- Applies DISTINCT to handle the one-to-many relationship in the geolocation data
  -- (multiple ping records per user), resulting in a clean list of unique users and their segments.
  DISTINCT final_user_id,
  customer_type
FROM classified_customers
ORDER BY customer_type;

-------------------------------------------------------------------------------------------------
-- 2.
/*
Question 2:: Employee Segmentation
Objective: Generate a detailed roster of all employees, including the total size of their respective departments.
*/

SELECT
DISTINCT device_id,
role,
-- Utilizing a Window Function to display the total department headcount alongside each individual employee record
COUNT(DISTINCT device_id) OVER(PARTITION BY role) as total_in_department

FROM `bqproj-435911.Final_Project_2025.geolocation`

WHERE role IN (
'manager', -- Branch Manager
'cashier', -- Checkout Staff
'butcher', -- Butchery Staff
'general_worker', -- General Operations Worker
'senior_general_worker', -- Senior Operations Worker
'security_guy', -- Security Personnel
'delivery_guy' -- Supplier / Delivery
)

-- Sorting the list by role (as required) and by employee ID
ORDER BY role, device_id;
---------------------------------------------------------------------------------------
-- 2. Bonus:
WITH 
-- 1. Daily Activity: Aggregating dwell time per device per day
-- Logic: We need to know how many hours a user spends daily to distinguish employees from customers.
daily_activity AS (
  SELECT
    device_id,
    DATE(timestamp) as visit_date,
    TIMESTAMP_DIFF(MAX(timestamp), MIN(timestamp), MINUTE) / 60.0 as daily_hours
  FROM `bqproj-435911.Final_Project_2025.geolocation`
  GROUP BY device_id, visit_date
),

-- 2. Home Zone Analysis: Identifying the primary area for each user
-- Logic: The area where a user spends the most time determines their potential employee role (e.g., Warehouse vs. Office).
device_home_zone AS (
  SELECT 
    device_id,
    area as main_area,
    ROW_NUMBER() OVER(PARTITION BY device_id ORDER BY COUNT(*) DESC) as rn
  FROM `bqproj-435911.Final_Project_2025.geolocation`
  GROUP BY device_id, area
),

-- 3. User Stats Profile: Creating a high-level summary per user
-- Metrics included: Average daily hours, global lifespan (first seen to last seen), and seniority rank in their main area.
user_stats AS (
  SELECT 
    d.device_id,
    AVG(d.daily_hours) as avg_daily_hours, 
    MIN(g.timestamp) as first_seen_global, 
    MAX(g.timestamp) as last_seen_global,  
    h.main_area,
    -- Rank ensures we can identify the most senior employee in a specific zone if needed
    RANK() OVER(PARTITION BY h.main_area ORDER BY MIN(g.timestamp) ASC) as area_seniority_rank
  FROM daily_activity d
  JOIN `bqproj-435911.Final_Project_2025.geolocation` g ON d.device_id = g.device_id
  JOIN device_home_zone h ON d.device_id = h.device_id
  WHERE h.rn = 1 -- Filter to keep only the primary zone
  GROUP BY d.device_id, h.main_area
),

-- 4. Visit Frequency Analysis: Calculating gaps between visits
-- Logic: Essential for distinguishing between 'One-Time' and 'Repeat' customers.
visit_gaps AS (
  SELECT 
    device_id,
    AVG(days_diff) as avg_gap_days,
    COUNT(*) + 1 as total_visits -- Counting distinct visit days
  FROM (
    SELECT 
      device_id,
      DATE_DIFF(visit_date, LAG(visit_date) OVER(PARTITION BY device_id ORDER BY visit_date), DAY) as days_diff
    FROM daily_activity
  )
  WHERE days_diff IS NOT NULL
  GROUP BY device_id
),

-- 5. Payment Validation: Cross-referencing with Sales Log
-- Logic: Users not found in this list are classified as non-paying visitors.
paying_indicators AS (
  SELECT DISTINCT customer_id 
  FROM `bqproj-435911.Final_Project_2025.log_sales`
  WHERE customer_id IS NOT NULL
),

-- 6. Classification Engine: The Core Business Logic
gps_user_classification AS (
  SELECT
    CAST(u.device_id AS STRING) as final_user_id,
    
    CASE
      -- === Level 1: Employee Identification ===
      -- Criteria: High dwell time (>3 hours) OR access to restricted zones.
      WHEN u.avg_daily_hours >= 3 OR u.main_area IN ('WAREHOUSE', 'HEAD_OFFICE') THEN
        CASE 
          WHEN u.main_area = 'WAREHOUSE' THEN 'delivery_guy'
          WHEN u.main_area = 'HEAD_OFFICE' THEN 'manager'
          WHEN u.main_area = 'CASH_REGISTERS' THEN 'cashier'
          WHEN u.main_area = 'BUTCHERY' THEN 'butcher'
          WHEN u.main_area = 'PARKING' THEN 'security_guy'
          WHEN u.main_area = 'SUPERMARKET' THEN 
              IF(u.area_seniority_rank = 1, 'senior_general_worker', 'general_worker')
          ELSE 'general_worker'
        END

      -- === Level 2: Non-Paying Visitors ===
      -- Criteria: Device detected but no matching transaction in sales log.
      WHEN p.customer_id IS NULL THEN 'not_paying'

      -- === Level 3: Repeat Customers (Strict Logic) ===
      -- Criteria refined to filter out sporadic visitors:
      -- 1. Consistency: Avg gap <= 12 days.
      -- 2. Loyalty (Tenure): Must be active for at least 60 days.
      -- 3. Frequency: Must have visited at least 10 times.
      WHEN v.avg_gap_days <= 12 
           AND DATE_DIFF(u.last_seen_global, u.first_seen_global, DAY) >= 60 
           AND v.total_visits >= 10
           THEN 'repeat_customer'
           
      -- === Level 4: One-Time Customers ===
      -- Fallback for any paying customer who doesn't meet the strict 'Repeat' criteria.
      ELSE 'one_time_customer'
      
    END as final_calculated_role

  FROM user_stats u
  LEFT JOIN visit_gaps v ON u.device_id = v.device_id
  LEFT JOIN paying_indicators p ON u.device_id = p.customer_id
),

-- 7. Final Data Integration
final_dataset AS (
  SELECT * FROM gps_user_classification

  UNION ALL

  -- Handling "No Phone" Transactions
  -- Adding sales records that have no associated device_id (manual cash transactions etc.)
  SELECT
    CAST(sale_id AS STRING) as final_user_id,
    'no_phone' as final_calculated_role
  FROM `bqproj-435911.Final_Project_2025.log_sales`
  WHERE customer_id IS NULL
)

-- Output the clean, classified dataset
SELECT * FROM final_dataset
ORDER BY final_calculated_role, final_user_id;

---------------------------------------------------------------------------------------
-- 3a.1.
SELECT
  COUNT(*) AS total_signals,
  COUNTIF(accuracy_m > 30) as outlier_signals,
  ROUND(COUNTIF(accuracy_m > 30) / COUNT(*), 2) as outlier_ratio
  FROM `bqproj-435911.Final_Project_2025.geolocation`;

---------------------------------------------
--3a.3

SELECT
  role,
  
  -- 1. Volume Analysis: Total signals vs. Low Accuracy signals
  COUNT(*) as total_signals,
  COUNTIF(accuracy_m > 30) as outlier_signals, -- Signals with deviation > 30m
  
  -- 2. Impact Analysis: Percentage of unreliable data per role
  ROUND(COUNTIF(accuracy_m > 30) / COUNT(*), 2) as outlier_ratio,
  
  -- Optional: Cross-reference with the most frequent area to validate the "Parking/Structure" hypothesis
  APPROX_TOP_COUNT(area, 1)[OFFSET(0)].value as primary_area

FROM `bqproj-435911.Final_Project_2025.geolocation`
GROUP BY role
ORDER BY outlier_ratio DESC;

-- 4

WITH SpatialCleanedData AS (
  SELECT
    t1.*, -- Select all original columns
    
    -- Create a standardized 'Corrected Area' column for spatial analysis
    CASE
      -- 1. High Accuracy: Use the original reported area
      WHEN accuracy_m <= 30 THEN area
      
      -- 2. Low Accuracy (Static Roles): Impute area based on known fixed locations (Hard Coding)
      WHEN accuracy_m > 30 AND role = 'security_guy' THEN 'PARKING'
      WHEN accuracy_m > 30 AND role = 'butcher' THEN 'BUTCHERY'
      WHEN accuracy_m > 30 AND role = 'cashier' THEN 'CASH_REGISTERS'
      
      ELSE NULL -- Dynamic roles with low accuracy are filtered out in the WHERE clause
    END as spatial_area_fixed

  FROM 
    `bqproj-435911.Final_Project_2025.geolocation` t1 
  WHERE
    -- Filter Logic: Retain high-accuracy signals OR low-accuracy signals from static roles
    (accuracy_m <= 30)
    OR
    (accuracy_m > 30 AND role IN ('security_guy', 'butcher', 'cashier'))
)

-- Retrieve cleaned data for analysis
-- (Current filter focuses on the imputed outliers)
SELECT * FROM SpatialCleanedData
WHERE accuracy_m > 30;

----------------------------------------------------------------
-- 3b
/*
Preliminary Analysis - Daily Visit Frequency per Customer (Sessionization):
Checking the distribution of total arrivals ("Shopping Sessions") to the store by each day per customer
 */
WITH raw_data AS (
  SELECT
    customer_id, -- Note: Verify if the correct column name is 'customer_id' or 'client_id' in the source table.
    timestamp,
    DATE(timestamp) as visit_date
  FROM `bqproj-435911.Final_Project_2025.log_sales` -- Note: Ensure this references the Geolocation table, not the Sales log.
),

time_diffs AS (
  SELECT
    customer_id,
    visit_date,
    timestamp,
    -- Retrieve the timestamp of the previous signal for the same customer on the same day
    LAG(timestamp) OVER (PARTITION BY customer_id, visit_date ORDER BY timestamp) as prev_ts
  FROM raw_data
),

visit_flags AS (
  SELECT
    customer_id,
    visit_date,
    timestamp,
    -- Flag a new visit if it's the first signal of the day (NULL) or if the gap exceeds 60 minutes
    CASE
      WHEN prev_ts IS NULL THEN 1
      WHEN TIMESTAMP_DIFF(timestamp, prev_ts, MINUTE) > 60 THEN 1
      ELSE 0
    END as is_new_visit
  FROM time_diffs
),

visits_per_day AS (
  SELECT
    customer_id,
    visit_date,
    SUM(is_new_visit) as daily_visits_count
  FROM visit_flags
  GROUP BY 1, 2
)

-- Final Aggregation: Analyze the frequency distribution of daily visits (e.g., single vs. multiple visits per day)
SELECT
  daily_visits_count as visits_in_one_day,
  COUNT(*) as number_of_cases,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM visits_per_day
GROUP BY 1
ORDER BY 1 ASC;

/*
/*
QUESTION 3b: App Performance Analysis (Capture Rate Verification)

Objective: Determine if the app meets the requirement of capturing at least 40% of expected signals (1 per minute).

Challenge: Simple daily calculation (Max Time - Min Time) is flawed because users/employees
often leave the store and return later (Split Visits), creating false "dead time".

Solution: "Sessionization" Algorithm.
We divide user activity into distinct "Sessions". A gap of >60 minutes implies a new visit.
*/

WITH
-- Step 1: Calculate time gaps between consecutive signals for each device
raw_gaps AS (
SELECT
device_id,
timestamp,
-- Calculate minutes elapsed since the previous signal
TIMESTAMP_DIFF(timestamp, LAG(timestamp) OVER(PARTITION BY device_id ORDER BY timestamp), MINUTE) as gap_minutes
FROM `bqproj-435911.Final_Project_2025.geolocation`
-- Note: We include ALL signals (even low accuracy) because we are testing app transmission, not GPS precision.
),

-- Step 2: Flag the start of new sessions
-- Logic: A gap larger than 60 minutes indicates the user left the store (New Session)
flag_new_sessions AS (
SELECT
device_id,
timestamp,
CASE
WHEN gap_minutes > 60 OR gap_minutes IS NULL THEN 1
ELSE 0
END as is_new_session
FROM raw_gaps
),

-- Step 3: Generate Unique Session IDs using Cumulative Sum
-- This technique groups all consecutive signals of a single visit under one unique ID
create_session_id AS (
SELECT
device_id,
timestamp,
SUM(is_new_session) OVER(PARTITION BY device_id ORDER BY timestamp) as session_id
FROM flag_new_sessions
),

-- Step 4: Aggregate statistics per Session (Visit)
session_stats AS (
SELECT
device_id,
session_id,
COUNT(*) as actual_pings, -- Total signals actually received by the server
TIMESTAMP_DIFF(MAX(timestamp), MIN(timestamp), MINUTE) as session_duration_minutes -- Expected signals (1 per minute)
FROM create_session_id
GROUP BY device_id, session_id
-- Filter: Exclude momentary glitches or single-ping visits (0-1 min) to avoid division by zero errors
HAVING session_duration_minutes > 1
)
-- Step 5: Final Performance Calculation (Global Ratio)
SELECT
  SUM(actual_pings) as total_pings_received,
  SUM(session_duration_minutes) as total_expected_pings,
  
  -- The Performance Score: Actual Pings / Expected Pings
  ROUND(SUM(actual_pings) / SUM(session_duration_minutes), 2) as app_performance_score
FROM session_stats;

/*
-- 3c. Bonus
Query: Financial Outlier Detection (IQR Method)
Objective: Identify transactions with abnormal value ("Whales") and measure their impact on the Average Basket Size KPI.
Method: Using Interquartile Range (IQR). Outlier > Q3 + 1.5 * IQR.
*/

WITH stats AS (
  SELECT
    -- Calculate Quartiles (25th and 75th percentiles) to determine data distribution
    APPROX_QUANTILES(total, 100)[OFFSET(25)] as q1,
    APPROX_QUANTILES(total, 100)[OFFSET(75)] as q3,
    AVG(total) as current_avg_ticket
  FROM `bqproj-435911.Final_Project_2025.log_sales`
),

bounds AS (
  SELECT 
    q1, 
    q3, 
    (q3 - q1) as iqr,
    -- Define the statistical threshold (Upper Fence) for an outlier
    q3 + (1.5 * (q3 - q1)) as upper_fence,
    current_avg_ticket
  FROM stats
)

SELECT
  b.current_avg_ticket as avg_with_outliers,
  
  -- Recalculate average EXCLUDING the outliers to find the "True" B2C average
  (SELECT AVG(total) FROM `bqproj-435911.Final_Project_2025.log_sales` WHERE total <= b.upper_fence) as avg_without_outliers,
  
  -- Count the volume of outlier transactions
  (SELECT COUNT(*) FROM `bqproj-435911.Final_Project_2025.log_sales` WHERE total > b.upper_fence) as outliers_count,
  
  b.upper_fence as threshold_value

FROM bounds b;

/*
Query: Whale Demographics & Segmentation
Objective: Break down high-value transactions (> IQR Threshold) by customer role to identify the source of these outliers.
*/

WITH stats AS (
  -- Step 1: Recalculate the statistical threshold (same logic as above)
  SELECT
    APPROX_QUANTILES(total, 100)[OFFSET(75)] as q3,
    APPROX_QUANTILES(total, 100)[OFFSET(25)] as q1
  FROM `bqproj-435911.Final_Project_2025.log_sales`
),

bounds AS (
  SELECT q3 + (1.5 * (q3 - q1)) as upper_fence FROM stats
),

high_value_txns AS (
  -- Step 2: Retrieve ONLY the specific "Whale" transactions exceeding the threshold
  SELECT 
    s.customer_id,
    s.sale_id,
    s.total
  FROM `bqproj-435911.Final_Project_2025.log_sales` s, bounds b
  WHERE s.total > b.upper_fence
),

user_roles AS (
  -- Step 3: Fetch user roles per device (using DISTINCT to ensure 1:1 mapping)
  SELECT DISTINCT device_id, role 
  FROM `bqproj-435911.Final_Project_2025.geolocation`
)

SELECT
  -- Classification Logic: If the ID exists in Geo data, use the role. If not, classify as 'No App'.
  COALESCE(u.role, 'No App (Walk-in)') as customer_role,
  
  -- Key Metrics per Segment
  COUNT(h.sale_id) as total_txns, -- Volume of high-value transactions
  ROUND(COUNT(h.sale_id) * 100.0 / SUM(COUNT(h.sale_id)) OVER(), 1) as share_of_whales_pct, -- % of total outliers
  ROUND(AVG(h.total), 0) as avg_ticket_size, -- Average spend for this segment
  ROUND(MAX(h.total), 0) as max_ticket_size -- Maximum spend observed

FROM high_value_txns h
LEFT JOIN user_roles u
  ON h.customer_id = u.device_id

GROUP BY 1
ORDER BY 2 DESC;

----------------------------------------------------------------



-- 4a
SELECT
  DISTINCT device_id,
  role
FROM `bqproj-435911.Final_Project_2025.geolocation`
WHERE
  role = 'delivery_guy'
ORDER BY device_id;

-- 4b. Bonus

/* QUESTION 4b: Pattern-Based Identification
Objective: Identify delivery devices without using the 'role' column.
Logic: Intersection of WAREHOUSE area + Delivery Days (Mon/Thu) + Early Morning Window (05:00-07:00).
*/

SELECT DISTINCT
device_id
FROM `bqproj-435911.Final_Project_2025.geolocation`
WHERE area = 'WAREHOUSE'
AND EXTRACT(DAYOFWEEK FROM timestamp) IN (2, 5) -- 2=Monday, 5=Thursday
AND EXTRACT(HOUR FROM timestamp) BETWEEN 5 AND 7
ORDER BY device_id;

-- 4c.

/* QUESTION 4c: Missed Delivery Detection
Objective: Find active business days where no warehouse activity occurred during the supply window.
*/

SELECT DISTINCT
DATE(t1.timestamp) as missed_date,
FORMAT_DATE('%A', t1.timestamp) as day_name
FROM `bqproj-435911.Final_Project_2025.geolocation` t1
WHERE
-- 1. Filter for the Expected Schedule (Mon/Thu)
EXTRACT(DAYOFWEEK FROM t1.timestamp) IN (2, 5)

-- 2. "Heartbeat Check": Ensure the store was open (filter out holidays/closed days)
-- We assume if non-security staff were present, the store was operational.
AND t1.role != 'security_guy'

-- 3. The Negative Filter: Exclude days where a delivery actually happened
AND DATE(t1.timestamp) NOT IN (
SELECT DISTINCT DATE(t2.timestamp)
FROM `bqproj-435911.Final_Project_2025.geolocation` t2
WHERE t2.area = 'WAREHOUSE'
AND EXTRACT(HOUR FROM t2.timestamp) BETWEEN 5 AND 7
)
ORDER BY missed_date;

-- 5a.

/* QUESTION 5a: Top 5 Frequent Customers
Logic: Count unique visiting DAYS per customer to identify frequency.
*/
SELECT
device_id,
COUNT(DISTINCT DATE(timestamp)) as arrival_days_count
FROM `bqproj-435911.Final_Project_2025.geolocation`
WHERE role = 'repeat_customer'
GROUP BY device_id
ORDER BY arrival_days_count DESC
LIMIT 5;




-- 5b.
/* QUESTION 5b: Top 5 Spenders
Logic: Sum the 'total' amount from log_sales.
Note: Avoided JOIN with geolocation to prevent data duplication (fan-out).
*/
SELECT
customer_id as device_id,
ROUND(SUM(total), 2) as total_expenses
FROM `bqproj-435911.Final_Project_2025.log_sales`
WHERE customer_id IS NOT NULL
GROUP BY 1
ORDER BY total_expenses DESC
LIMIT 5;

-- 6.

WITH all_activity AS (
-- 1. Geolocation Data: Capture users with the app installed
SELECT
timestamp,
device_id as distinct_user_id
FROM `bqproj-435911.Final_Project_2025.geolocation`
WHERE role IN ('customer', 'repeat_customer', 'one_time_customer', 'not_paying')

UNION ALL

-- 2. Sales Data: Capture users without the app ('no_phone')
SELECT
timestamp,
-- Logic: If customer_id is NULL (no app), use sale_id as a unique user proxy for that timestamp
COALESCE(customer_id, sale_id) as distinct_user_id
FROM `bqproj-435911.Final_Project_2025.log_sales`
)

-- Main Query: Aggregate traffic by Day and Hour
SELECT
FORMAT_DATE('%A', timestamp) as day_name, -- Day name for visualization (e.g., Sunday)
EXTRACT(DAYOFWEEK FROM timestamp) as day_index, -- Index for sorting (1=Sunday, 7=Saturday)
EXTRACT(HOUR FROM timestamp) as hour_of_day, -- Hour (0-23)

-- Metric: Count unique people (removes duplicates if a user appears in both tables same hour)
COUNT(DISTINCT distinct_user_id) as total_foot_traffic
FROM all_activity
GROUP BY 1, 2, 3
ORDER BY day_index, hour_of_day;

-- Part 3 - Business Recommendations:
--3a.2
WITH CustomerProfiles AS (
    SELECT 
        customer_id,
        AVG(total) as avg_lifetime_basket,
        CASE 
            WHEN AVG(total) < 300 THEN '1. Small (<300)'
            WHEN AVG(total) BETWEEN 300 AND 800 THEN '2. Medium (300-800)'
            WHEN AVG(total) BETWEEN 800 AND 1139 THEN '3. Large (800-1139)'
            ELSE '4. Whale (>1139)' -- עקביות מלאה עם סעיף 3ג
        END as customer_segment
    FROM `bqproj-435911.Final_Project_2025.log_sales`
    GROUP BY 1
),

HourlyTransactions AS (
    SELECT 
        s.sale_id,
        cp.customer_segment,
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM s.timestamp) IN (5, 6) THEN 'Peak (Thu-Fri)'
            WHEN EXTRACT(HOUR FROM s.timestamp) BETWEEN 14 AND 17 
                 AND EXTRACT(DAYOFWEEK FROM s.timestamp) < 5 THEN 'Off-Peak (Quiet)'
            ELSE 'Regular'
        END as time_category
    FROM `bqproj-435911.Final_Project_2025.log_sales` s
    JOIN CustomerProfiles cp ON s.customer_id = cp.customer_id
)

SELECT 
    time_category,
    customer_segment,
    COUNT(*) as num_sales,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY time_category), 2) as share_pct
FROM HourlyTransactions
WHERE time_category != 'Regular'
GROUP BY 1, 2
ORDER BY 1, 2;

-- 3a.3
-- Cashier Workforce Optimization: Demand vs. Capacity Gap Analysis

-----------------------------------------------------------------
-- Part 1: Average Checkout Time Calculation (Supply) - Session Logic
-----------------------------------------------------------------
WITH 
RawGeoData AS (
    SELECT 
        device_id, 
        timestamp,
        TIMESTAMP_DIFF(timestamp, LAG(timestamp) OVER(PARTITION BY device_id ORDER BY timestamp), MINUTE) as gap_minutes
    FROM 
        `bqproj-435911.Final_Project_2025.geolocation`
    WHERE 
        area = 'CASH_REGISTERS'
        AND role IN ('repeat_customer', 'one_time_customer', 'no_phone')
),

SessionFlags AS (
    SELECT
        device_id,
        timestamp,
        CASE WHEN gap_minutes > 20 OR gap_minutes IS NULL THEN 1 ELSE 0 END as is_new_session
    FROM RawGeoData
),

SessionIDs AS (
    SELECT
        device_id,
        timestamp,
        SUM(is_new_session) OVER(PARTITION BY device_id ORDER BY timestamp) as session_id
    FROM SessionFlags
),

SessionDurations AS (
    SELECT
        device_id,
        session_id,
        EXTRACT(DAYOFWEEK FROM MIN(timestamp)) as num_day,
        FORMAT_DATE('%A', MIN(timestamp)) as day_name,
        EXTRACT(HOUR FROM MIN(timestamp)) as hour,
        TIMESTAMP_DIFF(MAX(timestamp), MIN(timestamp), MINUTE) as duration_minutes
    FROM SessionIDs
    GROUP BY device_id, session_id
    HAVING duration_minutes >= 1 
),

AvgCheckoutTime AS (
    SELECT num_day, day_name, hour, AVG(duration_minutes) as avg_time_per_customer_min
    FROM SessionDurations
    GROUP BY 1, 2, 3
),

-----------------------------------------------------------------
-- Part 2: Sales Volume Calculation (Demand - Required)
-----------------------------------------------------------------
SalesVolume AS (
    SELECT 
        EXTRACT(DAYOFWEEK FROM timestamp) as num_day,
        FORMAT_DATE('%A', timestamp) as day_name,
        EXTRACT(HOUR FROM timestamp) as hour,
        COUNT(sale_id) as total_customers_per_hour
    FROM 
        `bqproj-435911.Final_Project_2025.log_sales`
    GROUP BY 1, 2, 3
),

-----------------------------------------------------------------
-- Part 3: Actual Staffing Status (Actual - Existing)
-----------------------------------------------------------------
ActualStaffing AS (
    SELECT
        EXTRACT(DAYOFWEEK FROM timestamp) as num_day,
        EXTRACT(HOUR FROM timestamp) as hour,
        -- Count unique cashier devices
        COUNT(DISTINCT device_id) as actual_registers_open
    FROM
        `bqproj-435911.Final_Project_2025.geolocation`
    WHERE
        role = 'cashier'
        -- AND area = 'CASH_REGISTERS' -- Recommended to verify they are stationed at the registers
    GROUP BY 1, 2
)

-----------------------------------------------------------------
-- Part 4: Final Join and Gap Calculation
-----------------------------------------------------------------
SELECT 
    s.num_day,
    s.day_name,
    s.hour,
    
    -- Auxiliary Data
    s.total_customers_per_hour AS customers_demand,
    ROUND(t.avg_time_per_customer_min, 2) AS avg_process_time,

    -- 1. Required (Optimal number based on model)
    CEIL(s.total_customers_per_hour / (60 / t.avg_time_per_customer_min)) AS optimal_registers_needed,
    
    -- 2. Actual (How many actually worked)
    -- Use COALESCE to return 0 instead of NULL if no cashiers are found
    COALESCE(a.actual_registers_open, 0) AS actual_registers_open,
    
    -- 3. The Gap (Actual minus Required)
    -- Positive = Overstaffing (Waste), Negative = Understaffing (Shortage)
    COALESCE(a.actual_registers_open, 0) - 
    CEIL(s.total_customers_per_hour / (60 / t.avg_time_per_customer_min)) AS staffing_gap

FROM 
    SalesVolume s
JOIN 
    AvgCheckoutTime t ON s.num_day = t.num_day AND s.hour = t.hour
LEFT JOIN
    ActualStaffing a ON s.num_day = a.num_day AND s.hour = a.hour

ORDER BY 
    s.num_day, s.hour;
-- ---------------------------------------------------------

-- Part 4: Regression
-- ---------------------------------------------------------
-- Regression 1: Unique Devices per Week (Trend Analysis)
-- X = Week Number, Y = Unique Visitors count
-- ---------------------------------------------------------

SELECT 
    -- Extract Week Number
    EXTRACT(WEEK FROM timestamp) as week_number,
    
    -- Extract Start Date of the week for better visualization in charts later
    DATE_TRUNC(DATE(timestamp), WEEK) as week_start_date,
    
    -- Target Variable (Y): Count unique devices per week
    COUNT(DISTINCT device_id) as unique_visitors_count

FROM `bqproj-435911.Final_Project_2025.geolocation`

WHERE 
    -- Filter logic: Include only relevant customers (exclude employees from traffic trend)
    role IN ('repeat_customer', 'one_time_customer', 'not_paying')

GROUP BY 1, 2
ORDER BY 1 ASC;

-- ---------------------------------------------------------
-- Regression 2: Purchase Amount vs. Dwell Time
-- X = Dwell Minutes, Y = Purchase Total
-- ---------------------------------------------------------

SELECT 
    -- Independent Variable (X): Time spent in store
    s.dwell_minutes,
    
    -- Dependent Variable (Y): How much they spent
    s.total as purchase_amount,
    
    -- Optional: Calculated variable mentioned in instructions (Ratio)
    ROUND(s.total / s.dwell_minutes, 2) as dollars_per_minute

FROM `bqproj-435911.Final_Project_2025.log_sales` s

WHERE 
    -- Logic: We can only correlate time vs money for users who have the app (geolocation data)
    s.customer_id IS NOT NULL 
    AND s.dwell_minutes IS NOT NULL 
    AND s.dwell_minutes > 1   -- Filter noise (0-1 min visits)
    AND s.dwell_minutes < 180 -- Filter extreme outliers (over 3 hours)
    AND s.total > 0           -- Filter returns or zero-value transactions

ORDER BY s.dwell_minutes ASC;

-- 5.
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




