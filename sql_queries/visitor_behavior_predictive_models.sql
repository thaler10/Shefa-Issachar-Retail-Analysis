-- Regression Analysis: Purchase Amount vs. Dwell Time
SELECT 
    -- Independent Variable (X): Time spent in store
    s.dwell_minutes,
    -- Dependent Variable (Y): How much they spent
    s.total as purchase_amount,
    -- Calculated ratio
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
--------------------------------------------------------------
-- Unique Devices per Week (Trend Analysis)
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
--------------------------------------
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
-----------------------------------------------------------------
/*
App Performance Analysis (Capture Rate Verification)

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
---------------------------------------------------------

