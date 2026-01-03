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


/*
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

-- Payment Method Distribution:
SELECT
  payment_method,
  COUNT(*) AS total,  -- Count the number of transactions for each payment method
  COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() AS percentage  -- Calculate the percentage of total transactions
FROM `bqproj-435911.Final_Project_2025.log_sales`
GROUP BY payment_method;


