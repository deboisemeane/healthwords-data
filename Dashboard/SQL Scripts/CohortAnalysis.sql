WITH categorized_events AS (
  SELECT
    user_pseudo_id,
    TIMESTAMP_MICROS(event_timestamp) AS event_ts,
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS event_date,
    CASE 
      WHEN event_name = 'page_view' THEN
        CASE 
          WHEN REGEXP_EXTRACT(
                 (SELECT value.string_value 
                  FROM UNNEST(event_params) 
                  WHERE key = 'page_location'),
                 r'^https://www\.healthwords\.ai[^?]+'
               ) = 'https://www.healthwords.ai/' THEN 'Home'
          WHEN (SELECT value.string_value 
                FROM UNNEST(event_params) 
                WHERE key = 'page_location') LIKE '%/explore%' THEN 'Explore'
          WHEN (SELECT value.string_value 
                FROM UNNEST(event_params) 
                WHERE key = 'page_location') LIKE '%/shop%' THEN 'Shop'
          WHEN (SELECT value.string_value 
                FROM UNNEST(event_params) 
                WHERE key = 'page_location') LIKE '%/chat%' THEN 'Chat'
          ELSE 'Other'
        END
      ELSE event_name
    END AS event_category
  FROM `caidr-2.analytics_228783167.events_*`
  WHERE _TABLE_SUFFIX BETWEEN @DS_START_DATE AND @DS_END_DATE
),
first_event AS (
  -- Identify each user’s first event date where the event category matches @FirstEvent
  SELECT 
    user_pseudo_id,
    MIN(event_date) AS first_event_date
  FROM categorized_events
  WHERE event_category = @FirstEvent
  GROUP BY user_pseudo_id
),
return_event AS (
  -- Identify return events where the event category matches @ReturnEvent
  SELECT 
    user_pseudo_id,
    event_date AS return_event_date
  FROM categorized_events
  WHERE event_category = @ReturnEvent
),
retained_users AS (
  -- Join first_event and return_event ensuring the return event occurs on or after the first event date
  SELECT 
    f.user_pseudo_id,
    f.first_event_date,
    r.return_event_date
  FROM first_event f
  LEFT JOIN return_event r
    ON f.user_pseudo_id = r.user_pseudo_id
   AND r.return_event_date >= f.first_event_date
),
cumulative_retention AS (
  -- Count users returning exactly on each day (after the first event)
  SELECT 
    first_event_date,
    return_event_date,
    COUNT(DISTINCT user_pseudo_id) AS users_returning_that_day
  FROM retained_users
  WHERE return_event_date IS NOT NULL
  GROUP BY first_event_date, return_event_date
),
final_retention AS (
  -- Calculate the cumulative retention (which should never increase over time)
  SELECT 
    first_event_date,
    return_event_date,
    SUM(users_returning_that_day) OVER (
      PARTITION BY first_event_date 
      ORDER BY return_event_date 
      ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
    ) AS cumulative_retained_users
  FROM cumulative_retention
),
all_cohorts AS (
  -- Get all unique cohort start dates
  SELECT DISTINCT first_event_date
  FROM first_event
),
date_series AS (
  -- For each cohort, generate a series of dates from the first event date until the end date
  SELECT
    f.first_event_date,
    d AS return_event_date
  FROM all_cohorts f,
       UNNEST(GENERATE_DATE_ARRAY(f.first_event_date, PARSE_DATE('%Y%m%d', @DS_END_DATE))) AS d
)
SELECT
  ds.first_event_date,
  ds.return_event_date,
  -- Use LAST_VALUE to carry forward the last non-null cumulative value so that days with no new returns
  -- still show the previous total
  COALESCE(fr.cumulative_retained_users,
    LAST_VALUE(fr.cumulative_retained_users IGNORE NULLS) OVER (
      PARTITION BY ds.first_event_date 
      ORDER BY ds.return_event_date 
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
  ) AS cumulative_retained_users
FROM date_series ds
LEFT JOIN final_retention fr
  ON ds.first_event_date = fr.first_event_date
 AND ds.return_event_date = fr.return_event_date
ORDER BY ds.first_event_date, ds.return_event_date;
