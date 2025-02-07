-- Set Date Range
DECLARE start_date STRING DEFAULT '20250120';
DECLARE end_date STRING DEFAULT '20250130';

WITH 
-- Identify users who have triggered any event before the current period
previous_events AS (
    SELECT DISTINCT user_pseudo_id
    FROM `caidr-2.analytics_228783167.events_*`
    WHERE _TABLE_SUFFIX < start_date  -- Before current analysis period
),

-- Find users who signed up within the date range
user_signup AS ( 
    SELECT
        user_pseudo_id,
        MIN(event_timestamp) AS signup_time
    FROM `caidr-2.analytics_228783167.events_*`
    WHERE event_name = 'signUp' 
        AND _TABLE_SUFFIX BETWEEN start_date AND end_date
    GROUP BY user_pseudo_id
),

-- Find the start and end of each of the user's sessions (Corrected for pre-signup)
user_sessions AS (
    SELECT
        e.user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        MIN(event_timestamp) AS session_start,
        MAX(event_timestamp) AS session_end
    FROM `caidr-2.analytics_228783167.events_*` e
    LEFT JOIN user_signup su ON e.user_pseudo_id = su.user_pseudo_id
    WHERE e._TABLE_SUFFIX BETWEEN start_date AND end_date
      AND (su.signup_time IS NULL OR e.event_timestamp <= su.signup_time)
    GROUP BY user_pseudo_id, ga_session_id
),

-- Find the user's traffic source of their first session
first_traffic AS (
    SELECT 
        user_pseudo_id,
        FIRST_VALUE(traffic_source.source) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS first_traffic_source,
        FIRST_VALUE(traffic_source.medium) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS first_traffic_medium
    FROM `caidr-2.analytics_228783167.events_*`
    WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
),

-- Calculate pageviews on explore and shop pages (Corrected for pre-signup)
explore_shop_pageviews AS (
  SELECT
    e.user_pseudo_id,
    COUNTIF(e.event_name = 'page_view' AND (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'page_location') LIKE '%/explore/%' AND (su.signup_time IS NULL OR e.event_timestamp <= su.signup_time)) AS explore_page_views,
    COUNTIF(e.event_name = 'page_view' AND (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'page_location') LIKE '%/shop/%' AND (su.signup_time IS NULL OR e.event_timestamp <= su.signup_time)) AS shop_page_views
  FROM
    `caidr-2.analytics_228783167.events_*` e
  LEFT JOIN user_signup su ON e.user_pseudo_id = su.user_pseudo_id
  WHERE e._TABLE_SUFFIX BETWEEN start_date AND end_date
  GROUP BY e.user_pseudo_id
),

-- Calculate engagement time using engagement_time_msec (Corrected for pre-signup)
user_engagement AS (
  SELECT
    e.user_pseudo_id,
    SUM(CAST(ep.value.int_value AS INT64)) AS total_engagement_msec  -- Sum engagement time
  FROM
    `caidr-2.analytics_228783167.events_*` e,
    UNNEST(e.event_params) AS ep
  LEFT JOIN user_signup su ON e.user_pseudo_id = su.user_pseudo_id
  WHERE e._TABLE_SUFFIX BETWEEN start_date AND end_date
    AND ep.key = 'engagement_time_msec'  -- Filter for engagement time parameter
    AND (su.signup_time IS NULL OR e.event_timestamp <= su.signup_time) -- Pre-signup or all for non-signups
  GROUP BY e.user_pseudo_id
),

-- Find the user's first page_view page_location as the landing page.
first_page_location AS (
    SELECT
        user_pseudo_id,
        FIRST_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location')) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS first_page_location
    FROM `caidr-2.analytics_228783167.events_*`
    WHERE event_name = 'page_view'
        AND _TABLE_SUFFIX BETWEEN start_date AND end_date
)

-- Select signed-up label and relevant features for classification (Final Query)
SELECT
    e.user_pseudo_id,
    -- Target variable: whether the user signed up during the date range
    MAX(IF(su.signup_time IS NOT NULL, 1, 0)) AS signed_up,
    -- Engagement features (pre-signup)
    COUNT(DISTINCT s.ga_session_id) AS total_sessions,
    COALESCE(CAST(ue.total_engagement_msec AS INT64), 0) / 1000 AS total_engagement_sec,  -- Use engagement time, convert ms to seconds
    -- Traffic source from first session (simplified)
    CASE
        WHEN f.first_traffic_medium = 'organic' THEN 'Organic Search'
        WHEN f.first_traffic_medium = 'cpc' THEN 'Paid Search'
        WHEN f.first_traffic_source = '(direct)' AND f.first_traffic_medium = '(none)' THEN 'Direct'
        WHEN f.first_traffic_medium = 'referral' THEN 'Referral'
        ELSE 'Other'
    END AS first_traffic_channel_group,
    -- Explore and Shop pageviews
    COALESCE(ep.explore_page_views, 0) AS explore_page_views,  -- Handle potential NULLs
    COALESCE(ep.shop_page_views, 0) AS shop_page_views,
    -- Landing page categorisation as Home, Explore, Shop, Chat, or Other
    CASE
        WHEN REGEXP_EXTRACT(p.first_page_location, r'^https://www\.healthwords\.ai[^?]+') = 'https://www.healthwords.ai/' THEN 'Home'
        WHEN p.first_page_location LIKE '%/explore%' THEN 'Explore'
        WHEN p.first_page_location LIKE '%/shop%' THEN 'Shop'
        WHEN p.first_page_location LIKE '%/chat%' THEN 'Chat'
        ELSE 'Other'
    END AS landing_page_category
FROM `caidr-2.analytics_228783167.events_*` e
LEFT JOIN user_signup su ON CAST(e.user_pseudo_id as STRING) = su.user_pseudo_id
LEFT JOIN user_sessions s ON CAST(e.user_pseudo_id as STRING) = s.user_pseudo_id 
LEFT JOIN first_traffic f ON CAST(e.user_pseudo_id as STRING) = f.user_pseudo_id
LEFT JOIN explore_shop_pageviews ep ON CAST(e.user_pseudo_id as STRING) = ep.user_pseudo_id
LEFT JOIN user_engagement ue ON CAST(e.user_pseudo_id as STRING) = ue.user_pseudo_id
LEFT JOIN first_page_location p ON CAST(e.user_pseudo_id as STRING) = p.user_pseudo_id
-- Exclude users who triggered any event before the analysis period
WHERE e.user_pseudo_id NOT IN (SELECT user_pseudo_id FROM previous_events)
    AND ((su.signup_time IS NOT NULL AND e.event_timestamp <= su.signup_time) OR (su.signup_time IS NULL)) 
    AND e._TABLE_SUFFIX BETWEEN start_date AND end_date
GROUP BY e.user_pseudo_id, first_traffic_channel_group, explore_page_views, shop_page_views, ue.total_engagement_msec, p.first_page_location; -- Group by all non-aggregated columns