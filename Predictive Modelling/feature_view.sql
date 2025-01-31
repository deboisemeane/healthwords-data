-- Set Date Range
DECLARE start_date STRING DEFAULT '20250120';
DECLARE end_date STRING DEFAULT '20250130';

WITH 
-- Find the users who have signed up and the time they signed up.
user_signup AS ( 
    SELECT
        user_pseudo_id,
        MIN(event_timestamp) AS signup_time
    FROM `caidr-2.analytics_228783167.events_*`
    WHERE event_name = 'signUp' 
        AND _TABLE_SUFFIX BETWEEN start_date AND end_date
    GROUP BY user_pseudo_id
),

-- Find the start and end of the user's sessions
user_sessions AS (
    SELECT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        MIN(event_timestamp) AS session_start,
        MAX(event_timestamp) AS session_end
    FROM `caidr-2.analytics_228783167.events_*`
    WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
    GROUP BY user_pseudo_id, ga_session_id
),

-- Find the user's traffic source of their first session
first_traffic AS (
    SELECT 
        user_pseudo_id, 
        FIRST_VALUE(traffic_source.source) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS first_traffic_source
    FROM `caidr-2.analytics_228783167.events_*`
    WHERE _TABLE_SUFFIX BETWEEN start_date AND end_date
)


-- Select signed-up label and relevant features for classification
SELECT
    e.user_pseudo_id,  -- Explicitly specify table alias to resolve ambiguity
    -- Target variable: whether the user signed up or not
    MAX(IF(su.signup_time IS NOT NULL, 1, 0)) AS signed_up,
    -- Engagement features (pre-signup)
    COUNT(DISTINCT s.ga_session_id) AS total_sessions,
    SUM(TIMESTAMP_DIFF(TIMESTAMP_MICROS(s.session_end), TIMESTAMP_MICROS(s.session_start), SECOND)) AS total_engagement_sec,
    -- Traffic source from first session
    f.first_traffic_source AS first_traffic_source
FROM `caidr-2.analytics_228783167.events_*` e
LEFT JOIN user_signup su ON e.user_pseudo_id = su.user_pseudo_id
LEFT JOIN user_sessions s ON e.user_pseudo_id = s.user_pseudo_id
LEFT JOIN first_traffic f ON e.user_pseudo_id = f.user_pseudo_id
WHERE
    ((su.signup_time IS NOT NULL AND e.event_timestamp <= su.signup_time)
    OR (su.signup_time IS NULL)) 
    AND _TABLE_SUFFIX BETWEEN start_date AND end_date
GROUP BY e.user_pseudo_id, f.first_traffic_source;
