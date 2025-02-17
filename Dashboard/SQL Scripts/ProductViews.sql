WITH page_views AS (
    SELECT 
        user_pseudo_id,
        event_name,
        TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') AS page_title,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        LAG((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location')) OVER (PARTITION BY user_pseudo_id, (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') ORDER BY TIMESTAMP_MICROS(event_timestamp)) AS previous_page_location,
  		LAG((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title')) OVER (PARTITION BY user_pseudo_id, (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') ORDER BY TIMESTAMP_MICROS(event_timestamp)) AS previous_page_title
    FROM 
        `caidr-2.analytics_228783167.events_*` 
    WHERE 
        event_name = 'page_view' AND
        _TABLE_SUFFIX BETWEEN @DS_START_DATE AND @DS_END_DATE
),
categorized_previous_page AS (
    SELECT
        user_pseudo_id,
        event_name,
        event_timestamp,
        page_location,
        page_title,
        ga_session_id,
        previous_page_location,
  		previous_page_title,
        CASE
            WHEN previous_page_location LIKE '%/chat%' THEN 'Chat'
            WHEN previous_page_location LIKE '%/shop/products%' THEN 'Shop Products'
            WHEN previous_page_location LIKE '%/shop%' THEN 'Shop Home'
            WHEN previous_page_location LIKE '%/shop/search%' THEN 'Shop Search'
            WHEN previous_page_location LIKE '%/explore%' THEN 'Explore'
            ELSE 'Other'
        END AS previous_page_category
    FROM page_views
    WHERE page_location LIKE '%/products%' AND (page_location IS DISTINCT FROM previous_page_location)
)
SELECT * FROM categorized_previous_page;