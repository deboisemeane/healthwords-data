
#Find sequential events with dynamic event filtering
WITH categorized_events AS (
  SELECT 
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    event_name,
    REGEXP_EXTRACT(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
      r'^https://www\.healthwords\.ai[^?]+'
    ) AS page_location
  FROM 
    `caidr-2.analytics_228783167.events_*` 
  WHERE 
    _TABLE_SUFFIX BETWEEN @DS_START_DATE AND @DS_END_DATE
    AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') IS NOT NULL
    AND event_name IN (
      'page_view',
      'signUp',
      'basket_view',
      'add_to_basket',
      'add_to_cart',
      'checkout_view',
      'purchase',
      'marketplace_get_started',
      'marketplace_select_specialist',
      'marketplace_payment_completed',
      'Product_Click_AI_Conversation',
      'Article_Click_AI_Conversation'
    )
),

prepared_events AS (
  SELECT 
    ga_session_id,
    event_timestamp,
    CASE 
      WHEN event_name = 'page_view' THEN 
        CASE 
          WHEN REGEXP_EXTRACT(page_location, r'^https://www\.healthwords\.ai[^?]+') = 'https://www.healthwords.ai/' THEN 'Home'
          WHEN page_location LIKE '%/explore%' THEN 'Explore'
          WHEN page_location LIKE '%/shop%' THEN 'Shop'
          WHEN page_location LIKE '%/chat%' THEN 'Chat'
          ELSE 'Other'
        END
      ELSE event_name
    END AS event_name
  FROM categorized_events
  WHERE 
    -- Early filtering of selected events
    (ARRAY_LENGTH(@SelectedEvents) = 0 OR
    CASE 
      WHEN event_name = 'page_view' THEN 
        CASE 
          WHEN REGEXP_EXTRACT(page_location, r'^https://www\.healthwords\.ai[^?]+') = 'https://www.healthwords.ai/' THEN 'Home'
          WHEN page_location LIKE '%/explore%' THEN 'Explore'
          WHEN page_location LIKE '%/shop%' THEN 'Shop'
          WHEN page_location LIKE '%/chat%' THEN 'Chat'
          ELSE 'Other'
        END
      ELSE event_name
    END IN UNNEST(@SelectedEvents))
),

event_sequence AS (
  SELECT 
    ga_session_id,
    event_timestamp,
    event_name,
    LAG(event_name) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp) AS prev_event
  FROM prepared_events
),

deduplicated_sequence AS (
  SELECT 
    ga_session_id,
    event_timestamp,
    event_name
  FROM event_sequence
  WHERE (event_name != prev_event OR prev_event IS NULL)
),

base AS (
  SELECT 
    ga_session_id,
    event_timestamp,
    ROW_NUMBER() OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC) AS rk,
    event_name,
    LEAD(event_name) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC) AS next_event
  FROM deduplicated_sequence
),

base2 AS (
  SELECT 
    *, 
    CONCAT(rk, '_', event_name) AS source_event,
    CONCAT(rk+1, '_', next_event) AS destination_event
  FROM base
)

SELECT source_event, destination_event, COUNT(DISTINCT ga_session_id) AS sessions
FROM base2
WHERE next_event IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2;