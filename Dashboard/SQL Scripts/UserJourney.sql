-- Extract events with relevant parameters
WITH categorized_events AS (
  SELECT 
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') AS ga_session_number,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    event_name,
    REGEXP_EXTRACT(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
      r'^https://www\.healthwords\.ai[^?]+'
    ) AS page_location,
    -- Extract first session traffic parameters
  FROM 
    `caidr-2.analytics_228783167.events_*` 
  WHERE 
    _TABLE_SUFFIX BETWEEN @DS_START_DATE AND @DS_END_DATE
    AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') IS NOT NULL
    AND event_name IN (
      'session_start',
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
      'Article_Click_AI_Conversation',
      'click_on_search_result'
    )
),
-- Get each user’s earliest signup session (if any)
user_signups AS (
  SELECT
    user_pseudo_id,
    MIN(ga_session_number) AS sign_up_session_number
  FROM categorized_events
  WHERE event_name = 'signUp'
  GROUP BY user_pseudo_id
),
-- Determine first session channel group for each user
first_traffic_1 AS (
    SELECT 
        user_pseudo_id,
        FIRST_VALUE(traffic_source.source) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS 		first_traffic_source,
        FIRST_VALUE(traffic_source.medium) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS first_traffic_medium
    FROM `caidr-2.analytics_228783167.events_*`
    WHERE _TABLE_SUFFIX BETWEEN @DS_START_DATE AND @DS_END_DATE
),

first_traffic_2 AS (
  SELECT
    user_pseudo_id,
    CASE
        WHEN first_traffic_medium = 'organic' THEN 'Organic Search'
        WHEN first_traffic_medium = 'cpc' THEN 'Paid Search'
        WHEN first_traffic_source = '(direct)' AND first_traffic_medium = '(none)' THEN 'Direct'
        WHEN first_traffic_medium = 'referral' THEN 'Referral'
        ELSE 'Other'
    END AS first_traffic_channel_group,
  FROM first_traffic_1

),
-- Filter events based on signup status, channel group and session number
filtered_events AS (
  SELECT 
    ce.*
  FROM categorized_events ce
  LEFT JOIN user_signups us ON ce.user_pseudo_id = us.user_pseudo_id
  LEFT JOIN first_traffic_2 fs ON ce.user_pseudo_id = fs.user_pseudo_id
  WHERE
    (
    -- For signed-up users, include all their sessions (not just after signup)
    (@SignUpFilter = 'signedUp' AND us.sign_up_session_number IS NOT NULL)
    OR
    -- For non-signed-up users, exclude any user who has signed up
    (@SignUpFilter = 'notSignedUp' AND us.user_pseudo_id IS NULL)
    OR
    -- If no signup filter is specified, include all sessions
    (@SignUpFilter IS NULL OR @SignUpFilter = 'All')
	)
    AND
    -- Filter by first session channel group if provided
    ((@ChannelGroup IS NULL)
  	OR fs.first_traffic_channel_group IN UNNEST(@ChannelGroup))

    AND
    -- Filter by session number if provided
    ((@SessionNumberLower IS NULL OR ce.ga_session_number >= CAST(@SessionNumberLower AS INT64)) 
	AND (@SessionNumberUpper IS NULL OR ce.ga_session_number <= CAST(@SessionNumberUpper AS INT64)))
),
-- Prepare events for journey mapping (including renaming of page_view)
prepared_events AS (
  SELECT 
    ga_session_id,
    event_timestamp,
    CASE 
      WHEN event_name = 'page_view' THEN 
        CASE 
          WHEN REGEXP_EXTRACT(page_location, r'^https://www\.healthwords\.ai[^?]+') = 'https://www.healthwords.ai/' THEN 'Home'
          WHEN REGEXP_EXTRACT(page_location, r'^https://www\.healthwords\.ai[^?]+') = 'https://www.healthwords.ai/speak-to-a-doctor' THEN 'Speak-to-a-Doctor'
          WHEN page_location LIKE '%/explore%' THEN 'Explore'
          WHEN page_location LIKE '%/shop%' THEN 'Shop'
          WHEN page_location LIKE '%/chat%' THEN 'Chat'
          ELSE 'Other'
        END
      ELSE event_name
    END AS event_name
  FROM filtered_events
  WHERE 
    -- Early filtering if a list of selected events is provided
    (ARRAY_LENGTH(@SelectedEvents) = 0 OR
      CASE 
        WHEN event_name = 'page_view' THEN 
          CASE 
            WHEN REGEXP_EXTRACT(page_location, r'^https://www\.healthwords\.ai[^?]+') = 'https://www.healthwords.ai/' THEN 'Home'
            WHEN REGEXP_EXTRACT(page_location, r'^https://www\.healthwords\.ai[^?]+') = 'https://www.healthwords.ai/speak-to-a-doctor' THEN 'Speak-to-a-Doctor'
            WHEN page_location LIKE '%/explore%' THEN 'Explore'
            WHEN page_location LIKE '%/shop%' THEN 'Shop'
            WHEN page_location LIKE '%/chat%' THEN 'Chat'
            ELSE 'Other'
          END
        ELSE event_name
      END IN UNNEST(@SelectedEvents)
    )
),
-- Build event sequence per session
event_sequence AS (
  SELECT 
    ga_session_id,
    event_timestamp,
    event_name,
    LAG(event_name) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp) AS prev_event
  FROM prepared_events
),
-- Remove duplicate consecutive events
deduplicated_sequence AS (
  SELECT 
    ga_session_id,
    event_timestamp,
    event_name
  FROM event_sequence
  WHERE (event_name != prev_event OR prev_event IS NULL)
),
-- Assign row numbers to order events in each session and get the following event
base AS (
  SELECT 
    ga_session_id,
    event_timestamp,
    ROW_NUMBER() OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC, CASE WHEN event_name = 'session_start' THEN 0 ELSE 1 END) AS rk,
    event_name,
    LEAD(event_name) OVER (PARTITION BY ga_session_id ORDER BY event_timestamp ASC) AS next_event
  FROM deduplicated_sequence
),
-- Create source and destination labels for the sankey diagram
base2 AS (
  SELECT 
    *, 
    CONCAT(rk, '_', event_name) AS source_event,
    CONCAT(rk+1, '_', next_event) AS destination_event
  FROM base
)
-- Aggregate counts for the sankey diagram
SELECT 
  source_event, 
  destination_event, 
  COUNT(DISTINCT ga_session_id) AS sessions
FROM base2
WHERE next_event IS NOT NULL
GROUP BY source_event, destination_event
ORDER BY source_event, destination_event;
