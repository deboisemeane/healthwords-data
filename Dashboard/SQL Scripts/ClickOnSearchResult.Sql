
with search_clicks AS(
SELECT 
    user_pseudo_id,
    event_name,
    TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'internal_search_term') AS internal_search_term,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'event_action') AS event_action,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
	(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'event_label') AS event_label,
FROM 
    `caidr-2.analytics_228783167.events_*` 
WHERE 
    event_name = 'click_on_search_result' AND
    _TABLE_SUFFIX BETWEEN @DS_START_DATE AND @DS_END_DATE
GROUP BY 
    user_pseudo_id,
    event_name,
    event_timestamp,
    internal_search_term,
    event_action,
    page_location,
    event_label
)

-- Convert page location to either /search or /chat

SELECT 
    user_pseudo_id,
    event_name,
    event_timestamp,
    internal_search_term,
    event_action,
    page_location,
    REGEXP_EXTRACT(event_label, r'^(.*)!') AS event_label,
    REGEXP_REPLACE(
    REGEXP_EXTRACT(event_label, r'^(.*)!'),  
    r'/condition/|/symptom/|/treatment/', '/article/') AS adjusted_event_label -- Adjusted to be like /article to match Bang's content list
    CASE
        WHEN page_location LIKE '%/?search%' THEN 'Search'
        WHEN page_location LIKE '%/chat%' THEN 'Chat'
        WHEN page_location LIKE '%/shop%' THEN 'Shop'
        WHEN page_location LIKE '%/explore%' THEN 'Explore'
        ELSE 'Other'
    END AS click_source
FROM search_clicks
GROUP BY 
    user_pseudo_id,
    event_name,
    event_timestamp,
    internal_search_term,
    event_action,
    page_location,
    event_label,
    click_source


