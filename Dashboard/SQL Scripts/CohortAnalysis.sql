SELECT
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
    TIMESTAMP_MICROS(event_timestamp) AS session_timestamp,
    MIN(TIMESTAMP_MICROS(event_timestamp)) OVER (PARTITION BY user_pseudo_id) AS first_session_timestamp
FROM
    `caidr-2021.analytics_243522242.events_*`
WHERE
    event_name = 'session_start' AND
    _TABLE_SUFFIX BETWEEN @DS_START_DATE AND @DS_END_DATE
    