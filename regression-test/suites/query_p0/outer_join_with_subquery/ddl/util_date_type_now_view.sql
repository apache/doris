CREATE VIEW `util_date_type_now_view` COMMENT 'VIEW' AS SELECT
    `date_type` AS `date_type`,
    'now' AS `flag_type`,
    CASE
    `date_type`
    WHEN 'week' THEN
    date_sub( concat( to_date ( now()), ' 00:00:00' ), INTERVAL dayofweek( now()) - 2 DAY )
    WHEN '7day' THEN
    date_sub( concat( to_date ( now()), ' 00:00:00' ), INTERVAL 6 DAY )
    WHEN '30day' THEN
    date_sub( concat( to_date ( now()), ' 00:00:00' ), INTERVAL 29 DAY )
    WHEN 'month' THEN
    date_sub( concat( to_date ( now()), ' 00:00:00' ), INTERVAL dayofmonth( now()) - 1 DAY )
    WHEN 'year' THEN
    date_sub( concat( to_date ( now()), ' 00:00:00' ), INTERVAL dayofyear( now()) - 1 DAY ) ELSE concat( to_date ( now()), ' 00:00:00' )
END AS `start_time`,
	now() AS `end_time`
FROM
	`util_date_type`;
