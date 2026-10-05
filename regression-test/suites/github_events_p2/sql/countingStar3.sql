SELECT count() FROM github_events WHERE event_type = 'WatchEvent' AND repo_name IN ('ClickHouse/ClickHouse', 'yandex/ClickHouse') GROUP BY action
