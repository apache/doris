SELECT
  repo_name,
  count() AS stars
FROM github_events
WHERE (event_type = 'WatchEvent') AND (actor_login IN
(
    SELECT actor_login
    FROM github_events
    WHERE (event_type = 'WatchEvent') AND (repo_name IN ('ClickHouse/ClickHouse', 'yandex/ClickHouse'))
)) AND (repo_name NOT IN ('ClickHouse/ClickHouse', 'yandex/ClickHouse'))
GROUP BY repo_name
ORDER BY stars DESC
LIMIT 50
