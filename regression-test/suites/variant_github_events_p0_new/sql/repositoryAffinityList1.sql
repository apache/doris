SELECT /*+SET_VAR(enable_fallback_to_original_planner=false) */
  cast(v["repo"]["name"] as string) as repo_name,
  count() AS stars
FROM github_events
WHERE (cast(v["type"] as string) = 'WatchEvent') AND (cast(v["actor"]["login"] as string) IN
(
    SELECT cast(v["actor"]["login"] as string)
    FROM github_events
    WHERE (cast(v["type"] as string) = 'WatchEvent') AND (cast(v["repo"]["name"] as string) IN ('apache/spark', 'prakhar1989/awesome-courses'))
)) AND (cast(v["repo"]["name"] as string) NOT IN ('ClickHouse/ClickHouse', 'yandex/ClickHouse'))
GROUP BY repo_name
ORDER BY stars DESC, repo_name 
LIMIT 50
