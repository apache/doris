SELECT cast(v["repo"]["name"] as string) as repo_name, count() AS stars FROM github_events WHERE cast(v["type"] as string) = 'WatchEvent' GROUP BY repo_name ORDER BY stars DESC, 1 LIMIT 50
