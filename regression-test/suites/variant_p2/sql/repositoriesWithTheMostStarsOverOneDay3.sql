SELECT cast(repo:name as string) as repo_name, count() AS stars FROM github_events WHERE type = 'WatchEvent' GROUP BY cast(repo:name as string) ORDER BY stars DESC, repo_name LIMIT 50
