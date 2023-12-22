SELECT count(), cast(v["repo"]["name"] as string) as repo_name FROM github_events WHERE cast(v["type"] as string) = 'WatchEvent' GROUP BY repo_name ORDER BY length(repo_name) DESC, repo_name LIMIT 50
