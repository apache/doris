SELECT repo_name, count() FROM github_events WHERE event_type = 'WatchEvent' AND repo_name LIKE '%_/_%' GROUP BY repo_name ORDER BY length(repo_name) ASC, repo_name LIMIT 50
