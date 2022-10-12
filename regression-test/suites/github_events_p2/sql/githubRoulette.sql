SELECT repo_name FROM github_events WHERE event_type = 'WatchEvent' ORDER BY created_at LIMIT 50
