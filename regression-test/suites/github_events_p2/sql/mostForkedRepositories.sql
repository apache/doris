SELECT repo_name, count() AS forks FROM github_events WHERE event_type = 'ForkEvent' GROUP BY repo_name ORDER BY forks DESC LIMIT 50
