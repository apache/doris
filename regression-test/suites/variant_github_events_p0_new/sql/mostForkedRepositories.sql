SELECT cast(v["repo"]["name"] as string) as repo_name, count() AS forks FROM github_events WHERE cast(v["type"] as string) = 'ForkEvent' GROUP BY repo_name ORDER BY forks DESC, 1 LIMIT 50
