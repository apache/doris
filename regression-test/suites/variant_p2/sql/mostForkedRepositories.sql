SELECT cast(repo["name"] as string), count() AS forks FROM github_events WHERE type = 'ForkEvent' GROUP BY cast(repo["name"] as string) ORDER BY forks DESC, 1 LIMIT 50
