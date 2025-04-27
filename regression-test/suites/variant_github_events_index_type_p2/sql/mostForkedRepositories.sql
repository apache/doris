SELECT cast(v["repo"]["name"] as string), count() AS forks FROM github_events WHERE cast(v["type"] as string) = 'ForkEvent' GROUP BY cast(v["repo"]["name"] as string) ORDER BY forks DESC, 1 LIMIT 50
