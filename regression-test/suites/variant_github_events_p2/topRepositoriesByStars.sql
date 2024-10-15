SELECT cast(v["repo"]["name"] as string), count() AS stars FROM github_events WHERE cast(v["type"] as string) = 'WatchEvent' GROUP BY cast(v["repo"]["name"] as string) ORDER BY stars DESC, 1 LIMIT 50
