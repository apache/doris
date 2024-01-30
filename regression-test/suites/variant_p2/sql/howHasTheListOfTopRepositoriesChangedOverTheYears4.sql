SELECT cast(repo["name"] as string), count() AS stars FROM github_events WHERE type = 'WatchEvent' AND year(created_at) = '2015' GROUP BY cast(repo["name"] as string) ORDER BY stars DESC, 1 LIMIT 50
