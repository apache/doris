SELECT cast(actor["login"] as string), count() AS stars FROM github_events WHERE type = 'WatchEvent' GROUP BY cast(actor["login"] as string) ORDER BY stars DESC, 1 LIMIT 50
