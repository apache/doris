SELECT cast(v["actor"]["login"] as string) as actor_login, count() AS stars FROM github_events WHERE cast(v["type"] as string) = 'WatchEvent' GROUP BY actor_login ORDER BY stars DESC, 1 LIMIT 50
