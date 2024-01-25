SELECT cast(v:actor.login as string), count() AS stars FROM github_events WHERE cast(v:type as string) = 'WatchEvent' GROUP BY cast(v:actor.login as string) ORDER BY stars DESC, 1 LIMIT 50
