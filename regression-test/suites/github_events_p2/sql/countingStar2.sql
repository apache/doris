SELECT action, count() FROM github_events WHERE event_type = 'WatchEvent' GROUP BY action
