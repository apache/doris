SELECT dayofweek(created_at) AS day, count() AS stars FROM github_events WHERE event_type = 'WatchEvent' GROUP BY day ORDER BY day
