SELECT dayofweek(cast(v:created_at as datetime)) AS day, count() AS stars FROM github_events WHERE cast(v:type as string) = 'WatchEvent' GROUP BY day ORDER BY day
