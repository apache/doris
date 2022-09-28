SELECT year(created_at) AS year, count() AS stars FROM github_events WHERE event_type = 'WatchEvent' GROUP BY year ORDER BY year
