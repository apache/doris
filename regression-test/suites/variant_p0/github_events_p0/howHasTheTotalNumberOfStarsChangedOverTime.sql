SELECT year(cast(v:created_at as datetime)) AS year, count() AS stars FROM github_events WHERE cast(v:type as string) = 'WatchEvent' GROUP BY year ORDER BY year

