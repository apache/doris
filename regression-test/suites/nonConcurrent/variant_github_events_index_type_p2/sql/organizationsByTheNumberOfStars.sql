SELECT
    lower(split_part(cast(v["repo"]["name"] as string), '/', 1)) AS org,
    count() AS stars
FROM github_events
WHERE cast(v["type"] as string) = 'WatchEvent'
GROUP BY org
ORDER BY stars DESC, 1
LIMIT 50
