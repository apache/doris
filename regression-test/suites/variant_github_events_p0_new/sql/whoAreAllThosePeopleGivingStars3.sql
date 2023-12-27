SELECT
    cast(v["repo"]["name"] as string) as repo_name,
    count() AS stars
FROM github_events
WHERE (cast(v["type"] as string) = 'WatchEvent') AND (cast(v["repo"]["name"] as string) IN
(
    SELECT cast(v["repo"]["name"] as string)
    FROM github_events
    WHERE (cast(v["type"] as string) = 'WatchEvent') AND (cast(v["actor"]["login"] as string) = 'cliffordfajardo')
))
GROUP BY repo_name 
ORDER BY stars DESC, repo_name
LIMIT 50
