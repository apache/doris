SELECT
    lower(split_part(cast(v:repo.name as string), '/', 1)) AS org,
    count(distinct cast(v:repo.name as string)) AS repos
FROM
(
    SELECT cast(v:repo.name as string)
    FROM github_events
    WHERE cast(v:type as string) = 'WatchEvent'
    GROUP BY cast(v:repo.name as string)
    HAVING count() >= 10
) t
GROUP BY org
ORDER BY repos DESC, org ASC
LIMIT 50
