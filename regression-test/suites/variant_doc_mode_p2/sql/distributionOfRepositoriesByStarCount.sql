SELECT
    pow(10, floor(log10(c))) AS stars,
    count(distinct k)
FROM
(
    SELECT
        cast(v["repo"]["name"] as string) as k,
        count() AS c
    FROM github_events
    WHERE cast(v["type"] as string) = 'WatchEvent'
    GROUP BY cast(v["repo"]["name"] as string)
) t
GROUP BY stars
ORDER BY stars ASC
