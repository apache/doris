SELECT
    cast(v:repo.name as string),
    sum(num_star) AS num_stars,
    sum(num_comment) AS num_comments
FROM
(
    SELECT
        cast(v:repo.name as string),
        CASE WHEN cast(v:type as string) = 'WatchEvent' THEN 1 ELSE 0 END AS num_star,
        CASE WHEN lower(body) LIKE '%clickhouse%' THEN 1 ELSE 0 END AS num_comment
    FROM github_events
    WHERE (lower(body) LIKE '%clickhouse%') OR (cast(v:type as string) = 'WatchEvent')
) t
GROUP BY cast(v:repo.name as string)
HAVING num_comments > 0
ORDER BY num_stars DESC,num_comments DESC,cast(v:repo.name as string) ASC
LIMIT 50
