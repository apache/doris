SELECT
    cast(v:repo.name as string),
    sum(fork) AS forks,
    sum(star) AS stars,
    round(sum(star) / sum(fork), 3) AS ratio
FROM
(
    SELECT
        cast(v:repo.name as string),
        CASE WHEN cast(v:type as string) = 'ForkEvent' THEN 1 ELSE 0 END AS fork,
        CASE WHEN cast(v:type as string) = 'WatchEvent' THEN 1 ELSE 0 END AS star
    FROM github_events
    WHERE cast(v:type as string) IN ('ForkEvent', 'WatchEvent')
) t
GROUP BY cast(v:repo.name as string)
HAVING (stars > 100) AND (forks > 100)
ORDER BY ratio DESC, cast(v:repo.name as string)
LIMIT 50
