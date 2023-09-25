SELECT
    repo,
    year,
    cnt
FROM
(
    SELECT
        row_number() OVER (PARTITION BY year ORDER BY cnt DESC) AS r,
        repo,
        year,
        cnt
    FROM
    (
        SELECT
        lower(cast(v:repo.name as string)) AS repo,
        year(cast(v:created_at as datetime)) AS year,
        count() AS cnt
        FROM github_events
        WHERE (cast(v:type as string) = 'WatchEvent') AND (year(cast(v:created_at as datetime)) >= 2015)
        GROUP BY
            repo,
            year
    ) t
) t2
WHERE r <= 10
ORDER BY
    year ASC,
    cnt DESC, repo
