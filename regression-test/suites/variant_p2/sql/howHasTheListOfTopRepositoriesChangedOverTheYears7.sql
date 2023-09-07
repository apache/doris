SELECT
    repo_name,
    year,
    cnt
FROM
(
    SELECT
        row_number() OVER (PARTITION BY year ORDER BY cnt DESC) AS r,
        repo_name,
        year,
        cnt
    FROM
    (
        SELECT
        lower(cast(repo:name as string)) AS repo_name,
        year(created_at) AS year,
        count() AS cnt
        FROM github_events
        WHERE (type = 'WatchEvent') AND (year(created_at) >= 2015)
        GROUP BY
            repo_name,
            year
    ) t
) t2
WHERE r <= 10
ORDER BY
    year ASC,
    cnt DESC,
    repo_name
