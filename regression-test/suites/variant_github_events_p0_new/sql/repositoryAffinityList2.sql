SELECT
  repo_name,
  total_stars,
  round(spark_stars / total_stars, 2) AS ratio
FROM
(
    SELECT
        cast(v["repo"]["name"] as string) as repo_name,
        count(distinct cast(v["actor"]["login"] as string)) AS total_stars
    FROM github_events
    WHERE (cast(v["type"] as string) = 'WatchEvent') AND (cast(v["repo"]["name"] as string) NOT IN ('apache/spark'))
    GROUP BY repo_name
    HAVING total_stars >= 10
) t1
JOIN
(
    SELECT
        count(distinct cast(v["actor"]["login"] as string)) AS spark_stars
    FROM github_events
    WHERE (cast(v["type"] as string) = 'WatchEvent') AND (cast(v["repo"]["name"] as string) IN ('apache/spark'))
) t2
ORDER BY ratio DESC, repo_name
LIMIT 50
