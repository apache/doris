SELECT
    repo_name,
    sum(num_star) AS num_stars,
    sum(num_comment) AS num_comments
FROM
(
    SELECT
        cast(v:repo.name as string) as repo_name,
        CASE WHEN cast(v:type as string) = 'WatchEvent' THEN 1 ELSE 0 END AS num_star,
        CASE WHEN lower(cast(v:payload.comment.body as string)) LIKE '%apache%' THEN 1 ELSE 0 END AS num_comment
    FROM github_events
    WHERE (lower(cast(v:payload.comment.body as string)) LIKE '%apache%') OR (cast(v:type as string) = 'WatchEvent')
) t
GROUP BY repo_name 
HAVING num_comments > 0
ORDER BY num_stars DESC,num_comments DESC, repo_name ASC
LIMIT 50
