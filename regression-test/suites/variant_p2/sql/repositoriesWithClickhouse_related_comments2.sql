SELECT
    repo_name,
    sum(num_star) AS num_stars,
    sum(num_comment) AS num_comments
FROM
(
    SELECT
        cast(repo:name as string) as repo_name,
        CASE WHEN type = 'WatchEvent' THEN 1 ELSE 0 END AS num_star,
        CASE WHEN lower(cast(payload:comment.body as string)) LIKE '%apache%' THEN 1 ELSE 0 END AS num_comment
    FROM github_events
    WHERE (lower(cast(payload:comment.body as string)) LIKE '%apache%') OR (type = 'WatchEvent')
) t
GROUP BY repo_name 
HAVING num_comments > 0
ORDER BY num_stars DESC,num_comments DESC, repo_name ASC
LIMIT 50
