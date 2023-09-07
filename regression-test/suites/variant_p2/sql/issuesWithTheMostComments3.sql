SELECT
    repo_name,
    comments,
    issues,
    cast(round(comments / issues, 0) as int) AS ratio
FROM
(
    SELECT
        cast(repo:name as string) as repo_name,
        count() AS comments,
        count(distinct cast(payload:issue.`number` as int)) AS issues
    FROM github_events
    WHERE type = 'IssueCommentEvent'
    GROUP BY cast(repo:name as string)
) t
ORDER BY comments DESC, 1, 3, 4
LIMIT 50
