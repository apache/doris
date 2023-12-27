SELECT
    repo_name,
    comments,
    issues,
    round(comments / issues, 2) AS ratio
FROM
(
    SELECT
        cast(v:repo.name as string) as repo_name,
        count() AS comments,
        count(distinct cast(v:payload.issue.`number` as int)) AS issues
    FROM github_events
    WHERE cast(v:type as string) = 'IssueCommentEvent'
    GROUP BY cast(v:repo.name as string)
) t
ORDER BY comments DESC, 1, 3, 4
LIMIT 50
