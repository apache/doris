SELECT
    cast(v:repo.name as string),
    comments,
    issues,
    round(comments / issues, 2) AS ratio
FROM
(
    SELECT
        cast(v:repo.name as string),
        count() AS comments,
        count(distinct number) AS issues
    FROM github_events
    WHERE cast(v:type as string) = 'IssueCommentEvent'
    GROUP BY cast(v:repo.name as string)
) t
ORDER BY comments DESC
LIMIT 50
