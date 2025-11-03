SELECT
    repo_name,
    comments,
    issues,
    round(comments / issues, 2) AS ratio
FROM
(
    SELECT
        repo_name,
        count() AS comments,
        count(distinct number) AS issues
    FROM github_events
    WHERE event_type = 'IssueCommentEvent'
    GROUP BY repo_name
) t
ORDER BY comments DESC
LIMIT 50
