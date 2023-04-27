SELECT
    repo_name,
    number,
    count() AS comments
FROM github_events
WHERE event_type = 'IssueCommentEvent' AND (action = 'created')
GROUP BY repo_name, number
ORDER BY comments DESC, number ASC
LIMIT 50
