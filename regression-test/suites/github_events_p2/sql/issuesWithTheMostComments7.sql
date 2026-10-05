SELECT
    repo_name,
    count() AS comments,
    count(distinct actor_login) AS authors
FROM github_events
WHERE event_type = 'CommitCommentEvent'
GROUP BY repo_name
ORDER BY count() DESC
LIMIT 50
