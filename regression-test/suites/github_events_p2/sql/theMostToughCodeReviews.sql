SELECT
    concat('https://github.com/', repo_name, '/pull/', CAST(number AS STRING)) AS URL,
    count(distinct actor_login) AS authors
FROM github_events
WHERE (event_type = 'PullRequestReviewCommentEvent') AND (action = 'created')
GROUP BY
    repo_name,
    number
ORDER BY authors DESC, URL ASC
LIMIT 50
