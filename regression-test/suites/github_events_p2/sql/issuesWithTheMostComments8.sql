SELECT
    concat('https://github.com/', repo_name, '/commit/', commit_id) AS URL,
    count() AS comments,
    count(distinct actor_login) AS authors
FROM github_events
WHERE (event_type = 'CommitCommentEvent') AND commit_id != ""
GROUP BY
    repo_name,
    commit_id
HAVING authors >= 10
ORDER BY count() DESC, URL, authors
LIMIT 50
