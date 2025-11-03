SELECT
    concat('https://github.com/', cast(repo["name"] as string), '/commit/', cast(payload["commit_id"] as string)) URL,
    cast(payload["commit_id"] as string) AS commit_id,
    count() AS comments,
    count(distinct cast(actor["login"] as string)) AS authors
FROM github_events
WHERE (type = 'CommitCommentEvent') AND cast(payload["commit_id"] as string) != ""
GROUP BY
    cast(repo["name"] as string),
    commit_id 
HAVING authors >= 10
ORDER BY count() DESC, URL, authors
LIMIT 50
