SELECT cast(repo["name"] as string), count() FROM github_events WHERE type = 'IssueCommentEvent' GROUP BY cast(repo["name"] as string) ORDER BY count() DESC, 1 LIMIT 50
