SELECT cast(v["repo"]["name"] as string) as repo_name, count() FROM github_events WHERE cast(v["type"] as string) = 'IssueCommentEvent' GROUP BY repo_name ORDER BY count() DESC, 1 LIMIT 50
