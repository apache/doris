SELECT repo_name, count() FROM github_events WHERE event_type = 'IssueCommentEvent' GROUP BY repo_name ORDER BY count() DESC LIMIT 50
