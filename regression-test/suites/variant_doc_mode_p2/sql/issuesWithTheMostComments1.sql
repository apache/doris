SELECT count() FROM github_events WHERE cast(v["type"] as string) = 'IssueCommentEvent'
