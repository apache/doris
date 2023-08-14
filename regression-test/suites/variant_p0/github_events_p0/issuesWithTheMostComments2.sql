SELECT cast(v:repo.name as string), count() FROM github_events WHERE cast(v:type as string) = 'IssueCommentEvent' GROUP BY cast(v:repo.name as string) ORDER BY count() DESC LIMIT 50
