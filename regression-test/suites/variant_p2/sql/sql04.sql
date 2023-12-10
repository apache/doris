SELECT repo:id, payload:issue  FROM github_events WHERE cast(payload:issue.state as string) = "open" order by cast(repo:id as int), id limit 10;
