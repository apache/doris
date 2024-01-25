SELECT count(distinct cast(repo:name as string)) FROM github_events
