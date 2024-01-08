SELECT count(distinct cast(actor:login as string)) FROM github_events WHERE type = 'PullRequestEvent' AND cast(payload:action as string) = 'opened'
