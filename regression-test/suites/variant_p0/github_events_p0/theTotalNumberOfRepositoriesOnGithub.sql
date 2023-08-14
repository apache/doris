SELECT count(distinct cast(v:repo.name as string)) FROM github_events
