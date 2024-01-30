SELECT count(distinct cast(v["actor"]["login"] as string)) FROM github_events
