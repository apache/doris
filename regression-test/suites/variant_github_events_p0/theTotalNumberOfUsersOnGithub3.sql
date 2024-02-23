SELECT count(distinct cast(v["actor"]["login"] as string)) FROM github_events WHERE cast(v["type"] as string) = 'PushEvent'
