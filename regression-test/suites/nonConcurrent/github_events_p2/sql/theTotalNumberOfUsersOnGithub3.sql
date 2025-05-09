SELECT count(distinct actor_login) FROM github_events WHERE event_type = 'PushEvent'
