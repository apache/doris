SELECT count(cast(payload["commits"] as string)) FROM github_events WHERE cast(payload["push_id"] as int) > 100;
