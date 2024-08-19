set two_phase_read_limit_threshold = false;
SELECT payload["commits"] FROM github_events order by id limit 10;