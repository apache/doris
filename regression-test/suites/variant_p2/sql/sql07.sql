set enable_two_phase_read_opt = false;
SELECT payload["commits"] FROM github_events order by id limit 10;