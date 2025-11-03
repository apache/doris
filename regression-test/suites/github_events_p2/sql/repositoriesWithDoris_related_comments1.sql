SELECT repo_name, count() FROM github_events WHERE lower(body) LIKE '%doris%' GROUP BY repo_name ORDER BY count() DESC, repo_name ASC LIMIT 50
