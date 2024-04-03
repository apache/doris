SELECT /*+SET_VAR(enable_fallback_to_original_planner=false) */
    cast(v["repo"]["name"] as string) as repo_name,
    count() AS prs,
    count(distinct cast(v["actor"]["login"] as string)) AS authors
FROM github_events
WHERE (cast(v["type"] as string) = 'PullRequestEvent') AND (cast(v["payload"]["action"] as string) = 'opened') AND (cast(v["actor"]["login"] as string) IN
(
    SELECT cast(v["actor"]["login"] as string)
    FROM github_events
    WHERE (cast(v["type"] as string) = 'PullRequestEvent') AND (cast(v["payload"]["action"] as string)= 'opened') AND (cast(v["repo"]["name"] as string) IN ('rspec/rspec-core', 'golden-warning/giraffedraft-server', 'apache/spark'))
)) AND (lower(cast(v["repo"]["name"] as string)) NOT LIKE '%clickhouse%')
GROUP BY repo_name
ORDER BY authors DESC, prs DESC, repo_name DESC
LIMIT 50
