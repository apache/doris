insert into github_events select * from github_events;
alter table github_events ADD COLUMN var2 variant;

SELECT
    cast(v["repo"]["name"] as string),
    count() AS prs,
    count(distinct cast(v["actor"]["login"] as string)) AS authors
FROM github_events
WHERE (cast(v["type"] as string) = 'PullRequestEvent') AND (cast(v["payload"]["action"] as string) = 'opened') AND (cast(v["actor"]["login"] as string) IN
(
    SELECT cast(v["actor"]["login"] as string)
    FROM github_events
    WHERE (cast(v["type"] as string) = 'PullRequestEvent') AND (cast(v["payload"]["action"] as string)= 'opened') AND (cast(v["repo"]["name"] as string) IN ('rspec/rspec-core', 'golden-warning/giraffedraft-server', 'apache/spark'))
)) AND (lower(cast(v["repo"]["name"] as string)) NOT LIKE '%clickhouse%')
GROUP BY cast(v["repo"]["name"] as string)
ORDER BY authors DESC, prs DESC, cast(v["repo"]["name"] as string) DESC
LIMIT 50;

alter table github_events DROP COLUMN var2;
