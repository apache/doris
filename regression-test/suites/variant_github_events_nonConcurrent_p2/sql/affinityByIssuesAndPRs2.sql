SELECT
    cast(v["repo"]["name"] as string),
    count() AS prs,
    count(distinct cast(v["actor"]["login"] as string)) AS authors
FROM github_events
WHERE (cast(v["type"] as string) = 'IssuesEvent') AND (cast(v["payload"]["action"] as string) = 'opened') AND (cast(v["actor"]["login"] as string) IN
(
    SELECT cast(v["actor"]["login"] as string)
    FROM github_events
    WHERE (cast(v["type"] as string) = 'IssuesEvent') AND (cast(v["payload"]["action"] as string) = 'opened') AND (cast(v["repo"]["name"] as string) IN ('No-CQRT/GooGuns', 'ivolunteerph/ivolunteerph', 'Tribler/tribler'))
)) AND (lower(cast(v["repo"]["name"] as string)) NOT LIKE '%clickhouse%')
GROUP BY cast(v["repo"]["name"] as string)
ORDER BY authors DESC, prs DESC, cast(v["repo"]["name"] as string) ASC
LIMIT 50
