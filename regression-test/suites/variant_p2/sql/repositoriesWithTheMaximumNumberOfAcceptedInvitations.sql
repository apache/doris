SELECT
    repo_name,
    sum(invitation) AS invitations,
    sum(star) AS stars
FROM
(
    SELECT
        cast(repo["name"] as string) as repo_name,
        CASE WHEN type = 'MemberEvent' THEN 1 ELSE 0 END AS invitation,
        CASE WHEN type = 'WatchEvent' THEN 1 ELSE 0 END AS star
    FROM github_events
    WHERE type IN ('MemberEvent', 'WatchEvent')
) t
GROUP BY repo_name
HAVING stars >= 2
ORDER BY invitations DESC, stars DESC, repo_name
LIMIT 50
