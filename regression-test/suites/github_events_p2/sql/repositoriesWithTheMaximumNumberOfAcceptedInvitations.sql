SELECT
    repo_name,
    sum(invitation) AS invitations,
    sum(star) AS stars
FROM
(
    SELECT
        repo_name,
        CASE WHEN event_type = 'MemberEvent' THEN 1 ELSE 0 END AS invitation,
        CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END AS star
    FROM github_events
    WHERE event_type IN ('MemberEvent', 'WatchEvent')
) t
GROUP BY repo_name
HAVING stars >= 100
ORDER BY invitations DESC, stars DESC
LIMIT 50
