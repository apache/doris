SELECT
    cast(v:repo.name as string),
    sum(invitation) AS invitations,
    sum(star) AS stars
FROM
(
    SELECT
        cast(v:repo.name as string),
        CASE WHEN cast(v:type as string) = 'MemberEvent' THEN 1 ELSE 0 END AS invitation,
        CASE WHEN cast(v:type as string) = 'WatchEvent' THEN 1 ELSE 0 END AS star
    FROM github_events
    WHERE cast(v:type as string) IN ('MemberEvent', 'WatchEvent')
) t
GROUP BY cast(v:repo.name as string)
HAVING stars >= 100
ORDER BY invitations DESC, stars DESC
LIMIT 50
