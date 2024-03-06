SELECT /*+SET_VAR(enable_fallback_to_original_planner=false) */
    repo_name,
    sum(invitation) AS invitations,
    sum(star) AS stars
FROM
(
    SELECT
        cast(v["repo"]["name"] as string) as repo_name,
        CASE WHEN cast(v["type"] as string) = 'MemberEvent' THEN 1 ELSE 0 END AS invitation,
        CASE WHEN cast(v["type"] as string) = 'WatchEvent' THEN 1 ELSE 0 END AS star
    FROM github_events
    WHERE cast(v["type"] as string) IN ('MemberEvent', 'WatchEvent')
) t
GROUP BY repo_name
HAVING stars >= 2
ORDER BY invitations DESC, stars DESC, repo_name
LIMIT 50
