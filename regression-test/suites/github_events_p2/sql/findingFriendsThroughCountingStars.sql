SELECT
    actor_login,
    sum(my_repo) AS stars_my,
    sum(not_my_repo) AS stars_other,
    round(sum(my_repo) / (203 + sum(not_my_repo)), 3) AS ratio
FROM
(
    SELECT
        actor_login,
        CASE WHEN my_repo_name IS NOT NULL THEN 1 ELSE 0 END AS my_repo,
        CASE WHEN my_repo_name IS NOT NULL THEN 0 ELSE 1 END AS not_my_repo
    FROM
    (
        SELECT
            actor_login,
            repo_name AS all_repo_name
        FROM github_events
        WHERE event_type = 'WatchEvent'
    ) all_repos
    FULL OUTER JOIN
    (
        SELECT repo_name AS my_repo_name
        FROM github_events
        WHERE (event_type = 'WatchEvent') AND (actor_login IN ('alexey-milovidov'))
        GROUP BY my_repo_name
    ) my_repos
    ON all_repos.all_repo_name = my_repos.my_repo_name
) t
GROUP BY actor_login
ORDER BY ratio DESC
LIMIT 50
