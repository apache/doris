SELECT
      cast(v["actor"]["login"] as string) as actor_login,
      count() AS c,
      count(distinct cast(v["repo"]["name"] as string)) AS repos
  FROM github_events
  WHERE cast(v["type"] as string) = 'PushEvent'
  GROUP BY actor_login
  ORDER BY c DESC, 1, 3
  LIMIT 50
