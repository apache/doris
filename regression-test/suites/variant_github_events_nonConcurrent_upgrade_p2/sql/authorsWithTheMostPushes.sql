SELECT
      cast(v["actor"]["login"] as string),
      count() AS c,
      count(distinct cast(v["repo"]["name"] as string)) AS repos
  FROM github_events
  WHERE cast(v["type"] as string) = 'PushEvent'
  GROUP BY cast(v["actor"]["login"] as string)
  ORDER BY c DESC, 1, 3
  LIMIT 50
