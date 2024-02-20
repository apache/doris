 SELECT
      cast(actor['login'] as string),
      count() AS c,
      count(distinct cast(repo['name'] as string)) AS repos
  FROM github_events
  WHERE type = 'PushEvent'
  GROUP BY cast(actor['login'] as string)
  ORDER BY c DESC, 1, 3
  LIMIT 50
