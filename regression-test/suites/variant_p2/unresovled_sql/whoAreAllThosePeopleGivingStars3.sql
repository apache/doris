-- ERROR: unmatched column
-- SELECT
--     cast(repo:name as string),
--     count() AS stars
-- FROM github_events
-- WHERE (type = 'WatchEvent') AND (cast(repo:name as string) IN
-- (
--     SELECT cast(repo:name as string)
--     FROM github_events
--     WHERE (type = 'WatchEvent') AND (cast(actor:login as string) = 'cliffordfajardo')
-- ))
-- GROUP BY cast(repo:name as string)
-- ORDER BY stars DESC
-- LIMIT 50



