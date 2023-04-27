-- database: presto; groups: no_from
SELECT * FROM (
SELECT 1 a
UNION ALL
SELECT 2 a
UNION ALL
SELECT 4*5 a
UNION ALL
SELECT -5 a
) t
ORDER BY a;
