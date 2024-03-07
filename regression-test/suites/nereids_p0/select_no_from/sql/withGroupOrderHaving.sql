-- database: presto; groups: no_from
SET enable_nereids_planner = TRUE;
SELECT MIN(10), 3 as col1 GROUP BY 2 HAVING 6 > 5 ORDER BY 1;
SELECT 1 AS a, COUNT(*), SUM(2), AVG(1), RANK() OVER() AS w_rank
WHERE 1 = 1
GROUP BY a, w_rank
HAVING COUNT(*) IN (1, 2) AND w_rank = 1
ORDER BY a;