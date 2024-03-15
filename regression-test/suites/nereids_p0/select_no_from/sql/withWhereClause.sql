-- database: presto; groups: no_from
SET enable_nereids_planner = TRUE;
SELECT 1 WHERE TRUE AND 2=2;
