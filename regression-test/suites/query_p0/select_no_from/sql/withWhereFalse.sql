-- database: presto; groups: no_from
SET enable_nereids_planner = TRUE;
SELECT COUNT(*), 1 WHERE FALSE;