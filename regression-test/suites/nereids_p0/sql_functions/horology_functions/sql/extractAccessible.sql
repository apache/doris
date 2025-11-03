-- database: presto; groups: qe, horology_functions
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT extract(day from TIMESTAMP '2001-08-22 03:04:05.321')