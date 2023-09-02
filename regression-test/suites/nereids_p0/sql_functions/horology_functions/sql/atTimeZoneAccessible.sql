/*
-- database: presto; groups: qe, horology_functions
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT timezone_hour(TIMESTAMP '2001-08-22 03:04:05.321' at time zone 'Asia/Oral'),
       timezone_minute(TIMESTAMP '2001-08-22 03:04:05.321' at time zone 'Asia/Oral')
*/
