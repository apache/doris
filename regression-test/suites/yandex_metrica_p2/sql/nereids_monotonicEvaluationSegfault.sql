SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT max(0) FROM visits WHERE (CAST(CAST(StartDate AS DATETIME) AS BIGINT)) > 1000000000
