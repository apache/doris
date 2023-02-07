SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT max(0) FROM visits WHERE (CAST(CAST(StartDate AS DATETIME) AS INT)) > 1000000000
