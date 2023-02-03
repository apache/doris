SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT count() FROM hits WHERE URL LIKE '%_/avtomobili_s_probegom/_%__%__%__%'
