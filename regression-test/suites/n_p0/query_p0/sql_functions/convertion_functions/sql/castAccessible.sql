-- database: presto; groups: qe, conversion_functions
SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT CAST(10 as VARCHAR)
