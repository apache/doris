-- database: presto; groups: no_from
SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT 1, 1.1, 100*5.1, 'a', 'dummy values', TRUE, FALSE