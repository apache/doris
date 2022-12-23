-- database: presto; groups: no_from
SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

SELECT abs(-10.0E0), log2(4), TRUE AND FALSE, TRUE OR FALSE
