-- database: presto; groups: no_from
SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select * from (SELECT 1 a
UNION ALL
SELECT 2 a
UNION ALL
SELECT 4*5 a
UNION ALL
SELECT -5 a) b order by a
