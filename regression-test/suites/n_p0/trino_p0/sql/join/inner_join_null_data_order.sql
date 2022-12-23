SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select n_name, department, name, salary from nation, workers where n_nationkey = department
