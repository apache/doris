SET enable_vectorized_engine=true;
SET enable_nereids_planner=true;
SET enable_fallback_to_original_planner=false;

select avg(p_retailprice), p_mfgr from part group by 2 order by count(*) limit 20
