set enable_nereids_planner=true;
set enable_fallback_to_original_planner=false;

select t1.k1,t2.k1 from test_join t1 left join (select k1 from test_join group by k1) t2 on t1.k1=t2.k1 order by t1.k1, t2.k1