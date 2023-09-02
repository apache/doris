 set enable_nereids_planner=true;
 set enable_fallback_to_original_planner=false;

 select l.k1, group_concat(r.no) from left_table l left join right_table r on l.k1=r.k1 group by l.k1 order by k1;