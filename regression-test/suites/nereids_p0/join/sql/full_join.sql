set enable_nereids_planner=true;
set enable_fallback_to_original_planner=false;

SELECT 'n fj n', t1.x, t2.x FROM full_join_table AS t1 FULL JOIN full_join_table AS t2 ON t1.x = t2.x ORDER BY t1.x;
SELECT 'n fj n', t1.x, t2.x FROM full_join_table AS t1 FULL JOIN full_join_table AS t2 ON t1.x <=> t2.x ORDER BY t1.x;
