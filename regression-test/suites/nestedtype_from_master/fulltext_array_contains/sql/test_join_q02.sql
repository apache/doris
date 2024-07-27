

-- not support: because column 'name' in join_t2_dk_array
-- select * FROM join_t1_dk_array left join join_t2_dk_array on venue_id = entity_id 
--         where name match_any 'aberdeen' and dt = '2003-05-23 19:30:00';

-- select * FROM join_t1_dk_array left join join_t2_dk_array on venue_id = entity_id 
--         where name match_any 'aberdeen' and dt = '2003-05-23 19:30:00';


select * FROM join_t1_dk_array right join join_t2_dk_array on venue_id = entity_id 
        where array_contains(name, 'aberdeen') and dt = '2003-05-23 19:30:00';