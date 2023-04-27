

-- not support: because column 'name' in join_t2_dk
-- select * from join_t1_uk left join join_t2_uk on venue_id = entity_id 
--         where name match_any 'aberdeen' and dt = '2003-05-23 19:30:00';

-- select * from join_t1_uk left join join_t2_uk on venue_id = entity_id 
--         where name match_any 'aberdeen' and dt = '2003-05-23 19:30:00';

select * from join_t1_uk right join join_t2_uk on venue_id = entity_id 
        where name match_any 'aberdeen' and dt = '2003-05-23 19:30:00';

select * from join_t1_uk right join join_t2_uk on venue_id = entity_id 
        where name match_any 'aberdeen' and dt = '2003-05-23 19:30:00';