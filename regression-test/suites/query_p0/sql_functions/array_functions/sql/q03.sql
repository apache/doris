ADMIN SET FRONTEND CONFIG ('enable_array_type' = 'true');
set enable_vectorized_engine=true;
DROP TABLE IF EXISTS array_insert_select_test;
CREATE TABLE array_insert_select_test (id int, c_array array<int(11)>) ENGINE = Olap DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num' = '1');
insert into array_insert_select_test select k1, collect_list(k3) from test_query_db.test group by k1;
select c_array from array_insert_select_test;
