DROP TABLE IF EXISTS array_element_test;
CREATE TABLE IF NOT EXISTS array_element_test (x int, arr array<int>, id int) ENGINE = Olap DUPLICATE KEY(x) DISTRIBUTED BY HASH(x) BUCKETS 1 PROPERTIES("replication_num" = "1");;
insert into array_element_test VALUES (1, [11,12,13], 2), (2, [11,12], 3), (3, [11,12,13], -1), (4, [11,12], -2), (5, [11,12], -3), (6, [11], 0);
--- Nereids does't support array function
--- select arr[id] from array_element_test;


DROP TABLE IF EXISTS array_element_test;
CREATE TABLE IF NOT EXISTS array_element_test (x int, arr array<int>, id int) ENGINE = Olap DUPLICATE KEY(x) DISTRIBUTED BY HASH(x) BUCKETS 1 PROPERTIES("replication_num" = "1");;
insert into array_element_test VALUES (1, [11,12,13], 2), (2, [11,12], 3), (3, [11,12,13], 1), (4, [11,12], 4), (5, [11], 0);
--- Nereids does't support array function
--- select arr[id] from array_element_test;


DROP TABLE IF EXISTS array_element_test;
CREATE TABLE IF NOT EXISTS array_element_test (x int, arr array<string>, id int) ENGINE = Olap DUPLICATE KEY(x) DISTRIBUTED BY HASH(x) BUCKETS 1 PROPERTIES("replication_num" = "1");;
insert into array_element_test VALUES (1, ['Abc','Df','Q'], 2), (2, ['Abc','DEFQ'], 3), (3, ['ABC','Q','ERT'], -1), (4, ['Ab','ber'], -2), (5, ['AB','asd'], -3), (6, ['A'], 0);
--- Nereids does't support array function
--- select arr[id] from array_element_test;


DROP TABLE IF EXISTS array_element_test;
CREATE TABLE IF NOT EXISTS array_element_test (x int, arr array<string>, id int) ENGINE = Olap DUPLICATE KEY(x) DISTRIBUTED BY HASH(x) BUCKETS 1 PROPERTIES("replication_num" = "1");;
--- Nereids does't support array function
--- select arr[id] from array_element_test;


DROP TABLE array_element_test;

