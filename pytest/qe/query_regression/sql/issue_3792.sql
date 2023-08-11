-- https://github.com/apache/incubator-doris/issues/3792
drop database if exists issue_3792

create database issue_3792
use issue_3792
-- create table
DROP TABLE IF EXISTS `test_basic`
CREATE TABLE IF NOT EXISTS `test_basic` (`id_int` int(11) NOT NULL, `id_tinyint` tinyint NOT NULL, `id_smallint` smallint NOT NULL, `id_bigint` bigint NOT NULL, `id_largeint` largeint NOT NULL, `id_float` float NOT NULL, `id_double` double NOT NULL, `id_char` char(10) NOT NULL, `id_varchar` varchar(100) NOT NULL) ENGINE=OLAP DUPLICATE KEY(`id_int`) DISTRIBUTED BY HASH(`id_int`) BUCKETS 10 PROPERTIES ("replication_num" = "1")

INSERT INTO test_basic (id_int, id_tinyint, id_smallint, id_bigint, id_largeint , id_float, id_double, id_char, id_varchar) VALUES (1, 10, 100, 1000, 10000 , 100000.1, 1000000.1, 'kks_char', 'kks_varchar'), (2, 20, 200, 2000, 20000 , 200000.1, 2000000.1, 'kks_char2', 'kks_varchar2'), (3, 30, 300, 3000, 30000 , 300000.1, 3000000.1, 'kks_char3', 'kks_varchar3'), (4, 40, 400, 4000, 40000 , 400000.1, 4000000.1, 'kks_char4', 'kks_varchar4')

select min(id_largeint) from test_basic
select max(id_largeint) from test_basic
select min(id_float) from test_basic
select max(id_float) from test_basic
select min(id_double) from test_basic
select max(id_double) from test_basic

drop database issue_3792
