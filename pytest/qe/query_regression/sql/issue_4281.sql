-- https://github.com/apache/incubator-doris/issues/4281
drop database if exists issue_4281

create database issue_4281
use issue_4281
CREATE TABLE t0(c0 INT , c1 DOUBLE )DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES("replication_num"="1")
insert into t0(`c0`, `c1`) values (1, 2.22)

-- query
select  MIN(CAST(0.15508025062004804 AS DATE)) from t0

drop database issue_4281
