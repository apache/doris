-- https://github.com/apache/incubator-doris/issues/3520
drop database if exists issue_3520

create database issue_3520
use issue_3520
CREATE TABLE `t1` (`siteid` int(11) NULL COMMENT "", `citycode` smallint(6) NULL COMMENT "", `username` varchar(32) NULL COMMENT "", `uv` hll HLL_UNION COMMENT "") AGGREGATE KEY(`siteid`, `citycode`, `username`) DISTRIBUTED BY HASH(`siteid`) BUCKETS 1

insert into t1 (siteid, citycode, username, uv) VALUES (1, 1, 'jim', hll_hash('jim')), (2, 1, 'grace', hll_hash('grace')), (3, 2, 'tom', hll_hash('tom')), (4, 3, 'bush', hll_hash('bush')), (5, 3, 'helen', hll_hash('helen'))
select citycode, hll_union_agg(case when length(username) >= 100 then uv end) from t1 group by citycode

insert into t1 (siteid, citycode, username, uv) VALUES (6, 1, 'aaaaaaaaaaaaaaa', hll_hash('aaaaaaaaaaaaaaa')), (7, 1, 'bbbbbbbbbbbbbbb', hll_hash('bbbbbbbbbbbbbbb'))
select citycode, hll_union_agg(case when length(username) >= 100 then uv end) from t1 group by citycode
select citycode, hll_union_agg(case when length(username) >= 10 then uv end) from t1 group by citycode

drop database issue_3520
