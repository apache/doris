CREATE TABLE if not EXISTS `wftest1` (
 `aa` varchar(200) NULL COMMENT "",
 `bb` int NULL COMMENT ""
)
ENGINE = OLAP
UNIQUE KEY (`aa`)
COMMENT "aa"
DISTRIBUTED BY HASH (`aa`) BUCKETS 3 PROPERTIES (
"replication_num" = "1"
);

CREATE TABLE if not EXISTS `wftest2` (
 `cc` varchar(200) NULL COMMENT "",
 `dd` int NULL COMMENT ""
)
ENGINE = OLAP
UNIQUE KEY (`cc`)
COMMENT "cc"
DISTRIBUTED BY HASH (`cc`) BUCKETS 3 PROPERTIES (
 "replication_num" = "1"
);


INSERT INTO  wftest1 VALUES ('a', 1),('b', 1),('c', 1);
INSERT INTO  wftest2 VALUES ('a', 1),('b', 1),('d', 1);


select  * from wftest1 t1  left join wftest2 t2 on t1.aa=t2.cc order by t1.aa;
select  t.* from ( select  * from wftest1 t1  left join wftest2 t2 on t1.aa=t2.cc  ) t  where   dayofweek(current_date())=2;
