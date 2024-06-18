// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite ("subquery_unnesting") {
    sql """ SET enable_nereids_planner=true"""
    sql "set enable_fallback_to_original_planner=false"

    sql "drop table if exists t1"
    sql "drop table if exists t2"
    sql "drop table if exists t3"
    
    sql """create table t1
                    (k1 bigint, k2 bigint)
                    ENGINE=OLAP
            DUPLICATE KEY(k1, k2)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(k2) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1"
            );"""
    sql """create table t2
                    (k1 int, k2 varchar(128), k3 bigint, v1 bigint, v2 bigint)
                    ENGINE=OLAP
            DUPLICATE KEY(k1, k2)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(k2) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1"
            );"""
    sql """create table t3
                    (k1 bigint, k2 bigint)
                    ENGINE=OLAP
            DUPLICATE KEY(k1, k2)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(k2) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1"
            );"""
    sql """insert into t1 values (1,null),(null,1),(1,2), (null,2),(1,3), (2,4), (2,5), (3,3), (3,4), (20,2), (22,3), (24,4),(null,null);"""
    sql """insert into t2 values (1,'abc',2,3,4), (1,'abcd',3,3,4), (2,'xyz',2,4,2), (2,'uvw',3,4,2), (2,'uvw',3,4,2), (3,'abc',4,5,3), (3,'abc',4,5,3), (null,null,null,null,null);"""
    sql """insert into t3 values (1,null),(null,1),(1,4), (1,2), (null,3), (2,4), (3,7), (3,9),(null,null),(5,1);"""

    sql "drop table if exists sub_query_correlated_subquery1"
    sql "drop table if exists sub_query_correlated_subquery3"

    sql """create table if not exists sub_query_correlated_subquery1
            (k1 bigint, k2 bigint)
            duplicate key(k1)
            distributed by hash(k2) buckets 1
            properties('replication_num' = '1');"""
    sql """create table if not exists sub_query_correlated_subquery3
            (k1 int, k2 varchar(128), k3 bigint, v1 bigint, v2 bigint)
            distributed by hash(k2) buckets 1
            properties('replication_num' = '1');"""
    sql """insert into sub_query_correlated_subquery1 values (1,null),(null,1),(1,2), (null,2),(1,3), (2,4), (2,5), (3,3), (3,4), (20,2), (22,3), (24,4),(null,null);"""
    sql """insert into sub_query_correlated_subquery3 values (1,"abc",2,3,4), (1,"abcd",3,3,4), (2,"xyz",2,4,2), (2,"uvw",3,4,2), (2,"uvw",3,4,2), (3,"abc",4,5,3), (3,"abc",4,5,3), (null,null,null,null,null);"""

    qt_select1 """select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 < (select sum(sub_query_correlated_subquery3.k3) from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = sub_query_correlated_subquery1.k2) OR k1 < 10 order by k1, k2;"""
    qt_select2 """select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 < (select sum(sub_query_correlated_subquery3.k3) from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = sub_query_correlated_subquery1.k2) order by k1, k2;"""
    qt_select3 """SELECT * FROM sub_query_correlated_subquery1 WHERE k1 > (SELECT AVG(k1) FROM sub_query_correlated_subquery3) OR k1 < 10 order by k1, k2;"""
    qt_select4 """select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 < (select sum(sub_query_correlated_subquery3.k3) from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = 2) order by k1, k2;"""
    qt_select5 """select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = sub_query_correlated_subquery1.k2) OR k1 < 10 order by k1, k2;"""
    qt_select6 """select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = sub_query_correlated_subquery1.k2) order by k1, k2;"""
    qt_select7 """SELECT * FROM sub_query_correlated_subquery1 WHERE k1 IN (SELECT k1 FROM sub_query_correlated_subquery3 WHERE sub_query_correlated_subquery1.k1 > sub_query_correlated_subquery3.k3) OR k1 < 10 ORDER BY k1, k2;"""
    qt_select8 """select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 < sub_query_correlated_subquery1.k2) order by k1, k2;"""
    qt_select9 """SELECT * FROM sub_query_correlated_subquery1 WHERE k1 IN (SELECT k1 FROM sub_query_correlated_subquery3) OR k1 < 10 order by k1, k2;"""
    qt_select10 """select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3) order by k1, k2;"""
    qt_select11 """select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 not in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = sub_query_correlated_subquery1.k2) or k1 < 10 order by k1, k2;"""
    qt_select12 """select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 not in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = sub_query_correlated_subquery1.k2) order by k1, k2;"""
    qt_select13 """select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 not in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 > sub_query_correlated_subquery1.k2) or k1 < 10 order by k1, k2;"""
    qt_select14 """select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 not in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 > sub_query_correlated_subquery1.k2) order by k1, k2;"""
    qt_select15 """select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 not in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 ) or k1 < 10 order by k1, k2;"""
    qt_select16 """select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 not in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = 2) order by k1, k2;"""
    qt_select17 """select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.v2) or k1 < 10 order by k1, k2;"""
    qt_select18 """select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.v2) order by k1, k2;"""
    qt_select19 """select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 > sub_query_correlated_subquery3.v2) or k1 < 10 order by k1, k2;"""
    qt_select20 """select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 > sub_query_correlated_subquery3.v2) order by k1, k2;"""
    qt_select21 """SELECT * FROM sub_query_correlated_subquery1 WHERE EXISTS (SELECT k1 FROM sub_query_correlated_subquery3 WHERE k1 = 10) OR k1 < 10 order by k1, k2;"""
    qt_select22 """select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3) order by k1, k2;"""
    qt_select23 """select * from sub_query_correlated_subquery1 where not exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.v2) or k1 < 10 order by k1, k2;"""
    qt_select24 """select * from sub_query_correlated_subquery1 where not exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.v2) order by k1, k2;"""
    qt_select25 """select * from sub_query_correlated_subquery1 where not exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 > sub_query_correlated_subquery3.v2) or k1 < 10 order by k1, k2;"""
    qt_select26 """select * from sub_query_correlated_subquery1 where not exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 != sub_query_correlated_subquery3.v2) order by k1, k2;"""
    qt_select27 """SELECT * FROM sub_query_correlated_subquery1 WHERE not EXISTS (SELECT k1 FROM sub_query_correlated_subquery3 WHERE k1 = 10) OR k1 < 10 order by k1, k2;"""
    qt_select28 """select * from sub_query_correlated_subquery1 where not exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3) order by k1, k2;"""
    qt_select29 """select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.v2 limit 1) order by k1, k2;"""
    qt_select30 """select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 > sub_query_correlated_subquery3.v2 limit 1) order by k1, k2;"""
    qt_select31 """select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 limit 1) order by k1, k2;"""

    qt_select32 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and t1.k1 < ( select max(k1) from t3 where t1.k2 = t3.k2 ) OR t1.k1 < 10 order by t1.k1, t1.k2;"""
    qt_select33 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and t1.k1 < ( select max(k1) from t3 where t1.k2 = t3.k2 ) order by t1.k1, t1.k2;"""
    qt_select34 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and t1.k1 < ( select max(k1) from t3 ) or t1.k1 < 10 order by t1.k1, t1.k2;"""
    qt_select35 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and t1.k1 < ( select max(k1) from t3 ) order by t1.k1, t1.k2;"""
    qt_select36 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and t1.k1 in ( select t3.k1 from t3 where t1.k2 = t3.k2 ) or t1.k1 < 10 order by t1.k1, t1.k2;"""
    qt_select37 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and t1.k1 in ( select t3.k1 from t3 where t1.k2 = t3.k2 ) order by t1.k1, t1.k2;"""
    qt_select38 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and t1.k1 in ( select t3.k1 from t3 where t1.k2 < t3.k2 ) or t1.k1 < 10 order by t1.k1, t1.k2;"""
    qt_select39 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and t1.k1 in ( select t3.k1 from t3 where t1.k2 < t3.k2 ) order by t1.k1, t1.k2;"""
    qt_select40 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and t1.k1 in ( select t3.k1 from t3 ) or t1.k1 < 10 order by t1.k1, t1.k2;"""
    qt_select41 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and t1.k1 in ( select t3.k1 from t3 ) order by t1.k1, t1.k2;"""
    qt_select42 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and t1.k1 not in ( select t3.k1 from t3 where t1.k2 = t3.k2 ) or t1.k1 < 10 order by t1.k1, t1.k2;"""
    qt_select43 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and t1.k1 not in ( select t3.k1 from t3 where t1.k2 = t3.k2 ) order by t1.k1, t1.k2;"""
    qt_select44 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and t1.k1 not in ( select t3.k1 from t3 where t1.k2 < t3.k2 ) or t1.k1 < 10 order by t1.k1, t1.k2;"""
    qt_select45 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and t1.k1 not in ( select t3.k1 from t3 where t1.k2 < t3.k2 ) order by t1.k1, t1.k2;"""
    qt_select46 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and t1.k1 not in ( select t3.k1 from t3 ) or t1.k1 < 10 order by t1.k1, t1.k2;"""
    qt_select47 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and t1.k1 not in ( select t3.k1 from t3 ) order by t1.k1, t1.k2;"""
    qt_select48 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and exists ( select t3.k1 from t3 where t1.k2 = t3.k2 ) or t1.k1 < 10 order by t1.k1, t1.k2;"""
    qt_select49 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and exists ( select t3.k1 from t3 where t1.k2 = t3.k2 ) order by t1.k1, t1.k2;"""
    qt_select50 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and exists ( select t3.k1 from t3 where t1.k2 < t3.k2 ) or t1.k1 < 10 order by t1.k1, t1.k2;"""
    qt_select51 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and exists ( select t3.k1 from t3 where t1.k2 < t3.k2 ) order by t1.k1, t1.k2;"""
    qt_select52 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and exists ( select t3.k1 from t3 ) or t1.k1 < 10 order by t1.k1, t1.k2;"""
    qt_select53 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and exists ( select t3.k1 from t3 ) order by t1.k1, t1.k2;"""
    qt_select54 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and not exists ( select t3.k1 from t3 where t1.k2 = t3.k2 ) or t1.k1 < 10 order by t1.k1, t1.k2;"""
    qt_select55 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and not exists ( select t3.k1 from t3 where t1.k2 = t3.k2 ) order by t1.k1, t1.k2;"""
    qt_select56 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and not exists ( select t3.k1 from t3 where t1.k2 < t3.k2 ) or t1.k1 < 10 order by t1.k1, t1.k2;"""
    qt_select57 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and not exists ( select t3.k1 from t3 where t1.k2 < t3.k2 ) order by t1.k1, t1.k2;"""
    qt_select58 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and not exists ( select t3.k1 from t3 ) or t1.k1 < 10 order by t1.k1, t1.k2;"""
    qt_select59 """select t1.* from t1 left join t2 on t1.k2 = t2.k3 and not exists ( select t3.k1 from t3 ) order by t1.k1, t1.k2;"""
    qt_select60 """select * from t1 where exists(select distinct k1 from t2 where t1.k1 > t2.k3 or t1.k2 < t2.v1) order by t1.k1, t1.k2;"""
    qt_select61 """SELECT * FROM t1 AS t1 WHERE EXISTS (SELECT k1 FROM t1 AS t2 WHERE t1.k1 <> t2.k1 + 7 GROUP BY k1 HAVING k1 >= 100);"""
    qt_select62 """select * from t1 left semi join ( select * from t1 where t1.k1 < -1 ) l on true;"""
    qt_select63 """SELECT * FROM t1 AS t1 WHERE EXISTS (SELECT k1 FROM t1 AS t2 WHERE t1.k1 <> t2.k1 + 7 GROUP BY k1 HAVING sum(k2) >= 1) order by t1.k1, t1.k2;"""
}
