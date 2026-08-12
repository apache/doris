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
    sql "set enable_sql_cache=false;"

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

    qt_select64 """
        select case
                when t1.k1=1 then (select count(*) from t2 where t1.k2=t2.k2)
                when t1.k1=2 then (select count(*) from t3 where t1.k2=t3.k2)
                else 0 end as kk
        from t1
        order by kk
        """
    qt_select65 """
        SELECT COUNT(*) AS c
        FROM (SELECT 1 AS x) t
        WHERE 1 NOT IN (SELECT CAST(NULL AS INT));
    """

    // =====================================================================
    // mark join elimination in the join ON condition.
    //
    // inferMarkSlotNotNullMap returns a pair for each mark join slot:
    //   Pair.first : the null and false values of the mark slot are
    //                indistinguishable, so the mark slot can be treated as a
    //                non-nullable boolean (null is computed as false when
    //                producing the mark value). this never changes the number
    //                of output rows, so it's safe for all join types.
    //   Pair.second: the original mark join can be directly eliminated and
    //                turned into a plain semi join. a plain semi join only
    //                outputs the matched rows, while a mark join keeps all
    //                original rows plus the mark column, so eliminating the
    //                mark join is only safe for inner, cross and semi joins
    //                where dropping the unmatched rows is already part of the
    //                join semantics.
    //
    // take the query below as an example:
    //   select t1.* from t1 left join t2 on t1.k2 = t2.k3
    //          and t1.k1 in (select t3.k1 from t3 where t1.k2 = t3.k2)
    // for the outer join the mark join must be kept: the unmatched left rows
    // (mark = false/null) must be preserved with null columns of t2, while a
    // plain semi join would drop them. so the analyzed plan must keep
    // isMarkJoin=true and only infer the non-nullable mark
    // (isMarkJoinSlotNotNull=true). for inner/cross/semi join the unmatched
    // rows are dropped anyway, so the mark join can be safely eliminated
    // (isMarkJoin=false and the mark slot is replaced by the true literal).
    //
    // note: anti join also keeps the mark join for the null-aware semantics
    // of NOT IN, but executing an anti join with a subquery in its ON
    // condition is a pre-existing unsupported path in physical planning, so
    // only the analyzed plan is checked here. asof join's ON clause only
    // allows equal conjuncts, so a subquery can never appear in it and the
    // asof branch in the code is defensive only.
    // =====================================================================

    // inner join: the mark join is eliminated (isMarkJoin=false, MarkJoinSlotReference=empty)
    explain {
        sql("""analyzed plan select t1.* from t1 inner join t2 on t1.k2 = t2.k3
                and t1.k1 in (select t3.k1 from t3 where t1.k2 = t3.k2) order by t1.k1, t1.k2;""")
        contains("isMarkJoin=false")
        contains("MarkJoinSlotReference=empty")
    }
    // semi join: the mark join is eliminated too
    explain {
        sql("""analyzed plan select t1.* from t1 left semi join t2 on t1.k2 = t2.k3
                and t1.k1 in (select t3.k1 from t3 where t1.k2 = t3.k2) order by t1.k1, t1.k2;""")
        contains("isMarkJoin=false")
        contains("MarkJoinSlotReference=empty")
    }
    // outer join: the mark join must be kept, and the mark slot's null/false
    // equivalence (isMarkJoinSlotNotNull=true) is still inferred
    explain {
        sql("""analyzed plan select t1.* from t1 left join t2 on t1.k2 = t2.k3
                and t1.k1 in (select t3.k1 from t3 where t1.k2 = t3.k2) order by t1.k1, t1.k2;""")
        contains("isMarkJoin=true")
        contains("isMarkJoinSlotNotNull=true")
    }
    // anti join: the mark join must be kept for the null-aware semantics of NOT IN
    explain {
        sql("""analyzed plan select t1.* from t1 left anti join t2 on t1.k2 = t2.k3
                and t1.k1 not in (select t3.k1 from t3 where t1.k2 = t3.k2) order by t1.k1, t1.k2;""")
        contains("isMarkJoin=true")
        contains("isMarkJoinSlotNotNull=true")
    }

    // result checks: the mark join elimination must not change the query results.
    // (the outer join results with subquery in the ON condition are already
    // covered by qt_select37 / qt_select43, the mark join is kept there)
    // inner join with IN subquery in the ON condition (mark join eliminated)
    qt_select66 """select t1.* from t1 inner join t2 on t1.k2 = t2.k3 and t1.k1 in (select t3.k1 from t3 where t1.k2 = t3.k2) order by t1.k1, t1.k2;"""
    // inner join with NOT IN subquery in the ON condition (mark join eliminated)
    qt_select67 """select t1.* from t1 inner join t2 on t1.k2 = t2.k3 and t1.k1 not in (select t3.k1 from t3 where t1.k2 = t3.k2) order by t1.k1, t1.k2;"""
    // left semi join with IN subquery in the ON condition (mark join eliminated)
    qt_select68 """select t1.* from t1 left semi join t2 on t1.k2 = t2.k3 and t1.k1 in (select t3.k1 from t3 where t1.k2 = t3.k2) order by t1.k1, t1.k2;"""

    // error-behavior regression: the mark join must NOT be eliminated when the filter
    // contains assert_true (a NoneMovableFunction). although M = false and M = null both
    // fold the predicate to false (the row-truth proof), eliminating the mark join changes
    // which rows reach assert_true: the semi join prunes the unmatched rows before the
    // filter, so assert_true is no longer evaluated on them and its error is suppressed.
    // with the mark join kept, all rows reach the filter and assert_true throws on the
    // unmatched guard = false rows.
    // data: M = assert_t.k1 in (assert_s.k1 where assert_s.k2 = assert_t.k2), so only
    // row (2,2) matches; guard = assert_t.k2 = 2 is false exactly on the unmatched rows
    // (1,1) and (3,3)
    sql "drop table if exists assert_t"
    sql "drop table if exists assert_s"
    sql """create table assert_t (k1 bigint, k2 bigint) DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k2) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """create table assert_s (k1 bigint, k2 bigint) DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k2) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """insert into assert_t values (1,1),(2,2),(3,3);"""
    sql """insert into assert_s values (2,2);"""
    test {
        sql """select assert_t.k1 from assert_t
                where ifnull(
                    ifnull(assert_t.k1 in (select assert_s.k1 from assert_s
                        where assert_s.k2 = assert_t.k2), false)
                    and assert_true(assert_t.k2 = 2, 'assert failed'),
                    false);"""
        exception "assert failed"
    }

    // error-behavior regressions for the "complete evaluation domain" fence: the mark join
    // must not be eliminated when a NoneMovableFunction (assert_true) exists anywhere in the
    // affected evaluation domain, even if it is NOT inside the mark conjunct itself.
    sql "drop table if exists assert_u"
    sql """create table assert_u (k1 bigint, k2 bigint) DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k2) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """insert into assert_u values (1,1),(2,2),(3,3);"""

    // sibling conjunct: assert_true is a SIBLING conjunct of the eliminable mark conjunct.
    // the mark conjunct must be a MARKER-REQUIRING form (ifnull(k1 in (...), false)): a bare
    // `k1 not in (...)` is itself a top-level SubqueryExpr, so shouldOutputMarkJoinSlot returns
    // false and no mark slot is created (it becomes a plain left anti join that emits the
    // unmatched rows anyway, so the test would stay green even without the fence). with the
    // mark form the analyzed plan must keep isMarkJoin=true, which is the sensitive signal:
    // removing the complete evaluation domain fence turns it into isMarkJoin=false (the mark
    // join is eliminated into a semi join). note that the error behavior is NOT the sensitive
    // signal here — assert_true(k2 = 2) only references the outer columns, so the optimizer
    // pushes it below the join (into the outer scan) and it raises the error on every outer
    // row regardless of the elimination.
    explain {
        sql("""analyzed plan select assert_t.k1 from assert_t
                where ifnull(assert_t.k1 in (select assert_s.k1 from assert_s
                        where assert_s.k2 = assert_t.k2), false)
                  and assert_true(assert_t.k2 = 2, 'assert failed');""")
        contains("isMarkJoin=true")
    }

    // sensitive expression inside a later subquery plan: assert_true lives in the filter of
    // a LATER subquery (the EXISTS one). marker replacement erases that plan from the
    // earlier IN conjunct, so the complete evaluation domain (including all subquery plans)
    // must fence the earlier IN apply from being eliminated into a semi join. the 'assert
    // failed' error must still be raised.
    test {
        sql """select assert_t.k1 from assert_t
                where ifnull(assert_t.k1 in (select assert_s.k1 from assert_s
                        where assert_s.k2 = assert_t.k2), false)
                  and exists (select 1 from assert_u
                        where assert_u.k2 = assert_t.k2
                          and assert_true(assert_u.k1 = 2, 'assert failed'));"""
        exception "assert failed"
    }

    // error-behavior regression for the retained-mark non-nullable inference (pair.first):
    // ((M and assert_true(guard, 'bad')) or flag) keeps the mark join (pair.second = false)
    // and pair.first alone would mark M non-nullable. M = k1 in (select null) is NULL for
    // every row (null in the build side), and treating that null as false changes how
    // assert_true is evaluated: the vectorized AND evaluates its right operand for a
    // nullable null input but can return early for an all-false non-null column, so the
    // required 'assert failed' error would be suppressed. pair.first must be fenced to
    // false so M stays null and assert_true is evaluated on every row.
    sql "drop table if exists null_src"
    sql """create table null_src (v bigint null) DUPLICATE KEY(v)
            DISTRIBUTED BY HASH(v) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """insert into null_src values (null);"""
    sql "drop table if exists guard_t"
    sql """create table guard_t (k1 bigint, guard bigint) DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """insert into guard_t values (1,0),(2,0),(3,0);"""
    test {
        sql """select guard_t.k1 from guard_t
                where (guard_t.k1 in (select null from null_src)
                        and assert_true(guard_t.guard = 1, 'assert failed'))
                   or guard_t.guard = 2;"""
        exception "assert failed"
    }
}
