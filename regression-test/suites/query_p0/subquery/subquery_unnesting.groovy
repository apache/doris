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

    // =====================================================================
    // split-fence regression: for an uncorrelated NULLABLE positive IN in a join ON
    // condition with a sensitive SIBLING expression, pair.first (isMarkJoinSlotNotNull)
    // must be kept while pair.second (isMarkJoin=true) is fenced.
    //
    // the mark predicate `uncor_in_t1.k in (select c from uncor_in_t3)` is itself CLEAN
    // (no assert_true inside it), so the current-predicate fence does not apply to it.
    // the sibling assert_true only lives in the evaluation domain, so it fences pair.second
    // only: the mark join is kept (isMarkJoin=true) because eliminating it would prune the
    // unmatched rows before assert_true, but pair.first stays true because the sibling
    // cannot observe this generated marker's null-vs-false mapping (pair.first keeps the
    // apply and only maps the marker's null to false).
    //
    // this matters beyond the analyzed plan: with isMarkJoinSlotNotNull=true, InApplyToJoin
    // moves the (nullable) IN equality into the hash conjuncts, so JoinUtils.couldShuffle
    // stays true and the physical planner keeps the shuffle alternative. if pair.first were
    // wrongly fenced (isMarkJoinSlotNotNull=false), the equality would stay in the
    // markConjuncts only, producing a standalone mark join with no hash conjuncts, which
    // couldShuffle forces to broadcast. the uncorrelated IN (no correlation hash conjunct)
    // makes this mark join the only join deciding the distribution, so the regression pins
    // the isMarkJoinSlotNotNull signal directly.
    sql "drop table if exists uncor_in_t1"
    sql "drop table if exists uncor_in_t2"
    sql "drop table if exists uncor_in_t3"
    sql """create table uncor_in_t1 (k bigint, a bigint) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """create table uncor_in_t2 (b bigint) DUPLICATE KEY(b)
            DISTRIBUTED BY HASH(b) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """create table uncor_in_t3 (c bigint null) DUPLICATE KEY(c)
            DISTRIBUTED BY HASH(c) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """insert into uncor_in_t1 values (1,1),(2,2),(3,3);"""
    sql """insert into uncor_in_t2 values (1),(2);"""
    sql """insert into uncor_in_t3 values (1),(null);"""
    explain {
        sql("""analyzed plan select uncor_in_t1.* from uncor_in_t1
                join uncor_in_t2 on uncor_in_t1.a = uncor_in_t2.b
                    and uncor_in_t1.k in (select c from uncor_in_t3)
                    and assert_true(uncor_in_t1.k > 0, 'assert failed')
                order by uncor_in_t1.k;""")
        contains("isMarkJoin=true")
        contains("isMarkJoinSlotNotNull=true")
    }

    // =====================================================================
    // current-conjunct-own-subquery exclusion regression: a NoneMovableFunction inside
    // the CURRENT conjunct's OWN subquery plan must NOT fence the mark join elimination.
    //
    // the mark predicate `ifnull(k1 in (select ...), false)` is clean, and the assert_true
    // lives inside the subquery's own plan (its filter). the inner plan is evaluated
    // identically whether the mark join is kept or eliminated: both the apply and the
    // resulting semi join evaluate the subquery (per outer row for a correlated subquery),
    // only the output row set differs, so assert_true inside it cannot be affected by the
    // elimination. the evaluation domain must therefore exclude the current conjunct's own
    // subquery plans; otherwise the mark join is kept (isMarkJoin=true) purely because of a
    // sensitive expression the elimination cannot reach. with the exclusion the mark join is
    // eliminated (isMarkJoin=false) and the subquery is still evaluated, so assert_true
    // still raises its error on the inner row with k2 = 0.
    sql "drop table if exists inner_assert_t"
    sql "drop table if exists inner_assert_s"
    sql """create table inner_assert_t (k1 bigint, k2 bigint) DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k2) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """create table inner_assert_s (k1 bigint, k2 bigint) DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k2) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """insert into inner_assert_t values (1,1),(2,2),(3,3),(4,4);"""
    sql """insert into inner_assert_s values (2,2),(4,0);"""
    explain {
        sql("""analyzed plan select inner_assert_t.k1 from inner_assert_t
                where ifnull(inner_assert_t.k1 in (select inner_assert_s.k1 from inner_assert_s
                        where inner_assert_s.k1 = inner_assert_t.k1
                          and assert_true(inner_assert_s.k2 > 0, 'assert failed')), false)
                order by inner_assert_t.k1;""")
        contains("isMarkJoin=false")
    }
    test {
        sql """select inner_assert_t.k1 from inner_assert_t
                where ifnull(inner_assert_t.k1 in (select inner_assert_s.k1 from inner_assert_s
                        where inner_assert_s.k1 = inner_assert_t.k1
                          and assert_true(inner_assert_s.k2 > 0, 'assert failed')), false)
                order by inner_assert_t.k1;"""
        exception "assert failed"
    }

    // =====================================================================
    // opposite-side exclusion regression: in the JOIN path, a later correlated scalar on
    // the OPPOSITE side of the join must NOT fence the current mark join elimination.
    //
    // the join has two subquery conjuncts on opposite sides: a mark IN correlated to the
    // left (side_join_t.k in (select ... where side_join_s.g = side_join_t.g)) and a later
    // correlated scalar correlated to the right (side_join_u.b = (select side_join_v.c ...
    // where side_join_v.h = side_join_u.h)), which generates the runtime
    // assert_true(count(*) <= 1) in the right subtree. the right subtree is an independent
    // branch: eliminating the left mark join (semi join pruning the left rows) does not
    // change which right rows reach the generated assertion, so the fence on it is
    // unnecessary and the left mark join can be eliminated (isMarkJoin=false).
    // note: the positive `contains("isMarkJoin=false")` alone cannot pin this down, because
    // the right correlated scalar Apply never has a marker and independently prints
    // isMarkJoin=false; a regression that wrongly retains the IN marker would then show both
    // isMarkJoin=true and isMarkJoin=false and the positive check still passes. the
    // `notContains("isMarkJoin=true")` is the real signal: it fails as soon as the left IN
    // marker is retained.
    sql "drop table if exists side_join_t"
    sql "drop table if exists side_join_s"
    sql "drop table if exists side_join_u"
    sql "drop table if exists side_join_v"
    sql """create table side_join_t (k bigint, g bigint) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """create table side_join_s (k bigint, g bigint) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """create table side_join_u (b bigint, h bigint) DUPLICATE KEY(b)
            DISTRIBUTED BY HASH(b) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """create table side_join_v (c bigint, h bigint) DUPLICATE KEY(c)
            DISTRIBUTED BY HASH(c) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """insert into side_join_t values (1,1),(2,2);"""
    sql """insert into side_join_s values (1,1),(2,2),(3,3);"""
    sql """insert into side_join_u values (10,10),(20,20);"""
    sql """insert into side_join_v values (100,10),(200,20);"""
    explain {
        sql("""analyzed plan select side_join_t.k, side_join_u.b from side_join_t
                join side_join_u on side_join_t.k in (select side_join_s.k from side_join_s
                        where side_join_s.g = side_join_t.g)
                    and side_join_u.b = (select side_join_v.c from side_join_v
                        where side_join_v.h = side_join_u.h)
                order by side_join_t.k;""")
        contains("isMarkJoin=false")
        notContains("isMarkJoin=true")
    }

    // =====================================================================
    // same-conjunct apply-order regression: a higher scalar apply in the SAME conjunct
    // must fence a lower mark join. within one filter conjunct such as
    //   nvl(nvl(o.k in (select ...), false) and o.x = (select ...), false)
    // the applies are stacked in the conjunct's subquery order: the IN apply is built BELOW
    // the correlated scalar apply. eliminating the IN mark join would turn it into a
    // left-semi join that discards the only outer row whose scalar group has multiple
    // results BEFORE the later Count/AssertTrue runs, so the query would return instead of
    // raising 'correlate scalar subquery must return only 1 row'. the later-conjunct fence
    // does not cover this same-index shape, so the evaluation domain is resolved per target:
    // the target's own and already-lower applies are excluded while subsequent same-conjunct
    // applies' generated assertions are included, which keeps the IN mark join
    // (isMarkJoin=true) and the error is raised.
    sql "drop table if exists same_conj_o"
    sql "drop table if exists same_conj_i"
    sql "drop table if exists same_conj_s"
    sql """create table same_conj_o (k bigint, g bigint, x bigint) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """create table same_conj_i (k bigint, g bigint) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """create table same_conj_s (v bigint, g bigint) DUPLICATE KEY(v)
            DISTRIBUTED BY HASH(v) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """insert into same_conj_o values (1,10,7),(2,20,9);"""
    sql """insert into same_conj_i values (2,20);"""
    sql """insert into same_conj_s values (100,10),(101,10),(200,20);"""
    explain {
        sql("""analyzed plan select same_conj_o.k from same_conj_o
                where nvl(nvl(same_conj_o.k in (select same_conj_i.k from same_conj_i
                            where same_conj_i.g = same_conj_o.g), false)
                    and same_conj_o.x = (select same_conj_s.v from same_conj_s
                            where same_conj_s.g = same_conj_o.g), false)
                order by same_conj_o.k;""")
        contains("isMarkJoin=true")
    }
    test {
        sql """select same_conj_o.k from same_conj_o
                where nvl(nvl(same_conj_o.k in (select same_conj_i.k from same_conj_i
                            where same_conj_i.g = same_conj_o.g), false)
                    and same_conj_o.x = (select same_conj_s.v from same_conj_s
                            where same_conj_s.g = same_conj_o.g), false)
                order by same_conj_o.k;"""
        exception "correlate scalar subquery must return only 1 row"
    }

    // =====================================================================
    // downstream-reachability regression: only subquery plans that are actually DOWNSTREAM
    // of the target may fence its elimination. in the join path the two conjuncts' applies
    // live on opposite children of the join, so the right scalar's subquery plan (with
    // assert_true inside it) is in an independent subtree: eliminating the left IN mark join
    // cannot skip it, so the left mark join is eliminated (the analyzed plan contains no
    // isMarkJoin=true and the left IN conjunct is replaced by TRUE). before this fix the
    // plan collection scanned every non-current subquery plan regardless of the join child,
    // so the right scalar's assert_true fenced the left IN (isMarkJoin=true), retaining the
    // marker apply plus its join-reordering, runtime-filter, selectivity and exploration
    // barriers for no safety gain.
    sql "drop table if exists side_plan_t"
    sql "drop table if exists side_plan_s"
    sql "drop table if exists side_plan_u"
    sql "drop table if exists side_plan_v"
    sql """create table side_plan_t (k bigint, g bigint) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """create table side_plan_s (k bigint, g bigint) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """create table side_plan_u (b bigint, h bigint) DUPLICATE KEY(b)
            DISTRIBUTED BY HASH(b) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """create table side_plan_v (c bigint, h bigint) DUPLICATE KEY(c)
            DISTRIBUTED BY HASH(c) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    explain {
        sql("""analyzed plan select side_plan_t.k, side_plan_u.b from side_plan_t
                join side_plan_u on side_plan_t.k in (select side_plan_s.k from side_plan_s
                        where side_plan_s.g = side_plan_t.g)
                    and side_plan_u.b = (select side_plan_v.c from side_plan_v
                        where side_plan_v.h = side_plan_u.h
                          and assert_true(side_plan_v.c > 0, 'bad'))
                order by side_plan_t.k;""")
        notContains("isMarkJoin=true")
    }

    // =====================================================================
    // same-physical-join-child regression: the fence must compare the EFFECTIVE PHYSICAL
    // CHILDREN of the join, not the RelatedInfo enum identity. in JOIN_SUBQUERY_TO_APPLY
    // only RelatedToLeft maps to the left child; Unrelated and RelatedToRight both map to
    // the right child. for an inner-join ON conjunct list ordered as an uncorrelated EXISTS
    // (select 1 from the empty side_phys_e) followed by an output-used right-correlated
    // scalar
    //     join u on exists (select 1 from side_phys_e)
    //            and u.b = (select v.c from v where v.h = u.h)
    // both applies are built on the RIGHT child with the scalar's apply ABOVE the EXISTS
    // apply. the pre-fix check compared enum identity (Unrelated != RelatedToRight), so it
    // treated them as opposite sides, skipped the scalar's generated count-assertion, and
    // eliminated the EXISTS marker: the marker-free EXISTS became a cross join with
    // Limit(1, empty side_phys_e), removed every right row below the scalar apply, and
    // suppressed the duplicate-group cardinality error. comparing physical sides keeps the
    // EXISTS mark join (isMarkJoin=true) and the error is raised.
    sql "drop table if exists side_phys_t"
    sql "drop table if exists side_phys_u"
    sql "drop table if exists side_phys_v"
    sql "drop table if exists side_phys_e"
    sql """create table side_phys_t (k bigint) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """create table side_phys_u (b bigint, h bigint) DUPLICATE KEY(b)
            DISTRIBUTED BY HASH(b) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """create table side_phys_v (c bigint, h bigint) DUPLICATE KEY(c)
            DISTRIBUTED BY HASH(c) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """create table side_phys_e (e bigint) DUPLICATE KEY(e)
            DISTRIBUTED BY HASH(e) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """insert into side_phys_t values (1),(2);"""
    sql """insert into side_phys_u values (10,10),(20,20);"""
    sql """insert into side_phys_v values (100,10),(101,10),(200,20);"""
    // side_phys_e is intentionally left EMPTY: eliminating the EXISTS mark join would turn
    // it into a cross join with Limit(1, empty side_phys_e) that discards every right row
    explain {
        sql("""analyzed plan select side_phys_t.k, side_phys_u.b from side_phys_t
                join side_phys_u on exists (select 1 from side_phys_e)
                    and side_phys_u.b = (select side_phys_v.c from side_phys_v
                        where side_phys_v.h = side_phys_u.h)
                order by side_phys_t.k;""")
        contains("isMarkJoin=true")
    }
    test {
        sql """select side_phys_t.k, side_phys_u.b from side_phys_t
                join side_phys_u on exists (select 1 from side_phys_e)
                    and side_phys_u.b = (select side_phys_v.c from side_phys_v
                        where side_phys_v.h = side_phys_u.h)
                order by side_phys_t.k;"""
        exception "correlate scalar subquery must return only 1 row"
    }

    // =====================================================================
    // stale-correlation null-aware NOT IN regression: `ifnull(o.k not in (select s.v
    // from s where s.g = o.g or true), false)` records the outer slot o.g during analysis,
    // then normalization folds `s.g = o.g or true` to `true`, leaving a STALE correlation
    // slot on the apply (isCorrelated()=true but no correlation filter). the mark join is
    // eliminated (Pair.second) and InApplyToJoin must select the NULL_AWARE_LEFT_ANTI_JOIN
    // based on the EFFECTIVE correlation (a present correlation filter), not the stale slot:
    // an ordinary anti join would emit the row even though s.v contains NULL and the NOT IN
    // therefore evaluates to NULL, which the ifnull predicate must reject. both stale_o rows
    // have a NULL in the effective build set {1, NULL}, so the correct result is empty (the
    // pre-fix plan returned both rows).
    sql "drop table if exists stale_o"
    sql "drop table if exists stale_s"
    sql """create table stale_o (k bigint, g bigint) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """create table stale_s (v bigint null, g bigint) DUPLICATE KEY(v)
            DISTRIBUTED BY HASH(v) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """insert into stale_o values (5,10),(7,20);"""
    sql """insert into stale_s values (1,10),(null,10);"""
    qt_stale_null_notin """select stale_o.k from stale_o
            where ifnull(stale_o.k not in (select stale_s.v from stale_s
                    where stale_s.g = stale_o.g or true), false)
            order by stale_o.k;"""

    // =====================================================================
    // empty-EXISTS subtree-elimination regression: in
    //   nvl(o.x = (select s.x from s) and exists (select 1 where false), false)
    // the preorder stacks the uncorrelated scalar apply BELOW the higher empty EXISTS
    // apply. the scalar is lowered to CrossJoin(o, LogicalAssertNumRows(s)) and the
    // EXISTS marker is eliminated, leaving a non-mark CROSS join with an empty right
    // side. EliminateEmptyRelation must NOT replace that join with an empty relation,
    // because doing so deletes the lower scalar's LogicalAssertNumRows cardinality check
    // and silently returns empty instead of raising the
    // 'correlate scalar subquery must return only 1 row' error when s has multiple rows.
    sql "drop table if exists empty_scalar_o"
    sql "drop table if exists empty_scalar_s"
    sql """create table empty_scalar_o (k bigint, x bigint) DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """create table empty_scalar_s (x bigint) DUPLICATE KEY(x)
            DISTRIBUTED BY HASH(x) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """insert into empty_scalar_o values (1,10);"""
    sql """insert into empty_scalar_s values (1),(2);"""
    explain {
        sql("""shape plan select empty_scalar_o.x from empty_scalar_o
                where nvl(empty_scalar_o.x = (select empty_scalar_s.x from empty_scalar_s)
                        and exists (select 1 where false), false)
                order by empty_scalar_o.x;""")
        contains("AssertNumRows")
    }
    test {
        sql """select empty_scalar_o.x from empty_scalar_o
                where nvl(empty_scalar_o.x = (select empty_scalar_s.x from empty_scalar_s)
                        and exists (select 1 where false), false)
                order by empty_scalar_o.x;"""
        exception "Expected EQ 1 to be returned by expression"
    }

    // =====================================================================
    // clean EXISTS marker elimination (positive oracles): a bare EXISTS conjunct is
    // extracted into its own conjunct and never creates a mark slot, so it is wrapped in
    // ifnull(exists, false) — visitExists then creates the marker (the conjunct is a
    // compound holding a subquery) and the mark-join elimination (Pair.second, the ifnull
    // wrapper makes NULL and FALSE of the mark indistinguishable) drops it, leaving
    // ExistsApplyToJoin to lower the marker-free apply to a plain semi/anti/cross join.
    // these are the positive counterparts of the marker-RETAINED EXISTS regressions above:
    // disabling EXISTS elimination would leave every changed oracle green, so the
    // marker-absent (notContains isMarkJoin=true) plus result oracles here catch it.
    // both non-empty and empty inner sides are covered.
    sql "drop table if exists emi_t1"
    sql "drop table if exists emi_t3"
    sql """create table emi_t1 (id int not null, score int null) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """create table emi_t3 (id int not null, score int null) DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1');"""
    sql """insert into emi_t1 values (1,10),(2,20),(3,30);"""
    sql """insert into emi_t3 values (1,10),(3,30),(4,40);"""
    explain {
        sql("""analyzed plan select emi_t1.id from emi_t1
                where ifnull(exists (select 1 from emi_t3 where emi_t3.score = emi_t1.score), false)
                    and emi_t1.id > 0
                order by emi_t1.id;""")
        notContains("isMarkJoin=true")
    }
    qt_emi_corr_exists """select emi_t1.id from emi_t1
            where ifnull(exists (select 1 from emi_t3 where emi_t3.score = emi_t1.score), false)
                and emi_t1.id > 0
            order by emi_t1.id;"""
    qt_emi_uncorr_exists """select emi_t1.id from emi_t1
            where ifnull(exists (select 1 from emi_t3), false) and emi_t1.id > 0
            order by emi_t1.id;"""
    qt_emi_corr_not_exists """select emi_t1.id from emi_t1
            where ifnull(not exists (select 1 from emi_t3 where emi_t3.score = emi_t1.score), false)
                and emi_t1.id > 0
            order by emi_t1.id;"""
    qt_emi_uncorr_not_exists """select emi_t1.id from emi_t1
            where ifnull(not exists (select 1 from emi_t3), false) and emi_t1.id > 0
            order by emi_t1.id;"""
    sql "truncate table emi_t3;"
    qt_emi_empty_corr_exists """select emi_t1.id from emi_t1
            where ifnull(exists (select 1 from emi_t3 where emi_t3.score = emi_t1.score), false)
                and emi_t1.id > 0
            order by emi_t1.id;"""
    qt_emi_empty_uncorr_exists """select emi_t1.id from emi_t1
            where ifnull(exists (select 1 from emi_t3), false) and emi_t1.id > 0
            order by emi_t1.id;"""
    qt_emi_empty_corr_not_exists """select emi_t1.id from emi_t1
            where ifnull(not exists (select 1 from emi_t3 where emi_t3.score = emi_t1.score), false)
                and emi_t1.id > 0
            order by emi_t1.id;"""
    qt_emi_empty_uncorr_not_exists """select emi_t1.id from emi_t1
            where ifnull(not exists (select 1 from emi_t3), false) and emi_t1.id > 0
            order by emi_t1.id;"""
}
