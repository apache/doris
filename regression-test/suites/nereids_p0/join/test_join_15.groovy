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

suite("test_join_15", "nereids_p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql 'set parallel_fragment_exec_instance_num = 2;'
    sql "use nereids_test_query_db"

    def tbName1 = "test"
    def tbName2 = "baseall"
    def tbName3 = "bigtable"
    def empty_name = "empty"

    List selected = ["a.k1, b.k1, a.k2, b.k2, a.k3, b.k3", "count(a.k1), count(b.k1), count(a.k2), count(b.k2), count(*)"]
    List join_types = ["inner", "left outer", "right outer", ""]

    // complex join
    String col = "k1"
    for (t in join_types){
        qt_complex_join1"""select count(a.k1), count(b.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from ${tbName2} where k1 < 5 
                    order by k1) a ${t} join (select k1, k2, k6 from ${tbName2} where k1 < 5 
                    order by k1) b on (a.${col} = b.${col})"""
    }

    def res75 = sql"""select count(a.k1), count(b.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from ${tbName2} where k1 < 5 
            order by k1) a full outer join (select k1, k2, k6 from ${tbName2} where k1 < 5 
            order by k1) b on (a.k1 = b.k1)"""
    def res76 = sql"""select count(c.k1), count(c.m1), count(*) from 
            (select distinct a.*, b.* from (select k1 + 2 as m1, k2 + 1000 as m2, k6 as m6 
            from ${tbName2} where k1 < 5 order by k1) a left outer join 
            (select k1, k2, k6 from ${tbName2} where k1 < 5 order by k1) b on (a.m1 = b.k1) 
            union (select distinct a.*, b.* from 
            (select k1 + 2 as m1, k2 + 1000 as m2, k6 as m6 from ${tbName2} where k1 < 5 
            order by k1) a right outer join (select k1, k2, k6 from ${tbName2} where k1 < 5 
            order by k1) b on (a.m1 = b.k1))) c"""
    check2_doris(res75, res76)

    def res77 = sql"""select count(a.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from ${tbName2} where k1 < 5 
            order by k1) a left semi join (select k1, k2, k6 from ${tbName2} where k1 < 5 
            order by k1) b on (a.k1 = b.k1)"""
    def res78 = sql"""select count(a.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from ${tbName2} where k1 < 5 
            order by k1) a left outer join (select k1, k2, k6 from ${tbName2} where k1 < 5 
            order by k1) b on (a.k1 = b.k1) where b.k1 is not null """
    check2_doris(res77, res78)

    def res79 = sql"""select count(b.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from ${tbName2} where k1 < 5 
            order by k1) a right semi join (select k1, k2, k6 from ${tbName2} where k1 < 5 
            order by k1) b on (a.k1 = b.k1) """
    def res80 = sql"""select count(b.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from ${tbName2} where k1 < 5 
            order by k1) a right outer join (select k1, k2, k6 from ${tbName2} where k1 < 5 
            order by k1) b on (a.k1 = b.k1) where a.k1 is not null"""
    check2_doris(res79, res80)

    def res81 = sql"""select count(a.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from ${tbName2} where k1 < 5 
            order by k1) a left anti join (select k1, k2, k6 from ${tbName2} where k1 < 5 
            order by k1) b on (a.k1 = b.k1)"""
    def res82 = sql"""select count(a.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from ${tbName2} where k1 < 5 
            order by k1) a left outer join (select k1, k2, k6 from ${tbName2} where k1 < 5 
            order by k1) b on (a.k1 = b.k1) where b.k1 is null"""
    check2_doris(res81, res82)

    def res83 = sql"""select count(b.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from ${tbName2} where k1 < 5 
            order by k1) a right anti join (select k1, k2, k6 from ${tbName2} where k1 < 5 
            order by k1) b on (a.k1 = b.k1) """
    def res84 = sql"""select count(b.k1), count(*) from (select k1 + 2 as k1, k2 + 1000 as k2, k6 from ${tbName2} where k1 < 5 
            order by k1) a right outer join (select k1, k2, k6 from ${tbName2} where k1 < 5 
            order by k1) b on (a.k1 = b.k1) where a.k1 is null"""
    check2_doris(res83, res84)

    // join multi table
    String null_name = "nullable_15"
    sql"drop view if exists ${null_name}"
    sql"""create view ${null_name}(n1, n2) as select a.k1, b.k2 from baseall 
            a left join bigtable b on a.k1 = b.k1 + 10 where b.k2 is null"""


    for (t in join_types){
        qt_join_multi_table1"""select * from ${tbName2} a ${t} join ${null_name} b on a.k1 = b.n1 order by 
                    a.k1, b.n1"""
        qt_join_multi_table2"""select * from ${tbName2} a ${t} join ${null_name} b on a.k1 = b.n2 order by 
                    a.k1, b.n1"""
    }
    test {
        sql"""select a.k1, a.k2 from ${tbName2} a left semi join ${null_name} b on a.k1 = b.n2 
            order by a.k1"""
        check{result, exception, startTime, endTime ->
            assertTrue(result.isEmpty())
        }
    }
    test {
        sql"""select b.n1, b.n2 from ${tbName2} a right semi join ${null_name} b on a.k1 = b.n2 
           order by b.n1"""
        check{result, exception, startTime, endTime ->
            assertTrue(result.isEmpty())
        }
    }
    test {
        sql"""select b.k1, b.k2 from ${null_name} a right semi join ${tbName2} b on b.k1 = a.n2 
           order by b.k1"""
        check{result, exception, startTime, endTime ->
            assertTrue(result.isEmpty())
        }
    }
    test {
        sql"""select a.n1, a.n2 from ${null_name} a left semi join ${tbName2} b on b.k1 = a.n2 
           order by 1, 2"""
        check{result, exception, startTime, endTime ->
            assertTrue(result.isEmpty())
        }
    }

    def res85 = sql"""select a.k1, a.k2 from ${tbName2} a left anti join ${null_name} b on a.k1 = b.n2 
           order by 1, 2"""
    def res86 = sql"""select k1, k2 from ${tbName2} order by k1, k2"""
    check2_doris(res85, res86)

    def res87 = sql"""select b.n1, b.n2 from ${tbName2} a right anti join ${null_name} b on a.k1 = b.n2 
            order by 1, 2"""
    def res88 = sql"""select n1, n2 from ${null_name} order by n1, n2"""
    check2_doris(res87, res88)

    def res89 = sql"""select b.k1, b.k2 from ${null_name} a right anti join ${tbName2} b on b.k1 = a.n2 
           order by 1, 2"""
    def res90 = sql"""select k1, k2 from ${tbName2} order by k1, k2"""
    check2_doris(res89, res90)

    // join on predicate
    qt_join_on_predicate1"""select c.k1 from ${tbName2} a join ${tbName1} b on a.k2 between 0 and 1000 
            join ${tbName3} c on a.k10 = c.k10 order by k1 limit 65535"""
    qt_join_on_predicate2"""select a.k1 from baseall a join test b on b.k2 between 0 and 1000 and a.k1 = b.k1 order by k1;"""
    qt_join_on_predicate3"""select a.k1 from baseall a join test b on b.k2 between 0 and 1000 order by k1;"""
    qt_join_on_predicate4"""select a.k1 from baseall a join test b on b.k2 in (49, 60, 85) order by k1;"""
    qt_join_on_predicate5"""select a.k1 from baseall a join test b on b.k2 in (49, 60, 85) and a.k1 = b.k1 order by k1"""
    qt_join_on_predicate6"""select count(a.k1) from baseall a join test b on a.k1 < 10 and a.k1 = b.k1"""
    qt_join_on_predicate7"""SELECT t2.k1,t2.k2,t3.k1,t3.k2 FROM baseall t2 LEFT JOIN test t3 ON t2.k2=t3.k2 WHERE t2.k1 = 4 OR (t2.k1 > 4 AND t3.k1 IS NULL) order by 1, 2, 3, 4"""



    // <=> test cases
    qt_join41"""select 1 <=> 2, 1 <=> 1, "a"= \"a\""""
    qt_join42"""select 1 <=> null, null <=> null,  not("1" <=> NULL)"""
    def res93 = sql"""select  cast("2019-09-09" as int) <=> NULL, cast("2019" as int) <=> NULL"""
    def res94 = sql"""select  NULL <=> NULL, 2019 <=> NULL """
    check2_doris(res93, res94)

    def res95 = sql"""select (2019+10) <=> NULL, not (2019+10) <=> NULL, ("1"+"2") <=> NULL"""
    def res96 = sql"""select  2029 <=> NULL, not 2029 <=> NULL, 3 <=> NULL"""
    check2_doris(res95, res96)

    qt_join43"""select 2019 <=> NULL and NULL <=> NULL, NULL <=> NULL and NULL <=> NULL, 
       2019 <=> NULL or NULL <=> NULL"""


    // <=> in join test case
    String null_table_1 = "join_null_safe_equal_1"
    String null_table_2 = "join_null_safe_equal_2"
    sql"""drop table if exists ${null_table_1}"""
    sql"""drop table if exists ${null_table_2}"""
    sql"""create table if not exists ${null_table_1} (k1 tinyint, k2 decimal(9,3) NULL, k3 char(5) NULL,
                    k4 date NULL, k5 datetime NULL, 
                    k6 double sum) engine=olap 
                    distributed by hash(k1) buckets 2 properties("storage_type"="column", "replication_num" = "1")"""
    sql"""create table if not exists ${null_table_2} (k1 tinyint, k2 decimal(9,3) NULL, k3 char(5) NULL,
                    k4 date NULL, k5 datetime NULL, 
                    k6 double sum) engine=olap 
                    distributed by hash(k1) buckets 2 properties("storage_type"="column", "replication_num" = "1")"""
    sql"""insert into ${null_table_1} values (1, NULL,'null', NULL, NULL, 8.9),
                    (2, NULL,'2', NULL, NULL, 8.9),
                    (3, NULL,'null', '2019-09-09', NULL, 8.9);"""
    sql"""insert into ${null_table_2} values (1, NULL,'null', NULL, NULL, 8.9),
                    (2, NULL,'2', NULL, NULL, 8.9),
                    (3, NULL,'null', '2019-09-09', NULL, 8.9);"""
    sql"""insert into ${null_table_1} values (5, NULL,"null", NULL, "2019-09-09 00:00:00", 8.9)"""
    qt_join44"""select k1<=>NULL, k2<=>NULL, k4<=>NULL, k5<=>NULL, k6<=>NULL
      from ${null_table_1} order by k1, k2, k4, k5, k6"""
    for (index in range(1, 7)) {
        qt_left_join"""select * from ${null_table_1} a left join ${null_table_1} b on  a.k${index}<=>b.k${index} 
            order by a.k1, b.k1"""
        qt_right_join"""select * from ${null_table_1} a right join ${null_table_1} b on  a.k${index}<=>b.k${index}
            order by a.k1, b.k1"""
        qt_hash_right_join"""select * from ${null_table_1} a right join ${null_table_1} b on a.k${index}<=>b.k${index} and a.k2=b.k2
            order by a.k1, b.k1"""
        qt_hash_left_join"""select * from ${null_table_1} a left join ${null_table_1} b on a.k${index}<=>b.k${index} and a.k2=b.k2
            order by a.k1, b.k1"""
        qt_hash_inner_join"""select * from ${null_table_1} a inner join ${null_table_1} b on a.k${index}<=>b.k${index} and a.k2=b.k2
            order by a.k1, b.k1"""
        qt_cross_join"""select * from ${null_table_1} a right join ${null_table_1} b on  a.k${index}<=>b.k${index} and a.k2 !=b.k2
            order by a.k1, b.k1"""
        qt_cross_join"""select * from ${null_table_1} a right join ${null_table_1} b on  a.k${index}<=>b.k${index} and a.k1 > b.k1
            order by a.k1, b.k1"""
    }

    qt_left_join_with_other_conjunct """
        select * from join_null_safe_equal_1 a left join join_null_safe_equal_1 b on  b.k3<=>a.k3 and b.k1 > a.k1 order by a.k1, b.k1;
    """
    //  windows
     def res97 = sql"""select * from (select k1, k2, sum(k2) over (partition by k1) as ss from ${null_table_2})a
        left join ${null_table_1} b on  a.k2=b.k2 and a.k1 >b.k1 order by a.k1, b.k1"""
    def res98 = sql"""select * from (select k1, k2, k5 from ${null_table_2}) a left join ${null_table_1} b
      on  a.k2=b.k2 and a.k1 >b.k1 order by a.k1, b.k1"""
     check2_doris(res97, res98)
}
