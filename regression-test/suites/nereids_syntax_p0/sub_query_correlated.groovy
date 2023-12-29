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

suite ("sub_query_correlated") {
    // enable nereids and vectorized engine

    sql """
        SET enable_nereids_planner=true
    """

    sql """
        DROP TABLE IF EXISTS `sub_query_correlated_subquery1`
    """

    sql """
        DROP TABLE IF EXISTS `sub_query_correlated_subquery2`
    """

    sql """
        DROP TABLE IF EXISTS `sub_query_correlated_subquery3`
    """

    sql """
        DROP TABLE IF EXISTS `sub_query_correlated_subquery4`
    """

    sql """
        DROP TABLE IF EXISTS `sub_query_correlated_subquery5`
    """

    sql """
        DROP TABLE IF EXISTS `sub_query_correlated_subquery6`
    """

    sql """
        DROP TABLE IF EXISTS `sub_query_correlated_subquery7`
    """

    sql """
        DROP TABLE IF EXISTS `sub_query_correlated_subquery8`
    """

    sql """
        DROP TABLE IF EXISTS `sub_query_correlated_subquery9`
    """

    sql """
        DROP TABLE IF EXISTS `sub_query_correlated_subquery10`
    """

    sql """
        create table if not exists sub_query_correlated_subquery1
        (k1 bigint, k2 bigint)
        duplicate key(k1)
        distributed by hash(k2) buckets 1
        properties('replication_num' = '1') 
    """

    sql """
        create table if not exists sub_query_correlated_subquery2
        (k1 varchar(10), k2 bigint)
        partition by range(k2)
        (partition p1 values less than("10"))
        distributed by hash(k2) buckets 1
        properties('replication_num' = '1')
    """

    sql """
        create table if not exists sub_query_correlated_subquery3
        (k1 int not null, k2 varchar(128), k3 bigint, v1 bigint, v2 bigint)
        distributed by hash(k2) buckets 1
        properties('replication_num' = '1')
    """

    sql """
        create table if not exists sub_query_correlated_subquery4
        (k1 bigint, k2 bigint)
        duplicate key(k1)
        distributed by hash(k2) buckets 1
        properties('replication_num' = '1')
    """

    sql """
        create table if not exists sub_query_correlated_subquery5
        (k1 bigint, k2 bigint)
        duplicate key(k1)
        distributed by hash(k2) buckets 1
        properties('replication_num' = '1')
    """

    sql """
        create table if not exists sub_query_correlated_subquery6
        (k1 bigint, k2 bigint)
        duplicate key(k1)
        distributed by hash(k2) buckets 1
        properties('replication_num' = '1')
    """

    sql """
        create table if not exists sub_query_correlated_subquery7
            (k1 int, k2 varchar(128), k3 bigint, v1 bigint, v2 bigint)
            distributed by hash(k2) buckets 1
            properties('replication_num' = '1');
    """

    sql """
        create table if not exists sub_query_correlated_subquery8
        (k1 bigint, k2 bigint)
        duplicate key(k1)
        distributed by hash(k2) buckets 1
        properties('replication_num' = '1')
    """

    sql """
        create table if not exists sub_query_correlated_subquery9
            (k1 int, k2 varchar(128), k3 bigint, v1 bigint, v2 bigint)
            distributed by hash(k2) buckets 1
            properties('replication_num' = '1');
    """

    sql """
        create table if not exists sub_query_correlated_subquery10
            (k1 int, k2 varchar(128), k3 bigint, v1 bigint, v2 bigint)
            distributed by hash(k2) buckets 1
            properties('replication_num' = '1');
    """

    sql """
        insert into sub_query_correlated_subquery1 values (1,2), (1,3), (2,4), (2,5), (3,3), (3,4), (20,2), (22,3), (24,4)
    """

    sql """
        insert into sub_query_correlated_subquery2 values ("abc",2),("abc",3),("abcd",2),("abcde",4),("abcdef",5)
    """

    sql """
        insert into sub_query_correlated_subquery3 values (1,"abc",2,3,4), (1,"abcd",3,3,4), (2,"xyz",2,4,2),
                                     (2,"uvw",3,4,2), (2,"uvw",3,4,2), (3,"abc",4,5,3), (3,"abc",4,5,3)
    """

    sql """
        insert into sub_query_correlated_subquery4 values (5,4), (5,2), (8,3), (5,4), (6,7), (8,9)
    """

    sql """
        insert into sub_query_correlated_subquery5 values (5,4), (5,2), (8,3), (5,4), (6,7), (8,9)
    """

    sql """
        insert into sub_query_correlated_subquery6 values (1,null),(null,1),(1,2), (null,2),(1,3), (2,4), (2,5), (3,3), (3,4), (20,2), (22,3), (24,4),(null,null);
    """

    sql """
        insert into sub_query_correlated_subquery7 values (1,"abc",2,3,4), (1,"abcd",3,3,4), (2,"xyz",2,4,2),
            (2,"uvw",3,4,2), (2,"uvw",3,4,2), (3,"abc",4,5,3), (3,"abc",4,5,3), (null,null,null,null,null);
    """

    sql """
        insert into sub_query_correlated_subquery8 values (1,null),(null,1),(1,2), (null,2),(1,3), (2,4), (2,5), (3,3), (3,4), (20,2), (22,3), (24,4),(null,null);
    """

    sql """
        insert into sub_query_correlated_subquery9 values (1,"abc",2,3,4), (1,"abcd",3,3,4),
            (2,"xyz",2,4,2),(2,"uvw",3,4,2), (2,"uvw",3,4,2), (3,"abc",4,5,3), (3,"abc",4,5,3), (null,null,null,null,null);
    """

    sql "SET enable_fallback_to_original_planner=false"

    //------------------Correlated-----------------
    qt_scalar_less_than_corr """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 < (select sum(sub_query_correlated_subquery3.k3) from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = sub_query_correlated_subquery1.k2) order by k1, k2
    """
    
    qt_scalar_not_equal_corr """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 != (select sum(sub_query_correlated_subquery3.k3) from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = sub_query_correlated_subquery1.k2) order by k1, k2
    """
    
    qt_scalar_equal_to_corr """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 = (select sum(sub_query_correlated_subquery3.k3) from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = sub_query_correlated_subquery1.k2) order by k1, k2
    """
    
    qt_not_in_corr """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 not in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = sub_query_correlated_subquery1.k2) order by k1, k2
    """

    qt_in_subquery_corr """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = sub_query_correlated_subquery1.k2) order by k1, k2
    """
    
    qt_not_exist_corr """
        select * from sub_query_correlated_subquery1 where not exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.v2) order by k1, k2
    """

    qt_exist_corr """
        select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.v2) order by k1, k2
    """
    
    qt_in_with_in_and_scalar """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 in (
             select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where 
                sub_query_correlated_subquery3.k3 in (select sub_query_correlated_subquery4.k1 from sub_query_correlated_subquery4 where sub_query_correlated_subquery4.k1 = 3)
                and sub_query_correlated_subquery3.v2 > (select sum(sub_query_correlated_subquery2.k2) from sub_query_correlated_subquery2 where sub_query_correlated_subquery2.k2 = sub_query_correlated_subquery3.v1)) order by k1, k2
    """
    
    qt_exist_and_not_exist """
        select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.v2)
                               and not exists (select sub_query_correlated_subquery4.k2 from sub_query_correlated_subquery4 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery4.k2) order by k1, k2
    """

    //------------------Correlated----nonEqual-------------------

    qt_not_in_non_equal_corr """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 not in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 > sub_query_correlated_subquery1.k2) order by k1, k2
    """

    qt_in_subquery_non_equal_corr """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 < sub_query_correlated_subquery1.k2) order by k1, k2
    """

    qt_in_subquery_non_equal_corr """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 != sub_query_correlated_subquery1.k2) order by k1, k2
    """

    qt_not_exist_non_equal_corr """
        select * from sub_query_correlated_subquery1 where not exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 != sub_query_correlated_subquery3.v2) order by k1, k2
    """

    qt_exist_non_equal_corr """
        select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 > sub_query_correlated_subquery3.v2) order by k1, k2
    """
    //------------------unCorrelated-----------------
    qt_scalar_unCorrelated """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 < (select sum(sub_query_correlated_subquery3.k3) from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = 2) order by k1, k2
    """

    qt_scalar_equal_to_uncorr """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 = (select sum(sub_query_correlated_subquery3.k3) from sub_query_correlated_subquery3) order by k1, k2
    """

    qt_not_scalar_unCorrelated """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 != (select sum(sub_query_correlated_subquery3.k3) from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = 2) order by k1, k2
    """

    qt_scalar_not_equal_uncorr """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 != (select sum(sub_query_correlated_subquery3.k3) from sub_query_correlated_subquery3) order by k1, k2
    """

    qt_in_unCorrelated """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = 2) order by k1, k2
    """

    qt_in_subquery_uncorr """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3) order by k1, k2
    """

    qt_not_in_unCorrelated """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 not in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = 2) order by k1, k2
    """

    qt_not_in_uncorr """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 not in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3) order by k1, k2
    """

    qt_exist_unCorrelated """
        select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = 2) order by k1, k2
    """

    qt_exist_uncorr """
        select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3) order by k1, k2
    """

    qt_exist_unCorrelated_limit1 """
        select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = 2 limit 1) order by k1, k2
    """

    qt_exist_corr_limit1 """
        select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.v2 limit 1) order by k1, k2
    """

    qt_exist_unCorrelated_limit0 """
        select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = 2 limit 0) order by k1, k2
    """

    qt_exist_corr_limit0 """
        select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.v2 limit 0) order by k1, k2
    """

    qt_exist_unCorrelated_limit1_offset1 """
        select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = 2 limit 1 offset 1) order by k1, k2
    """

    test {
        sql("select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.v2 limit 1 offset 1) order by k1, k2")
        exception "Unsupported correlated subquery with a LIMIT clause with offset > 0"
    }

    qt_exist_unCorrelated_limit0_offset1 """
        select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = 2 limit 0 offset 1) order by k1, k2
    """

    qt_exist_corr_limit0_offset1 """
        select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.v2 limit 0 offset 1) order by k1, k2
    """

    //----------complex subqueries----------
    qt_scalar_subquery1 """
        select * from sub_query_correlated_subquery1
            where k1 = (select sum(k1) from sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k1 = sub_query_correlated_subquery3.v1 and sub_query_correlated_subquery3.v2 = 2)
            order by k1, k2
    """

    qt_scalar_subquery2 """
        SELECT *
        FROM sub_query_correlated_subquery1 t1
        WHERE coalesce(bitand( 
        cast(
            (SELECT sum(k1)
            FROM sub_query_correlated_subquery3 ) AS int), 
            cast(t1.k1 AS int)), 
            coalesce(t1.k1, t1.k2)) is NULL
        ORDER BY  t1.k1, t1.k2;
    """

    qt_in_subquery """
        select * from sub_query_correlated_subquery3
            where (k1 = 1 or k1 = 2 or k1 = 3) and v1 in (select k1 from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.v2 and sub_query_correlated_subquery1.k1 = 3)
            order by k1, k2
    """

    qt_exist_subquery """
        select * from sub_query_correlated_subquery3
            where k1 = 2 and exists (select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 = sub_query_correlated_subquery3.v2 and sub_query_correlated_subquery1.k2 = 4)
            order by k1, k2
    """

    //----------complex nonEqual subqueries----------
    qt_in_subquery """
        select * from sub_query_correlated_subquery3
            where (k1 = 1 or k1 = 2 or k1 = 3) and v1 in (select k1 from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k2 > sub_query_correlated_subquery3.v2 and sub_query_correlated_subquery1.k1 = 3)
            order by k1, k2
    """

    qt_exist_subquery """
        select * from sub_query_correlated_subquery3
            where k1 = 2 and exists (select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 < sub_query_correlated_subquery3.v2 and sub_query_correlated_subquery1.k2 = 4)
            order by k1, k2
    """

    //----------subquery with order----------
    order_qt_scalar_subquery_with_order """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 > (select sum(sub_query_correlated_subquery3.k3) a from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = sub_query_correlated_subquery1.k2 order by a);
    """

    order_qt_in_subquery_with_order """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 not in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = sub_query_correlated_subquery1.k2 order by k2);
    """

    order_qt_in_subquery_mark_with_order """
        select * from sub_query_correlated_subquery6 where sub_query_correlated_subquery6.k1 not in (select sub_query_correlated_subquery7.k3 from sub_query_correlated_subquery7 )  or k1 < 10;
    """

    order_qt_exists_subquery_with_order """
        select * from sub_query_correlated_subquery1 where exists (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = sub_query_correlated_subquery1.k2 order by k2);
    """

    //----------subquery with limit----------
    order_qt_scalar_subquery_with_limit """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 > (select sum(sub_query_correlated_subquery3.k3) a from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = sub_query_correlated_subquery1.k2 limit 1);
    """

    //----------subquery with order and limit----------
    order_qt_scalar_subquery_with_order_and_limit """
        select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 > (select sum(sub_query_correlated_subquery3.k3) a from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 = sub_query_correlated_subquery1.k2 order by a limit 1);
    """

    //---------subquery with Disjunctions-------------
	order_qt_scalar_subquery_with_disjunctions """
        SELECT DISTINCT k1 FROM sub_query_correlated_subquery1 i1 WHERE ((SELECT count(*) FROM sub_query_correlated_subquery1 WHERE ((k1 = i1.k1) AND (k2 = 2)) or ((k1 = i1.k1) AND (k2 = 1)) )  > 0);
    """

    //--------subquery case when-----------
    order_qt_case_when_subquery """
        SELECT CASE
            WHEN (
                SELECT COUNT(*) / 2
                FROM sub_query_correlated_subquery3
            ) > v1 THEN (
                SELECT AVG(v1)
                FROM sub_query_correlated_subquery3
            )
            ELSE (
                SELECT SUM(v2)
                FROM sub_query_correlated_subquery3
            )
            END AS kk4
        FROM sub_query_correlated_subquery3 ;
    """
    
    //---------subquery mark join() Disjunctions------------
    order_qt_in """
        SELECT * FROM sub_query_correlated_subquery1 WHERE k1 IN (SELECT k1 FROM sub_query_correlated_subquery3) OR k1 < 10;
    """

    order_qt_scalar """
        SELECT * FROM sub_query_correlated_subquery1 WHERE k1 > (SELECT AVG(k1) FROM sub_query_correlated_subquery3) OR k1 < 10;
    """

    order_qt_exists_true """
        SELECT * FROM sub_query_correlated_subquery1 WHERE EXISTS (SELECT k1 FROM sub_query_correlated_subquery3 WHERE k1 = 10) OR k1 < 10;
    """

    order_qt_in_exists_false """
        SELECT * FROM sub_query_correlated_subquery1 WHERE EXISTS (SELECT k1 FROM sub_query_correlated_subquery3 WHERE k1 > 10) OR k1 < 10;
    """

    order_qt_hash_join_with_other_conjuncts1 """
        SELECT * FROM sub_query_correlated_subquery1 WHERE k1 IN (SELECT k1 FROM sub_query_correlated_subquery3 WHERE sub_query_correlated_subquery1.k1 > sub_query_correlated_subquery3.k3) OR k1 < 10 ORDER BY k1,k2;
    """

    order_qt_hash_join_with_other_conjuncts2 """
        SELECT * FROM sub_query_correlated_subquery1 WHERE k1 IN (SELECT k1 FROM sub_query_correlated_subquery3 WHERE sub_query_correlated_subquery1.k1 < sub_query_correlated_subquery3.k3) OR k1 < 10 ORDER BY k1,k2;
    """

    order_qt_hash_join_with_other_conjuncts3 """
        SELECT * FROM sub_query_correlated_subquery1 WHERE k1 IN (SELECT k1 FROM sub_query_correlated_subquery3 WHERE sub_query_correlated_subquery1.k1 > sub_query_correlated_subquery3.k3) OR k1 < 11 ORDER BY k1,k2;
    """

    order_qt_hash_join_with_other_conjuncts4 """
        SELECT * FROM sub_query_correlated_subquery1 WHERE k1 IN (SELECT k1 FROM sub_query_correlated_subquery3 WHERE sub_query_correlated_subquery1.k1 < sub_query_correlated_subquery3.k3) OR k1 < 11 ORDER BY k1,k2;
    """

    order_qt_same_subquery_in_conjuncts """
        SELECT * FROM sub_query_correlated_subquery1 WHERE k1 IN (SELECT k1 FROM sub_query_correlated_subquery3) OR k1 IN (SELECT k1 FROM sub_query_correlated_subquery3) OR k1 < 10 ORDER BY k1,k2;
    """

    order_qt_two_subquery_in_one_conjuncts """
        SELECT * FROM sub_query_correlated_subquery1 WHERE k1 IN (SELECT k1 FROM sub_query_correlated_subquery3) OR k1 IN (SELECT k3 FROM sub_query_correlated_subquery3) OR k1 < 10 ORDER BY k1,k2;
    """

    order_qt_multi_subquery_in_and_scalry """
        SELECT * FROM sub_query_correlated_subquery1 WHERE k1 IN (SELECT k1 FROM sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.k1)
                                                     OR k1 < (SELECT sum(k1) FROM sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k1 = sub_query_correlated_subquery3.v1)
                                                     OR k1 < 10 ORDER BY k1;
    """

    order_qt_multi_subquery_in_and_exist """
        SELECT * FROM sub_query_correlated_subquery1 WHERE k1 IN (SELECT k1 FROM sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.k1)
                                                     OR exists (SELECT k1 FROM sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k1 = sub_query_correlated_subquery3.v1)
                                                     OR k1 < 10 ORDER BY k1;
    """

    order_qt_multi_subquery_in_and_exist_sum """
        SELECT * FROM sub_query_correlated_subquery1 WHERE k1 IN (SELECT k1 FROM sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.k1)
                                                     OR exists (SELECT sum(k1) FROM sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k1 = sub_query_correlated_subquery3.v1)
                                                     OR k1 < 10 ORDER BY k1;
    """

    order_qt_multi_subquery_in_and_in """
        SELECT * FROM sub_query_correlated_subquery1 WHERE k1 IN (SELECT k1 FROM sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.k1)
                                                     OR k2 in (SELECT k2 FROM sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k1 = sub_query_correlated_subquery3.v1)
                                                     OR k1 < 10 ORDER BY k1;
    """

    order_qt_multi_subquery_scalar_and_exist """
        SELECT * FROM sub_query_correlated_subquery1 WHERE k1 < (SELECT sum(k1) FROM sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.k1)
                                                     OR exists (SELECT sum(k1) FROM sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k1 = sub_query_correlated_subquery3.v1)
                                                     OR k1 < 10 ORDER BY k1;
    """

    order_qt_multi_subquery_scalar_and_scalar """
        SELECT * FROM sub_query_correlated_subquery1 WHERE k1 < (SELECT sum(k1) FROM sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.k1)
                                                     OR k2 < (SELECT sum(k1) FROM sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k1 = sub_query_correlated_subquery3.v1)
                                                     OR k1 < 10 ORDER BY k1;
    """

    order_qt_multi_subquery_in_first_or_in_and_in """
        SELECT * FROM sub_query_correlated_subquery1 WHERE (k1 in (SELECT k2 FROM sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.k1) 
                                                                or k2 in (SELECT k1 FROM sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.k1))
                                                            and k1 in (SELECT k1 FROM sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.k1)
    """

    order_qt_multi_subquery_in_second_or_in_and_in """
        SELECT * FROM sub_query_correlated_subquery1 WHERE k1 in (SELECT k2 FROM sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.k1) 
                                                           or k2 in (SELECT k1 FROM sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.k1)
                                                           and k1 in (SELECT k1 FROM sub_query_correlated_subquery3 where sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.k1)
    """

    order_qt_multi_subquery_scalar_and_in_or_scalar_and_exists_agg """
        SELECT * FROM sub_query_correlated_subquery1 WHERE ((k1 != (SELECT sum(k1) FROM sub_query_correlated_subquery3) and k1 = 1 OR k1 < 10) and k1 = 10 and k1 = 15)
                                        and (k1 IN (SELECT k1 FROM sub_query_correlated_subquery3 WHERE sub_query_correlated_subquery1.k1 = sub_query_correlated_subquery3.k1)
                                             OR k1 < (SELECT sum(k1) FROM sub_query_correlated_subquery3 WHERE sub_query_correlated_subquery1.k1 = sub_query_correlated_subquery3.k1))
                                        and exists (SELECT sum(k1) FROM sub_query_correlated_subquery3 WHERE sub_query_correlated_subquery1.k1 = sub_query_correlated_subquery3.k1);
    """

    order_qt_multi_subquery_scalar_and_in_or_scalar_and_exists """
        SELECT * FROM sub_query_correlated_subquery1 WHERE ((k1 != (SELECT sum(k1) FROM sub_query_correlated_subquery3) and k1 = 1 OR k1 < 10) and k1 = 10 and k1 = 15)
                                        and (k1 IN (SELECT k1 FROM sub_query_correlated_subquery3 WHERE sub_query_correlated_subquery1.k1 = sub_query_correlated_subquery3.k1)
                                             OR k1 < (SELECT sum(k1) FROM sub_query_correlated_subquery3 WHERE sub_query_correlated_subquery1.k1 = sub_query_correlated_subquery3.k1))
                                        and exists (SELECT k1 FROM sub_query_correlated_subquery3 WHERE sub_query_correlated_subquery1.k1 = sub_query_correlated_subquery3.k1);
    """
    
    //----------type coercion subquery-----------
    qt_cast_subquery_in """
        SELECT * FROM sub_query_correlated_subquery1 WHERE k1 < (cast('1.2' as decimal(2,1)) * (SELECT sum(k1) FROM sub_query_correlated_subquery3 WHERE sub_query_correlated_subquery1.k1 = sub_query_correlated_subquery3.k1)) order by k1, k2;
    """

    qt_cast_subquery_in_with_disconjunct """
        SELECT * FROM sub_query_correlated_subquery1 WHERE k1 < (cast('1.2' as decimal(2,1)) * (SELECT sum(k1) FROM sub_query_correlated_subquery3 WHERE sub_query_correlated_subquery1.k1 = sub_query_correlated_subquery3.k1)) or k1 > 100 order by k1, k2;
    """

    qt_imitate_tpcds_10 """
        SELECT * FROM sub_query_correlated_subquery1 WHERE exists (SELECT * FROM sub_query_correlated_subquery3, sub_query_correlated_subquery2 where sub_query_correlated_subquery1.k1 = sub_query_correlated_subquery3.k1 and sub_query_correlated_subquery2.k1 = sub_query_correlated_subquery3.v1)
                                        and (exists (SELECT * FROM sub_query_correlated_subquery3, sub_query_correlated_subquery4 WHERE sub_query_correlated_subquery1.k1 = sub_query_correlated_subquery3.k1 and sub_query_correlated_subquery3.v1 = sub_query_correlated_subquery4.k1)
                                             OR exists (SELECT * FROM sub_query_correlated_subquery3, sub_query_correlated_subquery5 WHERE sub_query_correlated_subquery1.k2 = sub_query_correlated_subquery3.v1 and sub_query_correlated_subquery3.v1 = sub_query_correlated_subquery5.k1))
    """

    qt_doris_6937 """
    SELECT *
        FROM sub_query_correlated_subquery1
        WHERE EXISTS 
            (SELECT k1
            FROM sub_query_correlated_subquery3
            WHERE sub_query_correlated_subquery1.k1 > sub_query_correlated_subquery3.v1)
                OR k1 < 10
        order by k1, k2;
    """

    qt_mark_join_nullable """
        select sub_query_correlated_subquery8.k1 in (select sub_query_correlated_subquery9.k3 from sub_query_correlated_subquery9) from sub_query_correlated_subquery8 order by k1, k2;
    """

    qt_cir_5218_in_ok """
        SELECT count(*)
        FROM sub_query_correlated_subquery6
        WHERE k1 IN 
            (SELECT k1
            FROM 
                (SELECT k1,
                sum(k3) AS bbb,
                count(k2) AS aaa
                FROM sub_query_correlated_subquery7
                WHERE k1 > 0
                        AND k3 > 0
                GROUP BY  k1 ) y
                WHERE y.aaa>0
                        AND k1>1); 
    """

    qt_cir_5218_exists_ok_1 """
        SELECT count(*)
        FROM sub_query_correlated_subquery6
        WHERE exists 
            (SELECT k1
            FROM 
                (SELECT k1,
                sum(k3) AS bbb,
                count(k2) AS aaa
                FROM sub_query_correlated_subquery7
                WHERE k1 > 0
                        AND k3 > 0
                GROUP BY  k1 ) y
                WHERE y.aaa>0
                        AND k1>1); 
    """

    qt_cir_5218_exists_ok_2 """
        SELECT count(*)
            FROM sub_query_correlated_subquery6
            WHERE exists
                (SELECT k1
                FROM 
                    (SELECT k1
                    FROM sub_query_correlated_subquery7
                    WHERE sub_query_correlated_subquery6.k1 > 7
                    GROUP BY  k1 ) y);
    """

    qt_cir_5218_exists_ok_3 """
        SELECT count(*)
            FROM sub_query_correlated_subquery6
            WHERE exists
                (SELECT k1
                FROM 
                    (SELECT k1
                    FROM sub_query_correlated_subquery7
                    WHERE sub_query_correlated_subquery6.k1 > sub_query_correlated_subquery7.k3
                    GROUP BY  k1 ) y);
    """

    qt_cir_5218_exists_ok_4 """
        SELECT count(*)
            FROM sub_query_correlated_subquery6
            WHERE exists
                (SELECT sum(k3)
                FROM 
                    sub_query_correlated_subquery7
                    WHERE sub_query_correlated_subquery6.k1 > sub_query_correlated_subquery7.k3);
    """

    qt_cir_5218_exists_ok_5 """
        SELECT count(*)
            FROM sub_query_correlated_subquery6
            WHERE exists
                (SELECT sum(k3)
                FROM 
                    sub_query_correlated_subquery10);
    """

    qt_cir_5218_exists_ok_6 """
        SELECT count(*)
            FROM sub_query_correlated_subquery6
            WHERE exists
                (SELECT sum(k3)
                FROM 
                    sub_query_correlated_subquery10 group by k2);
    """

    test {
        sql """
                SELECT count(*)
                    FROM sub_query_correlated_subquery6
                    WHERE k1 IN 
                        (SELECT k1
                        FROM 
                            (SELECT k1,
                            sum(k3) AS bbb,
                            count(k2) AS aaa
                            FROM sub_query_correlated_subquery7
                            WHERE k1 > 0
                                    AND k3 > 0 and sub_query_correlated_subquery6.k1 > 2
                            GROUP BY  k1 ) y
                            WHERE y.aaa>0
                                    AND k1>1); """
        exception "Unsupported correlated subquery with grouping and/or aggregation";
    }

    qt_doris_7643 """
        SELECT sub_query_correlated_subquery6.*
        FROM sub_query_correlated_subquery6
        JOIN sub_query_correlated_subquery7
            ON sub_query_correlated_subquery6.k2 = sub_query_correlated_subquery7.k3
                AND EXISTS 
            (SELECT sub_query_correlated_subquery8.k1
            FROM sub_query_correlated_subquery8 )
                AND sub_query_correlated_subquery6.k2 IN 
            (SELECT sub_query_correlated_subquery8.k2
            FROM sub_query_correlated_subquery8 )
                AND sub_query_correlated_subquery6.k1 IN 
            (SELECT sub_query_correlated_subquery8.k2
            FROM sub_query_correlated_subquery8
            WHERE sub_query_correlated_subquery6.k2 = sub_query_correlated_subquery8.k2 )
                AND sub_query_correlated_subquery7.k3 IN 
            (SELECT sub_query_correlated_subquery8.k1
            FROM sub_query_correlated_subquery8 )
                AND 10 > 
            (SELECT min(sub_query_correlated_subquery8.k2)
            FROM sub_query_correlated_subquery8 )
                AND sub_query_correlated_subquery7.k3 IN 
            (SELECT sub_query_correlated_subquery8.k2
            FROM sub_query_correlated_subquery8
            WHERE sub_query_correlated_subquery7.v1 = sub_query_correlated_subquery8.k2 )
        ORDER BY  sub_query_correlated_subquery6.k1, sub_query_correlated_subquery6.k2; 
        """

    // order_qt_doris_6937_2 """
    //     select * from sub_query_correlated_subquery1 where sub_query_correlated_subquery1.k1 not in (select sub_query_correlated_subquery3.k3 from sub_query_correlated_subquery3 where sub_query_correlated_subquery3.v2 > sub_query_correlated_subquery1.k2) or k1 < 10 order by k1, k2;
    // """
    sql """drop table if exists table_21_undef_partitions2_keys3;"""
    sql """drop table if exists table_1_undef_partitions2_keys3;"""
    sql """create table table_21_undef_partitions2_keys3 (
            `col_int_undef_signed` int   ,
            `col_varchar_10__undef_signed` varchar(10)   ,
            `pk` int
            ) engine=olap
            distributed by hash(pk) buckets 10
            properties('replication_num' = '1');"""
    sql """create table table_1_undef_partitions2_keys3 (
            `col_int_undef_signed` int   ,
            `col_varchar_10__undef_signed` varchar(10)   ,
            `pk` int
            ) engine=olap
            distributed by hash(pk) buckets 10
            properties('replication_num' = '1');"""
    sql """insert into table_21_undef_partitions2_keys3(pk,col_int_undef_signed,col_varchar_10__undef_signed) values (0,null,null),(1,6,'c'),(2,7,'m'),(3,null,null),(4,null,'b'),(5,4,null),(6,3,null),(7,0,'z'),(8,null,"me"),(9,6,null),(10,0,""),(11,null,'e'),(12,null,"up"),(13,null,""),(14,7,'s'),(15,null,""),(16,3,null),(17,null,""),(18,null,""),(19,4,""),(20,7,null);"""
    sql """insert into table_1_undef_partitions2_keys3(pk,col_int_undef_signed,col_varchar_10__undef_signed) values (0,null,null),(100,null,null);"""

    qt_select_exists1 """SELECT *
                            FROM table_1_undef_partitions2_keys3 AS t1
                            WHERE EXISTS (
                                    SELECT SUM(`pk`)
                                    FROM table_21_undef_partitions2_keys3 AS t2
                                    WHERE t1.pk = t2.pk and t1.pk = 9
                                ) order by t1.pk;"""
    qt_select_exists2 """SELECT *
                            FROM table_1_undef_partitions2_keys3 AS t1
                            WHERE not EXISTS (
                                    SELECT SUM(`pk`)
                                    FROM table_21_undef_partitions2_keys3 AS t2
                                    WHERE t1.pk = t2.pk and t1.pk = 9
                                ) order by t1.pk;"""
    qt_select_exists3 """SELECT *
                            FROM table_1_undef_partitions2_keys3 AS t1
                            WHERE EXISTS (
                                    SELECT SUM(`pk`)
                                    FROM table_21_undef_partitions2_keys3 AS t2
                                    WHERE t1.pk = 9
                                ) or t1.pk = 100 order by t1.pk;"""
    qt_select_exists4 """SELECT *
                            FROM table_1_undef_partitions2_keys3 AS t1
                            WHERE not EXISTS (
                                    SELECT SUM(`pk`)
                                    FROM table_21_undef_partitions2_keys3 AS t2
                                    WHERE t1.pk = 9
                                ) or t1.pk = 100 order by t1.pk;"""
    qt_select_exists5 """select EXISTS (
                                    SELECT SUM(`pk`)
                                    FROM table_21_undef_partitions2_keys3 AS t2
                                    WHERE t1.pk = t2.pk and t1.pk = 9
                                ) from table_1_undef_partitions2_keys3 AS t1;"""
    qt_select_exists6 """select not EXISTS (
                                    SELECT SUM(`pk`)
                                    FROM table_21_undef_partitions2_keys3 AS t2
                                    WHERE t1.pk = t2.pk and t1.pk = 9
                                ) from table_1_undef_partitions2_keys3 AS t1 order by t1.pk;"""
    qt_select_exists7 """select EXISTS (
                                    SELECT SUM(`pk`)
                                    FROM table_21_undef_partitions2_keys3 AS t2
                                    WHERE t1.pk = t2.pk and t1.pk = 9
                                ) or t1.pk = 100 from table_1_undef_partitions2_keys3 AS t1 order by t1.pk;"""
    qt_select_exists8 """select EXISTS (
                                    SELECT SUM(`pk`)
                                    FROM table_21_undef_partitions2_keys3 AS t2
                                    WHERE t1.pk = t2.pk and t1.pk = 9
                                ) and t1.pk = 100 from table_1_undef_partitions2_keys3 AS t1 order by t1.pk;"""
    qt_select_exists9 """select t1.* from table_1_undef_partitions2_keys3 AS t1 join table_21_undef_partitions2_keys3 AS t2 
                                on t1.pk = t2.pk and not exists ( SELECT SUM(`pk`)
                                    FROM table_21_undef_partitions2_keys3 AS t2
                                    WHERE t1.pk = t2.pk and t1.pk = 9 ) or t1.pk = 100 order by t1.pk;"""
    qt_select_exists10 """select t1.* from table_1_undef_partitions2_keys3 AS t1 join table_21_undef_partitions2_keys3 AS t2 
                                on t1.pk = t2.pk and not exists ( SELECT SUM(`pk`)
                                    FROM table_21_undef_partitions2_keys3 AS t2
                                    WHERE t1.pk = t2.pk and t1.pk = 9 ) or t1.pk > 100 order by t1.pk;"""
    qt_select_exists11 """select t1.* from table_1_undef_partitions2_keys3 AS t1  join table_21_undef_partitions2_keys3 AS t2 
                                on t1.pk = t2.pk and exists ( SELECT SUM(`pk`)
                                    FROM table_21_undef_partitions2_keys3 AS t2
                                    WHERE t1.pk = t2.pk and t1.pk = 9 ) or t1.pk = 100 order by t1.pk;"""
    qt_select_exists12 """select t1.* from table_1_undef_partitions2_keys3 AS t1  join table_21_undef_partitions2_keys3 AS t2 
                                on t1.pk = t2.pk and exists ( SELECT SUM(`pk`)
                                    FROM table_21_undef_partitions2_keys3 AS t2
                                    WHERE t1.pk = t2.pk and t1.pk = 9 ) or t1.pk > 100 order by t1.pk;"""                                                                                                                                            
}
