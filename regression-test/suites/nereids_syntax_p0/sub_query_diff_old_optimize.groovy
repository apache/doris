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
suite ("sub_query_diff_old_optimize") {
    // enable nereids and vectorized engine

    sql """
        SET enable_nereids_planner=true
    """

    sql """
        DROP TABLE IF EXISTS `sub_query_diff_old_optimize_subquery1`
    """

    sql """
        DROP TABLE IF EXISTS `sub_query_diff_old_optimize_subquery2`
    """

    sql """
        DROP TABLE IF EXISTS `sub_query_diff_old_optimize_subquery3`
    """

    sql """
        DROP TABLE IF EXISTS `sub_query_diff_old_optimize_subquery4`
    """

    sql """
        DROP TABLE IF EXISTS `sub_query_diff_old_optimize_subquery5`
    """

    sql """
        create table if not exists sub_query_diff_old_optimize_subquery1
        (k1 bigint, k2 bigint)
        duplicate key(k1)
        distributed by hash(k2) buckets 1
        properties('replication_num' = '1') 
    """

    sql """
        create table if not exists sub_query_diff_old_optimize_subquery2
        (k1 varchar(10), k2 bigint)
        partition by range(k2)
        (partition p1 values less than("10"))
        distributed by hash(k2) buckets 1
        properties('replication_num' = '1')
    """

    sql """
        create table if not exists sub_query_diff_old_optimize_subquery3
        (k1 int not null, k2 varchar(128), k3 bigint, v1 bigint, v2 bigint)
        distributed by hash(k2) buckets 1
        properties('replication_num' = '1')
    """

    sql """
        create table if not exists sub_query_diff_old_optimize_subquery4
        (k1 bigint, k2 bigint)
        duplicate key(k1)
        distributed by hash(k2) buckets 1
        properties('replication_num' = '1')
    """

    sql """
        create table if not exists sub_query_diff_old_optimize_subquery5
        (tm varchar(20))
        duplicate key(tm)
        distributed by hash(tm) buckets 1
        properties('replication_num' = '1')
    """

    sql """
        insert into sub_query_diff_old_optimize_subquery1 values (1,2), (1,3), (2,4), (2,5), (3,3), (3,4), (20,2), (22,3), (24,4)
    """

    sql """
        insert into sub_query_diff_old_optimize_subquery2 values ("abc",2),("abc",3),("abcd",2),("abcde",4),("abcdef",5)
    """

    sql """
        insert into sub_query_diff_old_optimize_subquery3 values (1,"abc",2,3,4), (1,"abcd",3,3,4), (2,"xyz",2,4,2),
                                     (2,"uvw",3,4,2), (2,"uvw",3,4,2), (3,"abc",4,5,3), (3,"abc",4,5,3)
    """

    sql """
        insert into sub_query_diff_old_optimize_subquery4 values (5,4), (5,2), (8,3), (5,4), (6,7), (8,9)
    """

    sql """
        insert into sub_query_diff_old_optimize_subquery5 values ('a')
    """

    sql "SET enable_fallback_to_original_planner=false"

    test {
        sql """
            select * from sub_query_diff_old_optimize_subquery1 where sub_query_diff_old_optimize_subquery1.k1 < (select sum(sub_query_diff_old_optimize_subquery3.k3) from sub_query_diff_old_optimize_subquery3 where sub_query_diff_old_optimize_subquery3.v2 < sub_query_diff_old_optimize_subquery1.k2) order by k1, k2
        """
        exception "java.sql.SQLException: errCode = 2, detailMessage = scalar subquery's correlatedPredicates's operator must be EQ"
    }

    test {
        sql """
            select * from sub_query_diff_old_optimize_subquery1 where sub_query_diff_old_optimize_subquery1.k1 != (select sum(sub_query_diff_old_optimize_subquery3.k3) from sub_query_diff_old_optimize_subquery3 where sub_query_diff_old_optimize_subquery3.v2 != sub_query_diff_old_optimize_subquery1.k2) order by k1, k2
        """
        exception "java.sql.SQLException: errCode = 2, detailMessage = scalar subquery's correlatedPredicates's operator must be EQ"
    }

    test {
        sql """
            select * from sub_query_diff_old_optimize_subquery1 where sub_query_diff_old_optimize_subquery1.k1 = (select sum(sub_query_diff_old_optimize_subquery3.k3) from sub_query_diff_old_optimize_subquery3 where sub_query_diff_old_optimize_subquery3.v2 > sub_query_diff_old_optimize_subquery1.k2) order by k1, k2
        """
        exception "java.sql.SQLException: errCode = 2, detailMessage = scalar subquery's correlatedPredicates's operator must be EQ"
    }

    test {
        sql """
            select * from sub_query_diff_old_optimize_subquery1
                where k1 = (select sum(k1) from sub_query_diff_old_optimize_subquery3 where sub_query_diff_old_optimize_subquery1.k1 != sub_query_diff_old_optimize_subquery3.v1 and sub_query_diff_old_optimize_subquery3.v2 = 2)
                order by k1, k2
        """
        exception "java.sql.SQLException: errCode = 2, detailMessage = scalar subquery's correlatedPredicates's operator must be EQ"
    }

    //----------with subquery alias----------
    //----------remove temporarily-----------
    qt_alias_scalar """
        select * from sub_query_diff_old_optimize_subquery1
            where sub_query_diff_old_optimize_subquery1.k1 < (select max(aa) from
                (select k1 as aa from sub_query_diff_old_optimize_subquery3 where sub_query_diff_old_optimize_subquery1.k2 = sub_query_diff_old_optimize_subquery3.v2) sub_query_diff_old_optimize_subquery3) order by k1, k2
    """

    qt_alias_in """
        select * from sub_query_diff_old_optimize_subquery1
            where sub_query_diff_old_optimize_subquery1.k1 in (select aa from
                (select k1 as aa from sub_query_diff_old_optimize_subquery3 where sub_query_diff_old_optimize_subquery1.k2 = sub_query_diff_old_optimize_subquery3.v2) sub_query_diff_old_optimize_subquery3) order by k1, k2
    """

    qt_alias_not_in """
        select * from sub_query_diff_old_optimize_subquery1
            where sub_query_diff_old_optimize_subquery1.k1 not in (select aa from
                (select k1 as aa from sub_query_diff_old_optimize_subquery3 where sub_query_diff_old_optimize_subquery1.k2 = sub_query_diff_old_optimize_subquery3.v2) sub_query_diff_old_optimize_subquery3) order by k1, k2
    """

    qt_alias_exist """
        select * from sub_query_diff_old_optimize_subquery1
            where exists (select aa from
                (select k1 as aa from sub_query_diff_old_optimize_subquery3 where sub_query_diff_old_optimize_subquery1.k2 = sub_query_diff_old_optimize_subquery3.v2) sub_query_diff_old_optimize_subquery3) order by k1, k2
    """

    qt_alias_not_exist """
        select * from sub_query_diff_old_optimize_subquery1
            where not exists (select aa from
                (select k1 as aa from sub_query_diff_old_optimize_subquery3 where sub_query_diff_old_optimize_subquery1.k2 = sub_query_diff_old_optimize_subquery3.v2) sub_query_diff_old_optimize_subquery3) order by k1, k2
    """

    qt_alias_issue30264 """
        with cte1 as (select * from sub_query_diff_old_optimize_subquery5) select tm from cte1 group by tm having tm > (select 'Apple');
    """

    //----------subquery with limit----------
    order_qt_exists_subquery_with_limit """
        select * from sub_query_diff_old_optimize_subquery1 where exists (select sub_query_diff_old_optimize_subquery3.k3 from sub_query_diff_old_optimize_subquery3 where sub_query_diff_old_optimize_subquery3.v2 = sub_query_diff_old_optimize_subquery1.k2 limit 1);
    """

    test {
        sql """
            select * from sub_query_diff_old_optimize_subquery1 where sub_query_diff_old_optimize_subquery1.k1 not in (select sub_query_diff_old_optimize_subquery3.k3 from sub_query_diff_old_optimize_subquery3 where sub_query_diff_old_optimize_subquery3.v2 = sub_query_diff_old_optimize_subquery1.k2 limit 1);
        """
        exception "Unsupported correlated subquery with a LIMIT clause LogicalLimit ( limit=1, offset=0, phase=ORIGIN )"

    }

    //----------subquery with order and limit-------
    order_qt_exists_subquery_with_order_and_limit """
        select * from sub_query_diff_old_optimize_subquery1 where exists (select sub_query_diff_old_optimize_subquery3.k3 from sub_query_diff_old_optimize_subquery3 where sub_query_diff_old_optimize_subquery3.v2 = sub_query_diff_old_optimize_subquery1.k2 order by k1 limit 1);
    """

    //----------subquery with disjunctions----------
    test {
        sql """
            SELECT DISTINCT k1 FROM sub_query_diff_old_optimize_subquery1 i1 WHERE ((SELECT count(*) FROM sub_query_diff_old_optimize_subquery1 WHERE ((k1 = i1.k1) AND (k2 = 2)) or ((k2 = i1.k1) AND (k2 = 1)) )  > 0);
        """
        exception "Unsupported correlated subquery with correlated predicate"

    }
}
