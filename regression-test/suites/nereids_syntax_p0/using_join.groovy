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

suite("nereids_using_join") {
    sql """DROP TABLE IF EXISTS nereids_using_join_t1"""
    sql """DROP TABLE IF EXISTS nereids_using_join_t2"""
    sql """DROP TABLE IF EXISTS nereids_using_join_t3"""
    sql """DROP TABLE IF EXISTS nereids_using_join_t4"""

    sql """
        CREATE TABLE `nereids_using_join_t1` (
            `c1` int,
            `c2` int,
            `c3` int,
            `c4` array<int>,
            `v1` int
        )
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        CREATE TABLE `nereids_using_join_t2` (
            `c1` int,
            `c2` int,
            `v2` int
        )
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        CREATE TABLE `nereids_using_join_t3` (
            `c1` int,
            `v3` int
        )
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        CREATE TABLE `nereids_using_join_t4` (
            `c1` int,
            `c5` int,
            `v4` int
        )
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """INSERT INTO nereids_using_join_t1 VALUES(1, 1, 1, [1, 2, 3, 4], 11)"""
    sql """INSERT INTO nereids_using_join_t1 VALUES(2, 3, 4, [5, 6, 7, 8], 12)"""
    sql """INSERT INTO nereids_using_join_t2 VALUES(1, 1, 21)"""
    sql """INSERT INTO nereids_using_join_t2 VALUES(1, 2, 22)"""
    sql """INSERT INTO nereids_using_join_t2 VALUES(1, 3, 23)"""
    sql """INSERT INTO nereids_using_join_t3 VALUES(1, 31)"""
    sql """INSERT INTO nereids_using_join_t3 VALUES(2, 32)"""
    sql """INSERT INTO nereids_using_join_t3 VALUES(3, 33)"""
    sql """INSERT INTO nereids_using_join_t3 VALUES(4, 34)"""
    sql """INSERT INTO nereids_using_join_t4 VALUES(1, 1, 41)"""
    sql """INSERT INTO nereids_using_join_t4 VALUES(1, 2, 42)"""
    sql """INSERT INTO nereids_using_join_t4 VALUES(1, 3, 43)"""
    sql """INSERT INTO nereids_using_join_t4 VALUES(1, 4, 44)"""
    sql """INSERT INTO nereids_using_join_t4 VALUES(1, 5, 45)"""
    sql """INSERT INTO nereids_using_join_t4 VALUES(1, 6, 46)"""
    sql """INSERT INTO nereids_using_join_t4 VALUES(1, 7, 47)"""
    sql """INSERT INTO nereids_using_join_t4 VALUES(1, 8, 48)"""

    order_qt_two_relations """
        select * from nereids_using_join_t1 join nereids_using_join_t2 using (c1);
    """

    order_qt_two_relations_by_two_slot """
        select * from nereids_using_join_t1 join nereids_using_join_t2 using (c1, c2);
    """

    order_qt_two_relations_with_right_slot """
        select *, nereids_using_join_t2.c1 from nereids_using_join_t1 join nereids_using_join_t2 using (c1);
    """

    order_qt_two_relations_with_alias """
        select * from nereids_using_join_t1 a join nereids_using_join_t2 b using (c1);
    """

    order_qt_three_relations """
        select * from nereids_using_join_t1 join nereids_using_join_t3 using (c1) join nereids_using_join_t2 using (c2);
    """

    order_qt_one_plus_two_relations """
        select * from nereids_using_join_t2 join (nereids_using_join_t1 join nereids_using_join_t3 using (c1)) using (c1);
    """

    order_qt_one_plus_two_cross_join """
        select * from nereids_using_join_t1 join (nereids_using_join_t2, nereids_using_join_t3) using (c2);
    """

    order_qt_one_cross_join_with_two """
        select * from nereids_using_join_t1, nereids_using_join_t2 join nereids_using_join_t3 using (c1);
    """

    order_qt_with_lateral_view """
        select * from nereids_using_join_t1 lateral view explode(c4) tmp as c5 join nereids_using_join_t3 using (c1) join nereids_using_join_t2 using (c2) join nereids_using_join_t4 using(c5);
    """

    order_qt_with_aggregate_project """
        select * from (select c3, sum(nereids_using_join_t2.c2) from nereids_using_join_t1 join nereids_using_join_t2 using (c1) group by c3) t       
    """

    order_qt_with_aggregate_by_right_slot_project """
        select * from (select nereids_using_join_t2.c1, sum(nereids_using_join_t2.c2) from nereids_using_join_t1 join nereids_using_join_t2 using (c1) group by nereids_using_join_t2.c1) t       
    """

    order_qt_with_project_aggregate """
        select c1, sum(c3) from (select * from nereids_using_join_t1 join nereids_using_join_t3 using (c1)) t group by c1
    """

    order_qt_with_extend_aggregate """
        select c1, c2, c3, sum(c3) from (select * from nereids_using_join_t1 join nereids_using_join_t3 using (c1)) t group by grouping sets ((c1), (c2), (c3))
    """

    order_qt_with_order_by """
        select * from (select * from nereids_using_join_t1 join nereids_using_join_t3 using (c1) order by c1) t join nereids_using_join_t2 using (c1) order by c1;
    """

    order_qt_with_limit """
        select * from (select * from nereids_using_join_t1 join nereids_using_join_t3 using (c1) limit 10) t join nereids_using_join_t2 using (c1) limit 5;
    """

    order_qt_with_union_all """
        select * from (select * from nereids_using_join_t1 join nereids_using_join_t3 using (c1) union all select * from nereids_using_join_t1 join nereids_using_join_t3 using (c1)) t
    """

    order_qt_with_filter """
        select * from nereids_using_join_t1 join nereids_using_join_t2 using (c1) where c1 < 3;
    """

}