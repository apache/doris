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

suite("test_half_join_nullable_build_side", "query,p0") {
    sql " set disable_join_reorder = 1; "
    sql " drop table if exists test_half_join_nullable_build_side_l; ";
    sql " drop table if exists test_half_join_nullable_build_side_l2; ";
    sql " drop table if exists test_half_join_nullable_build_side_r; ";
    sql " drop table if exists test_half_join_nullable_build_side_r2; ";
    sql """
        create table test_half_join_nullable_build_side_l (
            k1 int,
            v1 string not null,
            v2 int not null
        ) distributed by hash(k1) buckets 1
        properties("replication_num" = "1");
    """
    sql """
        create table test_half_join_nullable_build_side_l2 (
            k1 int,
            v1 string null,
            v2 int null
        ) distributed by hash(k1) buckets 1
        properties("replication_num" = "1");
    """
    sql """
        create table test_half_join_nullable_build_side_r (
            k1 int,
            v1 string null,
            v2 string null
        ) distributed by hash(k1) buckets 1
        properties("replication_num" = "1");
    """
    sql """
        create table test_half_join_nullable_build_side_r2 (
            k1 int,
            v1 string not null,
            v2 string not null
        ) distributed by hash(k1) buckets 1
        properties("replication_num" = "1");
    """

    sql """ insert into test_half_join_nullable_build_side_l values (1, 11, "11"), (2, 111, "111"), (3, 1111, "1111"), (4, 111, "111") """
    sql """ insert into test_half_join_nullable_build_side_l2 values (1, 11, "11"), (2, 111, "111"), (3, 1111, "1111"), (4, null, null), (5, 1111, "1111") """
    sql """ insert into test_half_join_nullable_build_side_r values (1, null, null), (2, 111, "111"), (3, 1111, "1111"), (4, null, null) """
    sql """ insert into test_half_join_nullable_build_side_r2 values (2, 111, "111"), (3, 1111, "1111") """

    qt_sql1 """
        select *
        from
            test_half_join_nullable_build_side_l l left join test_half_join_nullable_build_side_r r on  l.v1 = r.v1
        order by 1, 2, 3, 4, 5, 6;
    """

    qt_sql2 """
        select *
        from
            test_half_join_nullable_build_side_l l left join test_half_join_nullable_build_side_r r on  l.v2 = r.v2
        order by 1, 2, 3, 4, 5, 6;
    """

    qt_sql3 """
        select *
        from
            test_half_join_nullable_build_side_l l left join test_half_join_nullable_build_side_r r on  l.v1 = r.v1 and l.v2 = r.v2
        order by 1, 2, 3, 4, 5, 6;
    """

    qt_anti_sql4 """
        select *
        from
            test_half_join_nullable_build_side_l l left anti join test_half_join_nullable_build_side_r r on  l.v1 = r.v1
        order by 1, 2, 3;
    """

    qt_sql5 """
        select *
        from
            test_half_join_nullable_build_side_l l left semi join test_half_join_nullable_build_side_r r on  l.v1 = r.v1
        order by 1, 2, 3;
    """

    qt_anti_sql6 """
        select *
        from
            test_half_join_nullable_build_side_l l left anti join test_half_join_nullable_build_side_r r on  l.v2 = r.v2
        order by 1, 2, 3;
    """

    qt_sql7 """
        select *
        from
            test_half_join_nullable_build_side_l l left semi join test_half_join_nullable_build_side_r r on  l.v2 = r.v2
        order by 1, 2, 3;
    """

    qt_anti_sql8 """
        select *
        from
            test_half_join_nullable_build_side_l l left anti join test_half_join_nullable_build_side_r r on l.v1 = r.v1 and l.v2 = r.v2
        order by 1, 2, 3;
    """

    qt_sql9 """
        select *
        from
            test_half_join_nullable_build_side_l l left semi join test_half_join_nullable_build_side_r r on l.v1 = r.v1 and l.v2 = r.v2
        order by 1, 2, 3;
    """

    qt_anti_sql10 """
        select *
        from
            test_half_join_nullable_build_side_l l left anti join test_half_join_nullable_build_side_r r on  l.v1 <=> r.v1
        order by 1, 2, 3;
    """

    qt_sql11 """
        select *
        from
            test_half_join_nullable_build_side_l l left semi join test_half_join_nullable_build_side_r r on  l.v1 <=> r.v1
        order by 1, 2, 3;
    """

    qt_anti_sql12 """
        select *
        from
            test_half_join_nullable_build_side_l l left anti join test_half_join_nullable_build_side_r r on  l.v2 <=> r.v2
        order by 1, 2, 3;
    """

    qt_sql13 """
        select *
        from
            test_half_join_nullable_build_side_l l left semi join test_half_join_nullable_build_side_r r on  l.v2 <=> r.v2
        order by 1, 2, 3;
    """



    qt_anti_sql14 """
        select *
        from
            test_half_join_nullable_build_side_l l right anti join test_half_join_nullable_build_side_r r on  l.v1 = r.v1
        order by 1, 2, 3;
    """

    qt_sql15 """
        select *
        from
            test_half_join_nullable_build_side_l l right semi join test_half_join_nullable_build_side_r r on  l.v1 = r.v1
        order by 1, 2, 3;
    """

    qt_anti_sql16 """
        select *
        from
            test_half_join_nullable_build_side_l l right anti join test_half_join_nullable_build_side_r r on  l.v2 = r.v2
        order by 1, 2, 3;
    """

    qt_sql17 """
        select *
        from
            test_half_join_nullable_build_side_l l right semi join test_half_join_nullable_build_side_r r on  l.v2 = r.v2
        order by 1, 2, 3;
    """

    qt_anti_sql18 """
        select *
        from
            test_half_join_nullable_build_side_l l right anti join test_half_join_nullable_build_side_r r on l.v1 = r.v1 and l.v2 = r.v2
        order by 1, 2, 3;
    """

    qt_sql19 """
        select *
        from
            test_half_join_nullable_build_side_l l right semi join test_half_join_nullable_build_side_r r on l.v1 = r.v1 and l.v2 = r.v2
        order by 1, 2, 3;
    """

    qt_anti_sql20 """
        select *
        from
            test_half_join_nullable_build_side_l l right anti join test_half_join_nullable_build_side_r r on  l.v1 <=> r.v1
        order by 1, 2, 3;
    """

    qt_sql21 """
        select *
        from
            test_half_join_nullable_build_side_l l right semi join test_half_join_nullable_build_side_r r on  l.v1 <=> r.v1
        order by 1, 2, 3;
    """

    qt_anti_sql22 """
        select *
        from
            test_half_join_nullable_build_side_l l right anti join test_half_join_nullable_build_side_r r on  l.v2 <=> r.v2
        order by 1, 2, 3;
    """

    qt_sql23 """
        select *
        from
            test_half_join_nullable_build_side_l l right semi join test_half_join_nullable_build_side_r r on  l.v2 <=> r.v2
        order by 1, 2, 3;
    """

    qt_anti_sql24 """
        select *
        from
            test_half_join_nullable_build_side_l2 l left anti join test_half_join_nullable_build_side_r r on  l.v2 <=> r.v2
        order by 1, 2, 3;
    """

    qt_sql25 """
        select *
        from
            test_half_join_nullable_build_side_l2 l left semi join test_half_join_nullable_build_side_r r on  l.v2 <=> r.v2
        order by 1, 2, 3;
    """

    qt_anti_sql26 """
        select *
        from
            test_half_join_nullable_build_side_l2 l right anti join test_half_join_nullable_build_side_r r on  l.v2 <=> r.v2
        order by 1, 2, 3;
    """

    qt_sql27 """
        select *
        from
            test_half_join_nullable_build_side_l2 l right semi join test_half_join_nullable_build_side_r r on  l.v2 <=> r.v2
        order by 1, 2, 3;
    """

    qt_anti_sql28 """
        select *
        from
            test_half_join_nullable_build_side_l l right anti join test_half_join_nullable_build_side_r2 r on  l.v2 = r.v2
        order by 1, 2, 3;
    """

    qt_sql29 """
        select *
        from
            test_half_join_nullable_build_side_l l right semi join test_half_join_nullable_build_side_r2 r on  l.v2 = r.v2
        order by 1, 2, 3;
    """

    qt_anti_sql28 """
        select *
        from
            test_half_join_nullable_build_side_l2 l right anti join test_half_join_nullable_build_side_r2 r on  l.v2 = r.v2
        order by 1, 2, 3;
    """

    qt_sql29 """
        select *
        from
            test_half_join_nullable_build_side_l2 l right semi join test_half_join_nullable_build_side_r2 r on  l.v2 = r.v2
        order by 1, 2, 3;
    """

    qt_sql30 """
        select
            *
        from
            test_half_join_nullable_build_side_l2 l
            left join test_half_join_nullable_build_side_l r on  l.v2 <=> r.v2
        order by 1, 2, 3;
    """

    qt_shortcut """
    select *         from             test_half_join_nullable_build_side_l l left anti join test_half_join_nullable_build_side_r r on  l.v2 <=> r.v2 and r.k1=5         order by 1, 2, 3;
    """
}