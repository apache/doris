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

suite("test_bitmap_function") {
    // BITMAP_AND
    qt_sql """ select bitmap_count(bitmap_and(to_bitmap(1), to_bitmap(2))) cnt """
    qt_sql """ select bitmap_count(bitmap_and(to_bitmap(1), to_bitmap(1))) cnt """
    qt_sql """ select bitmap_to_string(bitmap_and(to_bitmap(1), to_bitmap(1))) """
    qt_sql """ select bitmap_to_string(bitmap_and(bitmap_from_string('1,2,3'), bitmap_from_string('1,2'), bitmap_from_string('1,2,3,4,5'))) """
    qt_sql """ select bitmap_to_string(bitmap_and(bitmap_from_string('1,2,3'), bitmap_from_string('1,2'), bitmap_from_string('1,2,3,4,5'),bitmap_empty())) """
    qt_sql """ select bitmap_to_string(bitmap_and(bitmap_from_string('1,2,3'), bitmap_from_string('1,2'), bitmap_from_string('1,2,3,4,5'),NULL)) """

    // BITMAP_CONTAINS
    qt_sql """ select bitmap_contains(to_bitmap(1),2) cnt """
    qt_sql """ select bitmap_contains(to_bitmap(1),1) cnt """

    // BITMAP_EMPTY
    qt_sql """ select bitmap_count(bitmap_empty()) """

    // BITMAP_FROM_STRING
    qt_sql """ select bitmap_to_string(bitmap_empty()) """
    qt_sql """ select bitmap_to_string(bitmap_from_string("0, 1, 2")) """
    qt_sql """ select bitmap_from_string("-1, 0, 1, 2") """

    // BITMAP_HAS_ANY
    qt_sql """ select bitmap_has_any(to_bitmap(1),to_bitmap(2)) cnt """
    qt_sql """ select bitmap_has_any(to_bitmap(1),to_bitmap(1)) cnt """

    // BITMAP_HAS_ALL
    qt_sql """ select bitmap_has_all(bitmap_from_string("0, 1, 2"), bitmap_from_string("1, 2")) cnt """
    qt_sql """ select bitmap_has_all(bitmap_empty(), bitmap_from_string("1, 2")) cnt """

    // BITMAP_HASH
    qt_sql_bitmap_hash1 """ select bitmap_count(bitmap_hash('hello')) """
    qt_sql_bitmap_hash2  """ select bitmap_count(bitmap_hash('')) """
    qt_sql_bitmap_hash3  """ select bitmap_count(bitmap_hash(null)) """

    // BITMAP_HASH64
    qt_sql_bitmap_hash64_1 """ select bitmap_count(bitmap_hash64('hello')) """
    qt_sql_bitmap_hash64_2  """ select bitmap_count(bitmap_hash64('')) """
    qt_sql_bitmap_hash64_3  """ select bitmap_count(bitmap_hash64(null)) """

    // BITMAP_OR
    qt_sql_bitmap_or1 """ select bitmap_count(bitmap_or(to_bitmap(1), to_bitmap(2))) cnt """
    qt_sql_bitmap_or2 """ select bitmap_count(bitmap_or(to_bitmap(1), to_bitmap(1))) cnt """
    qt_sql_bitmap_or3 """ select bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2))) """
    qt_sql_bitmap_or4 """ select bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2), to_bitmap(10), to_bitmap(0), NULL)) """
    qt_sql_bitmap_or5 """ select bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2), to_bitmap(10), to_bitmap(0), bitmap_empty())) """
    qt_sql_bitmap_or6 """ select bitmap_to_string(bitmap_or(to_bitmap(10), bitmap_from_string('1,2'), bitmap_from_string('1,2,3,4,5'))) """
    qt_sql_bitmap_or7 """ select bitmap_count(bitmap_or(to_bitmap(1), null)) cnt """

    // bitmap_or of all nullable column
    sql """ DROP TABLE IF EXISTS test_bitmap1 """
    sql """ DROP TABLE IF EXISTS test_bitmap2 """
    sql """
        CREATE TABLE test_bitmap1 (
          dt INT(11) NULL,
          id bitmap BITMAP_UNION NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(dt)
        DISTRIBUTED BY HASH(dt) BUCKETS 2
        properties (
            "replication_num" = "1"
        );
    """
    sql """
        insert into
            test_bitmap1
        values
            (1, to_bitmap(11)),
            (2, to_bitmap(22)),
            (3, to_bitmap(33)),
            (4, to_bitmap(44));
    """
    sql """
        CREATE TABLE test_bitmap2 (
          dt INT(11) NULL,
          id bitmap BITMAP_UNION NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(dt)
        DISTRIBUTED BY HASH(dt) BUCKETS 2
        properties (
            "replication_num" = "1"
        );
    """
    sql """
        insert into
            test_bitmap2
        values
            (1, to_bitmap(111)),
            (2, to_bitmap(222)),
            (5, to_bitmap(555));
    """
    qt_sql_bitmap_or8 """
        select
            l.dt,
            bitmap_count(bitmap_or(l.id, r.id)) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        order by l.dt, count
    """
    qt_sql_bitmap_or_count6 """
        select
            l.dt,
            bitmap_or_count(l.id, r.id) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        order by l.dt, count
    """
    qt_sql_bitmap_or9 """
        select
            l.dt,
            bitmap_count(bitmap_or(l.id, r.id)) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        where r.id is not null
        order by l.dt, count
    """
    qt_sql_bitmap_or_count7 """
        select
            l.dt,
            bitmap_or_count(l.id, r.id) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        where r.id is not null
        order by l.dt, count
    """
    qt_sql_bitmap_or10 """
        select
            l.dt,
            bitmap_to_string(bitmap_or(l.id, r.id)) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        order by l.dt
    """
    qt_sql_bitmap_or11 """
        select
            l.dt,
            bitmap_to_string(bitmap_or(l.id, r.id)) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        where r.id is not null
        order by l.dt
    """

    // bitmap_or of NOT NULLABLE column and nullable column
    sql """ DROP TABLE IF EXISTS test_bitmap1 """
    sql """
        CREATE TABLE test_bitmap1 (
          dt INT(11) NULL,
          id bitmap BITMAP_UNION NOT NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(dt)
        DISTRIBUTED BY HASH(dt) BUCKETS 2
        properties (
            "replication_num" = "1"
        );
    """
    sql """
        insert into
            test_bitmap1
        values
            (1, to_bitmap(11)),
            (2, to_bitmap(22)),
            (3, to_bitmap(33)),
            (4, to_bitmap(44));
    """
    qt_sql_bitmap_or12 """
        select
            l.dt,
            bitmap_count(bitmap_or(l.id, r.id)) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        order by l.dt, count
    """
    qt_sql_bitmap_or_count8 """
        select
            l.dt,
            bitmap_or_count(l.id, r.id) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        order by l.dt, count
    """
    qt_sql_bitmap_or13 """
        select
            l.dt,
            bitmap_count(bitmap_or(l.id, r.id)) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        where r.id is not null
        order by l.dt, count
    """
    qt_sql_bitmap_or_count9 """
        select
            l.dt,
            bitmap_or_count(l.id, r.id) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        where r.id is not null
        order by l.dt, count
    """
    qt_sql_bitmap_or14 """
        select
            l.dt,
            bitmap_to_string(bitmap_or(l.id, r.id)) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        order by l.dt
    """
    qt_sql_bitmap_or15 """
        select
            l.dt,
            bitmap_to_string(bitmap_or(l.id, r.id)) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        where r.id is not null
        order by l.dt
    """

    qt_sql_bitmap_or16_0 """ select bitmap_from_string("1") is null """
    qt_sql_bitmap_or16_1 """ select bitmap_from_string("a") is null """
    qt_sql_bitmap_or16 """ select bitmap_or(bitmap_from_string("a"), bitmap_from_string("b")) is null"""
    qt_sql_bitmap_or17 """ select bitmap_count(bitmap_or(bitmap_from_string("a"), bitmap_from_string("b"))) """
    qt_sql_bitmap_or_count10 """ select bitmap_or_count(bitmap_from_string("a"), bitmap_from_string("b")) """
    qt_sql_bitmap_or18 """ select bitmap_to_string(bitmap_or(bitmap_from_string("a"), bitmap_from_string("b"))) """
    qt_sql_bitmap_or19 """ select bitmap_or(null, null) is null"""
    // qt_sql_bitmap_or20 """ select bitmap_count(bitmap_or(null, null))"""
    qt_sql_bitmap_or21 """ select bitmap_to_string(bitmap_or(null, null))"""

    sql """ drop view if exists v1 """
    sql """ drop view if exists v2 """
    sql """
        create view v1 as
        (select
          l.dt ldt,
          l.id lid,
          r.dt rdt,
          r.id rid
        from
          test_bitmap1 l
          left join test_bitmap2 r on l.dt = r.dt
        where r.id is null);
    """
    sql """
        create view v2 as
        (select
          l.dt ldt,
          l.id lid,
          r.dt rdt,
          r.id rid
        from
          test_bitmap1 l
          right join test_bitmap2 r on l.dt = r.dt
        where l.id is null);
    """

    // test bitmap_or of all non-const null column values
    qt_sql_bitmap_or22_0 """ select ldt, bitmap_count(lid), bitmap_count(rid) from v1 where rid is null order by ldt; """
    qt_sql_bitmap_or22_1 """ select rdt, bitmap_count(lid), bitmap_count(rid) from v2 where lid is null order by rdt; """
    qt_sql_bitmap_or22 """ select v1.ldt, v1.rdt, v2.ldt, v2.rdt, bitmap_or(v1.rid, v2.lid) is null from v1, v2 order by v1.ldt, v2.rdt; """
    qt_sql_bitmap_or_count11 """ select v1.ldt, v1.rdt, v2.ldt, v2.rdt, bitmap_or_count(v1.rid, v2.lid) from v1, v2 order by v1.ldt, v2.rdt; """
    qt_sql_bitmap_or23 """ select v1.ldt, v1.rdt, v2.ldt, v2.rdt, bitmap_to_string(bitmap_or(v1.rid, v2.lid)) from v1, v2 order by v1.ldt, v2.rdt; """

    // bitmap_and_count
    qt_sql_bitmap_and_count1 """ select bitmap_and_count(bitmap_from_string('1,2,3'),bitmap_empty()) """
    qt_sql_bitmap_and_count2 """ select bitmap_and_count(bitmap_from_string('1,2,3'),bitmap_from_string('1,2,3')) """
    qt_sql_bitmap_and_count3 """ select bitmap_and_count(bitmap_from_string('1,2,3'),bitmap_from_string('3,4,5')) """
    qt_sql_bitmap_and_count4 """ select bitmap_and_count(bitmap_from_string('1,2,3'), bitmap_from_string('1,2'), bitmap_from_string('1,2,3,4,5')) """
    qt_sql_bitmap_and_count5 """ select bitmap_and_count(bitmap_from_string('1,2,3'), bitmap_from_string('1,2'), bitmap_from_string('1,2,3,4,5'),bitmap_empty()) """
    qt_sql_bitmap_and_count6 """ select bitmap_and_count(bitmap_from_string('1,2,3'), bitmap_from_string('1,2'), bitmap_from_string('1,2,3,4,5'), NULL) """

    // bitmap_or_count
    qt_sql_bitmap_or_count1 """ select bitmap_or_count(bitmap_from_string('1,2,3'),bitmap_empty()) """
    qt_sql_bitmap_or_count2 """ select bitmap_or_count(bitmap_from_string('1,2,3'),bitmap_from_string('1,2,3'))"""
    qt_sql_bitmap_or_count3 """ select bitmap_or_count(bitmap_from_string('1,2,3'),bitmap_from_string('3,4,5')) """
    qt_sql_bitmap_or_count4 """ select bitmap_or_count(bitmap_from_string('1,2,3'), bitmap_from_string('3,4,5'), to_bitmap(100), bitmap_empty()) """
    qt_sql_bitmap_or_count5 """ select bitmap_or_count(bitmap_from_string('1,2,3'), bitmap_from_string('3,4,5'), to_bitmap(100), NULL) """

    // BITMAP_XOR
    qt_sql """ select bitmap_count(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'))) cnt """
    qt_sql """ select bitmap_to_string(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'))) """
    qt_sql """ select bitmap_to_string(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'),bitmap_from_string('3,4,5'))) """
    qt_sql """ select bitmap_to_string(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'),bitmap_from_string('3,4,5'),bitmap_empty())) """
    qt_sql """ select bitmap_to_string(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'),bitmap_from_string('3,4,5'),NULL)) """

    // BITMAP_XOR_COUNT
    qt_sql_bitmap_xor_count1 """ select bitmap_xor_count(bitmap_from_string('1,2,3'),bitmap_from_string('3,4,5')) """
    qt_sql_bitmap_xor_count2 """ select bitmap_xor_count(bitmap_from_string('1,2,3'),bitmap_from_string('1,2,3')) """
    qt_sql_bitmap_xor_count3 """ select bitmap_xor_count(bitmap_from_string('1,2,3'),bitmap_from_string('4,5,6')) """
    qt_sql_bitmap_xor_count4 """ select (bitmap_xor_count(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'),bitmap_from_string('3,4,5'))) """
    qt_sql_bitmap_xor_count5 """ select (bitmap_xor_count(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'),bitmap_from_string('3,4,5'),bitmap_empty())) """
    qt_sql_bitmap_xor_count6 """ select (bitmap_xor_count(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'),bitmap_from_string('3,4,5'),NULL)) """

    // bitmap_and_count, bitmap_xor_count, bitmap_and_not_count of all nullable column
    sql """ DROP TABLE IF EXISTS test_bitmap1 """
    sql """ DROP TABLE IF EXISTS test_bitmap2 """
    sql """
        CREATE TABLE test_bitmap1 (
          dt INT(11) NULL,
          id bitmap BITMAP_UNION NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(dt)
        DISTRIBUTED BY HASH(dt) BUCKETS 2
        properties (
            "replication_num" = "1"
        );
    """
    sql """
        insert into
            test_bitmap1
        values
            (1, bitmap_from_string("11,111")),
            (2, bitmap_from_string("22,222")),
            (3, bitmap_from_string("33,333")),
            (4, bitmap_from_string("44,444"));
    """
    sql """
        CREATE TABLE test_bitmap2 (
          dt INT(11) NULL,
          id bitmap BITMAP_UNION NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(dt)
        DISTRIBUTED BY HASH(dt) BUCKETS 2
        properties (
            "replication_num" = "1"
        );
    """
    sql """
        insert into
            test_bitmap2
        values
            (1, bitmap_from_string("11,1111")),
            (2, bitmap_from_string("22,2222")),
            (5, bitmap_from_string("55,5555"));
    """
    qt_sql_bitmap_and_count7 """
        select
            l.dt,
            bitmap_and_count(l.id, r.id) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        order by l.dt, count
    """
    qt_sql_bitmap_xor_count7 """
        select
            l.dt,
            bitmap_xor_count(l.id, r.id) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        order by l.dt, count
    """
    qt_sql_bitmap_and_not_count3 """
        select
            l.dt,
            bitmap_and_not_count(l.id, r.id) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        order by l.dt, count
    """
    qt_sql_bitmap_andnot_count3 """
        select
            l.dt,
            bitmap_andnot_count(l.id, r.id) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        order by l.dt, count
    """
    qt_sql_bitmap_and_count8 """
        select
            l.dt,
            bitmap_and_count(l.id, r.id) + 1 count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        order by l.dt, count
    """
    qt_sql_bitmap_xor_count8 """
        select
            l.dt,
            bitmap_xor_count(l.id, r.id) + 1 count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        order by l.dt, count
    """
    qt_sql_bitmap_and_not_count4 """
        select
            l.dt,
            bitmap_and_not_count(l.id, r.id) + 1 count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        order by l.dt, count
    """
    qt_sql_bitmap_and_count9 """
        select
            l.dt,
            bitmap_and_count(l.id, r.id) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        where r.id is not null
        order by l.dt, count
    """
    qt_sql_bitmap_xor_count9 """
        select
            l.dt,
            bitmap_xor_count(l.id, r.id) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        where r.id is not null
        order by l.dt, count
    """
    qt_sql_bitmap_and_not_count5 """
        select
            l.dt,
            bitmap_and_not_count(l.id, r.id) count
        from
            test_bitmap1 l left join test_bitmap2 r
            on l.dt = r.dt
        where r.id is not null
        order by l.dt, count
    """
    // bitmap_and_count, bitmap_xor_count, bitmap_and_not_count of all not nullable column
    sql """ DROP TABLE IF EXISTS test_bitmap1 """
    sql """
        CREATE TABLE test_bitmap1 (
          dt INT(11) NOT NULL,
          id1 bitmap BITMAP_UNION NOT NULL,
          id2 bitmap BITMAP_UNION NOT NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(dt)
        DISTRIBUTED BY HASH(dt) BUCKETS 2
        properties (
            "replication_num" = "1"
        );
    """
    sql """
        insert into
            test_bitmap1
        values
            (1, bitmap_from_string("11,1111"), bitmap_from_string("11,111")),
            (2, bitmap_from_string("22,222,2222,22222"), bitmap_from_string("22,222,2222"))
    """
    qt_sql_bitmap_and_count10 """
        select
            dt,
            bitmap_and_count(id1, id2) count
        from
            test_bitmap1
        order by dt, count
    """
    qt_sql_bitmap_xor_count10 """
        select
            dt,
            bitmap_xor_count(id1, id2) count
        from
            test_bitmap1
        order by dt, count
    """
    qt_sql_bitmap_and_not_count6 """
        select
            dt,
            bitmap_and_not_count(id1, id2) count
        from
            test_bitmap1
        order by dt, count
    """
    qt_sql_bitmap_and_count11 """
        select
            dt,
            bitmap_and_count(id1, id2) + 1 count
        from
            test_bitmap1
        order by dt, count
    """
    qt_sql_bitmap_xor_count11 """
        select
            dt,
            bitmap_xor_count(id1, id2) + 1 count
        from
            test_bitmap1
        order by dt, count
    """
    qt_sql_bitmap_and_not_count7 """
        select
            dt,
            bitmap_and_not_count(id1, id2) + 1 count
        from
            test_bitmap1
        order by dt, count
    """

    // BITMAP_NOT
    qt_sql """ select bitmap_count(bitmap_not(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'))) cnt """
    qt_sql """ select bitmap_to_string(bitmap_not(bitmap_from_string('2,3,5'),bitmap_from_string('1,2,3,4'))) """

    // BITMAP_AND_NOT
    qt_sql """ select bitmap_count(bitmap_and_not(bitmap_from_string('1,2,3'),bitmap_from_string('3,4,5'))) cnt """
    qt_sql_bitmap_andnot """ select bitmap_count(bitmap_andnot(bitmap_from_string('1,2,3'),bitmap_from_string('3,4,5'))) cnt """

    // BITMAP_AND_NOT_COUNT
    qt_sql_bitmap_and_not_count1 """ select bitmap_and_not_count(bitmap_from_string('1,2,3'),bitmap_from_string('3,4,5')) cnt """
    qt_sql_bitmap_and_not_count2 """ select bitmap_and_not_count(bitmap_from_string('1,2,3'),null) cnt """

    // BITMAP_SUBSET_IN_RANGE
    qt_sql """ select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,2,3,4,5'), 0, 9)) value """
    qt_sql """ select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('1,2,3,4,5'), 2, 3)) value """

    // BITMAP_SUBSET_LIMIT
    qt_sql """ select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,2,3,4,5'), 0, 3)) value """
    qt_sql """ select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('1,2,3,4,5'), 4, 3)) value """

    // SUB_BITMAP
    qt_sql """ select bitmap_to_string(sub_bitmap(bitmap_from_string('1,0,1,2,3,1,5'), 0, 3)) value """
    qt_sql """ select bitmap_to_string(sub_bitmap(bitmap_from_string('1,0,1,2,3,1,5'), -3, 2)) value """
    qt_sql """ select bitmap_to_string(sub_bitmap(bitmap_from_string('1,0,1,2,3,1,5'), 2, 100)) value """

    // BITMAP_TO_STRING
    qt_sql """ select bitmap_to_string(null) """
    qt_sql """ select bitmap_to_string(bitmap_empty()) """
    qt_sql """ select bitmap_to_string(to_bitmap(1)) """
    qt_sql """ select bitmap_to_string(bitmap_or(to_bitmap(1), to_bitmap(2))) """

    // BITMAP_UNION
    def bitmapUnionTable = "test_bitmap_union"
    sql """ DROP TABLE IF EXISTS ${bitmapUnionTable} """
    sql """ create table if not exists ${bitmapUnionTable} (page_id int,user_id bitmap bitmap_union) aggregate key (page_id) distributed by hash (page_id) PROPERTIES("replication_num" = "1") """

    sql """ insert into ${bitmapUnionTable} values(1, to_bitmap(1)); """
    sql """ insert into ${bitmapUnionTable} values(1, to_bitmap(2)); """
    sql """ insert into ${bitmapUnionTable} values(1, to_bitmap(3)); """
    sql """ insert into ${bitmapUnionTable} values(2, to_bitmap(1)); """
    sql """ insert into ${bitmapUnionTable} values(2, to_bitmap(2)); """

    qt_sql """ select page_id, bitmap_union(user_id) from ${bitmapUnionTable} group by page_id order by page_id """
    qt_sql """ select page_id, bitmap_count(bitmap_union(user_id)) from ${bitmapUnionTable} group by page_id order by page_id """
    qt_sql """ select page_id, count(distinct user_id) from ${bitmapUnionTable} group by page_id order by page_id """

    sql """ drop table ${bitmapUnionTable} """

    // BITMAP_XOR
    qt_sql """ select bitmap_count(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'))) cnt; """
    qt_sql """ select bitmap_to_string(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'))); """
    qt_sql """ select bitmap_to_string(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'),bitmap_from_string('3,4,5'))); """
    qt_sql """ select bitmap_to_string(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'),bitmap_from_string('3,4,5'),bitmap_empty())); """
    qt_sql """ select bitmap_to_string(bitmap_xor(bitmap_from_string('2,3'),bitmap_from_string('1,2,3,4'),bitmap_from_string('3,4,5'),NULL)); """

    // TO_BITMAP
    qt_sql """ select bitmap_count(to_bitmap(10)) """
    qt_sql """ select bitmap_to_string(to_bitmap("-1")) """

    // BITMAP_MAX
    qt_sql """ select bitmap_max(bitmap_from_string('')) value; """
    qt_sql """ select bitmap_max(bitmap_from_string('1,9999999999')) value """

    // INTERSECT_COUNT
    def intersectCountTable = "test_intersect_count"
    sql """ DROP TABLE IF EXISTS ${intersectCountTable} """
    sql """ create table if not exists ${intersectCountTable} (dt int (11),page varchar (10),user_id bitmap BITMAP_UNION ) DISTRIBUTED BY HASH(dt) BUCKETS 2 PROPERTIES("replication_num" = "1") """


    sql """ insert into ${intersectCountTable} values(3,"110001", to_bitmap(1)); """
    sql """ insert into ${intersectCountTable} values(3,"110001", to_bitmap(2)); """
    sql """ insert into ${intersectCountTable} values(3,"110001", to_bitmap(3)); """
    sql """ insert into ${intersectCountTable} values(3,"110001", to_bitmap(4)); """
    sql """ insert into ${intersectCountTable} values(3,"110001", to_bitmap(5)); """
    sql """ insert into ${intersectCountTable} values(4,"110001", to_bitmap(1)); """
    sql """ insert into ${intersectCountTable} values(4,"110001", to_bitmap(2)); """
    sql """ insert into ${intersectCountTable} values(4,"110001", to_bitmap(3)); """

    qt_sql """ select dt,bitmap_to_string(user_id) from ${intersectCountTable} where dt in (3,4) order by dt desc; """
    qt_sql """ select intersect_count(user_id,dt,3,4) from ${intersectCountTable}; """

    // ARTHOGONAL_BITMAP_****
    def arthogonalBitmapTable = "test_arthogonal_bitmap"
    sql """ DROP TABLE IF EXISTS ${arthogonalBitmapTable} """
    sql """ CREATE TABLE IF NOT EXISTS ${arthogonalBitmapTable} (
        tag_group bigint(20) NULL COMMENT "标签组",
        bucket int(11) NOT NULL COMMENT "分桶字段",
        members bitmap BITMAP_UNION NULL COMMENT "人群") ENGINE=OLAP
        AGGREGATE KEY(tag_group,
                      bucket)
        DISTRIBUTED BY HASH(bucket) BUCKETS 64
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2");
    """

    sql """ insert into ${arthogonalBitmapTable} values (1, 1, bitmap_from_string("1,11,111")), (2, 2, to_bitmap(2)); """
    sql """ insert into ${arthogonalBitmapTable} values (11, 1, bitmap_from_string("1,11")), (12, 2, to_bitmap(2)); """

    // qt_sql """ select orthogonal_bitmap_intersect(members, tag_group, 1150000, 1150001, 390006) from ${arthogonalBitmapTable} where  tag_group in ( 1150000, 1150001, 390006); """
    // qt_sql """ select orthogonal_bitmap_intersect_count(members, tag_group, 1150000, 1150001, 390006) from ${arthogonalBitmapTable} where  tag_group in ( 1150000, 1150001, 390006); """
    // qt_sql """ select orthogonal_bitmap_union_count(members) from ${arthogonalBitmapTable} where  tag_group in ( 1150000, 1150001, 390006);  """
    // qt_sql_orthogonal_bitmap_intersect_count2 """ select orthogonal_bitmap_intersect_count(members, tag_group, 1,2) from test_arthogonal_bitmap; """
    // qt_sql_orthogonal_bitmap_intersect_count3_1 """ select /*+SET_VAR(parallel_fragment_exec_instance_num=1)*/orthogonal_bitmap_intersect_count(members, tag_group, 1,11) from test_arthogonal_bitmap; """
    // qt_sql_orthogonal_bitmap_intersect_count3_2 """ select /*+SET_VAR(parallel_fragment_exec_instance_num=2)*/orthogonal_bitmap_intersect_count(members, tag_group, 1,11) from test_arthogonal_bitmap; """
    // qt_sql_orthogonal_bitmap_intersect_count4 """ select orthogonal_bitmap_intersect_count(members, tag_group, 2,12) from test_arthogonal_bitmap; """
    qt_sql_orthogonal_bitmap_union_count2 """ select orthogonal_bitmap_union_count( cast(null as bitmap)) from test_arthogonal_bitmap; """
    qt_sql_orthogonal_bitmap_union_count3 """ select orthogonal_bitmap_union_count(members) from test_arthogonal_bitmap; """

    qt_sql """ select bitmap_to_array(user_id) from ${intersectCountTable} order by dt desc; """
    qt_sql """ select bitmap_to_array(bitmap_empty()); """
    qt_sql """ select bitmap_to_array(bitmap_from_string('100,200,3,4')); """

    qt_sql """ select bitmap_to_string(sub_bitmap(bitmap_from_string('1,2,3,4,5'), 0, 3)) value; """
    qt_sql """ select bitmap_to_string(sub_bitmap(bitmap_from_string('1'), 0, 3)) value;  """
    qt_sql """ select bitmap_to_string(bitmap_subset_limit(bitmap_from_string('100'), 0, 3)) value;  """
    qt_sql """ select bitmap_to_string(bitmap_subset_in_range(bitmap_from_string('20221103'), 0, 20221104)) date_list_bitmap;  """

    sql "drop table if exists d_table;"
    sql """
        create table d_table (
            k1 int null,
            k2 int not null,
            k3 bigint null,
            k4 varchar(100) null
        )
        duplicate key (k1,k2,k3)
        distributed BY hash(k1) buckets 3
        properties("replication_num" = "1");
    """
    sql "insert into d_table select -4,-4,-4,'d';"
    try_sql "select bitmap_union(to_bitmap_with_check(k2)) from d_table;"
    qt_sql "select bitmap_union(to_bitmap(k2)) from d_table;"

    // bug fix
    sql """ DROP TABLE IF EXISTS test_bitmap1 """
    sql """
        CREATE TABLE test_bitmap1 (
          dt INT(11) NULL,
          id bitmap BITMAP_UNION NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(dt)
        DISTRIBUTED BY HASH(dt) BUCKETS 1
        properties (
            "replication_num" = "1"
        );
    """
    sql """
        insert into
            test_bitmap1
        values
            (1, to_bitmap(11)),
            (2, to_bitmap(22)),
            (3, to_bitmap(33)),
            (4, to_bitmap(44)),
            (5, to_bitmap(44)),
            (6, to_bitmap(44)),
            (7, to_bitmap(44)),
            (8, to_bitmap(44)),
            (9, to_bitmap(44)),
            (10, to_bitmap(44)),
            (11, to_bitmap(44)),
            (12, to_bitmap(44)),
            (13, to_bitmap(44)),
            (14, to_bitmap(44)),
            (15, to_bitmap(44)),
            (16, to_bitmap(44)),
            (17, to_bitmap(44));
    """
    qt_sql_bitmap_subset_in_range """
        select /*+SET_VAR(parallel_fragment_exec_instance_num=1)*/
            bitmap_to_string(
                bitmap_subset_in_range(id, cast(null as bigint), cast(null as bigint))
            )
        from
            test_bitmap1;
    """

    sql """
        drop TABLE if exists test_bitmap_intersect;
    """

    sql """
        CREATE TABLE test_bitmap_intersect (
            dt1 date NOT NULL,
            dt2 date NOT NULL,
            id varchar(256) NULL,
            type smallint(6) MAX NULL,
            id_bitmap bitmap BITMAP_UNION NULL
        ) ENGINE = OLAP AGGREGATE KEY(dt1, dt2, id) PARTITION BY RANGE(dt1) (
            PARTITION p20230725
            VALUES
                [('2023-07-25'), ('2023-07-26')))
        DISTRIBUTED BY HASH(dt1, dt2) BUCKETS 1 properties("replication_num"="1");
    """

    sql """
        insert into test_bitmap_intersect
            select
                str_to_date('2023-07-25','%Y-%m-%d') as dt1,
                str_to_date('2023-07-25','%Y-%m-%d') as dt2,
                'aaaaaaaaaa' as id,
                1 as type,
                BITMAP_HASH64('aaaaaaaaaa') as id_bitmap;
    """
    qt_sql_bitmap_intersect_check0 """
        select intersect_count(id_bitmap, type, 1) as count2_bitmap from test_bitmap_intersect;
    """
    // qt_sql_bitmap_intersect_check1 """
    //     select bitmap_count(orthogonal_bitmap_intersect(id_bitmap, type, 1)) as count2_bitmap from test_bitmap_intersect;
    // """
    // qt_sql_bitmap_intersect_check2 """
    //     select orthogonal_bitmap_intersect_count(id_bitmap, type, 1) as count2_bitmap from test_bitmap_intersect;
    // """

    // test function intersect_count
    // test nereids
    sql """ set experimental_enable_nereids_planner=true; """
    // test pipeline
    sql """ set experimental_enable_pipeline_engine=true; """
    qt_sql_bitmap_intersect_nereids0 """
        select count(distinct if(type=1, id,null)) as count1,
            intersect_count(id_bitmap, type, 1) as count2_bitmap from test_bitmap_intersect;
    """
    sql """ set experimental_enable_pipeline_engine=false; """
    qt_sql_bitmap_intersect_nereids1 """
        select count(distinct if(type=1, id,null)) as count1,
            intersect_count(id_bitmap, type, 1) as count2_bitmap from test_bitmap_intersect;
    """

    // test not nereids
    sql """ set experimental_enable_nereids_planner=false; """
    // test pipeline
    sql """ set experimental_enable_pipeline_engine=true; """
    qt_sql_bitmap_intersect_no_nereids0 """
        select count(distinct if(type=1, id,null)) as count1,
            intersect_count(id_bitmap, type, 1) as count2_bitmap from test_bitmap_intersect;
    """
    sql """ set experimental_enable_pipeline_engine=false; """
    qt_sql_bitmap_intersect_no_nereids1 """
        select count(distinct if(type=1, id,null)) as count1,
            intersect_count(id_bitmap, type, 1) as count2_bitmap from test_bitmap_intersect;
    """

    sql """
        drop TABLE if exists test_orthog_bitmap_intersect;
    """
    sql """
        CREATE TABLE test_orthog_bitmap_intersect (
            tag int NOT NULL,
            hid int NOT NULL,
            id_bitmap bitmap BITMAP_UNION NULL
        ) ENGINE = OLAP AGGREGATE KEY(tag, hid)
        DISTRIBUTED BY HASH(hid) BUCKETS 1 properties("replication_num"="1");
    """

    sql """
        insert into test_orthog_bitmap_intersect
            select 0, 1, to_bitmap(1) as id_bitmap;
    """
    // test function orthogonal_bitmap_intersect
    // test nereids
    sql """ set experimental_enable_nereids_planner=true; """
    // TODO: case will stuck untile timeout, enable it when pipeline bug is fixed
    // test pipeline
    // sql """ set experimental_enable_pipeline_engine=true; """
    // qt_sql_orthogonal_bitmap_intersect_nereids0 """
    //     select count(distinct if(type=1, id,null)) as count1,
    //         bitmap_count(orthogonal_bitmap_intersect(id_bitmap, type, 1)) as count2_bitmap from test_orthog_bitmap_intersect;
    // """
    // sql """ set experimental_enable_pipeline_engine=false; """
    // qt_sql_orthogonal_bitmap_intersect_nereids1 """
    //     select count(distinct tag) as count1,
    //         bitmap_count(orthogonal_bitmap_intersect(id_bitmap, tag, 0)) as count2_bitmap from test_orthog_bitmap_intersect;
    // """

    // test not nereids
    sql """ set experimental_enable_nereids_planner=false; """
    // TODO: case will stuck untile timeout, enable it when pipeline bug is fixed
    // test pipeline
    // sql """ set experimental_enable_pipeline_engine=true; """
    // qt_sql_orthogonal_bitmap_intersect_not_nereids0 """
    //     select count(distinct if(type=1, id,null)) as count1,
    //         bitmap_count(orthogonal_bitmap_intersect(id_bitmap, type, 1)) as count2_bitmap from test_orthog_bitmap_intersect;
    // """
    // sql """ set experimental_enable_pipeline_engine=false; """
    // qt_sql_orthogonal_bitmap_intersect_not_nereids1 """
    //     select count(distinct tag) as count1,
    //         bitmap_count(orthogonal_bitmap_intersect(id_bitmap, tag, 0)) as count2_bitmap from test_orthog_bitmap_intersect;
    // """

    // test function orthogonal_bitmap_intersect_count
    // test nereids
    sql """ set experimental_enable_nereids_planner=true; """
    // test pipeline
    // sql """ set experimental_enable_pipeline_engine=true; """
    // qt_sql_orthogonal_bitmap_intersect_count_nereids0 """
    //     select count(distinct tag) as count1,
    //         orthogonal_bitmap_intersect_count(id_bitmap, tag, 0) as count2_bitmap from test_orthog_bitmap_intersect;
    // """
    // sql """ set experimental_enable_pipeline_engine=false; """
    // qt_sql_orthogonal_bitmap_intersect_count_nereids1 """
    //     select count(distinct tag) as count1,
    //         orthogonal_bitmap_intersect_count(id_bitmap, tag, 0) as count2_bitmap from test_orthog_bitmap_intersect;
    // """

    // test not nereids
    sql """ set experimental_enable_nereids_planner=false; """
    // test pipeline
    sql """ set experimental_enable_pipeline_engine=true; """
    // TODO: enable this case after issue #22771 if solved
    // qt_sql_orthogonal_bitmap_intersect_count_not_nereids0 """
    //     select count(distinct tag) as count1,
    //         orthogonal_bitmap_intersect_count(id_bitmap, tag, 0) as count2_bitmap from test_orthog_bitmap_intersect;
    // """
    // sql """ set experimental_enable_pipeline_engine=false; """
    // qt_sql_orthogonal_bitmap_intersect_count_not_nereids1 """
    //     select count(distinct tag) as count1,
    //         orthogonal_bitmap_intersect_count(id_bitmap, tag, 0) as count2_bitmap from test_orthog_bitmap_intersect;
    // """

    /////////////////////////////
    // test bitmap base64
    sql """ set experimental_enable_nereids_planner=true; """
    qt_sql_bitmap_base64_nereids0 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(null))); """
    qt_sql_bitmap_base64_nereids1 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(bitmap_from_string("")))); """
    qt_sql_bitmap_base64_nereids2 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(bitmap_from_string(" ")))); """
    qt_sql_bitmap_base64_nereids3 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(bitmap_from_string("1")))); """
    qt_sql_bitmap_base64_nereids4 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(bitmap_from_string("0, 1, 2, 3")))); """
    qt_sql_bitmap_base64_nereids5 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(bitmap_from_string("0, 1, 2, 3, 4294967296")))); """
    qt_sql_bitmap_base64_nereids6 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(bitmap_from_string("0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32")))) """
    qt_sql_bitmap_base64_nereids7 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(bitmap_from_string("0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,4294967296")))) """
    qt_sql_bitmap_base64_nereids8 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(to_bitmap(1)))); """

    // test nullable
    sql """ DROP TABLE IF EXISTS test_bitmap_base64 """ 
    sql """
        CREATE TABLE test_bitmap_base64 (
          dt INT(11) NULL,
          id bitmap BITMAP_UNION NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(dt)
        DISTRIBUTED BY HASH(dt) BUCKETS 2
        properties (
            "replication_num" = "1"
        );
    """
    sql """
        INSERT INTO
            test_bitmap_base64
        VALUES
            (0, to_bitmap(null)),
            (1, bitmap_from_string("0")),
            (2, bitmap_from_string("1,9999999")),
            (3, bitmap_from_string("0, 1, 2, 3, 4294967296")),
            (4, bitmap_from_string("0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32")),
            (5, bitmap_from_string("0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,4294967296"))
            ;
    """
    qt_sql_bitmap_base64_nereids9 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(id))) s from test_bitmap_base64 order by s; """

    sql """ set experimental_enable_nereids_planner=false; """
    qt_sql_bitmap_base64_0 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(null))); """
    qt_sql_bitmap_base64_1 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(bitmap_from_string("")))); """
    qt_sql_bitmap_base64_2 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(bitmap_from_string(" ")))); """
    qt_sql_bitmap_base64_3 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(bitmap_from_string("1")))); """
    qt_sql_bitmap_base64_4 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(bitmap_from_string("0, 1, 2, 3")))); """
    qt_sql_bitmap_base64_5 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(bitmap_from_string("0, 1, 2, 3, 4294967296")))); """
    qt_sql_bitmap_base64_6 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(bitmap_from_string("0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32")))) """
    qt_sql_bitmap_base64_7 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(bitmap_from_string("0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,4294967296")))) """
    qt_sql_bitmap_base64_8 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(to_bitmap(1)))); """

    qt_sql_bitmap_base64_9 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(id))) s from test_bitmap_base64 order by s; """

    // test not nullable
    sql """ set experimental_enable_nereids_planner=true; """
    sql """ DROP TABLE IF EXISTS test_bitmap_base64_not_null """ 
    sql """
        CREATE TABLE test_bitmap_base64_not_null (
          dt INT(11) NULL,
          id bitmap BITMAP_UNION NOT NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(dt)
        DISTRIBUTED BY HASH(dt) BUCKETS 2
        properties (
            "replication_num" = "1"
        );
    """
    sql """
        INSERT INTO
            test_bitmap_base64_not_null
        VALUES
            (1, bitmap_from_string("0")),
            (2, bitmap_from_string("1,9999999")),
            (3, bitmap_from_string("0, 1, 2, 3, 4294967296")),
            (4, bitmap_from_string("0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32")),
            (5, bitmap_from_string("0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,4294967296"))
            ;
    """
    qt_sql_bitmap_base64_not_null0 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(id))) s from test_bitmap_base64_not_null order by s; """
    sql """ set experimental_enable_nereids_planner=false; """
    qt_sql_bitmap_base64_not_null1 """ select bitmap_to_string(bitmap_from_base64(bitmap_to_base64(id))) s from test_bitmap_base64_not_null order by s; """

    ///////////////////
    // test bitmap_remove
    sql """ set experimental_enable_nereids_planner=true; """
    qt_sql_bitmap_remove_nereids0 """
        select bitmap_to_string(bitmap_remove(bitmap_from_string('1, 2, 3'), 3));
    """
    qt_sql_bitmap_remove_nereids1 """
        select bitmap_to_string(bitmap_remove(bitmap_from_string('1, 2, 3'), null));
    """
    // test nullable
    sql """ DROP TABLE IF EXISTS test_bitmap_remove """ 
    sql """
        CREATE TABLE test_bitmap_remove (
          dt INT(11) NULL,
          id bitmap BITMAP_UNION NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(dt)
        DISTRIBUTED BY HASH(dt) BUCKETS 2
        properties (
            "replication_num" = "1"
        );
    """
    sql """
        INSERT INTO
            test_bitmap_remove
        VALUES
            (0, to_bitmap(null)),
            (1, bitmap_from_string("0")),
            (2, bitmap_from_string("1,9999999")),
            (3, bitmap_from_string("0, 1, 2, 3, 4294967296")),
            (4, bitmap_from_string("0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32")),
            (5, bitmap_from_string("0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,4294967296"))
            ;
    """
    qt_sql_bitmap_remove_nereids2 """ select bitmap_to_string(bitmap_remove(id, 1)) s from test_bitmap_remove order by s; """
    qt_sql_bitmap_remove_nereids3 """ select bitmap_to_string(bitmap_remove(id, 32)) s from test_bitmap_remove order by s; """
    qt_sql_bitmap_remove_nereids4 """ select bitmap_to_string(bitmap_remove(id, 4294967296)) s from test_bitmap_remove order by s; """
    qt_sql_bitmap_remove_nereids5 """ select bitmap_to_string(bitmap_remove(id, null)) s from test_bitmap_remove order by s; """

    // experimental_enable_nereids_planner=false;
    sql """ set experimental_enable_nereids_planner=false; """
    qt_sql_bitmap_remove0 """
        select bitmap_to_string(bitmap_remove(bitmap_from_string('1, 2, 3'), 3));
    """
    qt_sql_bitmap_remove1 """
        select bitmap_to_string(bitmap_remove(bitmap_from_string('1, 2, 3'), null));
    """
    qt_sql_bitmap_remove2 """ select bitmap_to_string(bitmap_remove(id, 1)) s from test_bitmap_remove order by s; """
    qt_sql_bitmap_remove3 """ select bitmap_to_string(bitmap_remove(id, 32)) s from test_bitmap_remove order by s; """
    qt_sql_bitmap_remove4 """ select bitmap_to_string(bitmap_remove(id, 4294967296)) s from test_bitmap_remove order by s; """
    qt_sql_bitmap_remove5 """ select bitmap_to_string(bitmap_remove(id, null)) s from test_bitmap_remove order by s; """

    // test not nullable
    sql """ set experimental_enable_nereids_planner=true; """
    sql """ DROP TABLE IF EXISTS test_bitmap_remove_not_null """ 
    sql """
        CREATE TABLE test_bitmap_remove_not_null (
          dt INT(11) NULL,
          id bitmap BITMAP_UNION NOT NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(dt)
        DISTRIBUTED BY HASH(dt) BUCKETS 2
        properties (
            "replication_num" = "1"
        );
    """
    sql """
        INSERT INTO
            test_bitmap_remove_not_null
        VALUES
            (1, bitmap_from_string("0")),
            (2, bitmap_from_string("1,9999999")),
            (3, bitmap_from_string("0, 1, 2, 3, 4294967296")),
            (4, bitmap_from_string("0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32")),
            (5, bitmap_from_string("0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,4294967296"))
            ;
    """
    qt_sql_bitmap_remove_not_null2 """ select bitmap_to_string(bitmap_remove(id, 1)) s from test_bitmap_remove_not_null order by s; """
    qt_sql_bitmap_remove_not_null3 """ select bitmap_to_string(bitmap_remove(id, 32)) s from test_bitmap_remove_not_null order by s; """
    qt_sql_bitmap_remove_not_null4 """ select bitmap_to_string(bitmap_remove(id, 4294967296)) s from test_bitmap_remove_not_null order by s; """
    qt_sql_bitmap_remove_not_null5 """ select bitmap_to_string(bitmap_remove(id, null)) s from test_bitmap_remove_not_null order by s; """

    sql """ set experimental_enable_nereids_planner=false; """
    qt_sql_bitmap_remove_not_null6 """ select bitmap_to_string(bitmap_remove(id, 1)) s from test_bitmap_remove_not_null order by s; """
    qt_sql_bitmap_remove_not_null7 """ select bitmap_to_string(bitmap_remove(id, 32)) s from test_bitmap_remove_not_null order by s; """
    qt_sql_bitmap_remove_not_null8 """ select bitmap_to_string(bitmap_remove(id, 4294967296)) s from test_bitmap_remove_not_null order by s; """
    qt_sql_bitmap_remove_not_null9 """ select bitmap_to_string(bitmap_remove(id, null)) s from test_bitmap_remove_not_null order by s; """

    // BITMAP_FROM_ARRAY
    sql """ set experimental_enable_nereids_planner=false; """
    qt_sql """ select bitmap_to_string(BITMAP_FROM_ARRAY([]));"""

    sql """ set experimental_enable_nereids_planner=true; """
    qt_sql """ select bitmap_to_string(BITMAP_FROM_ARRAY([]));"""
}
