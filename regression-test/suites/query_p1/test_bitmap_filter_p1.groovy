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

suite("test_bitmap_filter", "query_p1,p1") {
    // create table & insert init data
    if ( isSqlValueEqualToTarget("show tables like 'bm_dup_data'", "", 1000, 1) == true ||
            isSqlValueEqualToTarget("select count(*) from bm_dup_data", "1011", 1000, 1) == false) {
        sql """drop table if exists bm_dup_data"""
        sql """drop table if exists test_k1_bitmap"""
        sql """drop table if exists test_k1_k2_k3_bitmap"""
        sql """
            CREATE TABLE IF NOT EXISTS `bm_dup_data` (
              `k1` int(11) NULL,
              `k2` int(11) NULL,
              `k3` int(11) NULL,
              `k4` bigint(20) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`, `k3`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """
        sql "insert into bm_dup_data select null, null, null, null"
        sql "begin"
        for (int i = -9; i <= 1000; i++) {
            sql "insert into bm_dup_data values ( ${(i % 10).toString()}, ${(i % 100).toString()}, " +
                    "${(i % 200).toString()}, ${i.toString()} )"
        }
        sql "commit"
        sql "sync"
        assertTrue(isSqlValueEqualToTarget("select count(*) from bm_dup_data", "1011", 1000, 3))

        sql """
            CREATE TABLE IF NOT EXISTS `test_k1_bitmap` (
              `k1` int(11) NULL,
              `bm_k4` bitmap BITMAP_UNION NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(`k1`)
            COMMENT 'OLAP'
            PARTITION BY RANGE(`k1`)
            (PARTITION p1 VALUES [("-2147483648"), ("-64")),
            PARTITION p2 VALUES [("-64"), ("0")),
            PARTITION p3 VALUES [("0"), ("64")),
            PARTITION p4 VALUES [("64"), (MAXVALUE)))
            DISTRIBUTED BY HASH(`k1`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """
        sql 'insert into test_k1_bitmap select k1, to_bitmap(k4) from bm_dup_data'
        assertTrue(isSqlValueEqualToTarget("select count(*) from test_k1_bitmap", "20", 1000, 3))

        sql """
            CREATE TABLE IF NOT EXISTS `test_k1_k2_k3_bitmap` (
              `k1` int(11) NULL,
              `k2` int(11) NULL,
              `k3` int(11) NULL,
              `bm_k1_k2_k3` bitmap BITMAP_UNION NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(`k1`, `k2`, `k3`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """
        sql "insert into test_k1_k2_k3_bitmap select k1, k2, k3, to_bitmap(k1) from bm_dup_data"
        sql 'insert into test_k1_k2_k3_bitmap select k1, k2, k3, to_bitmap(k2) from bm_dup_data'
        sql 'insert into test_k1_k2_k3_bitmap select k1, k2, k3, to_bitmap(k3) from bm_dup_data'
        assertTrue(isSqlValueEqualToTarget("select count(*) from test_k1_k2_k3_bitmap", "210", 1000, 3))
    }

    sql 'set runtime_filter_type="IN_OR_BLOOM_FILTER, BITMAP_FILTER"'
    sql 'set query_timeout=600'

    // uncorrelated subquery
    check_sql_equal(
            'select k1 from bm_dup_data where k4 in (select bm_k4 from test_k1_bitmap) and k4 = 250 order by k1',
            'select k1 from bm_dup_data where k4 = 250'
    )
    check_sql_equal(
            'select k1 from bm_dup_data where k4 in (select bm_k4 from test_k1_bitmap) and k4 = -1 order by k1',
            'select k1 from bm_dup_data where 1=0 order by k1'
    )
    check_sql_equal(
            'select k1 from bm_dup_data where 500 in (select bm_k4 from test_k1_bitmap) order by k1',
            'select k1 from bm_dup_data where 500 in (select k4 from bm_dup_data) order by k1'
    )
    check_sql_equal(
            'select count(k1) from bm_dup_data where k4 in (select bm_k4 from test_k1_bitmap order by k1)',
            'select count(k1) from bm_dup_data where k4 in (select k4 from bm_dup_data where k4 >= 0)'
    )
    check_sql_equal(
            'select * from bm_dup_data where k4 in (select bm_k4 from test_k1_bitmap order by k1 limit 15) order by k1, k2, k3, k4',
            'select * from bm_dup_data where k1 between 0 and 4 order by k1, k2, k3, k4'
    )
    check_sql_equal(
            'select k1 from bm_dup_data where k4 in (select bitmap_union(bm_k4) from test_k1_bitmap) order by k1',
            'select k1 from bm_dup_data where k4 >= 0 order by k1'
    )
    check_sql_equal(
            'select k1 from bm_dup_data where k4 in (select bitmap_union(bm_k4) from test_k1_bitmap where k1 > 0) order by k1',
            'select k1 from bm_dup_data where k1 > 0 order by k1'
    )
    check_sql_equal(
            'select k1 from bm_dup_data where k4 not in (select bm_k4 from test_k1_bitmap) and k4 != -5 order by k1',
            'select k1 from bm_dup_data where k4 < 0 and k4 != -5 order by k1'
    )
    // todo: maybe bitmap bug, expect return all, real is empty.
    //    check_sql_equal(
    //            'select k1 from test_k1_bitmap where -5 not in (select bm_k1_k2_k3 from test_k1_k2_k3_bitmap) order by k1',
    //            'select k1 from test_k1_bitmap order by k1'
    //    )
    check_sql_equal(
            'select k1 from bm_dup_data where k4 not in (select bm_k4 from test_k1_bitmap order by k1) order by k1',
            'select k1 from bm_dup_data where k4 < 0 order by k1'
    )
    check_sql_equal(
            'select k1 from bm_dup_data where k4 not in (select bm_k4 from test_k1_bitmap order by k1 desc limit 10) order by k1;',
            'select k1 from bm_dup_data where k4 < 0 order by k1'
    )
    check_sql_equal(
            'select k1 from bm_dup_data where k4 not in (select bitmap_union(bm_k4) from test_k1_bitmap) order by k1',
            'select k1 from bm_dup_data where k4 < 0 order by k1'
    )
    check_sql_equal(
            'select k1 from bm_dup_data where k4 not in (select bitmap_union(bm_k4) from test_k1_bitmap where k1 > 5) order by k1',
            'select k1 from bm_dup_data where k1 < =5 order by k1'
    )
    qt_sql1 'select count(*) from bm_dup_data a where k4 in (select bm_k4 from test_k1_bitmap b)'
    qt_sql2 'select count(*) from bm_dup_data a where k4 in (select bm_k4 from test_k1_bitmap b ) group by k1 order by k1'
    qt_sql3 'select count(*) from bm_dup_data a where k4 in (select bm_k4 from test_k1_bitmap b where k1 > 0) and ' +
            'k4 in (select bm_k4 from test_k1_bitmap b where k1 < 10)'
    qt_sql4 'select k1 from test_k1_bitmap a where k1 in (select bm_k1_k2_k3 from test_k1_k2_k3_bitmap b ' +
            'where bitmap_count(bm_k1_k2_k3) > 5) order by k1'
    // todo: query timeout
    // qt_sql5 'select count(k1) from bm_dup_data a where k4 in (select bm_k4 from test_k1_bitmap b where bitmap_count(bm_k4) = 100) group by k4 order by 1 desc limit 10'

    // correlated subquery
    test {
        sql 'select k1 from bm_dup_data a where k4 in (select bm_k4 from test_k1_bitmap b where a.k1 = b.k1 order by k1)'
        exception "In bitmap does not support correlated subquery"
    }
    // and / or
    qt_sql6 'select count(k1) from bm_dup_data a where k4 in (select bm_k4 from test_k1_bitmap b ' +
            'where bitmap_count(bm_k4) >= 100) and k1 in (1, 2, 3, 4)'
    qt_sql7 'select count(k1) from bm_dup_data a where k4 in (select bm_k4 from test_k1_bitmap b ' +
            'where bitmap_count(bm_k4) >= 100) and k1 is not null'
    qt_sql8 'select count(k1) from bm_dup_data a where k4 in (select bm_k4 from test_k1_bitmap b ' +
            'where bitmap_count(bm_k4) >= 100) and k4 > 234'
    qt_sql9 'select count(k1) from bm_dup_data a where k4 in (select bm_k4 from test_k1_bitmap b ' +
            'where bitmap_count(bm_k4) >= 100) and k4 between 456 and 789'
    qt_sql10 'select count(*) from bm_dup_data where k4 in (select bm_k4 from test_k1_bitmap) and ' +
            'k4 not in (select bm_k4 from test_k1_bitmap where bitmap_count(bm_k4) = 0)'
    qt_sql11 'select count(*) from bm_dup_data where k4 not in (select bm_k4 from test_k1_bitmap b) and ' +
            'k4 not in (select bm_k4 from test_k1_bitmap b where bitmap_count(bm_k4) = 0)'
    check_sql_equal(
        'select * from bm_dup_data where k1 in (select bm_k1_k2_k3 from test_k1_k2_k3_bitmap) and ' +
                'k2 in (select bm_k1_k2_k3 from test_k1_k2_k3_bitmap) ' +
                'and k3 in (select bm_k1_k2_k3 from test_k1_k2_k3_bitmap) order by k1, k2, k3, k4',
        'select * from bm_dup_data where k1 >= 0 order by k1, k2, k3, k4'
    )
    // datatype
    check_sql_equal(
            'select k1, k2 from test_k1_k2_k3_bitmap where k2 in (select bitmap_union(to_bitmap(k2)) from bm_dup_data) ' +
                    'order by k1, k2',
            'select k1, k2 from test_k1_k2_k3_bitmap where k2 >= 0 order by k1, k2'
    )
    qt_sql12 'select k1, k2 from (select 1 k1, 2 k2) tmp where k1 in (select bm_k1_k2_k3 from test_k1_k2_k3_bitmap)'
    qt_sql13 'select count(*) from bm_dup_data where k1 in  (select bm_k4 from test_k1_bitmap)'
    qt_sql14 'select count(*) from bm_dup_data where k2 in  (select bm_k4 from test_k1_bitmap)'
    qt_sql15 'select count(*) from bm_dup_data where k3 in  (select bm_k4 from test_k1_bitmap)'
    qt_sql16 'select count(*) from bm_dup_data where k4 in  (select bm_k4 from test_k1_bitmap)'
    qt_sql17 'select k1, k2 from bm_dup_data where k1 in (select bm_k4 from test_k1_bitmap) and k1 is null'
    // empty table：bm_empty_tb
    sql 'create table if not exists bm_empty_tb like test_k1_bitmap'
    qt_sql18 'select * from bm_empty_tb where k1 in (select bm_k4 from test_k1_bitmap)'
    qt_sql19 'select * from bm_empty_tb where k1 not in (select bm_k4 from test_k1_bitmap)'
    qt_sql20 'select k1 from test_k1_bitmap where k1 in (select bm_k4 from bm_empty_tb)'
    qt_sql21 'select k1 from test_k1_bitmap where k1 not in (select bm_k4 from bm_empty_tb) order by k1'
    sql 'drop table if exists bm_empty_tb'
    // 常数
    qt_sql22 'select * from bm_dup_data where k4 in (select to_bitmap(10) from test_k1_bitmap)'
    qt_sql23 'select * from bm_dup_data where k1 in (select bitmap_empty())'
    // subquery return empty / 1 row/ multi rows
    check_sql_equal(
            'select k4 from bm_dup_data where k1 in (select bm_k4 from test_k1_bitmap where k1 > 5) order by k4',
            'select k4 from bm_dup_data where k1 > 5'
    )
    check_sql_equal(
            'select * from bm_dup_data where k4 in (select bm_k1_k2_k3 from test_k1_k2_k3_bitmap where k1 = 6)',
            'select * from bm_dup_data where k1 = 6 and k4 < 200'
    )
    qt_sql25 'select * from bm_dup_data where k4 in (select bm_k4 from test_k1_bitmap where k1 = 200)'
    qt_sql26 'select count(*) from bm_dup_data where k2 in (select bitmap_union(bm_k4) from test_k1_bitmap)'
    check_sql_equal(
            'select * from bm_dup_data where k1 not in (select bm_k4 from test_k1_bitmap where k1 > 5) order by k1, k2, k3, k4',
            'select * from bm_dup_data where k1 <= 5 order by k1, k2, k3, k4'
    )
    check_sql_equal(
            'select * from bm_dup_data where k4 not in (select bm_k1_k2_k3 from test_k1_k2_k3_bitmap where k1 = 6) ' +
                    'order by k1, k2, k3, k4',
            'select * from bm_dup_data where not (k1 = 6 and k4 < 200) order by k1, k2, k3, k4'
    )
    check_sql_equal(
            'select k1 from test_k1_bitmap where k1 not in (select bm_k1_k2_k3 from test_k1_k2_k3_bitmap where k1 = 200)',
            'select k1 from test_k1_bitmap where k1 is not null'
    )
    qt_sql27 'select count(*) from bm_dup_data where k4 not in (select bitmap_union(bm_k4) from test_k1_bitmap)'
    qt_sql28 'select count(*) from bm_dup_data where exists (select bm_k4 from test_k1_bitmap)'
    qt_sql29 'select count(*) from bm_dup_data where exists (select bm_k4 from test_k1_bitmap where k1 = 200)'
    // todo: expect fail
    // test {
    //    sql 'select count(*) from bm_dup_data where not exists (select bm_k4 from test_k1_bitmap)'
    //    exception 'Unsupported uncorrelated NOT EXISTS subquery'
    // }

    // function & expression
    check_sql_equal(
            'select * from bm_dup_data a where k4 in (select bitmap_or(bm_k1_k2_k3, to_bitmap(k1)) ' +
                    'from test_k1_k2_k3_bitmap b where k1 > 5)',
            'select * from bm_dup_data a where k1 > 5 and k4 < 200'
    )
    check_sql_equal(
            'select * from bm_dup_data a where k4 + 1 in (select bitmap_or(bm_k1_k2_k3, to_bitmap(k1)) ' +
                    'from test_k1_k2_k3_bitmap b where k1 > 5)',
            'select * from bm_dup_data a where k1 between 5 and 8 and k4 < 200'
    )
    qt_sql30 'select k1, count(*) from bm_dup_data a where k4 + 1 in ' +
             '(select bitmap_or(bm_k1_k2_k3, to_bitmap(k1)) from test_k1_k2_k3_bitmap b where k1 > 5) ' +
             'group by k1 order by k1'
    qt_sql31 'select k1, count(*) from bm_dup_data a where k4 + 1 in ' +
             '(select bitmap_or(bm_k1_k2_k3, to_bitmap(k1)) from test_k1_k2_k3_bitmap b where k1 > 5) ' +
             'group by k1 having count(*) != 20'
    check_sql_equal(
            'select * from bm_dup_data where k3 in (select bitmap_and(to_bitmap(k2), bm_k1_k2_k3) from test_k1_k2_k3_bitmap) ' +
                    'order by k1, k2, k3, k4',
            'select * from bm_dup_data where k3 < 100 and k3 >= 0 order by k1, k2, k3, k4'
    )

    // join
    qt_sql32 'select * from bm_dup_data a join (select k3 from test_k1_k2_k3_bitmap ' +
             'where k2 in (select bm_k4 from test_k1_bitmap)) b order by b.k3, a.k1, a.k4 limit 5'
    qt_sql33 'select * from bm_dup_data a join (select k3 from test_k1_k2_k3_bitmap ' +
             'where k2 in (select bm_k4 from test_k1_bitmap)) b on b.k3=a.k3 order by b.k3, a.k1, a.k4 limit 5'
    qt_sql34 'select /*+SET_VAR(disable_join_reorder=true)*/ * from (select k3 from bm_dup_data where k4 not in (select bm_k4 from test_k1_bitmap)) a ' +
             'join (select k3 from test_k1_k2_k3_bitmap where k2 in (select bm_k4 from test_k1_bitmap)) b on b.k3=a.k3'
    qt_sql35 'select * from (select k3 from bm_dup_data where k4 not in (select bm_k4 from test_k1_bitmap)) a ' +
             'left join (select k3 from test_k1_k2_k3_bitmap where k2 in (select bm_k4 from test_k1_bitmap)) b on b.k3=a.k3 order by a.k3, b.k3'
    // union/except/intersect
    order_qt_sql36 'select k1 from bm_dup_data a ' +
            'where k1 in (select bitmap_or(bm_k4, to_bitmap(k1)) from test_k1_bitmap b where k1 > 10) union ' +
            'select k1 from bm_dup_data a ' +
            'where not k1 in (select bitmap_or(bm_k4, to_bitmap(k1)) from test_k1_bitmap b where k1 > 10)'
    order_qt_sql37 'select k1 from bm_dup_data a ' +
            'where k1 in (select bitmap_or(bm_k4, to_bitmap(k1)) from test_k1_bitmap b where k1 > 5) except ' +
            'select k1 from bm_dup_data a ' +
            'where not k1 in (select bitmap_or(bm_k4, to_bitmap(k1)) from test_k1_bitmap b where k1 > 0) order by k1'
    order_qt_sql38 'select k1 from bm_dup_data a ' +
            'where k1 in (select bitmap_or(bm_k4, to_bitmap(k1)) from test_k1_bitmap b where k1 > 5) intersect ' +
            'select k1 from bm_dup_data a ' +
            'where not k1 in (select bitmap_or(bm_k4, to_bitmap(k1)) from test_k1_bitmap b where k1 > 8)'
    // group by/having/order by/limit
    qt_sql39 'select distinct k1 from bm_dup_data a ' +
            'where k1 in (select bitmap_or(bm_k4, to_bitmap(k1)) from test_k1_bitmap b where k1 > 0) order by k1 limit 10'
    qt_sql40 'select count(distinct k1) from bm_dup_data a ' +
            'where k1 in (select bitmap_or(bm_k4, to_bitmap(k1)) from test_k1_bitmap b where k1 > 5)'
    qt_sql41 'select k1, count(*) from bm_dup_data a ' +
            'where k1 in (select bitmap_or(bm_k4, to_bitmap(k1)) from test_k1_bitmap b where k1 > 5) ' +
            'group by k1 having count(*) > 20 order by k1 desc limit 10 offset 2'
    qt_sql42 'select count(*) from bm_dup_data a where k4 in (select bitmap_union(bm_k4) from test_k1_bitmap b ' +
            'group by k1 order by k1 limit 15)'
    // subquery
    qt_sql43 'select count(*) from (select k1, count(*) from bm_dup_data ' +
            'where k4 in (select bitmap_union(bm_k4) from test_k1_bitmap) group by k1) tmp'
    qt_sql44 'select case (select max(k1) from bm_dup_data ' +
            'where k4 in (select bitmap_union(bm_k4) from test_k1_bitmap)) when 1.5 then k1 else "no" end a ' +
            'from test_k1_bitmap b order by a limit 5'
    // view
    sql 'drop view if exists bmv'
    sql 'CREATE VIEW `bmv` AS SELECT k1 FROM bm_dup_data WHERE k4 IN ((SELECT bm_k4 FROM test_k1_bitmap))'
    qt_sql45 'select a.k1, count(*) from bm_dup_data a join bmv b on a.k1=b.k1 group by a.k1 order by a.k1'
    qt_sql46 'select count(*) from bm_dup_data a join bmv b on a.k1=b.k1'
    sql 'drop view if exists bmv'
    // with cte
    qt_sql47 'with w as (select k1 from bm_dup_data where k4 not in (select bm_k4 from test_k1_bitmap)) ' +
            'select * from w order by k1'
    qt_sql48 'with w1 as (select k1 from bm_dup_data where k4 in (select bm_k4 from test_k1_bitmap)), ' +
            'w2 as (select k2 from bm_dup_data where k1 in (select bm_k1_k2_k3 from test_k1_k2_k3_bitmap)) ' +
            'select count(*) from (select * from w1 union all select * from w2 order by 1) tmp'
}
