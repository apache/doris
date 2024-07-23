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

suite("test_auto_partition_behavior") {
    /// unique key table
    sql "drop table if exists unique_table"
    sql """
        CREATE TABLE `unique_table` (
            `str` varchar not null,
            `dummy` int
        ) ENGINE=OLAP
        UNIQUE KEY(`str`)
        COMMENT 'OLAP'
        AUTO PARTITION BY LIST (`str`)
        (
            PARTITION `partition_origin` values in (("Xxx"), ("Yyy"))
        )
        DISTRIBUTED BY HASH(`str`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    // special characters
    sql """ insert into unique_table values (" ", 1), ("  ", 1), ("Xxx", 1), ("xxX", 1), (" ! ", 1), (" !  ", 1) """
    qt_sql1 """ select str,length(str) from unique_table order by `str` """
    def result = sql "show partitions from unique_table"
    assertEquals(result.size(), 6)
    sql """ insert into unique_table values (" ", 1), ("  ", 1), ("Xxx", 1), ("xxX", 1), (" ! ", 1), (" !  ", 1) """
    qt_sql2 """ select str,length(str) from unique_table order by `str` """
    result = sql "show partitions from unique_table"
    assertEquals(result.size(), 6)
    sql """ insert into unique_table values ("-", 1), ("--", 1), ("- -", 1), (" - ", 1) """
    result = sql "show partitions from unique_table"
    assertEquals(result.size(), 10)
    // add partition
    try {
        sql """ alter table unique_table add partition padd values in ("Xxx") """
        fail()
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("is conflict with current partitionKeys"))
    }
    // drop partition
    def partitions = sql "show partitions from unique_table order by PartitionName"
    def partition1_name = partitions[0][1]
    sql """ alter table unique_table drop partition ${partition1_name} """ // partition ' '
    result = sql "show partitions from unique_table"
    assertEquals(result.size(), 9)
    qt_sql3 """ select str,length(str) from unique_table order by `str` """

    /// duplicate key table
    sql "drop table if exists dup_table"
    sql """
        CREATE TABLE `dup_table` (
            `str` varchar not null
        ) ENGINE=OLAP
        DUPLICATE KEY(`str`)
        COMMENT 'OLAP'
        AUTO PARTITION BY LIST (`str`)
        (
            PARTITION `partition_origin` values in (("Xxx"), ("Yyy"))
        )
        DISTRIBUTED BY HASH(`str`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    // special characters
    sql """ insert into dup_table values (" "), ("  "), ("Xxx"), ("xxX"), (" ! "), (" !  ") """
    qt_sql1 """ select * from dup_table order by `str` """
    result = sql "show partitions from dup_table"
    assertEquals(result.size(), 6)
    sql """ insert into dup_table values (" "), ("  "), ("Xxx"), ("xxX"), (" ! "), (" !  ") """
    qt_sql2 """ select *,length(str) from dup_table order by `str` """
    result = sql "show partitions from dup_table"
    assertEquals(result.size(), 6)
    sql """ insert into dup_table values ("-"), ("--"), ("- -"), (" - ") """
    result = sql "show partitions from dup_table"
    assertEquals(result.size(), 10)
    // add partition
    try {
        sql """ alter table dup_table add partition padd values in ("Xxx") """
        fail()
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("is conflict with current partitionKeys"))
    }
    // drop partition
    partitions = sql "show partitions from dup_table order by PartitionName"
    partition1_name = partitions[0][1]
    sql """ alter table dup_table drop partition ${partition1_name} """
    result = sql "show partitions from dup_table"
    assertEquals(result.size(), 9)
    qt_sql3 """ select *,length(str) from dup_table order by `str` """
    // crop
    qt_sql4 """ select * ,length(str) from dup_table where str > ' ! ' order by str """

    /// agg key table
    sql "drop table if exists agg_dt6"
    sql """
        CREATE TABLE `agg_dt6` (
            `k0` datetime(6) not null,
            `k1` datetime(6) max not null
        ) ENGINE=OLAP
        AGGREGATE KEY(`k0`)
        COMMENT 'OLAP'
        auto partition by range (date_trunc(`k0`, 'year'))
        (
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    // modify when no partition
    sql """ alter table agg_dt6 add partition `p2010` values less than ('2010-01-01') """
    // insert
    sql """ insert into agg_dt6 values ('2020-12-12', '2020-12-12'), ('2020-12-12', '2020-12-12 12:12:12.123456'), ('2020-12-12', '20121212'), (20131212, 20131212) """
    sql """ insert into agg_dt6 values ('2009-12-12', '2020-12-12'), ('2010-12-12', '2020-12-12 12:12:12.123456'), ('2011-12-12', '20121212'), (20121212, 20131212) """
    qt_sql1 """ select * from agg_dt6 order by k0, k1 """
    // crop
    qt_sql2 """ select * from agg_dt6 where k1 <= '2020-12-12 12:12:12.123456' order by k0, k1 """
    qt_sql3 """ select * from agg_dt6 partition (p2010) order by k0, k1 """
    // add partition
    try {
        sql """ alter table agg_dt6 add partition padd values [("2013-05-05"), ("2014-05-05")) """
        fail()
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("is intersected with range: [types: [DATETIMEV2]; keys: [2013-01-01 00:00:00]; ..types: [DATETIMEV2]; keys: [2014-01-01 00:00:00];"))
    }
    // modify partition
    sql """ alter table agg_dt6 drop partition p2010 """
    qt_sql4 """ select * from agg_dt6 order by k0, k1 """
    sql """ insert into agg_dt6 values ('2020-12-12', '2020-12-12'), ('2020-12-12', '2020-12-12 12:12:12.123456'), ('2020-12-12', '20121212'), (20131212, 20131212) """
    result = sql "show partitions from agg_dt6"
    assertEquals(result.size(), 5)

    /// insert overwrite
    sql "drop table if exists `rewrite`"
    sql """
        CREATE TABLE `rewrite` (
            `str` varchar not null
        ) ENGINE=OLAP
        DUPLICATE KEY(`str`)
        COMMENT 'OLAP'
        AUTO PARTITION BY LIST (`str`)
        (
            PARTITION `p1` values in (("Xxx"), ("Yyy"))
        )
        DISTRIBUTED BY HASH(`str`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """ insert into rewrite values ("Xxx"); """
    // legacy planner
    sql " set experimental_enable_nereids_planner=false "
    try {
        sql """ insert overwrite table rewrite partition(p1) values ("XXX") """
        fail()
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Insert has filtered data in strict mode"))
    }
    sql """ insert overwrite table rewrite partition(p1) values ("Yyy") """
    qt_sql_overwrite1 """ select * from rewrite """ // Yyy
    // nereids planner
    sql " set experimental_enable_nereids_planner=true "
    try {
        sql """ insert overwrite table rewrite partition(p1) values ("") """
        fail()
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("Insert has filtered data in strict mode"))
    }
    sql """ insert overwrite table rewrite partition(p1) values ("Xxx") """
    qt_sql_overwrite2 """ select * from rewrite """ // Xxx

    sql " drop table if exists non_order; "
    sql """
        CREATE TABLE `non_order` (
            `k0` int not null,
            `k1` datetime(6) not null
        )
        auto partition by range (date_trunc(`k1`, 'year'))
        (
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    // insert
    sql """ insert into non_order values (1, '2020-12-12'); """
    sql """ insert into non_order values (2, '2023-12-12'); """
    sql """ insert into non_order values (3, '2013-12-12'); """
    qt_sql_non_order1 """ select * from non_order where k1 = '2020-12-12'; """
    qt_sql_non_order2 """ select * from non_order where k1 = '2023-12-12'; """
    qt_sql_non_order3 """ select * from non_order where k1 = '2013-12-12'; """

    // range partition can't auto create null partition
    sql " set experimental_enable_nereids_planner=true "
    sql "drop table if exists invalid_null_range"
    test {
        sql """
            create table invalid_null_range(
                k0 datetime(6) null
            )
            auto partition by range (date_trunc(k0, 'hour'))
            (
            )
            DISTRIBUTED BY HASH(`k0`) BUCKETS 2
            properties("replication_num" = "1");
        """
        exception "AUTO RANGE PARTITION doesn't support NULL column"
    }
    sql " set experimental_enable_nereids_planner=false "
    sql "drop table if exists invalid_null_range"
    test {
        sql """
            create table invalid_null_range(
                k0 datetime(6) null
            )
            auto partition by range (date_trunc(k0, 'hour'))
            (
            )
            DISTRIBUTED BY HASH(`k0`) BUCKETS 2
            properties("replication_num" = "1");
        """
        exception "AUTO RANGE PARTITION doesn't support NULL column"
    }

    sql "drop table if exists test_dynamic"
    sql """
            create table test_dynamic(
                k0 DATE not null
            )
            auto partition by range (date_trunc(k0, 'year')) ()
            DISTRIBUTED BY HASH(`k0`) BUCKETS auto
            properties("replication_num" = "1");
        """

    // PROHIBIT different timeunit of interval when use both auto & dynamic partition
    sql "set experimental_enable_nereids_planner=true;"
    test{
        sql """
            CREATE TABLE tbl3
            (
                k1 DATETIME NOT NULL,
                col1 int 
            )
            auto partition by range (date_trunc(`k1`, 'year')) ()
            DISTRIBUTED BY HASH(k1)
            PROPERTIES
            (
                "replication_num" = "1",
                "dynamic_partition.create_history_partition"="true",
                "dynamic_partition.enable" = "true",
                "dynamic_partition.time_unit" = "HOUR",
                "dynamic_partition.start" = "-2",
                "dynamic_partition.end" = "2",
                "dynamic_partition.prefix" = "p",
                "dynamic_partition.buckets" = "8"
            ); 
        """
        exception "Can't use Dynamic Partition and Auto Partition at the same time"
    }
    test {
        sql """
            ALTER TABLE test_dynamic set (
                "dynamic_partition.enable" = "true", 
                "dynamic_partition.time_unit" = "DAY", 
                "dynamic_partition.end" = "3", 
                "dynamic_partition.prefix" = "p", 
                "dynamic_partition.buckets" = "32"
            );
        """
        exception "Can't use Dynamic Partition and Auto Partition at the same time"
    }

    sql "set experimental_enable_nereids_planner=false;"
    test{
        sql """
            CREATE TABLE tbl3
            (
                k1 DATETIME NOT NULL,
                col1 int 
            )
            auto partition by range (date_trunc(`k1`, 'year')) ()
            DISTRIBUTED BY HASH(k1)
            PROPERTIES
            (
                "replication_num" = "1",
                "dynamic_partition.create_history_partition"="true",
                "dynamic_partition.enable" = "true",
                "dynamic_partition.time_unit" = "HOUR",
                "dynamic_partition.start" = "-2",
                "dynamic_partition.end" = "2",
                "dynamic_partition.prefix" = "p",
                "dynamic_partition.buckets" = "8"
            ); 
        """
        exception "Can't use Dynamic Partition and Auto Partition at the same time"
    }
    test {
        sql """
            ALTER TABLE test_dynamic set (
                "dynamic_partition.enable" = "true", 
                "dynamic_partition.time_unit" = "DAY", 
                "dynamic_partition.end" = "3", 
                "dynamic_partition.prefix" = "p", 
                "dynamic_partition.buckets" = "32"
            );
        """
        exception "Can't use Dynamic Partition and Auto Partition at the same time"
    }

    // prohibit too long value for partition column
    sql "drop table if exists `long_value`"
    sql """
        CREATE TABLE `long_value` (
            `str` varchar not null
        )
        DUPLICATE KEY(`str`)
        AUTO PARTITION BY LIST (`str`)
        ()
        DISTRIBUTED BY HASH(`str`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    test{
        sql """insert into `long_value` values ("jwklefjklwehrnkjlwbfjkwhefkjhwjkefhkjwehfkjwehfkjwehfkjbvkwebconqkcqnocdmowqmosqmojwnqknrviuwbnclkmwkj");"""
        def exception_str = isGroupCommitMode() ? "s length is over limit of 50." : "Partition name's length is over limit of 50."
        exception exception_str
    }

    // illegal partiton definetion
    sql "set experimental_enable_nereids_planner=false;"
    test{
        sql """
            create table illegal(
                k0 datetime(6) NOT null,
                k1 datetime(6) NOT null
            )
            auto partition by range (date_trunc(k0, k1, 'hour'))
            (
            )
            DISTRIBUTED BY HASH(`k0`) BUCKETS 2
            properties("replication_num" = "1");
        """
        exception "auto create partition only support one slotRef in function expr"
    }

    sql "set experimental_enable_nereids_planner=true;"
    sql "set enable_fallback_to_original_planner=false;"
    test{
        sql """
            create table illegal(
                k0 datetime(6) NOT null,
                k1 datetime(6) NOT null
            )
            auto partition by range (date_trunc(k0, k1, 'hour'))
            (
            )
            DISTRIBUTED BY HASH(`k0`) BUCKETS 2
            properties("replication_num" = "1");
        """
        exception "auto create partition only support one slotRef in function expr"
    }
    // test displacement of partition function
    test{
        sql """
            create table illegal(
                k0 datetime(6) NOT null,
                k1 int NOT null
            )
            auto partition by range (date_trunc(k1, 'hour'))
            (
            )
            DISTRIBUTED BY HASH(`k0`) BUCKETS 2
            properties("replication_num" = "1");
        """
        exception "partition expr date_trunc is illegal!"
    }
    sql "set experimental_enable_nereids_planner=false;"
    test{
        sql """
            create table illegal(
                k0 datetime(6) NOT null,
                k1 int NOT null
            )
            auto partition by range (date_trunc(k1, 'hour'))
            (
            )
            DISTRIBUTED BY HASH(`k0`) BUCKETS 2
            properties("replication_num" = "1");
        """
        exception "Auto range partition needs Date/DateV2/Datetime/DatetimeV2 column as partition column"
    }
}
