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
            `str` varchar not null
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
    sql """ insert into unique_table values (" "), ("  "), ("Xxx"), ("xxX"), (" ! "), (" !  ") """
    qt_sql1 """ select * from unique_table order by `str` """
    def result = sql "show partitions from unique_table"
    assertEquals(result.size(), 6)
    sql """ insert into unique_table values (" "), ("  "), ("Xxx"), ("xxX"), (" ! "), (" !  ") """
    qt_sql2 """ select * from unique_table order by `str` """
    result = sql "show partitions from unique_table"
    assertEquals(result.size(), 6)
    sql """ insert into unique_table values ("-"), ("--"), ("- -"), (" - ") """
    result = sql "show partitions from unique_table"
    assertEquals(result.size(), 10)
    // drop partition
    def partitions = sql "show partitions from unique_table order by PartitionName"
    def partition1_name = partitions[0][1]
    sql """ alter table unique_table drop partition ${partition1_name} """ // partition ' '
    result = sql "show partitions from unique_table"
    assertEquals(result.size(), 9)
    qt_sql3 """ select * from unique_table order by `str` """
    // modify value 
    sql """ update unique_table set str = "modified" where str in (" ", "  ") """ // only "  "
    qt_sql4 """ select * from unique_table where str = '  ' order by `str` """ // modified
    qt_sql5 """ select count() from unique_table where str = 'modified' """
    // crop
    qt_sql6 """ select * from unique_table where ((str > ' ! ' || str = 'modified') && str != 'Xxx') order by str """


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
    qt_sql2 """ select * from dup_table order by `str` """
    result = sql "show partitions from dup_table"
    assertEquals(result.size(), 6)
    sql """ insert into dup_table values ("-"), ("--"), ("- -"), (" - ") """
    result = sql "show partitions from dup_table"
    assertEquals(result.size(), 10)
    // drop partition
    partitions = sql "show partitions from dup_table order by PartitionName"
    partition1_name = partitions[0][1]
    sql """ alter table dup_table drop partition ${partition1_name} """
    result = sql "show partitions from dup_table"
    assertEquals(result.size(), 9)
    qt_sql3 """ select * from dup_table order by `str` """
    // crop
    qt_sql4 """ select * from dup_table where str > ' ! ' order by str """

    /// agg key table
    sql "drop table if exists agg_dt6"
    sql """
        CREATE TABLE `agg_dt6` (
            `k0` datetime(6) not null,
            `k1` datetime(6) max not null
        ) ENGINE=OLAP
        AGGREGATE KEY(`k0`)
        COMMENT 'OLAP'
        AUTO PARTITION BY RANGE date_trunc(`k0`, 'year')
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
    // modify partition
    sql """ alter table agg_dt6 drop partition p2010 """
    qt_sql4 """ select * from agg_dt6 order by k0, k1 """
    sql """ insert into agg_dt6 values ('2020-12-12', '2020-12-12'), ('2020-12-12', '2020-12-12 12:12:12.123456'), ('2020-12-12', '20121212'), (20131212, 20131212) """
    result = sql "show partitions from agg_dt6"
    assertEquals(result.size(), 5)
}
