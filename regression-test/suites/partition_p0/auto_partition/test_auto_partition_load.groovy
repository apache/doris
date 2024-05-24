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

suite("test_auto_partition_load") {
    sql "drop table if exists load_table1"
    sql """
        CREATE TABLE `load_table1` (
            `k1` INT,
            `k2` DATETIME NOT NULL,
            `k3` DATETIMEV2(6)
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        auto partition by range (date_trunc(`k2`, 'year'))
        (
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 16
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    streamLoad {
        table "load_table1"
        set 'column_separator', ','
        file "auto_partition_stream_load1.csv"
        time 20000
    }
    sql """ insert into load_table1 values (11, '2007-12-12 12:12:12.123', '2001-11-14 12:12:12.123456') """
    sql """ insert into load_table1 values (12, '2008-12-12 12:12:12.123', '2001-11-14 12:12:12.123456') """
    sql """ insert into load_table1 values (13, '2003-12-12 12:12:12.123', '2001-11-14 12:12:12.123456') """
    sql """ insert into load_table1 values (14, '2002-12-12 12:12:12.123', '2001-11-14 12:12:12.123456') """

    qt_select1 "select * from load_table1 order by k1"
    result1 = sql "show partitions from load_table1"
    logger.info("${result1}")
    assertEquals(result1.size(), 8)


    sql "drop table if exists load_table2"
    sql """
        CREATE TABLE `load_table2` (
            `k1` INT,
            `k2` VARCHAR(50) not null,
            `k3` DATETIMEV2(6)
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        AUTO PARTITION BY LIST (`k2`)
        (
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 16
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    streamLoad {
        table "load_table2"
        set 'column_separator', ','
        file "auto_partition_stream_load2.csv"
        time 20000
    }
    sql """ insert into load_table2 values (11, '11', '2123-11-14 12:12:12.123456') """
    sql """ insert into load_table2 values (12, 'Chengdu', '2123-11-14 12:12:12.123456') """
    sql """ insert into load_table2 values (13, '11', '2123-11-14 12:12:12.123456') """
    sql """ insert into load_table2 values (14, '12', '2123-11-14 12:12:12.123456') """

    qt_select2 "select * from load_table2 order by k1"
    result2 = sql "show partitions from load_table2"
    logger.info("${result2}")
    assertEquals(result2.size(), 11)
}
