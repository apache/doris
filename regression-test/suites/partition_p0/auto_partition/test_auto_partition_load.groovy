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
    def tblName1 = "load_table1"
    sql "drop table if exists ${tblName1}"
    sql """
        CREATE TABLE `${tblName1}` (
            `k1` INT,
            `k2` DATETIME,
            `k3` DATETIMEV2(6)
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        AUTO PARTITION BY RANGE date_trunc(`k2`, 'year')
        (
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 16
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    streamLoad {
        table "${tblName1}"
        set 'column_separator', ','
        file "auto_partition_stream_load1.csv"
        time 20000
    }
    sql """ insert into ${tblName1} values (11, '2007-12-12 12:12:12.123', '2001-11-14 12:12:12.123456') """
    sql """ insert into ${tblName1} values (12, '2008-12-12 12:12:12.123', '2001-11-14 12:12:12.123456') """
    sql """ insert into ${tblName1} values (13, '2003-12-12 12:12:12.123', '2001-11-14 12:12:12.123456') """
    sql """ insert into ${tblName1} values (14, '2002-12-12 12:12:12.123', '2001-11-14 12:12:12.123456') """

    qt_select1 "select * from ${tblName1} order by k1"
    result1 = sql "show partitions from ${tblName1}"
    logger.info("${result1}")
    assertEquals(result1.size(), 8)


    def tblName2 = "load_table2"
    sql "drop table if exists ${tblName2}"
    sql """
        CREATE TABLE `${tblName2}` (
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
        table "${tblName2}"
        set 'column_separator', ','
        file "auto_partition_stream_load2.csv"
        time 20000
    }
    sql """ insert into ${tblName2} values (11, '11', '2123-11-14 12:12:12.123456') """
    sql """ insert into ${tblName2} values (12, 'Chengdu', '2123-11-14 12:12:12.123456') """
    sql """ insert into ${tblName2} values (13, '11', '2123-11-14 12:12:12.123456') """
    sql """ insert into ${tblName2} values (14, '12', '2123-11-14 12:12:12.123456') """

    qt_select2 "select * from ${tblName2} order by k1"
    result2 = sql "show partitions from ${tblName2}"
    logger.info("${result2}")
    assertEquals(result2.size(), 11)
}
