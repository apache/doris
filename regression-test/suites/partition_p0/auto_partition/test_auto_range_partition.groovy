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

suite("test_auto_range_partition") {
    sql "drop table if exists range_table1"
    sql """
        CREATE TABLE `range_table1` (
        `TIME_STAMP` datetimev2 NOT NULL COMMENT '采集日期'
        ) ENGINE=OLAP
        DUPLICATE KEY(`TIME_STAMP`)
        COMMENT 'OLAP'
        AUTO PARTITION BY RANGE date_trunc(`TIME_STAMP`, 'day')
        (
        )
        DISTRIBUTED BY HASH(`TIME_STAMP`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """ insert into range_table1 values ('2022-12-14'), ('2022-12-15'), ('2022-12-16'), ('2022-12-17'), ('2022-12-18'), ('2022-12-19'), ('2022-12-20') """
    sql """ insert into range_table1 values ('2122-12-14'), ('2122-12-15'), ('2122-12-16'), ('2122-12-17'), ('2122-12-18'), ('2122-12-19'), ('2122-12-20') """

    qt_select00 """ select * from range_table1 order by TIME_STAMP """
    qt_select01 """ select * from range_table1 WHERE TIME_STAMP = '2022-12-15' order by TIME_STAMP """
    qt_select02 """ select * from range_table1 WHERE TIME_STAMP > '2022-12-15' order by TIME_STAMP """

    def tblDate = "range_table_date"
    sql "drop table if exists ${tblDate}"
    sql """
        CREATE TABLE `${tblDate}` (
        `TIME_STAMP` datev2 NOT NULL COMMENT '采集日期'
        ) ENGINE=OLAP
        DUPLICATE KEY(`TIME_STAMP`)
        COMMENT 'OLAP'
        AUTO PARTITION BY RANGE date_trunc(`TIME_STAMP`, 'month')
        (
        )
        DISTRIBUTED BY HASH(`TIME_STAMP`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """ insert into ${tblDate} values ('2022-11-14'), ('2022-12-15'), ('2022-12-16'), ('2022-12-17'), ('2022-05-18'), ('2022-12-19'), ('2022-12-20') """
    sql """ insert into ${tblDate} values ('2122-12-14'), ('2122-12-15'), ('2122-12-16'), ('2122-12-17'), ('2122-09-18'), ('2122-12-19'), ('2122-12-20') """

    qt_date1 """ select * from ${tblDate} order by TIME_STAMP """
    qt_date2 """ select * from ${tblDate} WHERE TIME_STAMP = '2022-12-15' order by TIME_STAMP """
    qt_date3 """ select * from ${tblDate} WHERE TIME_STAMP > '2022-12-15' order by TIME_STAMP """

    sql "drop table if exists range_table2"
    sql """
        CREATE TABLE `range_table2` (
        `TIME_STAMP` datetimev2(3) NOT NULL COMMENT '采集日期'
        ) ENGINE=OLAP
        DUPLICATE KEY(`TIME_STAMP`)
        COMMENT 'OLAP'
        AUTO PARTITION BY RANGE date_trunc(`TIME_STAMP`, 'day')
        (
        )
        DISTRIBUTED BY HASH(`TIME_STAMP`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """ insert into range_table2 values ('2022-12-14 22:22:22.222'), ('2022-12-15 22:22:22.222'), ('2022-12-16 22:22:22.222'), ('2022-12-17 22:22:22.222'), ('2022-12-18 22:22:22.222'), ('2022-12-19 22:22:22.222'), ('2022-12-20 22:22:22.222') """
    sql """ insert into range_table2 values ('2122-12-14 22:22:22.222'), ('2122-12-15 22:22:22.222'), ('2122-12-16 22:22:22.222'), ('2122-12-17 22:22:22.222'), ('2122-12-18 22:22:22.222'), ('2122-12-19 22:22:22.222'), ('2122-12-20 22:22:22.222') """
    sql """ insert into range_table2 values ('2022-11-14 22:22:22.222'), ('2022-11-15 22:22:22.222'), ('2022-11-16 22:22:22.222'), ('2022-11-17 22:22:22.222'), ('2022-11-18 22:22:22.222'), ('2022-11-19 22:22:22.222'), ('2022-11-20 22:22:22.222') """


    qt_select10 """ select * from range_table2 order by TIME_STAMP """
    qt_select11 """ select * from range_table2 WHERE TIME_STAMP = '2022-12-15 22:22:22.222' order by TIME_STAMP """
    qt_select12 """ select * from range_table2 WHERE TIME_STAMP > '2022-12-15 22:22:22.222' order by TIME_STAMP """

    sql "drop table if exists range_table3"
    sql """
        CREATE TABLE `range_table3` (
            `k1` INT,
            `k2` DATETIMEV2(3) NOT NULL,
            `k3` DATETIMEV2(6)
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        AUTO PARTITION BY RANGE date_trunc(`k2`, 'day')
        (
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 16
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """ insert into range_table3 values (1, '1990-01-01', '2000-01-01 12:12:12.123456'), (2, '1991-02-01', '2000-01-01'), (3, '1991-01-01', '2000-01-01'), (3, '1991-01-01', '2000-01-01') """
    result1 = sql "show partitions from range_table3"
    logger.info("${result1}")
    assertEquals(result1.size(), 3)

    sql "drop table if exists right_bound"
    sql """
            create table right_bound(
                k0 datetime(6) null
            )
            auto partition by range date_trunc(k0, 'second')
            (
            )
            DISTRIBUTED BY HASH(`k0`) BUCKETS auto
            properties("replication_num" = "1");
        """
    sql " insert into right_bound values ('9999-12-31 23:59:59'); "
    sql " insert into right_bound values ('9999-12-31 23:59:59.999999'); "
    qt_right_bound " select * from right_bound order by k0; "
    result2 = sql "show partitions from right_bound"
    logger.info("${result2}")
    assertEquals(result2.size(), 1)
}
