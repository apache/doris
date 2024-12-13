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
    sql "set enable_fallback_to_original_planner=false"

    sql "drop table if exists range_table1"
    sql """
        CREATE TABLE `range_table1` (
        `TIME_STAMP` datetimev2 NOT NULL COMMENT '采集日期'
        )
        DUPLICATE KEY(`TIME_STAMP`)
        auto partition by range (date_trunc(`TIME_STAMP`, 'day'))
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

    sql "drop table if exists range_table_date"
    sql """
        CREATE TABLE `range_table_date` (
        `TIME_STAMP` datev2 NOT NULL COMMENT '采集日期'
        ) ENGINE=OLAP
        DUPLICATE KEY(`TIME_STAMP`)
        COMMENT 'OLAP'
        auto partition by range (date_trunc(`TIME_STAMP`, 'month'))
        (
        )
        DISTRIBUTED BY HASH(`TIME_STAMP`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """ insert into range_table_date values ('2022-11-14'), ('2022-12-15'), ('2022-12-16'), ('2022-12-17'), ('2022-05-18'), ('2022-12-19'), ('2022-12-20') """
    sql """ insert into range_table_date values ('2122-12-14'), ('2122-12-15'), ('2122-12-16'), ('2122-12-17'), ('2122-09-18'), ('2122-12-19'), ('2122-12-20') """

    qt_date1 """ select * from range_table_date order by TIME_STAMP """
    qt_date2 """ select * from range_table_date WHERE TIME_STAMP = '2022-12-15' order by TIME_STAMP """
    qt_date3 """ select * from range_table_date WHERE TIME_STAMP > '2022-12-15' order by TIME_STAMP """

    sql "drop table if exists range_table2"
    sql """
        CREATE TABLE `range_table2` (
        `TIME_STAMP` datetimev2(3) NOT NULL COMMENT '采集日期'
        ) ENGINE=OLAP
        DUPLICATE KEY(`TIME_STAMP`)
        COMMENT 'OLAP'
        auto partition by range (date_trunc(`TIME_STAMP`, 'day'))
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

    sql "drop table if exists right_bound"
    sql """
            create table right_bound(
                k0 datetime(6) not null
            )
            auto partition by range (date_trunc(k0, 'second'))
            (
                partition pX values less than ("1970-01-01")
            )
            DISTRIBUTED BY HASH(`k0`) BUCKETS auto
            properties("replication_num" = "1");
        """
    sql " insert into right_bound values ('9999-12-31 23:59:59'); "
    sql " insert into right_bound values ('9999-12-31 23:59:59.999999'); "
    qt_right_bound " select * from right_bound order by k0; "
    def result2 = sql "show partitions from right_bound"
    logger.info("${result2}")
    assertEquals(result2.size(), 2)

    sql "drop table if exists week_range"
    sql """
        CREATE TABLE `week_range` (
        `TIME_STAMP` datev2 NOT NULL
        )
        DUPLICATE KEY(`TIME_STAMP`)
        auto partition by range (date_trunc(`TIME_STAMP`, 'week'))
        (
        )
        DISTRIBUTED BY HASH(`TIME_STAMP`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql " insert into week_range values (20240408), (20240409); "
    result2 = sql "show partitions from week_range"
    logger.info("${result2}")
    assertEquals(result2.size(), 1)

    sql "drop table if exists quarter_range"
    sql """
        CREATE TABLE `quarter_range` (
        `TIME_STAMP` datev2 NOT NULL
        )
        DUPLICATE KEY(`TIME_STAMP`)
        auto partition by range (date_trunc(`TIME_STAMP`, 'quarter'))
        (
        )
        DISTRIBUTED BY HASH(`TIME_STAMP`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql " insert into quarter_range values (20240102), (20240330), (20241001), (20241231); "
    result2 = sql "show partitions from quarter_range"
    logger.info("${result2}")
    assertEquals(result2.size(), 2)

    // insert into select have multi sender in load
    sql " drop table if exists isit "
    sql " drop table if exists isit_src "
    sql """
        CREATE TABLE isit (
            k DATE NOT NULL
        )
        AUTO PARTITION BY RANGE (date_trunc(k, 'day'))()
        DISTRIBUTED BY HASH(k) BUCKETS AUTO
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        ); 
    """
    sql """
        CREATE TABLE isit_src (
            k DATE NOT NULL
        )
        DISTRIBUTED BY HASH(k) BUCKETS AUTO
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        ); 
    """
    sql " insert into isit_src values (20201212); "
    sql " insert into isit select * from isit_src "
    sql " sync "
    qt_sql " select * from isit order by k "

    sql "drop table if exists awh_test_range_auto"
    test {
        sql """
            CREATE TABLE awh_test_range_auto (
                DATE_ID BIGINT NOT NULL,
                LAST_UPLOAD_TIME DATETIME
            )
            AUTO PARTITION BY RANGE (DATE_ID)()
            DISTRIBUTED BY HASH(DATE_ID) BUCKETS AUTO
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "auto create partition only support date_trunc function of RANGE partition"
    }
    test {
        sql """
            CREATE TABLE awh_test_range_auto (
                DATE_ID BIGINT NOT NULL,
                LAST_UPLOAD_TIME DATETIME
            )
            AUTO PARTITION BY RANGE (date(DATE_ID))()
            DISTRIBUTED BY HASH(DATE_ID) BUCKETS AUTO
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "auto create partition only support function call expr is"
    }
    test {
        sql """
            CREATE TABLE awh_test_range_auto (
                DATE_ID BIGINT NOT NULL,
                LAST_UPLOAD_TIME DATETIME
            )
            AUTO PARTITION BY RANGE (date_trunc(DATE_ID))()
            DISTRIBUTED BY HASH(DATE_ID) BUCKETS AUTO
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "partition expr date_trunc is illegal!"
    }
    test {
        sql """
            CREATE TABLE awh_test_range_auto (
                DATE_ID BIGINT NOT NULL,
                LAST_UPLOAD_TIME DATETIME
            )
            AUTO PARTITION BY RANGE (date_trunc(DATE_ID, 'year'))()
            DISTRIBUTED BY HASH(DATE_ID) BUCKETS AUTO
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "partition expr date_trunc is illegal!"
    }
    sql """
        CREATE TABLE awh_test_range_auto (
            DATE_ID BIGINT NOT NULL,
            LAST_UPLOAD_TIME DATETIME NOT NULL
        )
        AUTO PARTITION BY RANGE (date_trunc(LAST_UPLOAD_TIME, 'yeear'))()
        DISTRIBUTED BY HASH(DATE_ID) BUCKETS AUTO
        PROPERTIES (
            "replication_num" = "1"
        );
    """
    test {
        sql "insert into awh_test_range_auto values (1,'20201212')"
        exception "date_trunc function second param only support argument is"
    }
}
