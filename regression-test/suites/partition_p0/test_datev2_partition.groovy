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

suite("test_datev2_partition") {
    sql "drop table if exists test_datev2_partition1"
    sql """
        CREATE TABLE `test_datev2_partition1` (
        `TIME_STAMP` datev2 NOT NULL COMMENT '采集日期'
        ) ENGINE=OLAP
        DUPLICATE KEY(`TIME_STAMP`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`TIME_STAMP`)
        (PARTITION p20221214 VALUES [('2022-12-14'), ('2022-12-15')),
        PARTITION p20221215 VALUES [('2022-12-15'), ('2022-12-16')),
        PARTITION p20221216 VALUES [('2022-12-16'), ('2022-12-17')),
        PARTITION p20221217 VALUES [('2022-12-17'), ('2022-12-18')),
        PARTITION p20221218 VALUES [('2022-12-18'), ('2022-12-19')),
        PARTITION p20221219 VALUES [('2022-12-19'), ('2022-12-20')),
        PARTITION p20221220 VALUES [('2022-12-20'), ('2022-12-21')),
        PARTITION p20221221 VALUES [('2022-12-21'), ('2022-12-22')))
        DISTRIBUTED BY HASH(`TIME_STAMP`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """ insert into test_datev2_partition1 values ('2022-12-14'), ('2022-12-15'), ('2022-12-16'), ('2022-12-17'), ('2022-12-18'), ('2022-12-19'), ('2022-12-20') """

    qt_select """ select * from test_datev2_partition1 order by TIME_STAMP """
    qt_select """ select * from test_datev2_partition1 WHERE TIME_STAMP = '2022-12-15' order by TIME_STAMP """
    qt_select """ select * from test_datev2_partition1 WHERE TIME_STAMP > '2022-12-15' order by TIME_STAMP """

    sql "drop table if exists test_datev2_partition2"
    sql """
        CREATE TABLE `test_datev2_partition2` (
        `TIME_STAMP` datetimev2(3) NOT NULL COMMENT '采集日期'
        ) ENGINE=OLAP
        DUPLICATE KEY(`TIME_STAMP`)
        COMMENT 'OLAP'
        PARTITION BY RANGE(`TIME_STAMP`)
        (PARTITION p20221214 VALUES [('2022-12-14 11:11:11.111'), ('2022-12-15 11:11:11.111')),
        PARTITION p20221215 VALUES [('2022-12-15 11:11:11.111'), ('2022-12-16 11:11:11.111')),
        PARTITION p20221216 VALUES [('2022-12-16 11:11:11.111'), ('2022-12-17 11:11:11.111')),
        PARTITION p20221217 VALUES [('2022-12-17 11:11:11.111'), ('2022-12-18 11:11:11.111')),
        PARTITION p20221218 VALUES [('2022-12-18 11:11:11.111'), ('2022-12-19 11:11:11.111')),
        PARTITION p20221219 VALUES [('2022-12-19 11:11:11.111'), ('2022-12-20 11:11:11.111')),
        PARTITION p20221220 VALUES [('2022-12-20 11:11:11.111'), ('2022-12-21 11:11:11.111')),
        PARTITION p20221221 VALUES [('2022-12-21 11:11:11.111'), ('2022-12-22 11:11:11.111')))
        DISTRIBUTED BY HASH(`TIME_STAMP`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """ insert into test_datev2_partition2 values ('2022-12-14 22:22:22.222'), ('2022-12-15 22:22:22.222'), ('2022-12-16 22:22:22.222'), ('2022-12-17 22:22:22.222'), ('2022-12-18 22:22:22.222'), ('2022-12-19 22:22:22.222'), ('2022-12-20 22:22:22.222') """

    qt_select """ select * from test_datev2_partition2 order by TIME_STAMP """
    qt_select """ select * from test_datev2_partition2 WHERE TIME_STAMP = '2022-12-15 22:22:22.222' order by TIME_STAMP """
    qt_select """ select * from test_datev2_partition2 WHERE TIME_STAMP > '2022-12-15 22:22:22.222' order by TIME_STAMP """
}
