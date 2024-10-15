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


suite("test_time_round") {
    sql """ drop TABLE IF EXISTS dbround"""
    sql """ CREATE TABLE IF NOT EXISTS dbround (
              `id` INT NULL COMMENT "",
              `dt` datetime NULL COMMENT "",
              `p` int NULL COMMENT "",
              `o` datetime NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            ); """

    sql """INSERT INTO dbround VALUES(1,'2020-02-02 13:09:20' , 1 , '1999-12-31 11:45:14'); """
    sql """INSERT INTO dbround VALUES(2,'2020-02-02 13:09:20' , 2 , '1919-08-10 11:45:14'); """
    sql """INSERT INTO dbround VALUES(3,'2020-02-02 13:09:20' , 4 , '1145-01-04 19:19:00'); """
    sql """ set enable_nereids_planner=true , enable_fallback_to_original_planner=false;"""
    // fix by issues/9711, expect: '1970-01-01T01:00:30'
    qt_select "select hour_ceil('1970-01-01 01:00:10', 1, '1970-01-01 00:00:30')"

    // fix by issues/9711, expect: '1970-01-01T00:00:30'
    qt_select "select hour_floor('1970-01-01 01:00:10', 1, '1970-01-01 00:00:30')"

    // fix by issues/9711, expect: '2022-05-25'
    qt_select "select day_ceil('2022-05-25')"

    // fix by issues/9711, expect: '2022-05-01'
    qt_select "select month_ceil('2022-05-01')"

    qt_select "select minute_ceil('2022-05-25 00:05:10')"
    qt_select "select minute_floor('2022-05-25 00:05:10')"
    qt_select "select day_ceil('2022-05-25 02:00:00', 3, '2022-05-20 00:00:00')"
    qt_select "select day_floor('2022-05-25 02:00:00', 3, '2022-05-20 00:00:00')"
    qt_select "select week_ceil('2022-05-25 00:00:00')"
    qt_select "select week_floor('2022-05-25 00:00:00')"
    qt_select "select month_ceil('2022-05-25 00:00:00')"
    qt_select "select month_floor('2022-05-25 00:00:00')"
    qt_select "select year_ceil('2022-05-25 00:00:00')"
    qt_select "select year_floor('2022-05-25 00:00:00')"
    qt_select "select hour_floor(dt,p,o) from dbround order by id;"
    qt_select "select hour_floor(dt,2,o) from dbround order by id;"
    qt_select "select hour_floor(dt,p,'1919-08-10 11:45:14') from dbround order by id;"
    qt_select "select hour_floor(dt,2,'1919-08-10 11:45:14') from dbround order by id;"
    
    // fix by issues/9711, expect: '1970-01-01T01:00:30'
    qt_select "select hour_ceil('1970-01-01 01:00:10', 1, '1970-01-01 00:00:30')"

    // fix by issues/9711, expect: '1970-01-01T00:00:30'
    qt_select "select hour_floor('1970-01-01 01:00:10', 1, '1970-01-01 00:00:30')"

    // fix by issues/9711, expect: '2022-05-25'
    qt_select "select day_ceil('2022-05-25')"

    // fix by issues/9711, expect: '2022-05-01'
    qt_select "select month_ceil('2022-05-01')"

    qt_select "select minute_ceil('2022-05-25 00:05:10')"
    qt_select "select minute_floor('2022-05-25 00:05:10')"
    qt_select "select day_ceil('2022-05-25 02:00:00', 3, '2022-05-20 00:00:00')"
    qt_select "select day_floor('2022-05-25 02:00:00', 3, '2022-05-20 00:00:00')"
    qt_select "select week_ceil('2022-05-25 00:00:00')"
    qt_select "select week_floor('2022-05-25 00:00:00')"
    qt_select "select month_ceil('2022-05-25 00:00:00')"
    qt_select "select month_floor('2022-05-25 00:00:00')"
    qt_select "select year_ceil('2022-05-25 00:00:00')"
    qt_select "select year_floor('2022-05-25 00:00:00')"

    qt_select "select hour_floor(dt,p,o) from dbround order by id;"
    qt_select "select hour_floor(dt,2,o) from dbround order by id;"
    qt_select "select hour_floor(dt,p,'1919-08-10 11:45:14') from dbround order by id;"
    qt_select "select hour_floor(dt,2,'1919-08-10 11:45:14') from dbround order by id;"
}
