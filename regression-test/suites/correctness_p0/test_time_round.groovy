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
    qt_select_1 "select dt,hour_floor(dt,0) from dbround order by id;"
    
    /// Additional test cases for three-parameter versions
    // Week tests with three parameters
    qt_select "select week_ceil('2022-05-25 00:00:00', 1, '2022-01-02 00:00:00')"
    qt_select "select week_floor('2022-05-25 00:00:00', 1, '2022-01-02 00:00:00')"
    qt_select "select week_ceil('2022-05-25 00:00:00', 2, '2022-01-02 00:00:00')"
    qt_select "select week_floor('2022-05-25 00:00:00', 2, '2022-01-02 00:00:00')"
    qt_select "select week_ceil('2022-05-25 00:00:00', 4, '2022-01-02 00:00:00')"
    qt_select "select week_floor('2022-05-25 00:00:00', 4, '2022-01-02 00:00:00')"
    
    // Test with different origin dates
    qt_select "select week_ceil('2022-05-25 00:00:00', 1, '2022-01-03 00:00:00')"
    qt_select "select week_floor('2022-05-25 00:00:00', 1, '2022-01-03 00:00:00')"
    qt_select "select week_ceil('2022-05-25 00:00:00', 1, '2022-01-04 00:00:00')"
    qt_select "select week_floor('2022-05-25 00:00:00', 1, '2022-01-04 00:00:00')"
    qt_select "select week_ceil('2022-05-25 00:00:00', 1, '2022-01-05 00:00:00')"
    qt_select "select week_floor('2022-05-25 00:00:00', 1, '2022-01-05 00:00:00')"
    
    // Edge cases with dates far from origin
    qt_select "select week_ceil('2099-12-31 23:59:59', 1, '1970-01-04 00:00:00')"
    qt_select "select week_floor('2099-12-31 23:59:59', 1, '1970-01-04 00:00:00')"
    qt_select "select week_ceil('1900-01-01 00:00:00', 1, '1970-01-04 00:00:00')"
    qt_select "select week_floor('1900-01-01 00:00:00', 1, '1970-01-04 00:00:00')"
    
    // Month tests with three parameters
    qt_select "select month_ceil('2022-05-15 12:30:45', 1, '2022-01-10 00:00:00')"
    qt_select "select month_floor('2022-05-15 12:30:45', 1, '2022-01-10 00:00:00')"
    qt_select "select month_ceil('2022-05-15 12:30:45', 3, '2022-01-10 00:00:00')"
    qt_select "select month_floor('2022-05-15 12:30:45', 3, '2022-01-10 00:00:00')"
    qt_select "select month_ceil('2022-05-15 12:30:45', 6, '2022-01-10 00:00:00')"
    qt_select "select month_floor('2022-05-15 12:30:45', 6, '2022-01-10 00:00:00')"
    
    // Year tests with three parameters
    qt_select "select year_ceil('2022-05-15 12:30:45', 1, '2020-06-15 00:00:00')"
    qt_select "select year_floor('2022-05-15 12:30:45', 1, '2020-06-15 00:00:00')"
    qt_select "select year_ceil('2022-05-15 12:30:45', 5, '2020-06-15 00:00:00')"
    qt_select "select year_floor('2022-05-15 12:30:45', 5, '2020-06-15 00:00:00')"
    qt_select "select year_ceil('2022-05-15 12:30:45', 10, '2020-06-15 00:00:00')"
    qt_select "select year_floor('2022-05-15 12:30:45', 10, '2020-06-15 00:00:00')"
    
    // Day tests with three parameters
    qt_select "select day_ceil('2022-05-15 12:30:45', 1, '2022-05-10 06:00:00')"
    qt_select "select day_floor('2022-05-15 12:30:45', 1, '2022-05-10 06:00:00')"
    qt_select "select day_ceil('2022-05-15 12:30:45', 7, '2022-05-10 06:00:00')"
    qt_select "select day_floor('2022-05-15 12:30:45', 7, '2022-05-10 06:00:00')"
    qt_select "select day_ceil('2022-05-15 12:30:45', 15, '2022-05-10 06:00:00')"
    qt_select "select day_floor('2022-05-15 12:30:45', 15, '2022-05-10 06:00:00')"
    
    // Hour tests with three parameters
    qt_select "select hour_ceil('2022-05-15 12:30:45', 1, '2022-05-15 06:15:00')"
    qt_select "select hour_floor('2022-05-15 12:30:45', 1, '2022-05-15 06:15:00')"
    qt_select "select hour_ceil('2022-05-15 12:30:45', 3, '2022-05-15 06:15:00')"
    qt_select "select hour_floor('2022-05-15 12:30:45', 3, '2022-05-15 06:15:00')"
    qt_select "select hour_ceil('2022-05-15 12:30:45', 6, '2022-05-15 06:15:00')"
    qt_select "select hour_floor('2022-05-15 12:30:45', 6, '2022-05-15 06:15:00')"
    qt_select "select hour_ceil('2022-05-15 12:30:45', 12, '2022-05-15 06:15:00')"
    qt_select "select hour_floor('2022-05-15 12:30:45', 12, '2022-05-15 06:15:00')"
    
    // Minute tests with three parameters
    qt_select "select minute_ceil('2022-05-15 12:30:45', 1, '2022-05-15 12:15:30')"
    qt_select "select minute_floor('2022-05-15 12:30:45', 1, '2022-05-15 12:15:30')"
    qt_select "select minute_ceil('2022-05-15 12:30:45', 5, '2022-05-15 12:15:30')"
    qt_select "select minute_floor('2022-05-15 12:30:45', 5, '2022-05-15 12:15:30')"
    qt_select "select minute_ceil('2022-05-15 12:30:45', 15, '2022-05-15 12:15:30')"
    qt_select "select minute_floor('2022-05-15 12:30:45', 15, '2022-05-15 12:15:30')"
    qt_select "select minute_ceil('2022-05-15 12:30:45', 30, '2022-05-15 12:15:30')"
    qt_select "select minute_floor('2022-05-15 12:30:45', 30, '2022-05-15 12:15:30')"
    
    // Second tests with three parameters
    qt_select "select second_ceil('2022-05-15 12:30:45', 1, '2022-05-15 12:30:15')"
    qt_select "select second_floor('2022-05-15 12:30:45', 1, '2022-05-15 12:30:15')"
    qt_select "select second_ceil('2022-05-15 12:30:45', 5, '2022-05-15 12:30:15')"
    qt_select "select second_floor('2022-05-15 12:30:45', 5, '2022-05-15 12:30:15')"
    qt_select "select second_ceil('2022-05-15 12:30:45', 15, '2022-05-15 12:30:15')"
    qt_select "select second_floor('2022-05-15 12:30:45', 15, '2022-05-15 12:30:15')"
    qt_select "select second_ceil('2022-05-15 12:30:45', 30, '2022-05-15 12:30:15')"
    qt_select "select second_floor('2022-05-15 12:30:45', 30, '2022-05-15 12:30:15')"
    
    // Test with table data
    sql """INSERT INTO dbround VALUES(4,'2023-06-15 08:30:45' , 3 , '2023-01-01 00:00:00'); """
    sql """INSERT INTO dbround VALUES(5,'2023-06-15 08:30:45' , 6 , '2023-01-01 00:00:00'); """
    sql """INSERT INTO dbround VALUES(6,'2023-06-15 08:30:45' , 12 , '2023-01-01 00:00:00'); """
    
    qt_select "select week_ceil(dt, p, o) from dbround where id > 3 order by id;"
    qt_select "select week_floor(dt, p, o) from dbround where id > 3 order by id;"
    qt_select "select month_ceil(dt, p, o) from dbround where id > 3 order by id;"
    qt_select "select month_floor(dt, p, o) from dbround where id > 3 order by id;"
    qt_select_quarter1 "select quarter_ceil(dt, p, o) from dbround where id > 3 order by id;"
    qt_select_quarter2 "select quarter_floor(dt, p, o) from dbround where id > 3 order by id;"
    qt_select_quarter3 "select quarter_ceil(dt, o) from dbround where id > 3 order by id;"
    qt_select_quarter4 "select quarter_floor(dt, o) from dbround where id > 3 order by id;"
    qt_select_quarter5 "select quarter_ceil(dt, 1, '1999-12-31') from dbround where id > 3 order by id;"
    qt_select_quarter6 "select quarter_floor(dt, 1, '1999-12-31') from dbround where id > 3 order by id;"
    qt_select_quarter7 "select quarter_ceil(dt, 2) from dbround where id > 3 order by id;"
    qt_select_quarter8 "select quarter_floor(dt, 2) from dbround where id > 3 order by id;"
    qt_select_quarter9 "select quarter_ceil(dt, '2000-02-01') from dbround where id > 3 order by id;"
    qt_select_quarter10 "select quarter_floor(dt, '2000-02-01') from dbround where id > 3 order by id;"
    qt_select_quarter11 "select quarter_ceil(dt) from dbround where id > 3 order by id;"
    qt_select_quarter12 "select quarter_floor(dt) from dbround where id > 3 order by id;"
    qt_select_quarter13 "select quarter_ceil(cast(dt as date)) from dbround where id > 3 order by id;"
    qt_select_quarter14 "select quarter_floor(cast(dt as date)) from dbround where id > 3 order by id;"
    qt_select_quarter15 "select quarter_ceil(cast(dt as date), p) from dbround where id > 3 order by id;"
    qt_select_quarter16 "select quarter_floor(cast(dt as date), p) from dbround where id > 3 order by id;"
    qt_select "select day_ceil(dt, p, o) from dbround where id > 3 order by id;"
    qt_select "select day_floor(dt, p, o) from dbround where id > 3 order by id;"
    qt_select "select hour_ceil(dt, p, o) from dbround where id > 3 order by id;"
    qt_select "select hour_floor(dt, p, o) from dbround where id > 3 order by id;"
    qt_select "select minute_ceil(dt, p, o) from dbround where id > 3 order by id;"
    qt_select "select minute_floor(dt, p, o) from dbround where id > 3 order by id;"
    qt_select "select second_ceil(dt, p, o) from dbround where id > 3 order by id;"
    qt_select "select second_floor(dt, p, o) from dbround where id > 3 order by id;"
    
    // invalid period
    qt_select_1 "select week_ceil('2022-05-25 00:00:00', 0, '2022-01-02 00:00:00')"
    qt_select_1 "select week_floor('2022-05-25 00:00:00', 0, '2022-01-02 00:00:00')"
    qt_select_1 "select week_ceil('2022-05-25 00:00:00', -1, '2022-01-02 00:00:00')"
    qt_select_1 "select week_floor('2022-05-25 00:00:00', -1, '2022-01-02 00:00:00')"
    
    // Test with large period values
    qt_select "select week_ceil('2022-05-25 00:00:00', 100, '2022-01-02 00:00:00')"
    qt_select "select week_floor('2022-05-25 00:00:00', 100, '2022-01-02 00:00:00')"
    qt_select "select month_ceil('2022-05-15 12:30:45', 100, '2022-01-10 00:00:00')"
    qt_select "select month_floor('2022-05-15 12:30:45', 100, '2022-01-10 00:00:00')"
    qt_select "select quarter_ceil('2022-05-15 12:30:45', 100, '2020-01-10 00:00:00')"
    qt_select "select quarter_floor('2022-05-15 12:30:45', 100, '2020-01-10 00:00:00')"
    testFoldConst("select quarter_ceil('2022-05-15 12:30:45', 100, '2020-01-10 00:00:00')")
    testFoldConst("select quarter_floor('2022-05-15 12:30:45', 100, '2020-01-10 00:00:00')")
    qt_select "select year_ceil('2022-05-15 12:30:45', 100, '2020-06-15 00:00:00')"
    qt_select "select year_floor('2022-05-15 12:30:45', 100, '2020-06-15 00:00:00')"
    
    // Test with same date as origin
    qt_select "select week_ceil('2022-01-02 00:00:00', 1, '2022-01-02 00:00:00')"
    qt_select "select week_floor('2022-01-02 00:00:00', 1, '2022-01-02 00:00:00')"
    qt_select "select month_ceil('2022-01-10 00:00:00', 1, '2022-01-10 00:00:00')"
    qt_select "select month_floor('2022-01-10 00:00:00', 1, '2022-01-10 00:00:00')"
    qt_select_quarter3 "select quarter_ceil('2022-01-10 00:00:00', 1, '2022-01-10 00:00:00')"
    qt_select_quarter4 "select quarter_floor('2022-01-10 00:00:00', 1, '2022-01-10 00:00:00')"
    testFoldConst("select quarter_ceil('2022-01-10 00:00:00', 1, '2022-01-10 00:00:00')")
    testFoldConst("select quarter_floor('2022-01-10 00:00:00', 1, '2022-01-10 00:00:00')")
    qt_select "select year_ceil('2020-06-15 00:00:00', 1, '2020-06-15 00:00:00')"
    qt_select "select year_floor('2020-06-15 00:00:00', 1, '2020-06-15 00:00:00')"
    
    // Test with date earlier than origin
    qt_select "select week_ceil('2021-01-02 00:00:00', 1, '2022-01-02 00:00:00')"
    qt_select "select week_floor('2021-01-02 00:00:00', 1, '2022-01-02 00:00:00')"
    qt_select "select month_ceil('2021-01-10 00:00:00', 1, '2022-01-10 00:00:00')"
    qt_select "select month_floor('2021-01-10 00:00:00', 1, '2022-01-10 00:00:00')"
    qt_select_quarter5 "select quarter_ceil('2021-01-10 00:00:00', 1, '2022-01-10 00:00:00')"
    qt_select_quarter6 "select quarter_floor('2021-01-10 00:00:00', 1, '2022-01-10 00:00:00')"
    testFoldConst("select quarter_ceil('2021-01-10 00:00:00', 1, '2022-01-10 00:00:00')")
    testFoldConst("select quarter_floor('2021-01-10 00:00:00', 1, '2022-01-10 00:00:00')")
    qt_select "select year_ceil('2019-06-15 00:00:00', 1, '2020-06-15 00:00:00')"
    qt_select "select year_floor('2019-06-15 00:00:00', 1, '2020-06-15 00:00:00')"

    // quarter's many test
    qt_sql "select quarter_floor('2020-12-12')"
    qt_sql "select quarter_ceil('2020-12-12')"
    qt_sql "select quarter_floor('2020-12-12', 1)"
    qt_sql "select quarter_floor('2020-12-12', 2)"
    qt_sql "select quarter_floor('2020-12-12', 3)"
    qt_sql "select quarter_floor('2020-12-12', 4)"
    qt_sql "select quarter_floor('2020-12-12', 6)"
    qt_sql "select quarter_floor('2020-12-12', 10)"
    qt_sql "select quarter_floor('2020-12-12', 12)"
    qt_sql "select quarter_ceil('2020-12-12', 1)"
    qt_sql "select quarter_ceil('2020-12-12', 2)"
    qt_sql "select quarter_ceil('2020-12-12', 3)"
    qt_sql "select quarter_ceil('2020-12-12', 4)"
    qt_sql "select quarter_ceil('2020-12-12', 6)"
    qt_sql "select quarter_ceil('2020-12-12', 10)"
    qt_sql "select quarter_ceil('2020-12-12', 12)"
    qt_sql "select quarter_floor('2020-12-12', '2020-01-01')"
    qt_sql "select quarter_floor('2020-12-12', '2020-02-01')"
    qt_sql "select quarter_ceil('2020-12-12', '2020-01-01')"
    qt_sql "select quarter_ceil('2020-12-12', '2020-02-01')"
    qt_sql "select quarter_floor('2020-10-02', 1, '2020-01-03');"
    qt_sql "select quarter_floor('2020-04-02');"
    qt_sql "select quarter_floor('2020-05-01');"
    qt_sql "select quarter_floor('2020-05-02');"

    testFoldConst("select quarter_floor('2020-12-12')")
    testFoldConst("select quarter_ceil('2020-12-12')")
    testFoldConst("select quarter_floor('2020-12-12', 1)")
    testFoldConst("select quarter_floor('2020-12-12', 2)")
    testFoldConst("select quarter_floor('2020-12-12', 3)")
    testFoldConst("select quarter_floor('2020-12-12', 4)")
    testFoldConst("select quarter_floor('2020-12-12', 6)")
    testFoldConst("select quarter_floor('2020-12-12', 10)")
    testFoldConst("select quarter_floor('2020-12-12', 12)")
    testFoldConst("select quarter_ceil('2020-12-12', 1)")
    testFoldConst("select quarter_ceil('2020-12-12', 2)")
    testFoldConst("select quarter_ceil('2020-12-12', 3)")
    testFoldConst("select quarter_ceil('2020-12-12', 4)")
    testFoldConst("select quarter_ceil('2020-12-12', 6)")
    testFoldConst("select quarter_ceil('2020-12-12', 10)")
    testFoldConst("select quarter_ceil('2020-12-12', 12)")
    testFoldConst("select quarter_floor('2020-12-12', '2020-01-01')")
    testFoldConst("select quarter_floor('2020-12-12', '2020-02-01')")
    testFoldConst("select quarter_ceil('2020-12-12', '2020-01-01')")
    testFoldConst("select quarter_ceil('2020-12-12', '2020-02-01')")
    testFoldConst("select quarter_floor('2020-10-02', 1, '2020-01-03');")
    testFoldConst("select quarter_floor('2020-04-02');")
    testFoldConst("select quarter_floor('2020-05-01');")
    testFoldConst("select quarter_floor('2020-05-02');")
}
