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

suite("fold_constant_date_arithmatic") {
    def db = "fold_constant_date_arithmatic"
    sql "create database if not exists ${db}"

    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set enable_fold_constant_by_be=false"

    testFoldConst("select substr(now(), 1, 10);")
    testFoldConst("select substr(now(3), 1, 10);")
    testFoldConst("select substr(curdate(), 1, 10);")
    testFoldConst("select substr(current_date(), 1, 10);")
    testFoldConst("select substr(current_timestamp(), 1, 10);")
    testFoldConst("select substr(current_timestamp(3), 1, 10);")

    testFoldConst("SELECT date_format('2020-12-01 00:00:30.01', '%h');")
    testFoldConst("SELECT date_format('2020-12-01 00:00:30.01', '%I');")
    testFoldConst("SELECT date_format('2020-12-01 00:00:30.01', '%l');")
    testFoldConst("SELECT date_format('2020-12-01 00:00:30.01', '%r');")
    testFoldConst("SELECT date_format('2020-12-01 12:00:30.01', '%h');")
    testFoldConst("SELECT date_format('2020-12-01 12:00:30.01', '%I');")
    testFoldConst("SELECT date_format('2020-12-01 12:00:30.01', '%l');")
    testFoldConst("SELECT date_format('2020-12-01 12:00:30.01', '%r');")
    
    testFoldConst("select str_to_date('2023-02-29', '%Y-%m-%d') AS result;")
    testFoldConst("select str_to_date('1900-02-29', '%Y-%m-%d') AS result;")
    testFoldConst("select str_to_date('2025-04-31', '%Y-%m-%d') AS result;")
    testFoldConst("select str_to_date('31-12-2020 23:59:59', '%d-%m-%Y %H:%i:%s');")
    testFoldConst("select str_to_date('2020-12-31T23:59:59', '%Y-%m-%dT%H:%i:%s');")
    testFoldConst("select str_to_date('20201231235959', '%Y%m%d%H%i%s');")
    testFoldConst("select str_to_date('31/12/2020 23:59', '%d/%m/%Y %H:%i');")
    testFoldConst("select str_to_date('31/12/2020 11:59 PM', '%d/%m/%Y %h:%i %p');")
    testFoldConst("select str_to_date('20201231T235959', '%Y%m%dT%H%i%s');")

    // test leap year and leap month edge cases
    testFoldConst("select months_between('2020-02-29', '2020-02-29')")
    testFoldConst("select months_between('2020-02-29', '2020-03-29')")
    testFoldConst("select months_between('2020-01-29', '2020-02-29')")
    testFoldConst("select months_between('2020-02-29', '2021-02-28')")
    testFoldConst("select months_between('2019-02-28', '2020-02-29')")
    
    // test with time components in leap year
    testFoldConst("select months_between('2020-02-29 12:00:00', '2020-02-29 15:00:00')")
    testFoldConst("select months_between('2020-02-29 23:59:59', '2020-03-29 00:00:00')")
    
    // test with different round_off settings in leap year
    testFoldConst("select months_between('2020-02-29', '2020-03-30', true)")
    testFoldConst("select months_between('2020-02-29', '2020-03-30', false)")
    
    // test across multiple leap years
    testFoldConst("select months_between('2020-02-29', '2024-02-29')")
    testFoldConst("select months_between('2020-02-29 23:59:59', '2024-02-29 00:00:00')")

    // test case with last day of the month
    testFoldConst("select months_between('2024-03-31', '2024-02-29')")
    testFoldConst("select months_between('2024-03-30', '2024-02-29')")
    testFoldConst("select months_between('2024-03-29', '2024-02-29')")

    // Test next_day
    testFoldConst("select next_day('2020-02-27', 'SAT');") // 2020-02-29 (leap year)
    testFoldConst("select next_day('2020-02-29', 'MON');") // 2020-03-02 (leap year to next month)
    testFoldConst("select next_day('2019-02-26', 'THU');") // 2019-02-28 (non-leap year)
    testFoldConst("select next_day('2019-02-28', 'SUN');") // 2019-03-03 (non-leap year to next month)
    
    // test unix_timestamp
    testFoldConst("select unix_timestamp('2023/04/31');")
    testFoldConst("select unix_timestamp('1970/01/01 00:00:00');")
    testFoldConst("select unix_timestamp('1970-01-01T00:00:00');")
    testFoldConst("select unix_timestamp('1970-01-01');")
    testFoldConst("select unix_timestamp('31/Apr/2023','%d/%b/%Y');")
    testFoldConst("select unix_timestamp('00-00-0000');")
    testFoldConst("select unix_timestamp('3000/02/29','%Y/%m/%d');")
    testFoldConst("select unix_timestamp('01.Jan.1970','%d.%b.%Y');")
    testFoldConst("select unix_timestamp('0000-00-00 00:00:00');")
    testFoldConst("select unix_timestamp('2021-02-29', '%Y-%m-%d');")
    testFoldConst("select unix_timestamp('2023/04/31', '%Y/%m/%d');")
    testFoldConst("select unix_timestamp('2023-04-31 12:00:00');")
    testFoldConst("select unix_timestamp('1970-01-01','%Y-%m-%d');")
    testFoldConst("select unix_timestamp('0');")
}
