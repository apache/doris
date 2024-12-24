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

suite("test_fold_constant_by_fe") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_fold_nondeterministic_fn=true'
    sql 'set enable_fold_constant_by_be=false'

    def results = sql 'select uuid(), uuid()'
    assertFalse(Objects.equals(results[0][0], results[0][1]))

    def test_date = [
            "2021-04-12", "1969-12-31", "1356-12-12", "0001-01-01", "9998-12-31",
            "2021-04-12", "1969-12-31", "1356-12-12", "0001-01-01", "9998-12-31",
            "2021-04-12 12:54:53", "1969-12-31 23:59:59", "1356-12-12 12:56:12", "0001-01-01 00:00:01", "9998-12-31 00:00:59",
            "2021-04-12 12:54:53", "1969-12-31 23:59:59", "1356-12-12 12:56:12", "0001-01-01 00:00:01", "9998-12-31 00:00:59"
    ]

    def test_int = [1, 10, 25, 50, 1024]

    for (date in test_date) {
        for (interval in test_int) {
            qt_sql "select date_add('${date}', ${interval}), date_sub('${date}', ${interval}), years_add('${date}', ${interval}), years_sub('${date}', ${interval})"
            qt_sql "select months_add('${date}', ${interval}), months_sub('${date}', ${interval}), days_add('${date}', ${interval}), days_sub('${date}', ${interval})"
            qt_sql "select hours_add('${date}', ${interval}), hours_sub('${date}', ${interval}), minutes_add('${date}', ${interval}), minutes_sub('${date}', ${interval})"
            qt_sql "select seconds_add('${date}', ${interval}), seconds_sub('${date}', ${interval})"
        }
    }

    for (date in test_date) {
        for (date1 in test_date) {
            qt_sql "select datediff('${date}', '${date1}')"
        }
    }

    for (date in test_date) {
        qt_sql "select year('${date}'), month('${date}'), dayofyear('${date}'), dayofmonth('${date}'), dayofweek('${date}'), day('${date}')"
        qt_sql "select hour('${date}'), minute('${date}'), second('${date}')"
    }

    for (date in test_date) {
        qt_sql "select date_format('${date}', '%Y-%m-%d'), to_monday('${date}'), last_day('${date}'), to_date('${date}'), to_days('${date}')"
    }

    for (date in test_date) {
        qt_sql "select date_trunc('${date}', 'year'), date_trunc('${date}', 'month'), date_trunc('${date}', 'day')"
        qt_sql "select date_trunc('${date}', 'hour'), date_trunc('${date}', 'minute'), date_trunc('${date}', 'second')"
    }

    for (date in test_date) {
        qt_sql "select to_monday('${date}'), last_day('${date}'), to_date('${date}'), to_days('${date}'), date('${date}'), datev2('${date}')"
    }

    def test_year = [2001, 2013, 123, 1969, 2023]
    for (year in test_year) {
        for (integer in test_int) {
            qt_sql "select /*+SET_VAR(time_zone=\"Asia/Shanghai\")*/ makedate(${year}, ${integer}), from_days(${year * integer}), from_unixtime(${year / 10 * year * integer})"
        }
    }

    for (date in test_date) {
        qt_sql "select unix_timestamp('${date}')"
    }

    String res

    // check fold constant
    for (date in test_date) {
        for (interval in test_int) {
            res = sql "explain select date_add('${date}', ${interval}), date_sub('${date}', ${interval}), years_add('${date}', ${interval}), years_sub('${date}', ${interval})"
            res = res.split('VUNION')[1]
            assertFalse(res.contains("add") || res.contains("sub"))
            res = sql "explain select months_add('${date}', ${interval}), months_sub('${date}', ${interval}), days_add('${date}', ${interval}), days_sub('${date}', ${interval})"
            res = res.split('VUNION')[1]
            assertFalse(res.contains("add") || res.contains("sub"))
            res = sql "explain select hours_add('${date}', ${interval}), hours_sub('${date}', ${interval}), minutes_add('${date}', ${interval}), minutes_sub('${date}', ${interval})"
            res = res.split('VUNION')[1]
            assertFalse(res.contains("add") || res.contains("sub"))
            res = sql "explain select seconds_add('${date}', ${interval}), seconds_sub('${date}', ${interval})"
            res = res.split('VUNION')[1]
            assertFalse(res.contains("add") || res.contains("sub"))
        }
    }

    for (date in test_date) {
        for (date1 in test_date) {
            res = sql "explain select datediff('${date}', '${date1}')"
            res = res.split('VUNION')[1]
            assertFalse(res.contains("datediff"))
        }
    }

    for (date in test_date) {
        res = sql "explain select year('${date}'), month('${date}'), dayofyear('${date}'), dayofmonth('${date}'), dayofweek('${date}'), day('${date}')"
        res = res.split('VUNION')[1]
        assertFalse(res.contains("year") || res.contains("month") || res.contains("dayofyear")
                || res.contains("dayofmonth") || res.contains("dayofweek") || res.contains("day"))
        res = sql "explain select hour('${date}'), minute('${date}'), second('${date}')"
        res = res.split('VUNION')[1]
        assertFalse(res.contains("hour") || res.contains("minute") || res.contains("second"))
    }

    for (date in test_date) {
        res = sql "explain select date_format('${date}', '%Y-%m-%d'), to_monday('${date}'), last_day('${date}'), to_date('${date}'), to_days('${date}')"
        res = res.split('VUNION')[1]
        assertFalse(res.contains("date_format"))
    }

    for (date in test_date) {
        res = sql "explain select date_trunc('${date}', 'year'), date_trunc('${date}', 'month'), date_trunc('${date}', 'day')"
        res = res.split('VUNION')[1]
        assertFalse(res.contains("date_trunc"))
        assertFalse(res.contains("cast"))
        res = sql "explain select date_trunc('${date}', 'hour'), date_trunc('${date}', 'minute'), date_trunc('${date}', 'second')"
        res = res.split('VUNION')[1]
        assertFalse(res.contains("date_trunc"))
        assertFalse(res.contains("cast"))
    }

    for (date in test_date) {
        res = sql "explain select to_monday('${date}'), last_day('${date}'), to_date('${date}'), to_days('${date}'), date('${date}'), datev2('${date}')"
        res = res.split('VUNION')[1]
        assertFalse(res.contains("day") || res.contains("date"))
    }

    // NOTE: For casts that has precision loss, new planner will not do const fold on fe.
    // But we do have some cases like select CAST(419074969.6 AS INT) that can be processed by FE,
    // this is actually an unexpected cast.
    // So after changing arguments of from_unixtime from int to bigint, we also changed test case to avoid precision loss cast on fe.
    for (year in test_year) {
        for (integer in test_int) {
            res = sql "explain select /*+SET_VAR(time_zone=\"Asia/Shanghai\")*/ makedate(${year}, ${integer}), from_days(${year * integer}), from_unixtime(${year * integer * 10})"
            res = res.split('VUNION')[1]
            assertFalse(res.contains("makedate") || res.contains("from"))
        }
    }

    for (date in test_date) {
        res = sql "explain select unix_timestamp('${date}')"
        res = res.split('VUNION')[1]
        assertFalse(res.contains("unix"))
    }

    // test null like string cause of fe need to fold constant like that to enable not null derive
    res = sql """explain select null like '%123%'"""
    assertFalse(res.contains("like"))
    // now fe fold constant still can not deal with this case
    res = sql """explain select "12" like '%123%'"""
    assertTrue(res.contains("like"))

    // Test Case 1: Append missing trailing character
    testFoldConst("select append_trailing_char_if_absent('hello', '!')")
    // Expected Output: 'hello!'

    // Test Case 2: Trailing character already present
    testFoldConst("select append_trailing_char_if_absent('hello!', '!')")
    // Expected Output: 'hello!'

    // Test Case 3: Append trailing space
    testFoldConst("select append_trailing_char_if_absent('hello', ' ')")
    // Expected Output: 'hello '

    // Test Case 4: Empty string input
    testFoldConst("select append_trailing_char_if_absent('', '!')")
    // Expected Output: '!'

    // Test Case 5: Append different character
    testFoldConst("select append_trailing_char_if_absent('hello', '?')")
    // Expected Output: 'hello?'

    // Test Case 6: String ends with a different character
    testFoldConst("select append_trailing_char_if_absent('hello?', '!')")
    // Expected Output: 'hello?!'

    // Edge and Unusual Usage Test Cases

    // Test Case 7: Input is NULL
    testFoldConst("select append_trailing_char_if_absent(NULL, '!')")
    // Expected Output: NULL

    // Test Case 8: Trailing character is NULL
    testFoldConst("select append_trailing_char_if_absent('hello', NULL)")
    // Expected Output: NULL

    // Test Case 9: Empty trailing character
//    testFoldConst("select append_trailing_char_if_absent('hello', '')")
    // Expected Output: Error or no change depending on implementation

    // Test Case 10: Trailing character is more than 1 character long
//    testFoldConst("select append_trailing_char_if_absent('hello', 'ab')")
    // Expected Output: Error

    // Test Case 11: Input string is a number
    testFoldConst("select append_trailing_char_if_absent(12345, '!')")
    // Expected Output: Error or '12345!'

    // Test Case 12: Trailing character is a number
    testFoldConst("select append_trailing_char_if_absent('hello', '1')")
    // Expected Output: 'hello1'

    // Test Case 13: Input is a single character
    testFoldConst("select append_trailing_char_if_absent('h', '!')")
    // Expected Output: 'h!'

    // Test Case 14: Unicode character as input and trailing character
    testFoldConst("select append_trailing_char_if_absent('こんにちは', '!')")
    // Expected Output: 'こんにちは!'

    // Test Case 15: Multibyte character as trailing character
//    testFoldConst("select append_trailing_char_if_absent('hello', '😊')")
    // Expected Output: 'hello😊'

    // Test Case 16: Long string input
    testFoldConst("select append_trailing_char_if_absent('This is a very long string', '.')")
    // Expected Output: 'This is a very long string.'

    // Error Handling Test Cases

    // Test Case 17: Invalid trailing character data type (numeric)
    testFoldConst("select append_trailing_char_if_absent('hello', 1)")
    // Expected Output: Error

    // Test Case 18: Invalid input data type (integer)
    testFoldConst("select append_trailing_char_if_absent(12345, '!')")
    // Expected Output: Error or '12345!'

    // Test Case 19: Non-ASCII characters
    testFoldConst("select append_trailing_char_if_absent('Привет', '!')")
    // Expected Output: 'Привет!'

    // Test Case 20: Trailing character with whitespace
    testFoldConst("select append_trailing_char_if_absent('hello', ' ')")
    // Expected Output: 'hello '
    testFoldConst("select DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) + INTERVAL 3600 SECOND")

}
