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

suite("nereids_scalar_fn_ArrayNullsafe", "p0") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    // table nullsafe array
    sql """DROP TABLE IF EXISTS fn_test_nullsafe_array"""
    sql """
        CREATE TABLE IF NOT EXISTS `fn_test_nullsafe_array` (
            `id` int not null,
            `int_array` ARRAY<INT>,
            `double_array` ARRAY<DOUBLE>,
            `string_array` ARRAY<STRING>,
            `date_array` ARRAY<DATE>,
            `ipv4_array` ARRAY<IPV4>,
            `ipv6_array` ARRAY<IPV6>
        ) engine=olap
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        properties("replication_num" = "1")
    """
    
    // insert into fn_test_nullsafe_array with null element
    sql """
        INSERT INTO fn_test_nullsafe_array VALUES
        (1, [1, 2, 3, 4, 5], [1.1, 2.2, 3.3, 4.4, 5.5], ['a', 'b', 'c', 'd', 'e'], ['2023-01-01', '2023-01-02', '2023-01-03'], ['192.168.1.1', '192.168.1.2'], ['2001:db8::1', '2001:db8::2']),
        (2, [1, null, 3, null, 5], [1.1, null, 3.3, null, 5.5], ['a', null, 'c', null, 'e'], ['2023-01-01', null, '2023-01-03'], ['192.168.1.1', null, '192.168.1.3'], ['2001:db8::1', null, '2001:db8::3']),
        (3, [1, 1, 2, 2, 2, 3, 1, 4], [1.1, 1.1, 2.2, 2.2, 2.2, 3.3, 1.1, 4.4], ['a', 'a', 'b', 'b', 'c'], ['2023-01-01', '2023-01-01', '2023-01-02'], ['192.168.1.1', '192.168.1.1', '192.168.1.2'], ['2001:db8::1', '2001:db8::1', '2001:db8::2']),
        (4, [], [], [], [], [], []),
        (5, NULL, NULL, NULL, NULL, NULL, NULL)
    """

    // test function behavior with nullsafe array
    // array_apply nullsafe tests
    qt_sql_array_apply1 "SELECT array_apply(int_array, '>', 2) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_apply2 "SELECT array_apply(double_array, '<', 1.1) FROM fn_test_nullsafe_array order by id"

    // array_compact nullsafe tests
    qt_sql_array_compact4 "SELECT array_compact(date_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_compact5 "SELECT array_compact(ipv4_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_compact6 "SELECT array_compact(ipv6_array) FROM fn_test_nullsafe_array order by id"

    // array_concat nullsafe tests
    qt_sql_array_concat1 "SELECT array_concat(int_array, [6, 7, 8]) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_concat2 "SELECT array_concat(string_array, NULL) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_concat3 "SELECT array_concat(double_array, []) FROM fn_test_nullsafe_array order by id"

    // array-contains nullsafe tests
    qt_sql_array_contains2 "SELECT array_contains(int_array, 3) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_contains4 "SELECT array_contains(ipv4_array, null) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_contains5 "SELECT array_contains(ipv6_array, '2001:db8::1') FROM fn_test_nullsafe_array order by id"
    
    // array-count nullsafe tests
    qt_sql_array_count1 "SELECT array_count(x -> x > 2, int_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_count2 "SELECT array_count(x -> x is null, string_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_count3 "SELECT array_count(x -> x > '192.168.1.1', ipv4_array) FROM fn_test_nullsafe_array order by id"
    
    // array-cum-sum nullsafe tests
    qt_sql_array_cum_sum1 "SELECT array_cum_sum(int_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_cum_sum2 "SELECT array_cum_sum(double_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_cum_sum3 "SELECT array_cum_sum(string_array) FROM fn_test_nullsafe_array order by id"

    
    // array-difference nullsafe tests
    qt_sql_array_difference1 "SELECT array_difference(double_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_difference2 "SELECT array_difference(date_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_difference3 "SELECT array_difference(string_array) FROM fn_test_nullsafe_array order by id"
    
    // array-distinct nullsafe tests
    qt_sql_array_distinct2 "SELECT array_distinct(string_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_distinct3 "SELECT array_distinct(ipv4_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_distinct4 "SELECT array_distinct(ipv6_array) FROM fn_test_nullsafe_array order by id"
    
    // array-except nullsafe tests
    qt_sql_array_except1 "SELECT array_except(int_array, [2, 4]) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_except2 "SELECT array_except(string_array, [null, 3]) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_except3 "SELECT array_except(ipv4_array, []) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_except4 "SELECT array_except(ipv6_array, NULL) FROM fn_test_nullsafe_array order by id"

    // array-filter nullsafe tests
    qt_sql_array_filter1 "SELECT array_filter(x -> x > 2, int_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_filter2 "SELECT array_filter(x -> x is not null, string_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_filter3 "SELECT array_filter(x -> x > 2.2, double_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_filter4 "SELECT array_filter(x -> x > '2023-01-02', date_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_filter5 "SELECT array_filter(x -> x = '192.168.1.2', ipv4_array) FROM fn_test_nullsafe_array order by id"

    // literal nullsafe tests for functions 
    qt_sql_literal_array_apply "SELECT array_apply([1, null, 3], '>', 2)"
    qt_sql_literal_array_compact "SELECT array_compact([1, null, null, 2, null, null, 3])"
    qt_sql_literal_array_concat "SELECT array_concat([1, null, 3], [4, 5])"
    qt_sql_literal_array_contains "SELECT array_contains([1, null, 3], null)"
    qt_sql_literal_array_count "SELECT array_count(x -> x is null, [null, 1, null, 2, null])"
    qt_sql_literal_array_cum_sum1 "SELECT array_cum_sum([1, null, 3, null, 5])"
    qt_sql_literal_array_cum_sum2 "SELECT array_cum_sum([null, null, 3, null, 5])"
    qt_sql_literal_array_cum_sum3 "SELECT array_cum_sum([null, null, null, null])"
    qt_sql_literal_array_difference "SELECT array_difference([1, null, 3, null, 5])"
    qt_sql_literal_array_distinct "SELECT array_distinct([1, null, 2, null, 3, null])"
    qt_sql_literal_array_exists "SELECT array_exists(x -> x is null, [null, 1, null, 2, null])"
    qt_sql_literal_array_except "SELECT array_except([1, null, 2, null, 3], [null, 2])"
    qt_sql_literal_array_filter "SELECT array_filter(x -> x is not null, [1, null, 3, null, 5])"

    def wait_for_latest_op_on_table_finish = { table_name ->
        for(int t = 0; t <= 60000; t += 1000) {
            def alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                return
            }
            logger.info(table_name + " latest alter job not finished, detail: " + alter_res)
            sleep(1000)
        }
        logger.info(table_name + " wait_for_latest_op_on_table_finish timeout")
    }
    // alter table add inverted index for array string and array ipv6
    sql """
        ALTER TABLE fn_test_nullsafe_array ADD INDEX idx_string_array (string_array) USING INVERTED
    """ 
    // wait for inverted index build
    wait_for_latest_op_on_table_finish("fn_test_nullsafe_array")
    sql """
        ALTER TABLE fn_test_nullsafe_array ADD INDEX idx_ipv6_array (ipv6_array) USING INVERTED
    """
    wait_for_latest_op_on_table_finish("fn_test_nullsafe_array")

    // test function behavior with inverted index
    qt_sql_array_contains_inverted_index "SELECT array_contains(string_array, 'c') FROM fn_test_nullsafe_array order by id"
    qt_sql_array_contains_inverted_index "SELECT array_contains(ipv6_array, '2001:db8::1') FROM fn_test_nullsafe_array order by id"
    // null param
    qt_sql_array_contains_inverted_index "SELECT array_contains(string_array, null) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_contains_inverted_index "SELECT array_contains(ipv6_array, null) FROM fn_test_nullsafe_array order by id"
}