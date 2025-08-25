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

    // array-enumerate-unique nullsafe tests
    qt_sql_array_enumerate_uniq1 "SELECT array_enumerate_uniq(int_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_enumerate_uniq2 "SELECT array_enumerate_uniq(string_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_enumerate_uniq3 "SELECT array_enumerate_uniq(ipv4_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_enumerate_uniq4 "SELECT array_enumerate_uniq(ipv6_array) FROM fn_test_nullsafe_array order by id"

    // array-enumerate nullsafe tests
    qt_sql_array_enumerate1 "SELECT array_enumerate(int_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_enumerate2 "SELECT array_enumerate(string_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_enumerate3 "SELECT array_enumerate(ipv4_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_enumerate4 "SELECT array_enumerate(ipv6_array) FROM fn_test_nullsafe_array order by id"

    // array-exists nullsafe tests
    qt_sql_array_exists1 "SELECT array_exists(x -> x is null, int_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_exists2 "SELECT array_exists(x -> x is null, string_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_exists3 "SELECT array_exists(x -> x is null, double_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_exists4 "SELECT array_exists(x -> x is null, date_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_exists5 "SELECT array_exists(x -> x is null, ipv4_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_exists6 "SELECT array_exists(x -> x is null, ipv6_array) FROM fn_test_nullsafe_array order by id"
    
    // array-first nullsafe tests
    qt_sql_array_first1 "SELECT array_first(x -> x is null, int_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_first2 "SELECT array_first(x -> x is null, string_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_first3 "SELECT array_first(x -> x is null, double_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_first4 "SELECT array_first(x -> x is null, date_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_first5 "SELECT array_first(x -> x is null, ipv4_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_first6 "SELECT array_first(x -> x is null, ipv6_array) FROM fn_test_nullsafe_array order by id"

    // array-last nullsafe tests
    qt_sql_array_last1 "SELECT array_last(x -> x is null, int_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_last2 "SELECT array_last(x -> x is null, string_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_last3 "SELECT array_last(x -> x is null, double_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_last4 "SELECT array_last(x -> x is null, date_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_last5 "SELECT array_last(x -> x is null, ipv4_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_last6 "SELECT array_last(x -> x is null, ipv6_array) FROM fn_test_nullsafe_array order by id"

    // array-first-index nullsafe tests
    qt_sql_array_first_index1 "SELECT array_first_index(x -> x is null, int_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_first_index2 "SELECT array_first_index(x -> x is null, string_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_first_index3 "SELECT array_first_index(x -> x is null, double_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_first_index4 "SELECT array_first_index(x -> x is null, date_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_first_index5 "SELECT array_first_index(x -> x is null, ipv4_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_first_index6 "SELECT array_first_index(x -> x is null, ipv6_array) FROM fn_test_nullsafe_array order by id"

    // array-last-index nullsafe tests
    qt_sql_array_last_index1 "SELECT array_last_index(x -> x is null, int_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_last_index2 "SELECT array_last_index(x -> x is null, string_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_last_index3 "SELECT array_last_index(x -> x is null, double_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_last_index4 "SELECT array_last_index(x -> x is null, date_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_last_index5 "SELECT array_last_index(x -> x is null, ipv4_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_last_index6 "SELECT array_last_index(x -> x is null, ipv6_array) FROM fn_test_nullsafe_array order by id"

    // array-intersect nullsafe tests
    qt_sql_array_intersect1 "SELECT array_intersect(int_array, [2, 4]) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_intersect2 "SELECT array_intersect(string_array, [null, 3]) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_intersect3 "SELECT array_intersect(ipv4_array, []) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_intersect4 "SELECT array_intersect(ipv6_array, NULL) FROM fn_test_nullsafe_array order by id"

    // array-map nullsafe tests
    qt_sql_array_map1 "SELECT array_map(x -> x + 1, int_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_map2 "SELECT array_map(x -> x + 1, string_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_map3 "SELECT array_map(x -> x + 1, double_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_map4 "SELECT array_map(x -> x + 1, date_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_map5 "SELECT array_map(x -> ipv4_to_ipv6(x), ipv4_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_map6 "SELECT array_map(x -> inet6_ntoa(x), ipv6_array) FROM fn_test_nullsafe_array order by id"
    // test x is null
    qt_sql_array_map7 "SELECT array_map(x -> x is null, int_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_map8 "SELECT array_map(x -> x is null, string_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_map9 "SELECT array_map(x -> x is null, double_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_map10 "SELECT array_map(x -> x is null, date_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_map11 "SELECT array_map(x -> x is null, ipv4_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_map12 "SELECT array_map(x -> x is null, ipv6_array) FROM fn_test_nullsafe_array order by id"


    // array-position nullsafe tests
    qt_sql_array_position1 "SELECT array_position(int_array, 2) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_position2 "SELECT array_position(string_array, 'c') FROM fn_test_nullsafe_array order by id"
    qt_sql_array_position3 "SELECT array_position(double_array, 2.2) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_position4 "SELECT array_position(date_array, '2023-01-02') FROM fn_test_nullsafe_array order by id"
    qt_sql_array_position5 "SELECT array_position(ipv4_array, '192.168.1.2') FROM fn_test_nullsafe_array order by id"
    qt_sql_array_position6 "SELECT array_position(ipv6_array, '2001:db8::1') FROM fn_test_nullsafe_array order by id"
    qt_sql_array_position7 "SELECT array_position(string_array, null) FROM fn_test_nullsafe_array order by id"

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

    // test null not can be calculated value for array_position
    // array_avg
    qt_sql_array_avg "SELECT array_avg(int_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_avg "SELECT array_avg(double_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_avg "SELECT array_avg(null) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_avg "SELECT array_avg([]) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_avg "SELECT array_avg([null]) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_avg "SELECT array_avg([null, 1, null, 2, null]) FROM fn_test_nullsafe_array order by id"

    // array_max
    qt_sql_array_max "SELECT array_max(int_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_max "SELECT array_max(double_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_max "SELECT array_max(null) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_max "SELECT array_max([]) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_max "SELECT array_max([null]) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_max "SELECT array_max([null, 1, null, 2, null]) FROM fn_test_nullsafe_array order by id"

    // array_min
    qt_sql_array_min "SELECT array_min(int_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_min "SELECT array_min(double_array) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_min "SELECT array_min(null) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_min "SELECT array_min([]) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_min "SELECT array_min([null]) FROM fn_test_nullsafe_array order by id"
    qt_sql_array_min "SELECT array_min([null, 1, null, 2, null]) FROM fn_test_nullsafe_array order by id"

}