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

suite("test_trim_array") {
    qt_trim_two "select trim_array([1, 2, 3, 4], 2)"
    qt_trim_zero "select trim_array([1, 2, 3, 4], 0)"
    qt_trim_one "select trim_array([1, 2, 3, 4], 1)"
    qt_trim_all "select trim_array([1, 2, 3, 4], 4)"
    qt_trim_string "select trim_array(['a', 'b', 'c', 'd'], 1)"
    qt_trim_null_element "select trim_array(['a', 'b', null, 'd'], 1)"
    qt_trim_nested "select trim_array([[1, 2, 3], [4, 5, 6]], 1)"
    qt_trim_empty "select trim_array(cast([] as array<int>), 0)"
    qt_trim_boolean "select trim_array(cast([true, false, true] as array<boolean>), 1)"
    qt_trim_tinyint "select trim_array(cast([-128, 0, 127] as array<tinyint>), 1)"
    qt_trim_bigint "select trim_array(cast([-9223372036854775808, 0, 9223372036854775807] as array<bigint>), 1)"
    qt_trim_double "select trim_array(cast([-1.7976931348623157E308, 0.0, 1.7976931348623157E308] as array<double>), 1)"
    qt_trim_decimal "select trim_array(cast([-99999999.99, 0.00, 99999999.99] as array<decimal(10, 2)>), 1)"
    qt_trim_date "select trim_array(cast(['0000-01-01', '2024-02-29', '9999-12-31'] as array<date>), 1)"
    qt_trim_null_array "select trim_array(cast(null as array<int>), 0)"
    qt_trim_null_size "select trim_array([1, 2, 3], cast(null as bigint))"

    test {
        sql "select trim_array([1, 2, 3, 4], 5)"
        exception "size must not exceed array cardinality 4: 5"
    }
    test {
        sql "select trim_array([1, 2, 3, 4], -1)"
        exception "size must not be negative: -1"
    }
    test {
        sql "select trim_array([1, 2, 3, 4], 9223372036854775807)"
        exception "size must not exceed array cardinality 4: 9223372036854775807"
    }
    test {
        sql "select trim_array([1, 2, 3, 4], -9223372036854775808)"
        exception "size must not be negative: -9223372036854775808"
    }

    sql "drop table if exists trim_array_test"
    sql """
        create table trim_array_test (
            id int,
            items array<int>,
            trim_size bigint
        ) distributed by hash(id) buckets 1
        properties('replication_num' = '1')
    """
    sql """
        insert into trim_array_test values
            (1, [1, 2, 3, 4], 2),
            (2, [5, 6], 0),
            (3, [], 0),
            (4, null, 0),
            (5, [7, 8], null)
    """
    order_qt_trim_columns "select id, trim_array(items, trim_size) from trim_array_test"
}
