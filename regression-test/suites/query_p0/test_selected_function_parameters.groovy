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

suite("test_selected_function_parameters", "p0") {
    // One stored row reaches the BE column paths. Each value is a boundary for its function.
    sql "drop table if exists test_selected_function_parameters"
    sql """create table test_selected_function_parameters (i int not null, n bigint not null)
        duplicate key(i) distributed by hash(i) buckets 1
        properties ("replication_num" = "1")"""
    sql "insert into test_selected_function_parameters values (-2147483648, -1)"

    // A one-character string has no delimiter. Both FE and BE must return the full string.
    order_qt_substring_literal "select substring_index('a', ',', -2147483648)"
    order_qt_substring_column """select substring_index('a', ',', i)
        from test_selected_function_parameters"""

    // Two elements are needed to make the zero-based start nonzero. Clamp length before addition.
    // Zero and negative lengths keep the existing empty-array result.
    order_qt_array_length """select array_slice([1,2], 2, 9223372036854775807),
        array_slice([1,2], -1, 9223372036854775807), array_slice([1,2], 2, 0),
        array_slice([1,2], 2, -1)"""

    // One UINT64_MAX element exposes the old -1 alias. Check both functions with BE columns
    // and with literals. A NULL bitmap must not hide an invalid known element during FE folding.
    ["bitmap_contains", "bitmap_remove"].each { function ->
        test {
            sql """select ${function}(to_bitmap('18446744073709551615'), n)
                from test_selected_function_parameters"""
            exception "element must be >= 0"
        }
        test {
            sql "select ${function}(to_bitmap('18446744073709551615'), -1)"
            exception "element must be between 0"
        }
        test {
            sql "select ${function}(null, -1)"
            exception "element must be between 0"
        }
    }
    order_qt_bitmap_zero """select bitmap_contains(to_bitmap(0), 0),
        bitmap_count(bitmap_remove(to_bitmap(0), 0))"""

    test {
        // CRC32 has no FE executor. Its zero result makes BE receive -1 with one false event.
        sql """select window_funnel_v2(cast(crc32('') as bigint) - 1, 'default',
            cast('2024-01-01' as datetime), false)"""
        exception "window must be a positive integer"
    }
    test {
        // The literal takes the FE validation path. The same negative window must fail there.
        sql """select window_funnel_v2(-1, 'default',
            cast('2024-01-01' as datetime), false)"""
        exception "window must be a positive integer"
    }
    order_qt_zero_window """select window_funnel_v2(0, 'default',
        cast('2024-01-01' as datetime), false)"""

    // The negative LARGEINT has low 64 bits equal to 1. Check before narrowing to INT.
    ["-18446744073709551615", "2147483648", "0"].each { size ->
        test {
            sql "select ngram_search('a', 'a', ${size})"
            exception "gram_num must be a positive constant"
        }
    }
    order_qt_ngram_one "select ngram_search('a', 'a', 1)"
}
