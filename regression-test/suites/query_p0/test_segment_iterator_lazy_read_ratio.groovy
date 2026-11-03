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

suite("test_segment_iterator_lazy_read_ratio") {
    sql "DROP TABLE IF EXISTS test_segment_iterator_lazy_read_ratio"
    sql """
        CREATE TABLE test_segment_iterator_lazy_read_ratio (
            k INT NOT NULL,
            p INT NOT NULL,
            payload VARCHAR(100),
            nullable_value INT NULL,
            arr ARRAY<INT>
        ) DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_segment_iterator_lazy_read_ratio
        SELECT CAST(number AS INT), CAST(number % 100 AS INT),
               CONCAT('payload-', CAST(number AS STRING)),
               IF(number % 7 = 0, NULL, CAST(number AS INT)),
               ARRAY(CAST(number AS INT), CAST(number + 1 AS INT))
        FROM numbers("number" = "1000")
    """

    for (int ratio : [0, 50, 100]) {
        sql "SET lazy_read_ratio = ${ratio}"
        "qt_session_ratio_${ratio}"("SHOW VARIABLES LIKE 'lazy_read_ratio'")
        "order_qt_all_${ratio}"("""
            SELECT COUNT(*), SUM(LENGTH(payload)), SUM(ARRAY_SIZE(arr))
            FROM test_segment_iterator_lazy_read_ratio
        """)
        "order_qt_empty_${ratio}"("""
            SELECT COUNT(*), SUM(LENGTH(payload)), SUM(ARRAY_SIZE(arr))
            FROM test_segment_iterator_lazy_read_ratio WHERE p < 0
        """)
        for (int bound : [99, 51, 50, 49, 1]) {
            "order_qt_filtered_${ratio}_${bound}"("""
                SELECT COUNT(*), SUM(LENGTH(payload)), SUM(nullable_value), SUM(ARRAY_SIZE(arr))
                FROM test_segment_iterator_lazy_read_ratio WHERE p < ${bound}
            """)
        }
        "order_qt_expr_${ratio}"("""
            SELECT COUNT(*), SUM(LENGTH(payload)), SUM(ARRAY_SIZE(arr))
            FROM test_segment_iterator_lazy_read_ratio WHERE ABS(p - 50) < 10
        """)
        "order_qt_asc_${ratio}"("""
            SELECT k, payload, nullable_value, arr
            FROM test_segment_iterator_lazy_read_ratio WHERE p < 80 ORDER BY k LIMIT 10
        """)
        "order_qt_desc_${ratio}"("""
            SELECT k, payload, nullable_value, arr
            FROM test_segment_iterator_lazy_read_ratio WHERE p < 80 ORDER BY k DESC LIMIT 10
        """)
    }
    test {
        sql "SET lazy_read_ratio = -1"
        exception "lazy_read_ratio"
    }
    test {
        sql "SET lazy_read_ratio = 101"
        exception "lazy_read_ratio"
    }
    sql "SET lazy_read_ratio = DEFAULT"
}
