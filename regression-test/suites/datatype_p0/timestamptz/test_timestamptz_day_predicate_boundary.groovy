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

suite("test_timestamptz_day_predicate_boundary") {
    sql "SET enable_sql_cache = false"
    sql "DROP TABLE IF EXISTS timestamptz_day_predicate_boundary"
    sql """
        CREATE TABLE timestamptz_day_predicate_boundary (id INT, ts TIMESTAMPTZ(6))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num' = '1')
    """
    sql """
        INSERT INTO timestamptz_day_predicate_boundary VALUES
        (1, '9999-12-29 23:00:00.123456+00:00'),
        (2, '0000-01-03 01:00:00.123456+00:00')
    """
    try {
        for (def skipFold : [false, true]) {
            sql "SET debug_skip_fold_constant = ${skipFold}"
            def tag = skipFold ? "be" : "fe"
            sql "SET time_zone = '+08:00'"
            // The forward operation is valid; the inverse cutoff has local year 10000.
            "order_qt_${tag}_local_upper"("""
                SELECT id FROM timestamptz_day_predicate_boundary
                WHERE id = 1 AND days_add(ts, 1)
                    < CAST('9999-12-31 23:00:00.123456+00:00' AS TIMESTAMPTZ(6))
            """)
            "order_qt_${tag}_local_upper_negative"("""
                SELECT id FROM timestamptz_day_predicate_boundary
                WHERE id = 1 AND days_sub(ts, -1)
                    < CAST('9999-12-31 23:00:00.123456+00:00' AS TIMESTAMPTZ(6))
            """)
            sql "SET time_zone = '-08:00'"
            // The lower cutoff is valid UTC, but its local year is -1.
            "order_qt_${tag}_local_lower"("""
                SELECT id FROM timestamptz_day_predicate_boundary
                WHERE id = 2 AND days_sub(ts, 1)
                    > CAST('0000-01-01 01:00:00.123456+00:00' AS TIMESTAMPTZ(6))
            """)
            "order_qt_${tag}_local_lower_negative"("""
                SELECT id FROM timestamptz_day_predicate_boundary
                WHERE id = 2 AND days_add(ts, -1)
                    > CAST('0000-01-01 01:00:00.123456+00:00' AS TIMESTAMPTZ(6))
            """)
            sql "SET time_zone = 'UTC'"
            // The inverse can also fail because its target year is outside the UTC range.
            "order_qt_${tag}_utc_upper"("""
                SELECT id FROM timestamptz_day_predicate_boundary
                WHERE id = 1 AND days_sub(ts, 1)
                    < CAST('9999-12-31 23:00:00.123456+00:00' AS TIMESTAMPTZ(6))
            """)
            "order_qt_${tag}_utc_lower"("""
                SELECT id FROM timestamptz_day_predicate_boundary
                WHERE id = 2 AND days_add(ts, 1)
                    > CAST('0000-01-01 01:00:00.123456+00:00' AS TIMESTAMPTZ(6))
            """)
        }
    } finally {
        sql "SET debug_skip_fold_constant = false"
        sql "SET enable_sql_cache = default"
        sql "SET time_zone = default"
    }
}
