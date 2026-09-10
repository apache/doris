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

suite("test_try_cast_conversion_errors") {
    sql "SET time_zone = '+00:00'"
    sql "SET enable_sql_cache = false"
    sql "DROP TABLE IF EXISTS test_try_cast_conversion_errors"
    sql """
        CREATE TABLE test_try_cast_conversion_errors (
            id INT NOT NULL,
            dt DATETIME(6),
            tz TIMESTAMPTZ(6),
            m MAP<STRING, STRING>
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_try_cast_conversion_errors VALUES
            (1, '2024-01-01 00:00:00.123456', '2024-01-01 00:00:00.123456+00:00',
                MAP(REPEAT('k', 255), 'value')),
            (2, '9999-12-31 23:59:59.999999', '9999-12-31 23:59:59.999999+00:00',
                MAP(REPEAT('k', 256), 'value')),
            (3, NULL, NULL, NULL),
            (4, '2024-06-01 12:34:56.999999', '2024-06-01 12:34:56.999999+00:00', MAP())
    """

    def conversions = [
        [source: "dt", target: "TIMESTAMPTZ(0)", zone: "+00:00", error: "can not cast"],
        [source: "tz", target: "TIMESTAMPTZ(0)", zone: "+00:00", error: "can not cast"],
        [source: "tz", target: "DATETIME(0)", zone: "+00:00", error: "can not cast"],
        // The precision is unchanged here; conversion overflows because of the time zone.
        [source: "dt", target: "TIMESTAMPTZ(6)", zone: "-01:00", error: "can not cast"],
        [source: "tz", target: "DATETIME(6)", zone: "+01:00", error: "can not cast"],
        [source: "m", target: "JSON", zone: "+00:00", error: "key size exceeds max limit"]
    ]
    conversions.each { conversion ->
        sql "SET time_zone = '${conversion.zone}'"
        [true, false].each { strict ->
            sql "SET enable_strict_cast = ${strict}"
            // Only the failed conversion becomes NULL; preserve valid rows and source NULLs.
            check_sqls_result_equal("""
                SELECT id, TRY_CAST(${conversion.source} AS ${conversion.target}) AS converted
                FROM test_try_cast_conversion_errors ORDER BY id
            """, """
                SELECT id, CAST(${conversion.source} AS ${conversion.target}) AS converted
                FROM test_try_cast_conversion_errors WHERE id != 2
                UNION ALL
                SELECT id, CAST(NULL AS ${conversion.target}) AS converted
                FROM test_try_cast_conversion_errors WHERE id = 2
                ORDER BY id
            """)
        }
        sql "SET enable_strict_cast = true"
        test {
            sql """
                SELECT CAST(${conversion.source} AS ${conversion.target})
                FROM test_try_cast_conversion_errors WHERE id = 2
            """
            exception conversion.error
        }
    }
}
