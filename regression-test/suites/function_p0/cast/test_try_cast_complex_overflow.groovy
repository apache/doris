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

suite("test_try_cast_complex_overflow") {
    sql "DROP TABLE IF EXISTS test_try_cast_complex_overflow"
    sql """
        CREATE TABLE test_try_cast_complex_overflow (
            id INT NOT NULL,
            a ARRAY<DECIMAL(6,3)> NOT NULL,
            m MAP<INT,INT> NOT NULL,
            s STRUCT<v:INT> NOT NULL
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_try_cast_complex_overflow VALUES
            (1, [12.340], MAP(1, 12), NAMED_STRUCT('v', 12)),
            (2, [123.456], MAP(1, 128), NAMED_STRUCT('v', 128)),
            (3, [NULL], MAP(1, NULL), NAMED_STRUCT('v', NULL)),
            (4, [], MAP(), NAMED_STRUCT('v', 0)),
            (5, [-12.340], MAP(1, -12), NAMED_STRUCT('v', -12)),
            (6, [-123.456], MAP(1, -129), NAMED_STRUCT('v', -129))
    """
    sql "SET enable_sql_cache=false"

    def targetTypes = [a: "ARRAY<DECIMAL(4,2)>", m: "MAP<INT,TINYINT>", s: "STRUCT<v:TINYINT>"]
    targetTypes.each { columnName, targetType ->
        sql "SET enable_strict_cast=true"
        // A failed nested conversion makes the whole row NULL in strict TRY_CAST.
        // Build the reference with ordinary CAST applied only to valid rows.
        check_sqls_result_equal("""
            SELECT id, TRY_CAST(${columnName} AS ${targetType}) AS converted
            FROM test_try_cast_complex_overflow ORDER BY id
        """, """
            SELECT id, CAST(${columnName} AS ${targetType}) AS converted
            FROM test_try_cast_complex_overflow WHERE id NOT IN (2, 6)
            UNION ALL
            SELECT id, CAST(NULL AS ${targetType}) AS converted
            FROM test_try_cast_complex_overflow WHERE id IN (2, 6)
            ORDER BY id
        """)
        test {
            sql """
                SELECT CAST(${columnName} AS ${targetType})
                FROM test_try_cast_complex_overflow WHERE id = 2
            """
            exception(columnName == "a" ? "Arithmetic overflow" : "Value 128 out of range")
        }

        sql "SET enable_strict_cast=false"
        // Non-strict conversion preserves the container and nulls only the invalid element.
        check_sqls_result_equal("""
            SELECT id, TRY_CAST(${columnName} AS ${targetType}) AS converted
            FROM test_try_cast_complex_overflow ORDER BY id
        """, """
            SELECT id, CAST(${columnName} AS ${targetType}) AS converted
            FROM test_try_cast_complex_overflow ORDER BY id
        """)
    }
}
