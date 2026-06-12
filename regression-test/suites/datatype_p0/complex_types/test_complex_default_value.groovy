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

suite("test_complex_default_value") {
    sql "DROP TABLE IF EXISTS test_complex_default_value_literal"
    sql "DROP TABLE IF EXISTS test_complex_default_value_null"
    sql "DROP TABLE IF EXISTS test_complex_default_value_alter_base"
    sql "DROP TABLE IF EXISTS test_complex_default_value_bad_array_elem"
    sql "DROP TABLE IF EXISTS test_complex_default_value_bad_map_key"
    sql "DROP TABLE IF EXISTS test_complex_default_value_bad_map_value"
    sql "DROP TABLE IF EXISTS test_complex_default_value_bad_struct_field"
    sql "DROP TABLE IF EXISTS test_complex_default_value_bad_alter_array_elem"
    sql "DROP TABLE IF EXISTS test_complex_default_value_bad_alter_map_key"
    sql "DROP TABLE IF EXISTS test_complex_default_value_bad_alter_struct_field"

    def waitForCancelledSchemaChange = { String tableName ->
        def alterResult = []
        for (int i = 0; i < 600; ++i) {
            alterResult = sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1"""
            if (alterResult.size() > 0 &&
                    (alterResult[0][9] == "FINISHED" || alterResult[0][9] == "CANCELLED")) {
                break
            }
            sleep(1000)
        }

        logger.info("schema change result for ${tableName}: ${alterResult}")
        assertTrue(alterResult.size() > 0)
        assertEquals("CANCELLED", alterResult[0][9])
        assertTrue(alterResult[0].toString().contains("parse number fail"))
        sql "SYNC"
    }

    test {
        sql """
            CREATE TABLE test_complex_default_value_bad_array (
                k INT,
                v ARRAY<INT> DEFAULT '{}'
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES('replication_num'='1')
        """
        exception "only supports array literals or DEFAULT NULL"
    }

    test {
        sql """
            CREATE TABLE test_complex_default_value_bad_map (
                k INT,
                v MAP<STRING, INT> DEFAULT '[]'
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES('replication_num'='1')
        """
        exception "only supports map literals or DEFAULT NULL"
    }

    test {
        sql """
            CREATE TABLE test_complex_default_value_bad_json (
                k INT,
                v JSON DEFAULT '{}'
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES('replication_num'='1')
        """
        exception "only supports DEFAULT NULL"
    }

    test {
        sql """
            CREATE TABLE test_complex_default_value_bad_variant (
                k INT,
                v VARIANT DEFAULT '{}'
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES('replication_num'='1')
        """
        exception "only supports DEFAULT NULL"
    }

    sql """
        CREATE TABLE test_complex_default_value_bad_array_elem (
            k INT,
            v ARRAY<INT> DEFAULT '["bad"]'
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """

    test {
        sql "INSERT INTO test_complex_default_value_bad_array_elem(k) VALUES (1)"
        exception "parse number fail"
    }

    sql """
        CREATE TABLE test_complex_default_value_bad_map_key (
            k INT,
            v MAP<INT, INT> DEFAULT '{"bad": 1}'
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """

    test {
        sql "INSERT INTO test_complex_default_value_bad_map_key(k) VALUES (1)"
        exception "parse number fail"
    }

    sql """
        CREATE TABLE test_complex_default_value_bad_map_value (
            k INT,
            v MAP<STRING, INT> DEFAULT '{"bad": "value"}'
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """

    test {
        sql "INSERT INTO test_complex_default_value_bad_map_value(k) VALUES (1)"
        exception "parse number fail"
    }

    sql """
        CREATE TABLE test_complex_default_value_bad_struct_field (
            k INT,
            v STRUCT<f1:INT> DEFAULT '{"bad"}'
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """

    test {
        sql "INSERT INTO test_complex_default_value_bad_struct_field(k) VALUES (1)"
        exception "parse number fail"
    }

    // Cloud mode may finish add-column schema change as a metadata-only operation, while local
    // storage rewrites existing rows and cancels when the invalid default is materialized.
    if (!isCloudMode()) {
        sql """
            CREATE TABLE test_complex_default_value_bad_alter_array_elem (
                k INT
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES(
                'replication_num'='1',
                'light_schema_change'='false'
            )
        """

        sql "INSERT INTO test_complex_default_value_bad_alter_array_elem VALUES (1)"
        sql """
            ALTER TABLE test_complex_default_value_bad_alter_array_elem
            ADD COLUMN v ARRAY<INT> DEFAULT '["bad"]'
        """
        waitForCancelledSchemaChange.call("test_complex_default_value_bad_alter_array_elem")

        sql """
            CREATE TABLE test_complex_default_value_bad_alter_map_key (
                k INT
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES(
                'replication_num'='1',
                'light_schema_change'='false'
            )
        """

        sql "INSERT INTO test_complex_default_value_bad_alter_map_key VALUES (1)"
        sql """
            ALTER TABLE test_complex_default_value_bad_alter_map_key
            ADD COLUMN v MAP<INT, INT> DEFAULT '{"bad": 1}'
        """
        waitForCancelledSchemaChange.call("test_complex_default_value_bad_alter_map_key")

        sql """
            CREATE TABLE test_complex_default_value_bad_alter_struct_field (
                k INT
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES(
                'replication_num'='1',
                'light_schema_change'='false'
            )
        """

        sql "INSERT INTO test_complex_default_value_bad_alter_struct_field VALUES (1)"
        sql """
            ALTER TABLE test_complex_default_value_bad_alter_struct_field
            ADD COLUMN v STRUCT<f1:INT> DEFAULT '{"bad"}'
        """
        waitForCancelledSchemaChange.call("test_complex_default_value_bad_alter_struct_field")
    }

    sql """
        CREATE TABLE test_complex_default_value_literal (
            k INT,
            arr_empty ARRAY<INT> DEFAULT '[]',
            arr_literal ARRAY<INT> DEFAULT '[1, 2]',
            map_empty MAP<STRING, INT> DEFAULT '{}',
            map_literal MAP<STRING, INT> DEFAULT '{"a": 10, "b": 20}',
            struct_empty STRUCT<f1:INT, f2:STRING> DEFAULT '{}',
            struct_literal STRUCT<f1:INT, f2:STRING> DEFAULT '{7, "x"}',
            arr_nested_null ARRAY<INT> DEFAULT '[NULL, nUlL, 5]',
            map_nested_null MAP<STRING, INT> DEFAULT '{"upper": NULL, "mixed": nUlL, "value": 6}',
            struct_nested_null STRUCT<f1:INT, f2:STRING, f3:INT> DEFAULT '{NULL, NuLl, 7}'
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """

    sql "INSERT INTO test_complex_default_value_literal(k) VALUES (1)"
    order_qt_literal_default """
        SELECT k, arr_empty, arr_literal, map_empty, map_literal, struct_empty, struct_literal,
            arr_nested_null, map_nested_null, struct_nested_null
        FROM test_complex_default_value_literal
        ORDER BY k
    """

    sql """
        CREATE TABLE test_complex_default_value_null (
            k INT,
            arr_col ARRAY<INT> DEFAULT NULL,
            map_col MAP<STRING, INT> DEFAULT NULL,
            struct_col STRUCT<f:INT> DEFAULT NULL,
            json_col JSON DEFAULT NULL,
            variant_col VARIANT DEFAULT NULL
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """

    sql "INSERT INTO test_complex_default_value_null(k) VALUES (1)"
    order_qt_null_default """
        SELECT k, arr_col, map_col, struct_col, json_col, variant_col
        FROM test_complex_default_value_null
        ORDER BY k
    """

    sql """
        CREATE TABLE test_complex_default_value_alter_base (
            k INT
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES(
            'replication_num'='1',
            'light_schema_change'='true'
        )
    """

    sql "INSERT INTO test_complex_default_value_alter_base VALUES (1)"
    sql """
        ALTER TABLE test_complex_default_value_alter_base
        ADD COLUMN arr_added ARRAY<INT> NOT NULL DEFAULT '[3, 4]',
        ADD COLUMN map_added MAP<STRING, INT> NOT NULL DEFAULT '{"z": 9}',
        ADD COLUMN struct_added STRUCT<f1:INT, f2:STRING> NOT NULL DEFAULT '{8, "y"}'
    """
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_complex_default_value_alter_base' ORDER BY createtime DESC LIMIT 1"""
        time 600
    }

    order_qt_alter_default """
        SELECT k, arr_added, map_added, struct_added
        FROM test_complex_default_value_alter_base
        ORDER BY k
    """

    sql "INSERT INTO test_complex_default_value_alter_base(k) VALUES (2)"
    order_qt_alter_default_after_insert """
        SELECT k, arr_added, map_added, struct_added
        FROM test_complex_default_value_alter_base
        ORDER BY k
    """
}
