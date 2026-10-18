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
    sql "DROP TABLE IF EXISTS test_complex_default_value_like"
    sql "DROP TABLE IF EXISTS test_complex_default_value_null"
    sql "DROP TABLE IF EXISTS test_complex_default_value_alter_base"
    sql "DROP TABLE IF EXISTS test_complex_default_value_alter_direct"
    sql "DROP TABLE IF EXISTS test_complex_default_value_bad_alter"
    sql "DROP TABLE IF EXISTS test_complex_default_value_replace_value"
    sql "DROP TABLE IF EXISTS test_complex_default_value_mapping"
    sql "DROP TABLE IF EXISTS test_complex_default_value_partial_update"
    sql "DROP TABLE IF EXISTS test_complex_default_value_utf8"

    def createTableRejects = { String columnDef, String message ->
        test {
            sql """
                CREATE TABLE test_complex_default_value_rejected (
                    k INT,
                    v ${columnDef}
                )
                DUPLICATE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES('replication_num'='1')
            """
            exception message
        }
    }

    // the default must be a literal of the column's own shape
    createTableRejects.call("ARRAY<INT> DEFAULT '{}'", "only supports array literals or DEFAULT NULL")
    createTableRejects.call("ARRAY<INT> DEFAULT '[1 + 1]'", "only supports array literals or DEFAULT NULL")
    createTableRejects.call("MAP<STRING, INT> DEFAULT '[]'", "only supports map literals or DEFAULT NULL")
    createTableRejects.call("STRUCT<f1:INT> DEFAULT '[]'", "only supports struct literals or DEFAULT NULL")
    createTableRejects.call("""STRUCT<f1:INT, f2:STRING> DEFAULT '{"f1": 1, "f2": "a"}'""", "only supports struct literals or DEFAULT NULL")
    createTableRejects.call("JSON DEFAULT '{}'", "only supports DEFAULT NULL")
    createTableRejects.call("VARIANT DEFAULT '{}'", "only supports DEFAULT NULL")

    // every nested value is cast to the declared nested type at DDL time
    createTableRejects.call("""ARRAY<INT> DEFAULT '["bad"]'""", "Invalid default value")
    createTableRejects.call("""MAP<INT, INT> DEFAULT '{"bad": 1}'""", "Invalid default value")
    createTableRejects.call("""MAP<STRING, INT> DEFAULT '{"bad": "value"}'""", "Invalid default value")
    createTableRejects.call("""STRUCT<f1:INT> DEFAULT '{"bad"}'""", "Invalid default value")
    createTableRejects.call("""STRUCT<f1:INT, f2:STRING> DEFAULT '{1}'""", "struct literal has 1 fields but the column has 2")
    createTableRejects.call("""ARRAY<ARRAY<INT>> DEFAULT '[[1], ["bad"]]'""", "Invalid default value")
    // an item that cannot be cast to the type inferred from the other items is rejected instead of being
    // replaced by NULL, which the SQL literal parser does under the default enable_strict_cast=false
    createTableRejects.call("""ARRAY<DATEV2> DEFAULT '[DATEV2 "2024-01-01", 1]'""", "Invalid default value")
    createTableRejects.call("""MAP<DATEV2, INT> DEFAULT '{DATEV2 "2024-01-01": 1, 1: 2}'""", "Invalid default value")
    createTableRejects.call("""MAP<STRING, DATEV2> DEFAULT '{"a": DATEV2 "2024-01-01", "b": 1}'""", "Invalid default value")
    createTableRejects.call("""STRUCT<f1:ARRAY<DATEV2>> DEFAULT '{[DATEV2 "2024-01-01", 1]}'""", "Invalid default value")
    // map keys that only collide after the cast to the key type are rejected
    createTableRejects.call("""MAP<INT, INT> DEFAULT '{"01": 1, "1": 2}'""", "map key 1 is repeated after casting to INT")
    // a nested value longer than the declared VARCHAR(n)/CHAR(n) is rejected like a scalar default instead of
    // being truncated
    createTableRejects.call("""ARRAY<VARCHAR(2)> DEFAULT '["abc"]'""", "Default value is too long: abc")
    createTableRejects.call("""ARRAY<CHAR(2)> DEFAULT '["abc"]'""", "Default value is too long: abc")
    createTableRejects.call("""STRUCT<f1:VARCHAR(2)> DEFAULT '{123}'""", "Default value is too long: 123")
    // the declared length is a byte limit, so a multi-byte character may exceed it even if it is a
    // single character, and a 4-byte character is two UTF-16 code units but four UTF-8 bytes
    createTableRejects.call("""ARRAY<VARCHAR(2)> DEFAULT '["中"]'""", "Default value is too long: 中")
    createTableRejects.call("""ARRAY<CHAR(2)> DEFAULT '["中"]'""", "Default value is too long: 中")
    createTableRejects.call("""MAP<STRING, VARCHAR(2)> DEFAULT '{"k": "中"}'""", "Default value is too long: 中")
    createTableRejects.call("""STRUCT<f1:VARCHAR(2)> DEFAULT '{"中"}'""", "Default value is too long: 中")
    createTableRejects.call("""ARRAY<VARCHAR(3)> DEFAULT '["😀"]'""", "Default value is too long: 😀")

    // nested string values are stored as plain double quoted text, so quotes and backslashes are rejected
    createTableRejects.call("""ARRAY<STRING> DEFAULT '["a""b"]'""", "must not contain quote or backslash")
    createTableRejects.call("""MAP<STRING, INT> DEFAULT '{"a\\\\\\\\b": 1}'""", "must not contain quote or backslash")
    createTableRejects.call("""ARRAY<STRING> DEFAULT '["it''s"]'""", "must not contain quote or backslash")

    // ADD COLUMN validates the default the same way, for both light and direct schema change
    sql """
        CREATE TABLE test_complex_default_value_bad_alter (
            k INT
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES('replication_num'='1', 'light_schema_change'='false')
    """
    sql "INSERT INTO test_complex_default_value_bad_alter VALUES (1)"
    test {
        sql """ALTER TABLE test_complex_default_value_bad_alter ADD COLUMN v ARRAY<INT> DEFAULT '["bad"]'"""
        exception "Invalid default value"
    }
    test {
        sql """ALTER TABLE test_complex_default_value_bad_alter ADD COLUMN v MAP<INT, INT> DEFAULT '{"bad": 1}'"""
        exception "Invalid default value"
    }
    test {
        sql """ALTER TABLE test_complex_default_value_bad_alter ADD COLUMN v STRUCT<f1:INT> DEFAULT '{"bad"}'"""
        exception "Invalid default value"
    }
    test {
        sql """ALTER TABLE test_complex_default_value_bad_alter ADD COLUMN v MAP<INT, INT> DEFAULT '{"01": 1, "1": 2}'"""
        exception "map key 1 is repeated after casting to INT"
    }
    test {
        sql """ALTER TABLE test_complex_default_value_bad_alter ADD COLUMN v ARRAY<DATEV2> DEFAULT '[DATEV2 "2024-01-01", 1]'"""
        exception "Invalid default value"
    }
    test {
        sql """ALTER TABLE test_complex_default_value_bad_alter ADD COLUMN v MAP<STRING, DATEV2> DEFAULT '{"a": DATEV2 "2024-01-01", "b": 1}'"""
        exception "Invalid default value"
    }
    test {
        sql """ALTER TABLE test_complex_default_value_bad_alter ADD COLUMN v ARRAY<VARCHAR(2)> DEFAULT '["abc"]'"""
        exception "Default value is too long: abc"
    }
    test {
        sql """ALTER TABLE test_complex_default_value_bad_alter ADD COLUMN v ARRAY<VARCHAR(2)> DEFAULT '["中"]'"""
        exception "Default value is too long: 中"
    }
    // CHAR values are cut at the first NUL by the page decoder and by the default value iterator,
    // but the row-store fill keeps every byte, so a nested CHAR default must not contain NUL
    test {
        sql """ALTER TABLE test_complex_default_value_bad_alter ADD COLUMN v ARRAY<CHAR(3)> DEFAULT '["a\\0b"]'"""
        exception "CHAR default value must not contain NUL"
    }
    test {
        sql """ALTER TABLE test_complex_default_value_bad_alter ADD COLUMN v MAP<CHAR(3), INT> DEFAULT '{"a\\0b": 1}'"""
        exception "CHAR default value must not contain NUL"
    }
    test {
        sql """ALTER TABLE test_complex_default_value_bad_alter ADD COLUMN v STRUCT<f1:CHAR(3)> DEFAULT '{"a\\0b"}'"""
        exception "CHAR default value must not contain NUL"
    }

    // a multi-byte value whose UTF-8 byte length exactly fits the declared length is accepted and
    // materialized for omitted columns on later inserts
    sql """
        CREATE TABLE test_complex_default_value_utf8 (
            k INT,
            arr_varchar ARRAY<VARCHAR(3)> DEFAULT '["中"]',
            arr_char ARRAY<CHAR(3)> DEFAULT '["中"]',
            arr_emoji ARRAY<VARCHAR(4)> DEFAULT '["😀"]'
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """
    sql "INSERT INTO test_complex_default_value_utf8(k) VALUES (1)"
    order_qt_utf8_default """
        SELECT * FROM test_complex_default_value_utf8 ORDER BY k
    """

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
            struct_nested_null STRUCT<f1:INT, f2:STRING, f3:INT> DEFAULT '{NULL, NuLl, 7}',
            arr_typed_date ARRAY<DATEV2> DEFAULT '[DATEV2 "2024-01-01", "2024-02-02"]',
            arr_exponent ARRAY<INT> DEFAULT '[1e3, "7"]',
            arr_bool ARRAY<BOOLEAN> DEFAULT '[true, false]',
            arr_decimal ARRAY<DECIMAL(10, 2)> DEFAULT '[1.234]',
            arr_string ARRAY<STRING> DEFAULT '["x,y", "[z]", "{k:v}", "null", ""]',
            map_repeated_key MAP<STRING, INT> DEFAULT '{"a": 1, "a": 2}',
            arr_nested ARRAY<ARRAY<INT>> DEFAULT '[[1], [], NULL]',
            arr_varchar ARRAY<VARCHAR(2)> DEFAULT '["ab", 1]',
            arr_negative_zero ARRAY<DOUBLE> DEFAULT '["-0", -0.0, "-1.5"]'
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """

    sql "INSERT INTO test_complex_default_value_literal(k) VALUES (1)"
    order_qt_literal_default """
        SELECT * FROM test_complex_default_value_literal ORDER BY k
    """

    // SHOW CREATE TABLE renders the canonical default in a form that CREATE TABLE LIKE can replay
    def showCreate = sql "SHOW CREATE TABLE test_complex_default_value_literal"
    def createSql = showCreate[0][1]
    logger.info("show create table: ${createSql}")
    assertTrue(createSql.contains('`arr_literal` array<int> NULL DEFAULT "[1, 2]"'))
    assertTrue(createSql.contains('`map_literal` map<text,int> NULL DEFAULT \'{"a":10, "b":20}\''))
    assertTrue(createSql.contains('`arr_typed_date` array<date> NULL DEFAULT \'["2024-01-01", "2024-02-02"]\''))
    assertTrue(createSql.contains('`arr_exponent` array<int> NULL DEFAULT "[1000, 7]"'))
    assertTrue(createSql.contains('`arr_bool` array<boolean> NULL DEFAULT "[1, 0]"'))
    assertTrue(createSql.contains('`map_repeated_key` map<text,int> NULL DEFAULT \'{"a":2}\''))
    assertTrue(createSql.contains('`arr_string` array<text> NULL DEFAULT \'["x,y", "[z]", "{k:v}", "null", ""]\''))
    // nested NULL is stored in lower case, the only spelling every BE consumer of the text accepts
    assertTrue(createSql.contains('`arr_nested_null` array<int> NULL DEFAULT "[null, null, 5]"'))
    assertTrue(createSql.contains('`struct_nested_null` struct<f1:int,f2:text,f3:int> NULL DEFAULT "{null, null, 7}"'))
    // a SQL numeric literal has no negative zero, so -0.0 is stored as 0.0 and replays unchanged
    assertTrue(createSql.contains('`arr_negative_zero` array<double> NULL DEFAULT "[0.0, 0.0, -1.5]"'))
    sql "CREATE TABLE test_complex_default_value_like LIKE test_complex_default_value_literal"
    sql "INSERT INTO test_complex_default_value_like(k) VALUES (1)"
    order_qt_like_default """
        SELECT * FROM test_complex_default_value_like ORDER BY k
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

    // rows written before the columns were added read the defaults through the BE default value iterator
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
        ADD COLUMN struct_added STRUCT<f1:INT, f2:STRING> NOT NULL DEFAULT '{8, "y"}',
        ADD COLUMN arr_typed_date ARRAY<DATEV2> DEFAULT '[DATEV2 "2024-01-01", "2024-02-02"]',
        ADD COLUMN arr_exponent ARRAY<INT> DEFAULT '[1e3, "7"]',
        ADD COLUMN arr_bool ARRAY<BOOLEAN> DEFAULT '[true, false]',
        ADD COLUMN arr_decimal ARRAY<DECIMAL(10, 2)> DEFAULT '[1.234]',
        ADD COLUMN arr_string ARRAY<STRING> DEFAULT '["x,y", "[z]", "{k:v}", "null", ""]',
        ADD COLUMN map_repeated_key MAP<STRING, INT> DEFAULT '{"a": 1, "a": 2}',
        ADD COLUMN arr_nested ARRAY<ARRAY<INT>> DEFAULT '[[1], [], NULL]',
        ADD COLUMN struct_empty STRUCT<f1:INT, f2:STRING> DEFAULT '{}',
        ADD COLUMN arr_struct ARRAY<STRUCT<f1:INT, f2:STRING>> DEFAULT '[{1, "p"}, {2, "q"}]',
        ADD COLUMN map_struct MAP<STRING, STRUCT<f1:INT, f2:STRING>> DEFAULT '{"k": {3, "r"}}'
    """
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_complex_default_value_alter_base' ORDER BY createtime DESC LIMIT 1"""
        time 600
    }

    order_qt_alter_default """
        SELECT * FROM test_complex_default_value_alter_base ORDER BY k
    """

    sql "INSERT INTO test_complex_default_value_alter_base(k) VALUES (2)"
    order_qt_alter_default_after_insert """
        SELECT * FROM test_complex_default_value_alter_base ORDER BY k
    """
    // nested column pruning reads only the accessed STRUCT fields, so the default of an old row is
    // projected onto the pruned STRUCT, also inside ARRAY items and MAP values
    order_qt_alter_default_pruned """
        SELECT k,
               struct_element(struct_added, 'f2'),
               struct_element(struct_empty, 'f2'),
               array_map(x -> struct_element(x, 'f2'), arr_struct),
               struct_element(element_at(map_struct, 'k'), 'f2'),
               size(arr_struct),
               struct_added IS NULL
        FROM test_complex_default_value_alter_base ORDER BY k
    """

    // direct schema change materializes the defaults into the rewritten rows
    sql """
        CREATE TABLE test_complex_default_value_alter_direct (
            k INT
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES(
            'replication_num'='1',
            'light_schema_change'='false'
        )
    """

    sql "INSERT INTO test_complex_default_value_alter_direct VALUES (1)"
    // a delete predicate on the base rowsets makes BE pick the direct strategy instead of linking
    // the old rowset, so the new columns are materialized by the schema change itself
    sql "DELETE FROM test_complex_default_value_alter_direct WHERE k = 0"
    sql """
        ALTER TABLE test_complex_default_value_alter_direct
        ADD COLUMN arr_added ARRAY<INT> NOT NULL DEFAULT '[3, 4]',
        ADD COLUMN map_repeated_key MAP<STRING, INT> DEFAULT '{"a": 1, "a": 2}',
        ADD COLUMN struct_added STRUCT<f1:INT, f2:ARRAY<DATEV2>> DEFAULT '{8, [DATEV2 "2024-01-01"]}',
        ADD COLUMN arr_string ARRAY<STRING> DEFAULT '["x,y", ""]'
    """
    waitForSchemaChangeDone {
        sql """SHOW ALTER TABLE COLUMN WHERE IndexName='test_complex_default_value_alter_direct' ORDER BY createtime DESC LIMIT 1"""
        time 600
    }

    order_qt_alter_direct_default """
        SELECT * FROM test_complex_default_value_alter_direct ORDER BY k
    """

    // a one-argument replace_value mapping falls back to the complex column default
    sql """
        CREATE TABLE test_complex_default_value_replace_value (
            k INT,
            v MAP<STRING, INT> DEFAULT '{"a": 1}'
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """
    streamLoad {
        table "test_complex_default_value_replace_value"
        set 'columns', 'k, v, v = replace_value(null)'
        file "test_complex_default_value_replace_value.csv"
        time 60
    }
    order_qt_replace_value """
        SELECT k, v FROM test_complex_default_value_replace_value ORDER BY k
    """

    // an explicit default_value / replace_value argument of a complex column is written as the
    // canonical literal, so 1e3 and "7" become INT elements instead of NULL
    sql """
        CREATE TABLE test_complex_default_value_mapping (
            k INT,
            v ARRAY<INT>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """
    streamLoad {
        table "test_complex_default_value_mapping"
        set 'columns', 'k, v, v = replace_value(null, \'[1e3, "7"]\')'
        file "test_complex_default_value_mapping.csv"
        time 60
    }
    order_qt_mapping_replace_value """
        SELECT k, v FROM test_complex_default_value_mapping ORDER BY k
    """
    streamLoad {
        table "test_complex_default_value_mapping"
        set 'columns', 'k, tmp, v = default_value(\'[1e3, "7"]\')'
        file "test_complex_default_value_mapping.csv"
        time 60
    }
    order_qt_mapping_default_value """
        SELECT k, v FROM test_complex_default_value_mapping ORDER BY k, v
    """
    test {
        sql """
            ALTER TABLE test_complex_default_value_mapping
            ADD COLUMN bad MAP<INT, INT> DEFAULT '{"01": 1, "1": 2}'
        """
        exception "map key 1 is repeated after casting to INT"
    }

    // a partial update of a new key fills the omitted complex columns from their defaults through the
    // BE JSON default parser, which must read a nested null the same way as the default value iterator
    sql """
        CREATE TABLE test_complex_default_value_partial_update (
            k INT,
            v INT,
            arr_nested_null ARRAY<STRING> DEFAULT '[NULL, "null", "x"]',
            map_nested_null MAP<STRING, STRING> DEFAULT '{"a": NULL, "b": "y"}',
            struct_nested_null STRUCT<f1:INT, f2:STRING> DEFAULT '{NULL, NULL}',
            struct_literal STRUCT<f1:INT, f2:STRING> DEFAULT '{7, "x"}',
            arr_literal ARRAY<INT> DEFAULT '[1, 2]'
        )
        UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES('replication_num'='1', 'enable_unique_key_merge_on_write'='true')
    """
    sql "INSERT INTO test_complex_default_value_partial_update(k, v) VALUES (1, 1)"
    sql "SET enable_unique_key_partial_update = true"
    sql "SET enable_insert_strict = false"
    sql "INSERT INTO test_complex_default_value_partial_update(k) VALUES (2)"
    sql "SET enable_unique_key_partial_update = false"
    sql "SET enable_insert_strict = true"
    order_qt_partial_update_default """
        SELECT k, arr_nested_null, array_contains(arr_nested_null, NULL), map_nested_null,
               struct_nested_null, struct_element(struct_nested_null, 'f2') IS NULL, struct_literal, arr_literal
        FROM test_complex_default_value_partial_update ORDER BY k
    """
}
