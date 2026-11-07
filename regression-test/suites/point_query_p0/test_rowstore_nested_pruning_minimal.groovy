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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

suite("test_rowstore_nested_pruning_minimal", "p0,nonConcurrent") {
    sql "DROP TABLE IF EXISTS rowstore_nested_pruning_minimal"
    sql """
        CREATE TABLE rowstore_nested_pruning_minimal (
            pk INT,
            typed STRUCT<string_leaf:STRING,typed_map:MAP<STRING,STRUCT<string_leaf:STRING>>>
        ) UNIQUE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "store_row_column" = "true"
        )
    """
    sql """
        INSERT INTO rowstore_nested_pruning_minimal VALUES
        (2, NAMED_STRUCT('string_leaf', 'b-typed-2',
            'typed_map', MAP('b_key', NAMED_STRUCT('string_leaf', 'b-map-typed-2'))))
    """
    sql "SYNC"
    sql "SET enable_sql_cache=false"
    sql "SET query_timeout=10"

    sql "SET enable_short_circuit_query=false"
    sql "SET enable_prune_nested_column=true"
    order_qt_without_short_circuit """
        SELECT CHARACTER_LENGTH(struct_element(element_at(
            struct_element(typed, 'typed_map'), 'tail'), 'string_leaf')) AS character_len
        FROM rowstore_nested_pruning_minimal WHERE pk=2
    """
    order_qt_existing_key_without_short_circuit """
        SELECT CHARACTER_LENGTH(struct_element(element_at(
            struct_element(typed, 'typed_map'), 'b_key'), 'string_leaf')) AS character_len
        FROM rowstore_nested_pruning_minimal WHERE pk=2
    """

    sql "SET enable_short_circuit_query=true"
    sql "SET enable_prune_nested_column=false"
    order_qt_without_pruning """
        SELECT CHARACTER_LENGTH(struct_element(element_at(
            struct_element(typed, 'typed_map'), 'tail'), 'string_leaf')) AS character_len
        FROM rowstore_nested_pruning_minimal WHERE pk=2
    """
    order_qt_existing_key_without_pruning """
        SELECT CHARACTER_LENGTH(struct_element(element_at(
            struct_element(typed, 'typed_map'), 'b_key'), 'string_leaf')) AS character_len
        FROM rowstore_nested_pruning_minimal WHERE pk=2
    """

    sql "SET enable_prune_nested_column=true"
    explain {
        sql """
            SELECT CHARACTER_LENGTH(struct_element(element_at(
                struct_element(typed, 'typed_map'), 'tail'), 'string_leaf')) AS character_len
            FROM rowstore_nested_pruning_minimal WHERE pk=2
        """
        notContains "SHORT-CIRCUIT"
    }
    order_qt_fallback_with_pruning """
        SELECT CHARACTER_LENGTH(struct_element(element_at(
            struct_element(typed, 'typed_map'), 'tail'), 'string_leaf')) AS character_len
        FROM rowstore_nested_pruning_minimal WHERE pk=2
    """
    order_qt_existing_key_fallback_with_pruning """
        SELECT CHARACTER_LENGTH(struct_element(element_at(
            struct_element(typed, 'typed_map'), 'b_key'), 'string_leaf')) AS character_len
        FROM rowstore_nested_pruning_minimal WHERE pk=2
    """
    order_qt_fallback_with_pruning_cached """
        SELECT CHARACTER_LENGTH(struct_element(element_at(
            struct_element(typed, 'typed_map'), 'tail'), 'string_leaf')) AS character_len
        FROM rowstore_nested_pruning_minimal WHERE pk=2
    """
    order_qt_existing_key_fallback_with_pruning_repeated """
        SELECT CHARACTER_LENGTH(struct_element(element_at(
            struct_element(typed, 'typed_map'), 'b_key'), 'string_leaf')) AS character_len
        FROM rowstore_nested_pruning_minimal WHERE pk=2
    """

    // Scalar-only queries retain the fast path even on a table containing complex columns.
    explain {
        sql "SELECT pk FROM rowstore_nested_pruning_minimal WHERE pk=2"
        contains "SHORT-CIRCUIT"
    }
    qt_scalar_control "SELECT pk FROM rowstore_nested_pruning_minimal WHERE pk=2"

    def evolution = "rowstore_nested_pruning_evolution"
    sql "DROP TABLE IF EXISTS ${evolution}"
    sql """CREATE TABLE ${evolution} (k INT NOT NULL, s STRUCT<a:INT>)
        UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ('replication_num'='1', 'enable_unique_key_merge_on_write'='true',
                    'light_schema_change'='true', 'store_row_column'='true',
                    'disable_auto_compaction'='true')"""
    sql "INSERT INTO ${evolution} VALUES (1, NAMED_STRUCT('a', 7))"
    // A scalar point query caches the complete old row-store blob when RowCache is enabled.
    qt_warm_old_blob "SELECT k FROM ${evolution} WHERE k=1"
    sql "ALTER TABLE ${evolution} MODIFY COLUMN s STRUCT<a:INT,b:STRING>"
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE COLUMN WHERE TableName='${evolution}' ORDER BY CreateTime DESC LIMIT 1"
        time 600
    }
    sql "ALTER TABLE ${evolution} ADD COLUMN v STRING NOT NULL DEFAULT 'abcd'"
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE COLUMN WHERE TableName='${evolution}' ORDER BY CreateTime DESC LIMIT 1"
        time 600
    }
    sql "ALTER TABLE ${evolution} ADD COLUMN v_null STRING NULL"
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE COLUMN WHERE TableName='${evolution}' ORDER BY CreateTime DESC LIMIT 1"
        time 600
    }
    // FE has the new columns, but no load has propagated their schema to BE.
    explain {
        sql "SELECT length(v), v_null IS NULL FROM ${evolution} WHERE k=1"
        notContains "SHORT-CIRCUIT"
    }
    qt_default_metadata_before_load "SELECT length(v), v_null IS NULL FROM ${evolution} WHERE k=1"
    explain {
        sql "SELECT struct_element(s, 'a') FROM ${evolution} WHERE k=1"
        notContains "SHORT-CIRCUIT"
    }
    qt_old_struct_before_load "SELECT struct_element(s, 'a'), struct_element(s, 'b') FROM ${evolution} WHERE k=1"
    sql "INSERT INTO ${evolution} VALUES (2, NAMED_STRUCT('a', 8, 'b', 'bee'), 'new', NULL)"
    qt_old_struct_after_load "SELECT struct_element(s, 'a'), struct_element(s, 'b') FROM ${evolution} WHERE k=1"
    qt_new_struct_after_load "SELECT struct_element(s, 'a'), struct_element(s, 'b') FROM ${evolution} WHERE k=2"
    qt_default_metadata_after_load "SELECT length(v), v_null IS NULL FROM ${evolution} WHERE k=1"
    sql "SET enable_prune_nested_column=false"
    qt_old_struct_without_pruning "SELECT struct_element(s, 'a'), struct_element(s, 'b') FROM ${evolution} WHERE k=1"
    sql "SET enable_prune_nested_column=true"

    // Reuse one actual server-prepared handle, rather than preparing each SQL independently.
    def prepareUrl = getServerPrepareJdbcUrl(context.config.jdbcUrl, context.dbName, false)
    connect(context.config.jdbcUser, context.config.jdbcPassword, prepareUrl) {
        sql "SET enable_short_circuit_query=true"
        sql "SET enable_sql_cache=false"
        sql "SET enable_prune_nested_column=true"
        def statement = prepareStatement("SELECT k FROM ${evolution} WHERE k=?")
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, statement.class)
        try {
            statement.setInt(1, 1)
            qe_prepared_scalar_first statement
            statement.setInt(1, 2)
            qe_prepared_scalar_reuse statement
            sql "SET enable_prune_nested_column=false"
            qe_prepared_scalar_options_changed statement
            sql "ALTER TABLE ${evolution} ADD COLUMN extra INT NULL"
            waitForSchemaChangeDone {
                sql "SHOW ALTER TABLE COLUMN WHERE TableName='${evolution}' ORDER BY CreateTime DESC LIMIT 1"
                time 600
            }
            statement.setInt(1, 1)
            qe_prepared_scalar_schema_changed statement
        } finally {
            statement.close()
        }
        def beforeHint = sql("SELECT @@enable_prune_nested_column")
        assertEquals(1, beforeHint.size())
        def hinted = prepareStatement("SELECT /*+ SET_VAR(enable_prune_nested_column=true) */ v FROM ${evolution} WHERE k=?")
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, hinted.class)
        try {
            hinted.setInt(1, 1)
            qe_prepared_hint_first hinted
            hinted.setInt(1, 2)
            qe_prepared_hint_repeated hinted
            assertEquals(beforeHint, sql("SELECT @@enable_prune_nested_column"))
        } finally {
            hinted.close()
        }
    }
    // Explicit rollups can add an auxiliary full base tuple. It is not the tuple decoded by BE.
    def rollupTable = "rowstore_nested_pruning_rollup"
    sql "DROP TABLE IF EXISTS ${rollupTable}"
    sql """CREATE TABLE ${rollupTable} (k1 INT NOT NULL, k2 INT NOT NULL, s STRUCT<a:INT>)
        UNIQUE KEY(k1,k2) DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ('replication_num'='1', 'enable_unique_key_merge_on_write'='true',
                    'light_schema_change'='true')"""
    sql "ALTER TABLE ${rollupTable} ADD ROLLUP r(k2,k1)"
    waitingMVTaskFinishedByMvName(context.dbName, rollupTable, "r")
    sql "ALTER TABLE ${rollupTable} SET ('store_row_column'='true')"
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE COLUMN WHERE TableName='${rollupTable}' ORDER BY CreateTime DESC LIMIT 1"
        time 600
    }
    explain {
        sql "SELECT k1 FROM ${rollupTable} INDEX r WHERE k1=999 AND k2=999"
        contains "SHORT-CIRCUIT"
    }
    qt_auxiliary_tuple_missing_key "SELECT k1 FROM ${rollupTable} INDEX r WHERE k1=999 AND k2=999"
    sql "DROP TABLE ${rollupTable}"
    sql "DROP TABLE ${evolution}"
    sql "DROP TABLE rowstore_nested_pruning_minimal"
}
