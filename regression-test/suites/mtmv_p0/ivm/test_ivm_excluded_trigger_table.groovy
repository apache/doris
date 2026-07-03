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

suite("test_ivm_excluded_trigger_table", "mtmv") {
    sql """drop materialized view if exists test_ivm_excluded_trigger_table_mv;"""
    sql """drop table if exists test_ivm_excluded_trigger_table_agg_base;"""

    sql """
        CREATE TABLE test_ivm_excluded_trigger_table_agg_base (
            k1 INT,
            v1 INT SUM
        )
        AGGREGATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """
        INSERT INTO test_ivm_excluded_trigger_table_agg_base VALUES
            (1, 10),
            (2, 20);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_excluded_trigger_table_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            'replication_num' = '1',
            'excluded_trigger_tables' = 'test_ivm_excluded_trigger_table_agg_base'
        )
        AS SELECT k1, v1
           FROM test_ivm_excluded_trigger_table_agg_base;
    """

    def queryMvRows = {
        sql("""SELECT k1, v1 FROM test_ivm_excluded_trigger_table_mv ORDER BY k1""")
                .collect { row -> [row[0] as int, row[1] as int] }
    }

    sql """REFRESH MATERIALIZED VIEW test_ivm_excluded_trigger_table_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_excluded_trigger_table_mv")
    assertEquals([[1, 10], [2, 20]], queryMvRows())

    sql """
        INSERT INTO test_ivm_excluded_trigger_table_agg_base VALUES
            (1, 5),
            (3, 30);
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_excluded_trigger_table_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_excluded_trigger_table_mv")
    // excluded_trigger_tables suppresses base-table changes even for manual INCREMENTAL refresh
    assertEquals([[1, 10], [2, 20]], queryMvRows())

    sql """REFRESH MATERIALIZED VIEW test_ivm_excluded_trigger_table_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_excluded_trigger_table_mv")
    assertEquals([[1, 15], [2, 20], [3, 30]], queryMvRows())

    sql """drop materialized view if exists test_ivm_excluded_trigger_table_alter_mv;"""
    sql """drop table if exists test_ivm_excluded_trigger_table_alter_base;"""

    sql """
        CREATE TABLE test_ivm_excluded_trigger_table_alter_base (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_excluded_trigger_table_alter_base VALUES
            (1, 10),
            (2, 20);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_excluded_trigger_table_alter_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k1, v1
           FROM test_ivm_excluded_trigger_table_alter_base;
    """

    def queryAlterMvRows = {
        sql("""SELECT k1, v1 FROM test_ivm_excluded_trigger_table_alter_mv ORDER BY k1""")
                .collect { row -> [row[0] as int, row[1] as int] }
    }

    sql """REFRESH MATERIALIZED VIEW test_ivm_excluded_trigger_table_alter_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_excluded_trigger_table_alter_mv")
    assertEquals([[1, 10], [2, 20]], queryAlterMvRows())

    sql """INSERT INTO test_ivm_excluded_trigger_table_alter_base VALUES (3, 30);"""
    sql """
        ALTER MATERIALIZED VIEW test_ivm_excluded_trigger_table_alter_mv
        SET ("excluded_trigger_tables" = "test_ivm_excluded_trigger_table_alter_base");
    """
    sql """REFRESH MATERIALIZED VIEW test_ivm_excluded_trigger_table_alter_mv INCREMENTAL FALLBACK"""
    waitingMTMVTaskFinishedByMvName("test_ivm_excluded_trigger_table_alter_mv")
    assertEquals([[1, 10], [2, 20], [3, 30]], queryAlterMvRows())

    def refreshMode = sql """
        SELECT RefreshMode FROM tasks('type'='mv')
        WHERE MvDatabaseName = '${context.dbName}'
          AND MvName = 'test_ivm_excluded_trigger_table_alter_mv'
        ORDER BY CreateTime DESC, TaskId DESC LIMIT 1
    """
    assertEquals("COMPLETE", refreshMode[0][0].toString())
}
