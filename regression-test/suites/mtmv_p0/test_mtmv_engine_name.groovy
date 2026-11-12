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

suite("test_mtmv_engine_name", "mtmv") {
    sql """drop materialized view if exists test_mtmv_engine_name_mv"""
    sql """drop view if exists test_mtmv_engine_name_view"""
    sql """drop table if exists test_mtmv_engine_name_base"""

    sql """
        CREATE TABLE test_mtmv_engine_name_base
        (
            k INT,
            v INT
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
        """
    sql """
        CREATE MATERIALIZED VIEW test_mtmv_engine_name_mv
        BUILD DEFERRED REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
        AS
        SELECT k, v FROM test_mtmv_engine_name_base;
        """
    sql """
        CREATE VIEW test_mtmv_engine_name_view AS SELECT k FROM test_mtmv_engine_name_base;
        """

    // An async materialized view is a BASE TABLE, and like every other table it names its engine.
    qt_information_schema_tables """
        SELECT TABLE_NAME, TABLE_TYPE, ENGINE FROM information_schema.tables
        WHERE TABLE_SCHEMA = '${context.dbName}' AND TABLE_NAME LIKE 'test_mtmv_engine_name%'
        ORDER BY TABLE_NAME
        """

    // SHOW TABLE STATUS reads the same column. Its other columns, such as the times, vary between runs.
    def tableStatus = sql """SHOW TABLE STATUS LIKE 'test_mtmv_engine_name_mv'"""
    assertEquals(1, tableStatus.size())
    assertEquals("MaterializedView", tableStatus[0][1]) // Engine
}
