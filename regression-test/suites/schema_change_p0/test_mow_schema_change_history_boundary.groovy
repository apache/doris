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

import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement

suite("test_mow_schema_change_history_boundary", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP TABLE IF EXISTS test_mow_schema_change_history_boundary"
    sql """
        CREATE TABLE test_mow_schema_change_history_boundary (
            k INT, a INT, b VARCHAR(16)
        ) UNIQUE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
    """
    sql "INSERT INTO test_mow_schema_change_history_boundary VALUES (1, 10, 'x')"

    def database = sql("SELECT DATABASE()")[0][0]
    def url = getServerPrepareJdbcUrl(context.config.jdbcUrl, database)
            .replace("&useServerPrepStmts=true", "") + "&useLocalSessionState=true"
    def alterState = {
        def jobs = sql_return_maparray("""
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'test_mow_schema_change_history_boundary'
            ORDER BY CreateTime DESC LIMIT 1
        """)
        jobs.isEmpty() ? "" : jobs[0].State
    }

    try (Connection first = DriverManager.getConnection(
                    url, context.config.jdbcUser, context.config.jdbcPassword);
         Connection second = DriverManager.getConnection(
                    url, context.config.jdbcUser, context.config.jdbcPassword);
         Statement firstTxn = first.createStatement();
         Statement secondTxn = second.createStatement()) {
        firstTxn.setQueryTimeout(90)
        secondTxn.setQueryTimeout(90)
        firstTxn.execute("BEGIN")
        firstTxn.execute("INSERT INTO test_mow_schema_change_history_boundary VALUES (7, 40, 'x')")
        secondTxn.execute("BEGIN")
        secondTxn.execute("INSERT INTO test_mow_schema_change_history_boundary VALUES (8, 60, 'x')")

        sql "ALTER TABLE test_mow_schema_change_history_boundary ORDER BY(k, b, a)"
        awaitUntil(90, { alterState() == "WAITING_TXN" })

        // v3 is double-written, while the older transaction's v4 only reaches
        // the original tablet. v5 must publish on the shadow despite that hole.
        sql "INSERT INTO test_mow_schema_change_history_boundary VALUES (9, 90, 'x')"
        firstTxn.execute("COMMIT")
        sql "INSERT INTO test_mow_schema_change_history_boundary VALUES (7, 50, 'x')"

        // This pre-watershed transaction must finish before history conversion.
        secondTxn.execute("COMMIT")
        awaitUntil(90, { alterState() == "FINISHED" })
    }

    order_qt_after_alter "SELECT k, a FROM test_mow_schema_change_history_boundary"
    sql "INSERT INTO test_mow_schema_change_history_boundary (k, a, b) VALUES (7, 70, 'x')"
    order_qt_after_publish "SELECT k, a FROM test_mow_schema_change_history_boundary"
}
