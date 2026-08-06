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

// Test that `set enable_unique_key_partial_update=true; insert into ...` works correctly
// when sent as a single multi-statement query (single COM_QUERY).
// This is a regression test for the issue where session variables set via SET in a
// multi-statement batch might not be visible during the parse phase of subsequent
// statements in the same batch, because all statements are parsed together before
// any of them are executed.
suite("test_partial_update_multi_stmt", "p0") {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    String jdbcUrl = context.config.jdbcUrl
    String urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
    def sql_ip = urlWithoutSchema.substring(0, urlWithoutSchema.indexOf(":"))
    def sql_port
    if (urlWithoutSchema.indexOf("/") >= 0) {
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1, urlWithoutSchema.indexOf("/"))
    } else {
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1)
    }

    def tableName1 = "test_partial_update_multi_stmt1"
    def tableName2 = "test_partial_update_multi_stmt2"
    def tableName3 = "test_partial_update_multi_stmt3"
    def tableName4 = "test_partial_update_multi_stmt4"

    def createTable = { tblName ->
        sql """ DROP TABLE IF EXISTS ${tblName} """
        sql """
            CREATE TABLE ${tblName} (
                `k` int(11) NOT NULL COMMENT "key",
                `v1` int(11) NULL DEFAULT "10" COMMENT "value1",
                `v2` int(11) NULL DEFAULT "20" COMMENT "value2",
                `v3` int(11) NULL DEFAULT "30" COMMENT "value3"
            ) UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true"
            ); """
        sql "insert into ${tblName} values(1, 1, 1, 1), (2, 2, 2, 2);"
        sql "sync;"
    }

    // ============================================================
    // Setup: create all tables with initial data (1,1,1,1),(2,2,2,2)
    // ============================================================
    connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
        sql "use ${db};"
        sql "sync;"
        createTable(tableName1)
        createTable(tableName2)
        createTable(tableName3)
        createTable(tableName4)
        qt_base1 "select * from ${tableName1} order by k;"
        qt_base2 "select * from ${tableName2} order by k;"
    }

    // ============================================================
    // Test 1 & 2: multi-statement with allowMultiQueries=true
    // SET + INSERT sent as a single COM_QUERY
    // ============================================================
    def multiStmtUrl = "jdbc:mysql://${sql_ip}:${sql_port}/${db}?useLocalSessionState=false&allowMultiQueries=true"
    logger.info("multi-statement JDBC URL: ${multiStmtUrl}")

    try (Connection conn = DriverManager.getConnection(multiStmtUrl,
            context.config.jdbcUser, context.config.jdbcPassword)) {
        Statement stmt = conn.createStatement()

        // Test 1: SET + INSERT VALUES as single COM_QUERY
        logger.info("Test 1: allowMultiQueries=true, SET + INSERT VALUES")
        stmt.execute(
            "set enable_unique_key_partial_update=true;" +
            "set enable_insert_strict=false;" +
            "insert into ${tableName1}(k, v1) values(1, 100), (2, 200);"
        )
        stmt.execute("set enable_unique_key_partial_update=false;")

        // Test 2: SET + INSERT SELECT as single COM_QUERY
        logger.info("Test 2: allowMultiQueries=true, SET + INSERT SELECT")
        stmt.execute(
            "set enable_unique_key_partial_update=true;" +
            "set enable_insert_strict=false;" +
            "insert into ${tableName2}(k, v1) select k, v1 + 500 from ${tableName2};"
        )
        stmt.execute("set enable_unique_key_partial_update=false;")

        stmt.close()
    }

    // ============================================================
    // Test 3 & 4: without allowMultiQueries (statements sent separately)
    // SET and INSERT are separate COM_QUERY packets
    // ============================================================
    def singleStmtUrl = "jdbc:mysql://${sql_ip}:${sql_port}/${db}?useLocalSessionState=false"
    logger.info("single-statement JDBC URL: ${singleStmtUrl}")

    try (Connection conn = DriverManager.getConnection(singleStmtUrl,
            context.config.jdbcUser, context.config.jdbcPassword)) {
        Statement stmt = conn.createStatement()

        // Test 3: SET then INSERT VALUES as separate COM_QUERY
        logger.info("Test 3: separate statements, SET + INSERT VALUES")
        stmt.execute("set enable_unique_key_partial_update=true;")
        stmt.execute("set enable_insert_strict=false;")
        stmt.execute("insert into ${tableName3}(k, v1) values(1, 100), (2, 200);")
        stmt.execute("set enable_unique_key_partial_update=false;")

        // Test 4: SET then INSERT SELECT as separate COM_QUERY
        logger.info("Test 4: separate statements, SET + INSERT SELECT")
        stmt.execute("set enable_unique_key_partial_update=true;")
        stmt.execute("set enable_insert_strict=false;")
        stmt.execute("insert into ${tableName4}(k, v1) select k, v1 + 500 from ${tableName4};")
        stmt.execute("set enable_unique_key_partial_update=false;")

        stmt.close()
    }

    // ============================================================
    // Verify all results
    // ============================================================
    connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
        sql "use ${db};"
        sql "sync;"

        // Test 1: allowMultiQueries + INSERT VALUES
        // Expected (partial update): v2,v3 keep old values (1,1) and (2,2)
        qt_multi_stmt_values "select * from ${tableName1} order by k;"

        // Test 2: allowMultiQueries + INSERT SELECT
        // Expected (partial update): v2,v3 keep old values
        qt_multi_stmt_select "select * from ${tableName2} order by k;"

        // Test 3: separate statements + INSERT VALUES
        // Expected (partial update): v2,v3 keep old values (1,1) and (2,2)
        qt_separate_stmt_values "select * from ${tableName3} order by k;"

        // Test 4: separate statements + INSERT SELECT
        // Expected (partial update): v2,v3 keep old values
        qt_separate_stmt_select "select * from ${tableName4} order by k;"

        sql """ DROP TABLE IF EXISTS ${tableName1} """
        sql """ DROP TABLE IF EXISTS ${tableName2} """
        sql """ DROP TABLE IF EXISTS ${tableName3} """
        sql """ DROP TABLE IF EXISTS ${tableName4} """
    }
}
