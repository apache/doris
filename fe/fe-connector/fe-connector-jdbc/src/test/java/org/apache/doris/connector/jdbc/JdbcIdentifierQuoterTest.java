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

package org.apache.doris.connector.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class JdbcIdentifierQuoterTest {

    // === quoteIdentifier: MySQL-style backtick group ===

    @Test
    void testQuoteIdentifierMysqlUsesBacktick() {
        Assertions.assertEquals("`users`", JdbcIdentifierQuoter.quoteIdentifier(JdbcDbType.MYSQL, "users"));
    }

    @Test
    void testQuoteIdentifierOceanBaseUsesBacktick() {
        Assertions.assertEquals("`orders`", JdbcIdentifierQuoter.quoteIdentifier(JdbcDbType.OCEANBASE, "orders"));
    }

    @Test
    void testQuoteIdentifierGbaseUsesBacktick() {
        Assertions.assertEquals("`col`", JdbcIdentifierQuoter.quoteIdentifier(JdbcDbType.GBASE, "col"));
    }

    // === quoteIdentifier: SQLServer brackets ===

    @Test
    void testQuoteIdentifierSqlServerUsesBrackets() {
        Assertions.assertEquals("[users]", JdbcIdentifierQuoter.quoteIdentifier(JdbcDbType.SQLSERVER, "users"));
    }

    // === quoteIdentifier: Oracle/DB2 uppercase double-quote ===

    @Test
    void testQuoteIdentifierOracleUppercases() {
        Assertions.assertEquals("\"MYCOLUMN\"",
                JdbcIdentifierQuoter.quoteIdentifier(JdbcDbType.ORACLE, "myColumn"));
    }

    @Test
    void testQuoteIdentifierDb2Uppercases() {
        Assertions.assertEquals("\"USERS\"", JdbcIdentifierQuoter.quoteIdentifier(JdbcDbType.DB2, "users"));
    }

    // === quoteIdentifier: double-quote group (no uppercase) ===

    @Test
    void testQuoteIdentifierPostgresqlPreservesCase() {
        Assertions.assertEquals("\"myColumn\"",
                JdbcIdentifierQuoter.quoteIdentifier(JdbcDbType.POSTGRESQL, "myColumn"));
    }

    @Test
    void testQuoteIdentifierClickhousePreservesCase() {
        Assertions.assertEquals("\"col\"", JdbcIdentifierQuoter.quoteIdentifier(JdbcDbType.CLICKHOUSE, "col"));
    }

    @Test
    void testQuoteIdentifierTrinoPreservesCase() {
        Assertions.assertEquals("\"t\"", JdbcIdentifierQuoter.quoteIdentifier(JdbcDbType.TRINO, "t"));
    }

    @Test
    void testQuoteIdentifierSapHanaPreservesCase() {
        Assertions.assertEquals("\"col\"", JdbcIdentifierQuoter.quoteIdentifier(JdbcDbType.SAP_HANA, "col"));
    }

    @Test
    void testQuoteIdentifierOceanBaseOraclePreservesCase() {
        Assertions.assertEquals("\"myCol\"",
                JdbcIdentifierQuoter.quoteIdentifier(JdbcDbType.OCEANBASE_ORACLE, "myCol"));
    }

    @Test
    void testQuoteIdentifierSnowflakePreservesCase() {
        Assertions.assertEquals("\"MY_COLUMN\"",
                JdbcIdentifierQuoter.quoteIdentifier(JdbcDbType.SNOWFLAKE, "MY_COLUMN"));
    }

    // === quoteRemoteIdentifier: never uppercases ===

    @Test
    void testQuoteRemoteIdentifierOracleDoesNotUppercase() {
        // quoteRemoteIdentifier should NOT uppercase — the name is already in remote case
        Assertions.assertEquals("\"myColumn\"",
                JdbcIdentifierQuoter.quoteRemoteIdentifier(JdbcDbType.ORACLE, "myColumn"));
    }

    @Test
    void testQuoteRemoteIdentifierDb2DoesNotUppercase() {
        Assertions.assertEquals("\"users\"",
                JdbcIdentifierQuoter.quoteRemoteIdentifier(JdbcDbType.DB2, "users"));
    }

    @Test
    void testQuoteRemoteIdentifierMysqlBacktick() {
        Assertions.assertEquals("`users`",
                JdbcIdentifierQuoter.quoteRemoteIdentifier(JdbcDbType.MYSQL, "users"));
    }

    @Test
    void testQuoteRemoteIdentifierSqlServerBrackets() {
        Assertions.assertEquals("[orders]",
                JdbcIdentifierQuoter.quoteRemoteIdentifier(JdbcDbType.SQLSERVER, "orders"));
    }

    // === quoteFullTableName ===

    @Test
    void testQuoteFullTableNameMysql() {
        String result = JdbcIdentifierQuoter.quoteFullTableName(JdbcDbType.MYSQL, "mydb", "mytable");
        Assertions.assertEquals("`mydb`.`mytable`", result);
    }

    @Test
    void testQuoteFullTableNamePostgresql() {
        String result = JdbcIdentifierQuoter.quoteFullTableName(JdbcDbType.POSTGRESQL, "public", "Users");
        Assertions.assertEquals("\"public\".\"Users\"", result);
    }

    @Test
    void testQuoteFullTableNameSqlServer() {
        String result = JdbcIdentifierQuoter.quoteFullTableName(JdbcDbType.SQLSERVER, "dbo", "Orders");
        Assertions.assertEquals("[dbo].[Orders]", result);
    }

    @Test
    void testQuoteFullTableNameSnowflake() {
        String result = JdbcIdentifierQuoter.quoteFullTableName(JdbcDbType.SNOWFLAKE, "ADS", "DIM_USER");
        Assertions.assertEquals("\"ADS\".\"DIM_USER\"", result);
    }

    // === buildInsertSql ===

    @Test
    void testBuildInsertSqlWithColumnMapping() {
        Map<String, String> remoteNames = new HashMap<>();
        remoteNames.put("local_id", "REMOTE_ID");
        remoteNames.put("local_name", "REMOTE_NAME");

        String sql = JdbcIdentifierQuoter.buildInsertSql(
                JdbcDbType.MYSQL, "testdb", "users", remoteNames,
                Arrays.asList("local_id", "local_name"));
        Assertions.assertEquals(
                "INSERT INTO `testdb`.`users`(`REMOTE_ID`,`REMOTE_NAME`) VALUES (?, ?)", sql);
    }

    @Test
    void testBuildInsertSqlWithNullColumnMapping() {
        String sql = JdbcIdentifierQuoter.buildInsertSql(
                JdbcDbType.POSTGRESQL, "public", "orders", null,
                Arrays.asList("id", "amount"));
        Assertions.assertEquals(
                "INSERT INTO \"public\".\"orders\"(\"id\",\"amount\") VALUES (?, ?)", sql);
    }

    @Test
    void testBuildInsertSqlPartialMapping() {
        Map<String, String> remoteNames = new HashMap<>();
        remoteNames.put("local_id", "ID");
        // "name" has no mapping — should use local name as-is

        String sql = JdbcIdentifierQuoter.buildInsertSql(
                JdbcDbType.MYSQL, "db", "t", remoteNames,
                Arrays.asList("local_id", "name"));
        Assertions.assertEquals(
                "INSERT INTO `db`.`t`(`ID`,`name`) VALUES (?, ?)", sql);
    }

    @Test
    void testBuildInsertSqlSingleColumn() {
        String sql = JdbcIdentifierQuoter.buildInsertSql(
                JdbcDbType.SQLSERVER, "dbo", "items", null,
                Arrays.asList("col1"));
        Assertions.assertEquals(
                "INSERT INTO [dbo].[items]([col1]) VALUES (?)", sql);
    }

    @Test
    void testBuildInsertSqlSnowflake() {
        Map<String, String> remoteNames = new HashMap<>();
        remoteNames.put("id", "ID");
        remoteNames.put("name", "NAME");

        String sql = JdbcIdentifierQuoter.buildInsertSql(
                JdbcDbType.SNOWFLAKE, "ADS", "DIM_USER", remoteNames,
                Arrays.asList("id", "name"));
        Assertions.assertEquals(
                "INSERT INTO \"ADS\".\"DIM_USER\"(\"ID\",\"NAME\") VALUES (?, ?)", sql);
    }
}
