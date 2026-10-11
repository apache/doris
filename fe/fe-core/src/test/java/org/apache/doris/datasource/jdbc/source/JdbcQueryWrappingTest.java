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

package org.apache.doris.datasource.jdbc.source;

import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.datasource.jdbc.client.JdbcClientConfig;
import org.apache.doris.datasource.jdbc.client.JdbcMySQLClient;
import org.apache.doris.thrift.TOdbcTableType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;

class JdbcQueryWrappingTest {
    @Test
    void mariaDbMssqlBracketIdentifiersPreserveContents() throws Exception {
        JdbcScanNode node = Mockito.mock(JdbcScanNode.class, Mockito.CALLS_REAL_METHODS);
        set(node, "jdbcType", TOdbcTableType.MYSQL);
        set(node, "projectsTimestamps", true);
        set(node, "columns", Arrays.asList("CAST(ts AS CHAR) AS ts"));
        set(node, "mysqlSqlMode", java.util.Collections.singleton("MSSQL"));
        Method wrap = JdbcScanNode.class.getDeclaredMethod("getTvfQuery");
        wrap.setAccessible(true);
        for (String query : Arrays.asList("SELECT ts AS [x'y] FROM t",
                "SELECT ts AS [x]]'y;--z] FROM t", "SELECT ts AS [x\\] FROM t")) {
            set(node, "query", query + "; -- tail");
            Assertions.assertEquals("SELECT CAST(ts AS CHAR) AS ts FROM (" + query
                    + " -- tail\n) doris_jdbc_source", wrap.invoke(node));
        }
    }

    @Test
    void mysqlTvfCapturesModeBeforeCatalogReset() throws Exception {
        org.apache.doris.catalog.Column column = new org.apache.doris.catalog.Column("ts",
                org.apache.doris.catalog.ScalarType.createTimeStampTzType(6));
        JdbcTable table = new JdbcTable(1, "events", Arrays.asList(column),
                org.apache.doris.catalog.Table.TableType.JDBC);
        table.setJdbcTypeName("mysql");
        table.setExternalTableName("events");
        table.setJdbcUrl("jdbc:mysql://localhost/events");
        table.setDriverUrl("mysql-connector.jar");
        table.setDriverClass("com.mysql.cj.jdbc.Driver");
        table.setResourceName("");
        org.apache.doris.analysis.TupleDescriptor tuple = new org.apache.doris.analysis.TupleDescriptor(
                new org.apache.doris.analysis.TupleId(0));
        tuple.setTable(table);
        org.apache.doris.analysis.SlotDescriptor slot = new org.apache.doris.analysis.SlotDescriptor(
                new org.apache.doris.analysis.SlotId(0), tuple);
        slot.setColumn(column);
        tuple.addSlot(slot);
        try (MockedConstruction<JdbcMySQLClient> clients = Mockito.mockConstruction(JdbcMySQLClient.class,
                (client, construction) -> {
                    JdbcClientConfig config = (JdbcClientConfig) construction.arguments().get(0);
                    Assertions.assertEquals(table.getJdbcUrl(), config.getJdbcUrl());
                    Assertions.assertEquals(table.getDriverUrl(), config.getDriverUrl());
                    Assertions.assertEquals(table.getDriverClass(), config.getDriverClass());
                    if (construction.getCount() == 1) {
                        Mockito.when(client.getSessionSqlMode())
                                .thenReturn(java.util.Collections.singleton("ANSI_QUOTES"));
                    } else {
                        Mockito.when(client.getSessionSqlMode())
                                .thenThrow(new IllegalStateException("Mode lookup failed"));
                    }
                })) {
            JdbcScanNode node = new JdbcScanNode(new org.apache.doris.planner.PlanNodeId(0), tuple, true,
                    "SELECT ts, 1 AS \"x\\\" FROM events;", org.apache.doris.planner.ScanContext.EMPTY);
            Assertions.assertEquals(1, clients.constructed().size());
            JdbcMySQLClient client = clients.constructed().get(0);
            Mockito.verify(client).getSessionSqlMode();
            Mockito.verify(client).closeClient();
            // A catalog reset/drop after setup must not trigger any lookup or connection during serialization.
            Mockito.reset(client);
            try (MockedStatic<org.apache.doris.catalog.Env> env = Mockito.mockStatic(
                    org.apache.doris.catalog.Env.class)) {
                env.when(org.apache.doris.catalog.Env::getCurrentEnv)
                        .thenThrow(new IllegalStateException("Catalog is no longer available"));
                Method project = JdbcScanNode.class.getDeclaredMethod("createJdbcColumns");
                project.setAccessible(true);
                project.invoke(node);
                org.apache.doris.thrift.TPlanNode message = new org.apache.doris.thrift.TPlanNode();
                node.toThrift(message);
                Assertions.assertEquals("SELECT CAST(`ts` AS CHAR) AS `ts` FROM ("
                        + "SELECT ts, 1 AS \"x\\\" FROM events\n) doris_jdbc_source",
                        message.jdbc_scan_node.getQueryString());
                Mockito.verifyNoInteractions(client);
            }
            Assertions.assertThrows(IllegalStateException.class, () -> new JdbcScanNode(
                    new org.apache.doris.planner.PlanNodeId(1), tuple, true, "SELECT ts FROM events",
                    org.apache.doris.planner.ScanContext.EMPTY));
            Assertions.assertEquals(2, clients.constructed().size());
            Mockito.verify(clients.constructed().get(1)).closeClient();
        }
    }

    @Test
    void preservesCommentsAndTrinoStatementPrefix() throws Exception {
        JdbcScanNode node = org.mockito.Mockito.mock(JdbcScanNode.class, org.mockito.Mockito.CALLS_REAL_METHODS);
        set(node, "jdbcType", TOdbcTableType.TRINO);
        set(node, "projectsTimestamps", true);
        set(node, "columns", Arrays.asList("at_timezone(ts, 'UTC') AS ts"));
        Method wrap = JdbcScanNode.class.getDeclaredMethod("getTvfQuery");
        wrap.setAccessible(true);
        set(node, "query", "SELECT ts FROM t; -- trailing");
        String sql = (String) wrap.invoke(node);
        Assertions.assertTrue(sql.contains("FROM (SELECT ts FROM t -- trailing\n)"), sql);
        set(node, "query", "WITH SESSION time_zone='UTC' SELECT ts FROM t;");
        sql = (String) wrap.invoke(node);
        Assertions.assertTrue(sql.startsWith("WITH SESSION time_zone='UTC' SELECT at_timezone"), sql);
        Assertions.assertTrue(sql.contains("FROM (SELECT ts FROM t\n)"), sql);
    }

    @Test
    void trinoBlockCommentsEndAtFirstClosingMarker() throws Exception {
        JdbcScanNode node = org.mockito.Mockito.mock(JdbcScanNode.class, org.mockito.Mockito.CALLS_REAL_METHODS);
        set(node, "jdbcType", TOdbcTableType.TRINO);
        set(node, "projectsTimestamps", true);
        set(node, "columns", Arrays.asList("at_timezone(ts, 'UTC') AS ts"));
        Method wrap = JdbcScanNode.class.getDeclaredMethod("getTvfQuery");
        wrap.setAccessible(true);
        String prefix = "WITH SESSION query_max_execution_time='1m' /* note /* marker */ ";
        set(node, "query", prefix + "SELECT ts FROM t;");
        Assertions.assertEquals(prefix + "SELECT at_timezone(ts, 'UTC') AS ts FROM (SELECT ts FROM t\n)"
                + " doris_jdbc_source", wrap.invoke(node));
        set(node, "query", "SELECT ts /* note /* marker */ FROM t; /* tail */");
        Assertions.assertEquals("SELECT at_timezone(ts, 'UTC') AS ts FROM ("
                + "SELECT ts /* note /* marker */ FROM t /* tail */\n) doris_jdbc_source", wrap.invoke(node));
        set(node, "jdbcType", TOdbcTableType.CLICKHOUSE);
        set(node, "columns", Arrays.asList("toUnixTimestamp64Micro(ts) AS ts"));
        set(node, "query", "SELECT ts /* outer /* inner */ ; still comment */ FROM t;");
        Assertions.assertEquals("SELECT toUnixTimestamp64Micro(ts) AS ts FROM ("
                + "SELECT ts /* outer /* inner */ ; still comment */ FROM t\n) doris_jdbc_source", wrap.invoke(node));
    }

    @Test
    void mysqlAnsiQuotedIdentifierEndingInBackslash() throws Exception {
        JdbcScanNode node = org.mockito.Mockito.mock(JdbcScanNode.class, org.mockito.Mockito.CALLS_REAL_METHODS);
        set(node, "jdbcType", TOdbcTableType.MYSQL);
        set(node, "projectsTimestamps", true);
        set(node, "columns", Arrays.asList("CAST(ts AS CHAR) AS ts"));
        set(node, "mysqlSqlMode", java.util.Collections.singleton("ANSI_QUOTES"));
        Method wrap = JdbcScanNode.class.getDeclaredMethod("getTvfQuery");
        wrap.setAccessible(true);
        String query = "SELECT ts, 1 AS \"x\\\" FROM t";
        set(node, "query", query + ";");
        Assertions.assertEquals("SELECT CAST(ts AS CHAR) AS ts FROM (" + query + "\n) doris_jdbc_source",
                wrap.invoke(node));
        set(node, "mysqlSqlMode", new java.util.HashSet<>(Arrays.asList("ANSI_QUOTES", "NO_BACKSLASH_ESCAPES")));
        Assertions.assertEquals("SELECT CAST(ts AS CHAR) AS ts FROM (" + query + "\n) doris_jdbc_source",
                wrap.invoke(node));
        // Without ANSI_QUOTES, a backslash escapes a double quote inside a string literal.
        set(node, "mysqlSqlMode", java.util.Collections.emptySet());
        query = "SELECT ts, \"x\\\"; y\" AS value FROM t";
        set(node, "query", query + ";");
        Assertions.assertEquals("SELECT CAST(ts AS CHAR) AS ts FROM (" + query + "\n) doris_jdbc_source",
                wrap.invoke(node));
    }

    @Test
    void readsBothMysqlQuotingModesFromOneSession() throws Exception {
        org.apache.doris.datasource.jdbc.client.JdbcClient client = org.mockito.Mockito.mock(
                org.apache.doris.datasource.jdbc.client.JdbcClient.class, org.mockito.Mockito.CALLS_REAL_METHODS);
        java.sql.Connection connection = org.mockito.Mockito.mock(java.sql.Connection.class);
        java.sql.Statement statement = org.mockito.Mockito.mock(java.sql.Statement.class);
        java.sql.ResultSet result = org.mockito.Mockito.mock(java.sql.ResultSet.class);
        org.mockito.Mockito.doReturn(connection).when(client).getConnection();
        org.mockito.Mockito.when(connection.createStatement()).thenReturn(statement);
        org.mockito.Mockito.when(statement.executeQuery("SELECT @@SESSION.sql_mode")).thenReturn(result);
        org.mockito.Mockito.when(result.next()).thenReturn(true);
        org.mockito.Mockito.when(result.getString(1)).thenReturn("ansi_quotes, NO_BACKSLASH_ESCAPES");
        Assertions.assertEquals(new java.util.HashSet<>(Arrays.asList("ANSI_QUOTES", "NO_BACKSLASH_ESCAPES")),
                client.getSessionSqlMode());
        org.mockito.Mockito.verify(connection).close();
        org.mockito.Mockito.verify(statement).close();
        org.mockito.Mockito.verify(result).close();
    }

    @Test
    void legacyTimestampBindsHaveExplicitServerCasts() throws Exception {
        for (TOdbcTableType type : Arrays.asList(TOdbcTableType.ORACLE, TOdbcTableType.PRESTO)) {
            JdbcTable table = new JdbcTable(1, "events", Arrays.asList(new org.apache.doris.catalog.Column(
                    "ts", org.apache.doris.catalog.ScalarType.createTimeStampTzType(6))),
                    org.apache.doris.catalog.Table.TableType.JDBC);
            table.setJdbcTypeName(type.name().toLowerCase(java.util.Locale.ROOT));
            table.setExternalTableName("events");
            String sql = table.getInsertSql(Arrays.asList("ts"));
            Assertions.assertTrue(sql.contains(type == TOdbcTableType.ORACLE ? "TO_TIMESTAMP_TZ(?"
                    : "CAST(? AS TIMESTAMP WITH TIME ZONE)"), sql);
        }
    }

    @Test
    void mysqlWrappingHonorsSqlModeAndArithmeticDashes() throws Exception {
        JdbcScanNode node = org.mockito.Mockito.mock(JdbcScanNode.class, org.mockito.Mockito.CALLS_REAL_METHODS);
        set(node, "jdbcType", TOdbcTableType.MYSQL);
        set(node, "projectsTimestamps", true);
        set(node, "columns", Arrays.asList("CAST(ts AS CHAR) AS ts"));
        set(node, "mysqlSqlMode", java.util.Collections.singleton("NO_BACKSLASH_ESCAPES"));
        Method wrap = JdbcScanNode.class.getDeclaredMethod("getTvfQuery");
        wrap.setAccessible(true);
        for (String query : Arrays.asList("SELECT ts, 1--1 AS n FROM t; -- tail",
                "SELECT ts, '\\' AS text FROM t; # tail")) {
            set(node, "query", query);
            String sql = (String) wrap.invoke(node);
            Assertions.assertFalse(sql.contains("FROM t;"), sql);
            Assertions.assertTrue(sql.endsWith("\n) doris_jdbc_source"), sql);
        }
        set(node, "projectsTimestamps", false);
        set(node, "mysqlSqlMode", null);
        set(node, "query", "SELECT 1;");
        Assertions.assertEquals("SELECT 1;", wrap.invoke(node));
    }

    @Test
    void mysqlProjectsOnlyInstantColumnsAsText() throws Exception {
        org.apache.doris.catalog.Column instant = new org.apache.doris.catalog.Column("ts",
                org.apache.doris.catalog.ScalarType.createTimeStampTzType(6));
        org.apache.doris.catalog.Column local = new org.apache.doris.catalog.Column("dt",
                org.apache.doris.catalog.ScalarType.createDatetimeV2Type(6));
        JdbcTable table = new JdbcTable(1, "events", Arrays.asList(instant, local),
                org.apache.doris.catalog.Table.TableType.JDBC);
        table.setJdbcTypeName("mysql");
        table.setExternalTableName("events");
        org.apache.doris.analysis.TupleDescriptor tuple = new org.apache.doris.analysis.TupleDescriptor(
                new org.apache.doris.analysis.TupleId(0));
        tuple.setTable(table);
        int id = 0;
        for (org.apache.doris.catalog.Column column : Arrays.asList(instant, local)) {
            org.apache.doris.analysis.SlotDescriptor slot = new org.apache.doris.analysis.SlotDescriptor(
                    new org.apache.doris.analysis.SlotId(id++), tuple);
            slot.setColumn(column);
            tuple.addSlot(slot);
        }
        JdbcScanNode node = new JdbcScanNode(new org.apache.doris.planner.PlanNodeId(0), tuple, false,
                org.apache.doris.planner.ScanContext.EMPTY);
        Method project = JdbcScanNode.class.getDeclaredMethod("createJdbcColumns");
        project.setAccessible(true);
        project.invoke(node);
        Field columns = JdbcScanNode.class.getDeclaredField("columns");
        columns.setAccessible(true);
        Assertions.assertEquals(Arrays.asList("CAST(`ts` AS CHAR) AS `ts`", "`dt`"), columns.get(node));
    }

    @Test
    void clickHouseEscapedBackticksPreserveIdentifierContents() throws Exception {
        assertClickHouseWrapped("SELECT ts, 1 AS `a\\`b` FROM t");
        assertClickHouseWrapped("SELECT ts, 1 AS `a\\`;b` FROM t");
    }

    @Test
    void clickHouseLineCommentsDoNotChangeQuoteState() throws Exception {
        for (String marker : Arrays.asList("//", "///", "#!", "# ")) {
            for (String newline : Arrays.asList("\n", "\r\n")) {
                assertClickHouseWrapped("SELECT ts FROM t " + marker + " 'note ` ;" + newline);
            }
        }
    }

    @Test
    void clickHouseHeredocsPreserveRawContents() throws Exception {
        for (String literal : Arrays.asList("$h$foo'bar$h$", "$$'\\\"`; /* --$$",
                "$tag_1$'text; $other$tag_1$", "$h$$h$")) {
            assertClickHouseWrapped("SELECT ts FROM t WHERE length(" + literal + ") > 0");
        }
        // An interior statement delimiter must remain visible to the remote parser.
        assertClickHouseWrapped("SELECT ts, $h$'raw$h$ FROM t; SELECT ts FROM t");
    }

    private void assertClickHouseWrapped(String query) throws Exception {
        JdbcScanNode node = org.mockito.Mockito.mock(JdbcScanNode.class, org.mockito.Mockito.CALLS_REAL_METHODS);
        set(node, "jdbcType", TOdbcTableType.CLICKHOUSE);
        set(node, "projectsTimestamps", true);
        set(node, "columns", Arrays.asList("toUnixTimestamp64Micro(ts) AS ts"));
        set(node, "query", query + ";");
        Method wrap = JdbcScanNode.class.getDeclaredMethod("getTvfQuery");
        wrap.setAccessible(true);
        Assertions.assertEquals("SELECT toUnixTimestamp64Micro(ts) AS ts FROM (" + query
                + "\n) doris_jdbc_source", wrap.invoke(node));
    }

    private static void set(Object target, String name, Object value) throws Exception {
        Field field = JdbcScanNode.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
