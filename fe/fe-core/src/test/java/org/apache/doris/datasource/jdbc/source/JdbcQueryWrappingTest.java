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
import org.apache.doris.thrift.TOdbcTableType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;

class JdbcQueryWrappingTest {
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
        set(node, "noBackslashEscapes", true);
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
        set(node, "noBackslashEscapes", null);
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

    private static void set(Object target, String name, Object value) throws Exception {
        Field field = JdbcScanNode.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
