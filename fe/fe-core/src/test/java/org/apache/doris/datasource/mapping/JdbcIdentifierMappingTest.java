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

package org.apache.doris.datasource.mapping;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JdbcIdentifierMappingTest {
    private String validJson;
    private String invalidJson;
    private String duplicateMappingJson;
    private String columnConflictJson;
    private String tableConflictJson;

    @Before
    public void setUp() {
        validJson = "{\n"
                + "  \"databases\": [\n"
                + "    {\"remoteDatabase\": \"DORIS\", \"mapping\": \"doris_local\"}\n"
                + "  ],\n"
                + "  \"tables\": [\n"
                + "    {\"remoteDatabase\": \"DORIS\", \"remoteTable\": \"TABLE_A\", \"mapping\": \"table_a_local\"}\n"
                + "  ],\n"
                + "  \"columns\": [\n"
                + "    {\"remoteDatabase\": \"DORIS\", \"remoteTable\": \"TABLE_A\", \"remoteColumn\": \"COLUMN_X\", "
                + "\"mapping\": \"column_x_local\"}\n"
                + "  ]\n"
                + "}";

        invalidJson = "{\n"
                + "  \"databases\": [\n"
                + "    {\"remoteDatabase\": \"DORIS\", \"mapping\": \"doris_local\"}\n"
                + "  ],\n"
                + "  \"tables\": [\n"
                + "    {\"remoteDatabase\": \"DORIS\", \"remoteTable\": \"TABLE_A\", \"mapping\": \"table_a_local\"}\n"
                + "  ], // Invalid JSON due to trailing comma\n"
                + "}";

        duplicateMappingJson = "{\n"
                + "  \"databases\": [\n"
                + "    {\"remoteDatabase\": \"DORIS\", \"mapping\": \"conflict\"},\n"
                + "    {\"remoteDatabase\": \"DORIS_DUP\", \"mapping\": \"CONFLICT\"}\n"
                + "  ]\n"
                + "}";

        columnConflictJson = "{\n"
                + "  \"columns\": [\n"
                + "    {\"remoteDatabase\": \"D1\", \"remoteTable\": \"T1\", \"remoteColumn\": \"C1\", \"mapping\": \"custom_col\"},\n"
                + "    {\"remoteDatabase\": \"D1\", \"remoteTable\": \"T1\", \"remoteColumn\": \"C2\", \"mapping\": \"CUSTOM_COL\"}\n"
                + "  ]\n"
                + "}";

        tableConflictJson = "{\n"
                + "  \"tables\": [\n"
                + "    {\"remoteDatabase\": \"D2\", \"remoteTable\": \"T1\", \"mapping\": \"custom_table\"},\n"
                + "    {\"remoteDatabase\": \"D2\", \"remoteTable\": \"T2\", \"mapping\": \"CUSTOM_TABLE\"}\n"
                + "  ]\n"
                + "}";
    }

    @Test
    public void testIsLowerCaseMetaNamesTrue() {
        JdbcIdentifierMapping mapping = new JdbcIdentifierMapping(true, false, validJson);

        String databaseName = mapping.fromRemoteDatabaseName("DORIS");
        String tableName = mapping.fromRemoteTableName("DORIS", "TABLE_A");
        String columnName = mapping.fromRemoteColumnName("DORIS", "TABLE_A", "COLUMN_X");

        Assert.assertEquals("doris_local", databaseName);
        Assert.assertEquals("table_a_local", tableName);
        Assert.assertEquals("column_x_local", columnName);
    }

    @Test
    public void testIsLowerCaseMetaNamesFalse() {
        JdbcIdentifierMapping mapping = new JdbcIdentifierMapping(false, false, validJson);

        String databaseName = mapping.fromRemoteDatabaseName("DORIS");
        String tableName = mapping.fromRemoteTableName("DORIS", "TABLE_A");
        String columnName = mapping.fromRemoteColumnName("DORIS", "TABLE_A", "COLUMN_X");

        Assert.assertEquals("doris_local", databaseName);
        Assert.assertEquals("table_a_local", tableName);
        Assert.assertEquals("column_x_local", columnName);
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidJson() {
        new JdbcIdentifierMapping(true, false, invalidJson);
    }

    @Test
    public void testDuplicateMappingWhenLowerCaseMetaNamesTrue() {
        try {
            new JdbcIdentifierMapping(false, true, duplicateMappingJson);
            Assert.fail("Expected RuntimeException due to duplicate mappings");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("Duplicate mapping found"));
        }
    }

    @Test
    public void testDuplicateMappingWhenLowerCaseMetaNamesFalse() {
        JdbcIdentifierMapping mapping = new JdbcIdentifierMapping(false, false, duplicateMappingJson);

        String databaseName1 = mapping.fromRemoteDatabaseName("DORIS");
        String databaseName2 = mapping.fromRemoteDatabaseName("DORIS_DUP");

        Assert.assertEquals("conflict", databaseName1);
        Assert.assertEquals("CONFLICT", databaseName2);
    }

    @Test
    public void testColumnCaseConflictAlwaysChecked() {
        try {
            new JdbcIdentifierMapping(false, false, columnConflictJson);
            Assert.fail("Expected RuntimeException due to column case-only conflict");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("case-only different mapping"));
        }
    }

    @Test
    public void testTableCaseConflictWhenLowerCaseMetaNamesFalseAndLowerCaseTableNamesFalse() {
        JdbcIdentifierMapping mapping = new JdbcIdentifierMapping(false, false, tableConflictJson);
        String tableName1 = mapping.fromRemoteTableName("D2", "T1");
        String tableName2 = mapping.fromRemoteTableName("D2", "T2");
        Assert.assertEquals("custom_table", tableName1);
        Assert.assertEquals("CUSTOM_TABLE", tableName2);
    }

    @Test
    public void testTableCaseConflictWhenLowerCaseMetaNamesTrue() {
        try {
            new JdbcIdentifierMapping(true, false, tableConflictJson);
            Assert.fail("Expected RuntimeException due to table case-only conflict");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("case-only different mapping"));
        }
    }

    @Test
    public void testTableCaseConflictWhenLowerCaseMetaNamesFalseButLowerCaseTableNamesTrue() {
        try {
            new JdbcIdentifierMapping(false, true, tableConflictJson);
            Assert.fail("Expected RuntimeException due to table case-only conflict");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("case-only different mapping"));
        }
    }

    @Test
    public void testUppercaseMappingForDBWhenLowerCaseMetaNamesTrue() {
        String json = "{\n"
                + "  \"databases\": [\n"
                + "    {\"remoteDatabase\": \"UPPER_DB\", \"mapping\": \"UPPER_LOCAL\"}\n"
                + "  ]\n"
                + "}";
        JdbcIdentifierMapping mapping = new JdbcIdentifierMapping(false, true, json);
        Assert.assertEquals("upper_local", mapping.fromRemoteDatabaseName("UPPER_DB"));
    }

    @Test
    public void testUppercaseMappingForDBWhenLowerCaseMetaNamesFalse() {
        String json = "{\n"
                + "  \"databases\": [\n"
                + "    {\"remoteDatabase\": \"UPPER_DB\", \"mapping\": \"UPPER_LOCAL\"}\n"
                + "  ]\n"
                + "}";
        JdbcIdentifierMapping mapping = new JdbcIdentifierMapping(false, false, json);
        Assert.assertEquals("UPPER_LOCAL", mapping.fromRemoteDatabaseName("UPPER_DB"));
    }

    @Test
    public void testUppercaseMappingForTableWhenLowerCaseTableNamesTrue() {
        String json = "{\n"
                + "  \"tables\": [\n"
                + "    {\"remoteDatabase\": \"DB\", \"remoteTable\": \"UPPER_TABLE\", \"mapping\": \"UPPER_TLOCAL\"}\n"
                + "  ]\n"
                + "}";
        JdbcIdentifierMapping mapping = new JdbcIdentifierMapping(true, false, json);
        Assert.assertEquals("UPPER_TLOCAL", mapping.fromRemoteTableName("DB", "UPPER_TABLE"));
    }

    @Test
    public void testUppercaseMappingForTableWhenLowerCaseTableNamesFalse() {
        String json = "{\n"
                + "  \"tables\": [\n"
                + "    {\"remoteDatabase\": \"DB\", \"remoteTable\": \"UPPER_TABLE\", \"mapping\": \"UPPER_TLOCAL\"}\n"
                + "  ]\n"
                + "}";
        JdbcIdentifierMapping mapping = new JdbcIdentifierMapping(false, false, json);
        Assert.assertEquals("UPPER_TLOCAL", mapping.fromRemoteTableName("DB", "UPPER_TABLE"));
    }

    @Test
    public void testUppercaseMappingForTableWhenLowerCaseMetaNamesTrue() {
        String json = "{\n"
                + "  \"tables\": [\n"
                + "    {\"remoteDatabase\": \"DB\", \"remoteTable\": \"UPPER_TABLE\", \"mapping\": \"UPPER_TLOCAL\"}\n"
                + "  ]\n"
                + "}";
        JdbcIdentifierMapping mapping = new JdbcIdentifierMapping(false, true, json);
        Assert.assertEquals("upper_tlocal", mapping.fromRemoteTableName("DB", "UPPER_TABLE"));
    }

    @Test
    public void testUppercaseMappingForTableWhenLowerCaseMetaNamesFalse() {
        String json = "{\n"
                + "  \"tables\": [\n"
                + "    {\"remoteDatabase\": \"DB\", \"remoteTable\": \"UPPER_TABLE\", \"mapping\": \"UPPER_TLOCAL\"}\n"
                + "  ]\n"
                + "}";
        JdbcIdentifierMapping mapping = new JdbcIdentifierMapping(false, false, json);
        Assert.assertEquals("UPPER_TLOCAL", mapping.fromRemoteTableName("DB", "UPPER_TABLE"));
    }

    @Test
    public void testUppercaseMappingForTableWhenAllCaseFalse() {
        String json = "{\n"
                + "  \"tables\": [\n"
                + "    {\"remoteDatabase\": \"DB\", \"remoteTable\": \"UPPER_TABLE\", \"mapping\": \"UPPER_TLOCAL\"}\n"
                + "  ]\n"
                + "}";
        JdbcIdentifierMapping mapping = new JdbcIdentifierMapping(false, false, json);
        Assert.assertEquals("UPPER_TLOCAL", mapping.fromRemoteTableName("DB", "UPPER_TABLE"));
    }

    @Test
    public void testUppercaseMappingForTableWhenAllCaseTrue() {
        String json = "{\n"
                + "  \"tables\": [\n"
                + "    {\"remoteDatabase\": \"DB\", \"remoteTable\": \"UPPER_TABLE\", \"mapping\": \"UPPER_TLOCAL\"}\n"
                + "  ]\n"
                + "}";
        JdbcIdentifierMapping mapping = new JdbcIdentifierMapping(true, true, json);
        Assert.assertEquals("upper_tlocal", mapping.fromRemoteTableName("DB", "UPPER_TABLE"));
    }

    @Test
    public void testUppercaseMappingForColumnWithoutLowerCaseMetaNames() {
        String json = "{\n"
                + "  \"columns\": [\n"
                + "    {\"remoteDatabase\": \"DB\", \"remoteTable\": \"TAB\", \"remoteColumn\": \"UPPER_COL\", \"mapping\": \"UPPER_CLOCAL\"}\n"
                + "  ]\n"
                + "}";
        JdbcIdentifierMapping mapping = new JdbcIdentifierMapping(false, false, json);
        Assert.assertEquals("UPPER_CLOCAL", mapping.fromRemoteColumnName("DB", "TAB", "UPPER_COL"));
    }

    @Test
    public void testUppercaseMappingForColumnWithLowerCaseMetaNamesTrue() {
        String json = "{\n"
                + "  \"columns\": [\n"
                + "    {\"remoteDatabase\": \"DB\", \"remoteTable\": \"TAB\", \"remoteColumn\": \"UPPER_COL\", \"mapping\": \"UPPER_CLOCAL\"}\n"
                + "  ]\n"
                + "}";
        JdbcIdentifierMapping mapping = new JdbcIdentifierMapping(false, true, json);
        Assert.assertEquals("upper_clocal", mapping.fromRemoteColumnName("DB", "TAB", "UPPER_COL"));
    }
}
