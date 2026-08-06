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

import java.util.List;
import java.util.Map;

/**
 * Tests for {@link JdbcIdentifierMapper} JSON parsing, focusing on the
 * regex fix for non-ASCII and special characters (P1-11).
 */
public class JdbcIdentifierMapperTest {

    @Test
    void testParseChineseFieldNames() {
        // Chinese characters: 用户库
        String json = "{\"databases\": [{\"remoteDatabase\": \"用户库\", \"mapping\": \"user_db\"}]}";
        List<Map<String, String>> result = JdbcIdentifierMapper.parseJsonArray(json, "databases");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("用户库", result.get(0).get("remoteDatabase"));
        Assertions.assertEquals("user_db", result.get(0).get("mapping"));
    }

    @Test
    void testParseFieldNamesWithSpaces() {
        String json = "{\"databases\": [{\"remoteDatabase\": \"my database\", \"mapping\": \"my_database\"}]}";
        List<Map<String, String>> result = JdbcIdentifierMapper.parseJsonArray(json, "databases");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("my database", result.get(0).get("remoteDatabase"));
        Assertions.assertEquals("my_database", result.get(0).get("mapping"));
    }

    @Test
    void testParseEscapedQuotesInValues() {
        String json = "{\"databases\": [{\"remoteDatabase\": \"db\\\"quoted\\\"\","
                + " \"mapping\": \"db_quoted\"}]}";
        List<Map<String, String>> result = JdbcIdentifierMapper.parseJsonArray(json, "databases");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("db\"quoted\"", result.get(0).get("remoteDatabase"));
    }

    @Test
    void testParseHyphenatedFieldNames() {
        String json = "{\"databases\": [{\"remoteDatabase\": \"my-database\", \"mapping\": \"my_database\"}]}";
        List<Map<String, String>> result = JdbcIdentifierMapper.parseJsonArray(json, "databases");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("my-database", result.get(0).get("remoteDatabase"));
    }

    @Test
    void testParseDotInFieldValues() {
        String json = "{\"tables\": [{\"remoteDatabase\": \"db\","
                + " \"remoteTable\": \"schema.table\", \"mapping\": \"my_table\"}]}";
        List<Map<String, String>> result = JdbcIdentifierMapper.parseJsonArray(json, "tables");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("schema.table", result.get(0).get("remoteTable"));
    }

    @Test
    void testParseBackslashEscape() {
        String json = "{\"databases\": [{\"remoteDatabase\": \"path\\\\db\","
                + " \"mapping\": \"path_db\"}]}";
        List<Map<String, String>> result = JdbcIdentifierMapper.parseJsonArray(json, "databases");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("path\\db", result.get(0).get("remoteDatabase"));
    }

    @Test
    void testParseStandardAsciiFieldsStillWork() {
        String json = "{\"databases\": [{\"remoteDatabase\": \"mydb\", \"mapping\": \"my_db\"}]}";
        List<Map<String, String>> result = JdbcIdentifierMapper.parseJsonArray(json, "databases");
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("mydb", result.get(0).get("remoteDatabase"));
        Assertions.assertEquals("my_db", result.get(0).get("mapping"));
    }

    @Test
    void testEndToEndChineseColumnMapping() {
        // Chinese characters: 用户名
        String json = "{\"columns\": [{\"remoteDatabase\": \"db1\","
                + " \"remoteTable\": \"tbl1\","
                + " \"remoteColumn\": \"用户名\","
                + " \"mapping\": \"username\"}]}";
        JdbcIdentifierMapper mapper = new JdbcIdentifierMapper(false, false, json);
        String result = mapper.fromRemoteColumnName("db1", "tbl1", "用户名");
        Assertions.assertEquals("username", result);
    }

    @Test
    void testEndToEndHyphenatedDbMapping() {
        String json = "{\"databases\": [{\"remoteDatabase\": \"my-prod-db\","
                + " \"mapping\": \"my_prod_db\"}]}";
        JdbcIdentifierMapper mapper = new JdbcIdentifierMapper(false, false, json);
        String result = mapper.fromRemoteDatabaseName("my-prod-db");
        Assertions.assertEquals("my_prod_db", result);
    }

    @Test
    void testEmptyMappingReturnsOriginalName() {
        JdbcIdentifierMapper mapper = new JdbcIdentifierMapper(false, false, null);
        Assertions.assertEquals("original", mapper.fromRemoteDatabaseName("original"));
    }

    @Test
    void testSameTableMappingAcrossDatabasesIsValid() {
        // db1.t1 -> foo and db2.t2 -> foo should be valid because table
        // mappings are resolved per-database at runtime
        String json = "{\"tables\": ["
                + "{\"remoteDatabase\": \"db1\", \"remoteTable\": \"t1\", \"mapping\": \"foo\"},"
                + "{\"remoteDatabase\": \"db2\", \"remoteTable\": \"t2\", \"mapping\": \"foo\"}"
                + "]}";
        Assertions.assertDoesNotThrow(() -> new JdbcIdentifierMapper(false, false, json));
    }

    @Test
    void testSameColumnMappingAcrossTablesIsValid() {
        // db1.tbl1.col_a -> x and db1.tbl2.col_b -> x should be valid because
        // column mappings are resolved per-(database, table) at runtime
        String json = "{\"columns\": ["
                + "{\"remoteDatabase\": \"db1\", \"remoteTable\": \"tbl1\","
                + " \"remoteColumn\": \"col_a\", \"mapping\": \"x\"},"
                + "{\"remoteDatabase\": \"db1\", \"remoteTable\": \"tbl2\","
                + " \"remoteColumn\": \"col_b\", \"mapping\": \"x\"}"
                + "]}";
        Assertions.assertDoesNotThrow(() -> new JdbcIdentifierMapper(false, false, json));
    }

    @Test
    void testDuplicateTableMappingWithinSameDatabaseIsRejected() {
        // db1.t1 -> foo and db1.t2 -> foo should be rejected (same database)
        String json = "{\"tables\": ["
                + "{\"remoteDatabase\": \"db1\", \"remoteTable\": \"t1\", \"mapping\": \"foo\"},"
                + "{\"remoteDatabase\": \"db1\", \"remoteTable\": \"t2\", \"mapping\": \"foo\"}"
                + "]}";
        Assertions.assertThrows(Exception.class,
                () -> new JdbcIdentifierMapper(false, false, json));
    }

    @Test
    void testDuplicateColumnMappingWithinSameTableIsRejected() {
        // Same table, different columns mapping to same name
        String json = "{\"columns\": ["
                + "{\"remoteDatabase\": \"db1\", \"remoteTable\": \"tbl1\","
                + " \"remoteColumn\": \"col_a\", \"mapping\": \"x\"},"
                + "{\"remoteDatabase\": \"db1\", \"remoteTable\": \"tbl1\","
                + " \"remoteColumn\": \"col_b\", \"mapping\": \"x\"}"
                + "]}";
        Assertions.assertThrows(Exception.class,
                () -> new JdbcIdentifierMapper(false, false, json));
    }
}
