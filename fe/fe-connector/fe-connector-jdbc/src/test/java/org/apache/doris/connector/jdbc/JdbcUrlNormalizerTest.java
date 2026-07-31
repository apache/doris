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

/**
 * Tests for {@link JdbcUrlNormalizer}, focusing on the setParamIfAbsent
 * duplicate-append fix (P1-7).
 */
public class JdbcUrlNormalizerTest {

    @Test
    void testSetParamIfAbsentDoesNotDuplicateWithDifferentValue() {
        // User already set characterEncoding=gbk; normalize should not append utf-8
        String url = "jdbc:mysql://host:3306/db?characterEncoding=gbk";
        String result = JdbcUrlNormalizer.normalize(url, JdbcDbType.MYSQL);
        // characterEncoding should appear exactly once (the user's gbk value)
        int count = countOccurrences(result, "characterEncoding=");
        Assertions.assertEquals(1, count,
                "characterEncoding should not be duplicated; got: " + result);
        Assertions.assertTrue(result.contains("characterEncoding=gbk"),
                "User's original value should be preserved");
    }

    @Test
    void testSetParamIfAbsentAddsWhenMissing() {
        String url = "jdbc:mysql://host:3306/db";
        String result = JdbcUrlNormalizer.normalize(url, JdbcDbType.MYSQL);
        Assertions.assertTrue(result.contains("characterEncoding=utf-8"),
                "characterEncoding should be added when missing");
    }

    @Test
    void testSetParamIfAbsentSkipsExactMatch() {
        String url = "jdbc:mysql://host:3306/db?characterEncoding=utf-8";
        String result = JdbcUrlNormalizer.normalize(url, JdbcDbType.MYSQL);
        int count = countOccurrences(result, "characterEncoding=");
        Assertions.assertEquals(1, count,
                "Should not add duplicate when exact match exists; got: " + result);
    }

    @Test
    void testMysqlNormalizationAddsAllExpectedParams() {
        String url = "jdbc:mysql://host:3306/db";
        String result = JdbcUrlNormalizer.normalize(url, JdbcDbType.MYSQL);
        Assertions.assertTrue(result.contains("yearIsDateType=false"), "yearIsDateType");
        Assertions.assertTrue(result.contains("tinyInt1isBit=false"), "tinyInt1isBit");
        Assertions.assertTrue(result.contains("useUnicode=true"), "useUnicode");
        Assertions.assertTrue(result.contains("characterEncoding=utf-8"), "characterEncoding");
        Assertions.assertTrue(result.contains("rewriteBatchedStatements=true"), "rewriteBatchedStatements");
    }

    @Test
    void testSetParamReplacesUnexpectedValue() {
        // User set yearIsDateType=true, normalize should flip to false
        String url = "jdbc:mysql://host:3306/db?yearIsDateType=true";
        String result = JdbcUrlNormalizer.normalize(url, JdbcDbType.MYSQL);
        Assertions.assertTrue(result.contains("yearIsDateType=false"),
                "Should replace unexpected value");
        Assertions.assertFalse(result.contains("yearIsDateType=true"),
                "Unexpected value should be gone");
    }

    @Test
    void testOceanBaseAddsUseCursorFetch() {
        String url = "jdbc:oceanbase://host:2881/db";
        String result = JdbcUrlNormalizer.normalize(url, JdbcDbType.OCEANBASE);
        Assertions.assertTrue(result.contains("useCursorFetch=true"),
                "OceanBase should have useCursorFetch=true");
    }

    @Test
    void testPostgresqlNormalization() {
        String url = "jdbc:postgresql://host:5432/db";
        String result = JdbcUrlNormalizer.normalize(url, JdbcDbType.POSTGRESQL);
        Assertions.assertTrue(result.contains("reWriteBatchedInserts=true"),
                "PostgreSQL should have reWriteBatchedInserts=true");
    }

    @Test
    void testSqlServerUsesSemicolonDelimiter() {
        String url = "jdbc:sqlserver://host:1433;databaseName=db";
        String result = JdbcUrlNormalizer.normalize(url, JdbcDbType.SQLSERVER);
        Assertions.assertTrue(result.contains(";useBulkCopyForBatchInsert=true"),
                "SQL Server should use semicolon delimiter; got: " + result);
    }

    @Test
    void testNullAndEmptyUrl() {
        Assertions.assertNull(JdbcUrlNormalizer.normalize(null, JdbcDbType.MYSQL));
        Assertions.assertEquals("", JdbcUrlNormalizer.normalize("", JdbcDbType.MYSQL));
    }

    @Test
    void testUnknownDbTypeReturnsUrlUnchanged() {
        String url = "jdbc:unknown://host/db";
        String result = JdbcUrlNormalizer.normalize(url, JdbcDbType.CLICKHOUSE);
        Assertions.assertEquals(url, result,
                "Unsupported DB type should leave URL unchanged");
    }

    @Test
    void testSqlServerEncryptOverrideWhenForced() {
        java.util.Map<String, String> env = java.util.Map.of(
                "force_sqlserver_jdbc_encrypt_false", "true");
        String url = "jdbc:sqlserver://host:1433;databaseName=test";
        String result = JdbcUrlNormalizer.normalize(url, JdbcDbType.SQLSERVER, env);
        Assertions.assertTrue(result.contains(";encrypt=false"),
                "encrypt=false should be added when force_sqlserver_jdbc_encrypt_false is true; got: " + result);
    }

    @Test
    void testSqlServerEncryptOverrideReplacesTrue() {
        java.util.Map<String, String> env = java.util.Map.of(
                "force_sqlserver_jdbc_encrypt_false", "true");
        String url = "jdbc:sqlserver://host:1433;encrypt=true;databaseName=test";
        String result = JdbcUrlNormalizer.normalize(url, JdbcDbType.SQLSERVER, env);
        Assertions.assertTrue(result.contains("encrypt=false"),
                "encrypt=true should be replaced with encrypt=false; got: " + result);
        Assertions.assertFalse(result.contains("encrypt=true"),
                "encrypt=true should not remain; got: " + result);
    }

    @Test
    void testSqlServerEncryptNotOverriddenByDefault() {
        String url = "jdbc:sqlserver://host:1433;databaseName=test";
        String result = JdbcUrlNormalizer.normalize(url, JdbcDbType.SQLSERVER);
        Assertions.assertFalse(result.contains("encrypt=false"),
                "encrypt=false should NOT be added without force flag; got: " + result);
    }

    private static int countOccurrences(String str, String sub) {
        int count = 0;
        int idx = 0;
        while ((idx = str.indexOf(sub, idx)) != -1) {
            count++;
            idx += sub.length();
        }
        return count;
    }
}
