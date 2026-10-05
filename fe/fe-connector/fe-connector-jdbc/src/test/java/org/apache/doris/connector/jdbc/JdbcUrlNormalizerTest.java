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

import org.apache.doris.connector.spi.ConnectorContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Tests for {@link JdbcUrlNormalizer}, focusing on the setParamIfAbsent
 * duplicate-append fix (P1-7), plus how this connector's two deployment-level settings are resolved.
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
        String url = "jdbc:sqlserver://host:1433;databaseName=test";
        String result = JdbcUrlNormalizer.normalize(url, JdbcDbType.SQLSERVER, true);
        Assertions.assertTrue(result.contains(";encrypt=false"),
                "encrypt=false should be added when the override is on; got: " + result);
    }

    @Test
    void testSqlServerEncryptOverrideReplacesTrue() {
        String url = "jdbc:sqlserver://host:1433;encrypt=true;databaseName=test";
        String result = JdbcUrlNormalizer.normalize(url, JdbcDbType.SQLSERVER, true);
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

    @Test
    void theEncryptOverrideIsReadFromThePluginConfFirstThenFeConf() {
        // The resolution the connector performs before calling normalize. Asserted here rather than
        // trusted: an administrator who turns the override on in jdbc.conf must not be silently
        // overruled by fe.conf's default, and vice versa an untouched deployment must keep reading
        // fe.conf exactly as before.
        Assertions.assertTrue(JdbcConf.forceSqlServerEncryptFalse(
                context(Map.of(JdbcConf.CONF_FORCE_SQLSERVER_ENCRYPT_FALSE, "true"),
                        Map.of(JdbcConf.ENV_FORCE_SQLSERVER_ENCRYPT_FALSE, "false"))));

        Assertions.assertTrue(JdbcConf.forceSqlServerEncryptFalse(
                context(Map.of(), Map.of(JdbcConf.ENV_FORCE_SQLSERVER_ENCRYPT_FALSE, "true"))));

        Assertions.assertFalse(JdbcConf.forceSqlServerEncryptFalse(context(Map.of(), Map.of())));
    }

    @Test
    void theDriversDirIsReadFromThePluginConfFirstThenFeConf() {
        // Through JdbcConf, which is what the connector calls: asserting on ConnectorConf.get with
        // hand-passed keys would only prove what this test passed it.
        Assertions.assertEquals("/from/plugin/conf", JdbcConf.driversDir(
                context(Map.of(JdbcConf.CONF_DRIVERS_DIR, "/from/plugin/conf"),
                        Map.of(JdbcConf.ENV_DRIVERS_DIR, "/from/fe/conf"))));

        Assertions.assertEquals("/from/fe/conf", JdbcConf.driversDir(
                context(Map.of(), Map.of(JdbcConf.ENV_DRIVERS_DIR, "/from/fe/conf"))));
    }

    @Test
    void theConfTemplateIsNamedAfterTheProvider() {
        // The engine reads <name>.conf, so a template under any other name deploys a file nothing ever
        // opens -- silently, with every setting in it ignored. Renaming getType() must break here.
        String expected = new JdbcConnectorProvider().name() + ".conf.template";
        Assertions.assertNotNull(getClass().getClassLoader().getResource(expected),
                "the plugin must ship " + expected + " on its classpath");
    }

    private static ConnectorContext context(Map<String, String> conf, Map<String, String> env) {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test_catalog";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }

            @Override
            public Map<String, String> getConnectorConfig() {
                return conf;
            }

            @Override
            public Map<String, String> getEnvironment() {
                return env;
            }
        };
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
