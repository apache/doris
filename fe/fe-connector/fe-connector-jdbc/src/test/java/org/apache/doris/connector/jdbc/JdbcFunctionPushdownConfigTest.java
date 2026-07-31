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

class JdbcFunctionPushdownConfigTest {

    // === MySQL blacklist model ===

    @Test
    void testMysqlBlacklistAllowsGenericFunction() {
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.MYSQL, null, true);
        Assertions.assertTrue(config.canPushDown("abs"));
        Assertions.assertTrue(config.canPushDown("upper"));
        Assertions.assertTrue(config.canPushDown("concat"));
    }

    @Test
    void testMysqlBlacklistBlocksUnsupported() {
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.MYSQL, null, true);
        Assertions.assertFalse(config.canPushDown("date_trunc"));
        Assertions.assertFalse(config.canPushDown("money_format"));
        Assertions.assertFalse(config.canPushDown("negative"));
    }

    @Test
    void testMysqlBlacklistCaseInsensitive() {
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.MYSQL, null, true);
        Assertions.assertFalse(config.canPushDown("DATE_TRUNC"));
        Assertions.assertFalse(config.canPushDown("Date_Trunc"));
    }

    // === ClickHouse whitelist model ===

    @Test
    void testClickhouseWhitelistAllowsListed() {
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.CLICKHOUSE, null, true);
        Assertions.assertTrue(config.canPushDown("from_unixtime"));
        Assertions.assertTrue(config.canPushDown("unix_timestamp"));
    }

    @Test
    void testClickhouseWhitelistBlocksUnlisted() {
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.CLICKHOUSE, null, true);
        Assertions.assertFalse(config.canPushDown("abs"));
        Assertions.assertFalse(config.canPushDown("concat"));
    }

    // === Oracle whitelist model ===

    @Test
    void testOracleWhitelistAllowsNvlAndIfnull() {
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.ORACLE, null, true);
        Assertions.assertTrue(config.canPushDown("nvl"));
        Assertions.assertTrue(config.canPushDown("ifnull"));
    }

    @Test
    void testOracleWhitelistBlocksUnlisted() {
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.ORACLE, null, true);
        Assertions.assertFalse(config.canPushDown("abs"));
    }

    @Test
    void testOceanBaseOracleUsesOracleRules() {
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.OCEANBASE_ORACLE, null, true);
        Assertions.assertTrue(config.canPushDown("nvl"));
        Assertions.assertFalse(config.canPushDown("abs"));
    }

    // === DB type with no rules ===

    @Test
    void testDbTypeWithNoRulesBlocksAll() {
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.POSTGRESQL, null, true);
        Assertions.assertFalse(config.canPushDown("abs"));
        Assertions.assertFalse(config.canPushDown("anything"));
    }

    // === pushdown disabled globally ===

    @Test
    void testDisabledPushdownBlocksAll() {
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.MYSQL, null, false);
        // Even normally pushable functions should be blocked
        Assertions.assertFalse(config.canPushDown("abs"));
    }

    // === function replacement ===

    @Test
    void testMysqlReplacementNvlToIfnull() {
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.MYSQL, null, true);
        Assertions.assertEquals("ifnull", config.rewriteFunctionName("nvl"));
        Assertions.assertEquals("date", config.rewriteFunctionName("to_date"));
    }

    @Test
    void testMysqlReplacementUnknownFunctionReturnsOriginal() {
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.MYSQL, null, true);
        Assertions.assertEquals("unknown_fn", config.rewriteFunctionName("unknown_fn"));
    }

    @Test
    void testClickhouseReplacements() {
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.CLICKHOUSE, null, true);
        Assertions.assertEquals("FROM_UNIXTIME", config.rewriteFunctionName("from_unixtime"));
        Assertions.assertEquals("toUnixTimestamp", config.rewriteFunctionName("unix_timestamp"));
    }

    @Test
    void testOracleReplacementIfnullToNvl() {
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.ORACLE, null, true);
        Assertions.assertEquals("nvl", config.rewriteFunctionName("ifnull"));
    }

    // === time arithmetic parsing ===

    @Test
    void testParseTimeArithmeticYearsAdd() {
        JdbcFunctionPushdownConfig.TimeArithmeticInfo info =
                JdbcFunctionPushdownConfig.parseTimeArithmetic("years_add");
        Assertions.assertNotNull(info);
        Assertions.assertEquals("date_add", info.getBaseFunction());
        Assertions.assertEquals("YEAR", info.getTimeUnit());
    }

    @Test
    void testParseTimeArithmeticMonthsSub() {
        JdbcFunctionPushdownConfig.TimeArithmeticInfo info =
                JdbcFunctionPushdownConfig.parseTimeArithmetic("months_sub");
        Assertions.assertNotNull(info);
        Assertions.assertEquals("date_sub", info.getBaseFunction());
        Assertions.assertEquals("MONTH", info.getTimeUnit());
    }

    @Test
    void testParseTimeArithmeticAllUnits() {
        String[] units = {"years", "months", "weeks", "days", "hours", "minutes", "seconds"};
        String[] expected = {"YEAR", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND"};
        for (int i = 0; i < units.length; i++) {
            JdbcFunctionPushdownConfig.TimeArithmeticInfo addInfo =
                    JdbcFunctionPushdownConfig.parseTimeArithmetic(units[i] + "_add");
            Assertions.assertNotNull(addInfo, "Failed for " + units[i] + "_add");
            Assertions.assertEquals(expected[i], addInfo.getTimeUnit());

            JdbcFunctionPushdownConfig.TimeArithmeticInfo subInfo =
                    JdbcFunctionPushdownConfig.parseTimeArithmetic(units[i] + "_sub");
            Assertions.assertNotNull(subInfo, "Failed for " + units[i] + "_sub");
            Assertions.assertEquals(expected[i], subInfo.getTimeUnit());
        }
    }

    @Test
    void testParseTimeArithmeticNonMatchReturnsNull() {
        Assertions.assertNull(JdbcFunctionPushdownConfig.parseTimeArithmetic("abs"));
        Assertions.assertNull(JdbcFunctionPushdownConfig.parseTimeArithmetic("date_add"));
        Assertions.assertNull(JdbcFunctionPushdownConfig.parseTimeArithmetic("years_multiply"));
    }

    // === JSON rule overrides ===

    @Test
    void testJsonRulesAddSupportedFunctions() {
        String json = "{\"supported\": [\"my_func\", \"custom_fn\"]}";
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.POSTGRESQL, json, true);
        // PostgreSQL has no built-in rules, but JSON adds a whitelist
        Assertions.assertTrue(config.canPushDown("my_func"));
        Assertions.assertTrue(config.canPushDown("custom_fn"));
        Assertions.assertFalse(config.canPushDown("other_fn"));
    }

    @Test
    void testJsonRulesAddUnsupportedToBlacklist() {
        String json = "{\"unsupported\": [\"concat\"]}";
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.MYSQL, json, true);
        Assertions.assertFalse(config.canPushDown("concat"));
        // Other functions still allowed
        Assertions.assertTrue(config.canPushDown("abs"));
    }

    @Test
    void testJsonRulesRewriteMapping() {
        String json = "{\"rewrite\": {\"my_func\": \"REMOTE_FUNC\"}}";
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.MYSQL, json, true);
        Assertions.assertEquals("REMOTE_FUNC", config.rewriteFunctionName("my_func"));
    }

    @Test
    void testJsonRulesEmptyStringIgnored() {
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.MYSQL, "", true);
        // Should behave same as null — default MySQL rules apply
        Assertions.assertFalse(config.canPushDown("date_trunc"));
        Assertions.assertTrue(config.canPushDown("abs"));
    }

    @Test
    void testJsonRulesMalformedDoesNotThrow() {
        // Malformed JSON should be logged as warning and ignored
        JdbcFunctionPushdownConfig config = JdbcFunctionPushdownConfig.create(
                JdbcDbType.MYSQL, "{invalid json[[[", true);
        // Default MySQL rules should still apply
        Assertions.assertTrue(config.canPushDown("abs"));
    }
}
