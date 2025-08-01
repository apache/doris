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

package org.apache.doris.datasource;

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.ExternalFunctionRules.FunctionPushDownRule;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExternalFunctionPushDownRulesTest {

    @Test
    public void testFunctionPushDownRuleCreateWithMysqlDataSource() {
        // Test MySQL datasource with default rules
        FunctionPushDownRule rule = FunctionPushDownRule.create("mysql", null);

        // MySQL has unsupported functions by default, supported functions is empty
        Assertions.assertFalse(rule.canPushDown("date_trunc"));
        Assertions.assertFalse(rule.canPushDown("money_format"));
        Assertions.assertFalse(rule.canPushDown("negative"));

        // Test case insensitivity
        Assertions.assertFalse(rule.canPushDown("DATE_TRUNC"));
        Assertions.assertFalse(rule.canPushDown("Money_Format"));

        // Functions not in unsupported list should be allowed (since supportedFunctions is empty)
        Assertions.assertTrue(rule.canPushDown("sum"));
        Assertions.assertTrue(rule.canPushDown("count"));
    }

    @Test
    public void testFunctionPushDownRuleCreateWithClickHouseDataSource() {
        // Test ClickHouse datasource with default rules
        FunctionPushDownRule rule = FunctionPushDownRule.create("clickhouse", null);

        // ClickHouse has supported functions by default, so only supported functions return true
        Assertions.assertTrue(rule.canPushDown("from_unixtime"));
        Assertions.assertTrue(rule.canPushDown("unix_timestamp"));

        // Test case insensitivity
        Assertions.assertTrue(rule.canPushDown("FROM_UNIXTIME"));
        Assertions.assertTrue(rule.canPushDown("Unix_Timestamp"));

        // Functions not in supported list should be denied (since supportedFunctions is not empty)
        Assertions.assertFalse(rule.canPushDown("unknown_function"));
        Assertions.assertFalse(rule.canPushDown("custom_func"));
        Assertions.assertFalse(rule.canPushDown("sum"));
        Assertions.assertFalse(rule.canPushDown("count"));
    }

    @Test
    public void testFunctionPushDownRuleCreateWithOracleDataSource() {
        // Test Oracle datasource with default rules
        FunctionPushDownRule rule = FunctionPushDownRule.create("oracle", null);

        // Oracle has supported functions by default, so only supported functions return true
        Assertions.assertTrue(rule.canPushDown("nvl"));
        Assertions.assertTrue(rule.canPushDown("ifnull"));

        // Test case insensitivity
        Assertions.assertTrue(rule.canPushDown("NVL"));
        Assertions.assertTrue(rule.canPushDown("IfNull"));

        // Functions not in supported list should be denied (since supportedFunctions is not empty)
        Assertions.assertFalse(rule.canPushDown("unknown_function"));
        Assertions.assertFalse(rule.canPushDown("custom_func"));
        Assertions.assertFalse(rule.canPushDown("sum"));
        Assertions.assertFalse(rule.canPushDown("count"));
    }

    @Test
    public void testFunctionPushDownRuleCreateWithUnknownDataSource() {
        // Test unknown datasource should have no default rules
        FunctionPushDownRule rule = FunctionPushDownRule.create("unknown", null);

        // With no rules, all functions should be denied
        Assertions.assertFalse(rule.canPushDown("any_function"));
        Assertions.assertFalse(rule.canPushDown("sum"));
        Assertions.assertFalse(rule.canPushDown("count"));
    }

    @Test
    public void testFunctionPushDownRuleCreateWithValidCustomRules() {
        // Test custom rules with supported functions
        String jsonRules = "{\n"
                + "  \"pushdown\": {\n"
                + "    \"supported\": [\"custom_func1\", \"Custom_Func2\"],\n"
                + "    \"unsupported\": [\"blocked_func1\", \"Blocked_Func2\"]\n"
                + "  }\n"
                + "}";

        FunctionPushDownRule rule = FunctionPushDownRule.create("mysql", jsonRules);

        // Since supportedFunctions is not empty, only supported functions return true
        Assertions.assertTrue(rule.canPushDown("custom_func1"));
        Assertions.assertTrue(rule.canPushDown("custom_func2")); // case insensitive
        Assertions.assertTrue(rule.canPushDown("CUSTOM_FUNC1"));

        // Functions not in supported list should be denied (even if not in unsupported list)
        Assertions.assertFalse(rule.canPushDown("other_function"));
        Assertions.assertFalse(rule.canPushDown("sum"));
        Assertions.assertFalse(rule.canPushDown("count"));

        // Functions in unsupported list should still be denied
        Assertions.assertFalse(rule.canPushDown("blocked_func1"));
        Assertions.assertFalse(rule.canPushDown("blocked_func2")); // case insensitive
        Assertions.assertFalse(rule.canPushDown("BLOCKED_FUNC1"));

        // Default MySQL unsupported functions should be denied
        Assertions.assertFalse(rule.canPushDown("date_trunc"));
    }

    @Test
    public void testFunctionPushDownRuleCreateWithOnlySupportedCustomRules() {
        // Test custom rules with only supported functions
        String jsonRules = "{\n"
                + "  \"pushdown\": {\n"
                + "    \"supported\": [\"allowed_func1\", \"allowed_func2\"]\n"
                + "  }\n"
                + "}";

        FunctionPushDownRule rule = FunctionPushDownRule.create("unknown", jsonRules);

        // Since supportedFunctions is not empty, only supported functions return true
        Assertions.assertTrue(rule.canPushDown("allowed_func1"));
        Assertions.assertTrue(rule.canPushDown("allowed_func2"));

        // Other functions should be denied (since supportedFunctions is not empty)
        Assertions.assertFalse(rule.canPushDown("other_function"));
        Assertions.assertFalse(rule.canPushDown("sum"));
        Assertions.assertFalse(rule.canPushDown("count"));
    }

    @Test
    public void testFunctionPushDownRuleCreateWithOnlyUnsupportedCustomRules() {
        // Test custom rules with only unsupported functions
        String jsonRules = "{\n"
                + "  \"pushdown\": {\n"
                + "    \"unsupported\": [\"blocked_func1\", \"blocked_func2\"]\n"
                + "  }\n"
                + "}";

        FunctionPushDownRule rule = FunctionPushDownRule.create("unknown", jsonRules);

        // Custom unsupported functions should be denied
        Assertions.assertFalse(rule.canPushDown("blocked_func1"));
        Assertions.assertFalse(rule.canPushDown("blocked_func2"));

        // Other functions should be allowed (default behavior)
        Assertions.assertTrue(rule.canPushDown("other_function"));
    }

    @Test
    public void testFunctionPushDownRuleCreateWithEmptyCustomRules() {
        // Test empty custom rules
        String jsonRules = "{\n"
                + "  \"pushdown\": {\n"
                + "    \"supported\": [],\n"
                + "    \"unsupported\": []\n"
                + "  }\n"
                + "}";

        FunctionPushDownRule rule = FunctionPushDownRule.create("unknown", jsonRules);

        // With empty rules, all functions should be denied
        Assertions.assertFalse(rule.canPushDown("any_function"));
        Assertions.assertFalse(rule.canPushDown("sum"));
    }

    @Test
    public void testFunctionPushDownRuleCreateWithNullCustomRules() {
        // Test null pushdown section
        String jsonRules = "{\n"
                + "  \"pushdown\": null\n"
                + "}";

        FunctionPushDownRule rule = FunctionPushDownRule.create("unknown", jsonRules);

        // With null rules, all functions should be denied
        Assertions.assertFalse(rule.canPushDown("any_function"));
    }

    @Test
    public void testFunctionPushDownRuleCreateWithInvalidJson() {
        // Test invalid JSON should not throw exception but return default rule
        String invalidJson = "{ invalid json }";

        FunctionPushDownRule rule = FunctionPushDownRule.create("unknown", invalidJson);

        // Should return a rule with no functions configured
        Assertions.assertFalse(rule.canPushDown("any_function"));
    }

    @Test
    public void testFunctionPushDownRuleCreateWithEmptyJsonRules() {
        // Test empty string rules
        FunctionPushDownRule rule = FunctionPushDownRule.create("mysql", "");

        // Should only have default MySQL rules
        Assertions.assertFalse(rule.canPushDown("date_trunc"));
        Assertions.assertTrue(rule.canPushDown("other_function"));
    }

    @Test
    public void testFunctionPushDownRuleCreateCaseInsensitiveDataSource() {
        // Test case insensitivity for datasource names
        FunctionPushDownRule mysqlRule = FunctionPushDownRule.create("MYSQL", null);
        FunctionPushDownRule clickhouseRule = FunctionPushDownRule.create("ClickHouse", null);
        FunctionPushDownRule oracleRule = FunctionPushDownRule.create("Oracle", null);

        // Should apply correct default rules regardless of case
        Assertions.assertFalse(mysqlRule.canPushDown("date_trunc"));
        Assertions.assertTrue(clickhouseRule.canPushDown("from_unixtime"));
        Assertions.assertTrue(oracleRule.canPushDown("nvl"));
    }

    @Test
    public void testFunctionPushDownRuleCanPushDownLogic() {
        // Test the canPushDown logic with different scenarios

        // 1. Both supported and unsupported are empty -> return false
        FunctionPushDownRule emptyRule = FunctionPushDownRule.create("unknown", null);
        Assertions.assertFalse(emptyRule.canPushDown("any_function"));

        // 2. Function in supported list -> return true (only when supportedFunctions is not empty)
        String supportedJson = "{\n"
                + "  \"pushdown\": {\n"
                + "    \"supported\": [\"func1\"]\n"
                + "  }\n"
                + "}";
        FunctionPushDownRule supportedRule = FunctionPushDownRule.create("unknown", supportedJson);
        Assertions.assertTrue(supportedRule.canPushDown("func1"));

        // 3. Function not in supported list when supportedFunctions is not empty -> return false
        Assertions.assertFalse(supportedRule.canPushDown("other_func"));

        // 4. Function in unsupported list -> return false
        String unsupportedJson = "{\n"
                + "  \"pushdown\": {\n"
                + "    \"unsupported\": [\"func1\"]\n"
                + "  }\n"
                + "}";
        FunctionPushDownRule unsupportedRule = FunctionPushDownRule.create("unknown", unsupportedJson);
        Assertions.assertFalse(unsupportedRule.canPushDown("func1"));

        // 5. Function not in unsupported list when supportedFunctions is empty -> return true
        Assertions.assertTrue(unsupportedRule.canPushDown("other_func"));

        // 6. Priority test: when supportedFunctions is not empty, only supported functions return true
        String bothJson = "{\n"
                + "  \"pushdown\": {\n"
                + "    \"supported\": [\"func1\"],\n"
                + "    \"unsupported\": [\"func2\"]\n"
                + "  }\n"
                + "}";
        FunctionPushDownRule bothRule = FunctionPushDownRule.create("unknown", bothJson);
        Assertions.assertTrue(bothRule.canPushDown("func1")); // in supported list
        Assertions.assertFalse(bothRule.canPushDown("func2")); // not in supported list (even though in unsupported)
        Assertions.assertFalse(bothRule.canPushDown("func3")); // not in supported list
    }

    @Test
    public void testFunctionPushDownRuleCheck() throws DdlException {
        // Test valid JSON rules
        String validJson = "{\n"
                + "  \"pushdown\": {\n"
                + "    \"supported\": [\"func1\", \"func2\"],\n"
                + "    \"unsupported\": [\"func3\", \"func4\"]\n"
                + "  }\n"
                + "}";

        // Should not throw exception
        Assertions.assertDoesNotThrow(() -> {
            FunctionPushDownRule.check(validJson);
        });
    }

    @Test
    public void testFunctionPushDownRuleCheckWithInvalidJson() {
        // Test invalid JSON rules
        String invalidJson = "{ invalid json }";

        // Should throw DdlException
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            FunctionPushDownRule.check(invalidJson);
        });

        Assertions.assertTrue(exception.getMessage().contains("Failed to parse push down rules"));
    }

    @Test
    public void testFunctionPushDownRuleCheckWithEmptyJson() throws DdlException {
        // Test empty JSON
        String emptyJson = "{}";

        // Should not throw exception
        Assertions.assertDoesNotThrow(() -> {
            FunctionPushDownRule.check(emptyJson);
        });
    }

    @Test
    public void testFunctionPushDownRuleCheckWithNullJson() throws DdlException {
        // Test null JSON
        String nullJson = null;

        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            FunctionPushDownRule.check(nullJson);
        });
        Assertions.assertTrue(exception.getMessage().contains("Failed to parse push down rules"));
    }

    @Test
    public void testFunctionPushDownRuleCheckWithMalformedJson() {
        // Test various malformed JSON strings
        String[] malformedJsons = {
                "{ \"pushdown\": }",                    // Missing value
                "{ \"pushdown\": { \"supported\": } }", // Missing array
                "{ \"pushdown\" { \"supported\": [] } }", // Missing colon
                "{ \"pushdown\": { \"supported\": [\"func1\",] } }", // Trailing comma
                "{ \"pushdown\": { \"supported\": [\"func1\" \"func2\"] } }" // Missing comma
        };

        for (String malformedJson : malformedJsons) {
            DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
                FunctionPushDownRule.check(malformedJson);
            });

            Assertions.assertTrue(exception.getMessage().contains("Failed to parse push down rules"));
        }
    }

    @Test
    public void testFunctionPushDownRuleWithComplexCustomRules() {
        // Test complex custom rules that override and extend default rules
        String complexJson = "{\n"
                + "  \"pushdown\": {\n"
                + "    \"supported\": [\"date_trunc\", \"custom_func\"],\n"
                + "    \"unsupported\": [\"from_unixtime\", \"another_func\"]\n"
                + "  }\n"
                + "}";

        // Test with MySQL (has default unsupported functions)
        FunctionPushDownRule mysqlRule = FunctionPushDownRule.create("mysql", complexJson);

        // Since supportedFunctions is not empty, only supported functions return true
        Assertions.assertTrue(mysqlRule.canPushDown("date_trunc")); // in supported list
        Assertions.assertTrue(mysqlRule.canPushDown("custom_func")); // in supported list

        // Functions not in supported list should be denied
        Assertions.assertFalse(mysqlRule.canPushDown("from_unixtime")); // not in supported list
        Assertions.assertFalse(mysqlRule.canPushDown("another_func")); // not in supported list
        Assertions.assertFalse(mysqlRule.canPushDown("money_format")); // not in supported list
        Assertions.assertFalse(mysqlRule.canPushDown("sum")); // not in supported list
        Assertions.assertFalse(mysqlRule.canPushDown("count")); // not in supported list
    }

    @Test
    public void testFunctionPushDownRuleNewLogicCases() {
        // Additional test cases for the new logic

        // Test case 1: Only unsupported functions defined (supportedFunctions is empty)
        String onlyUnsupportedJson = "{\n"
                + "  \"pushdown\": {\n"
                + "    \"unsupported\": [\"blocked_func\"]\n"
                + "  }\n"
                + "}";
        FunctionPushDownRule onlyUnsupportedRule = FunctionPushDownRule.create("unknown", onlyUnsupportedJson);

        // Functions in unsupported list should be denied
        Assertions.assertFalse(onlyUnsupportedRule.canPushDown("blocked_func"));

        // Other functions should be allowed (since supportedFunctions is empty)
        Assertions.assertTrue(onlyUnsupportedRule.canPushDown("allowed_func"));
        Assertions.assertTrue(onlyUnsupportedRule.canPushDown("sum"));

        // Test case 2: Both supported and unsupported functions defined
        String bothListsJson = "{\n"
                + "  \"pushdown\": {\n"
                + "    \"supported\": [\"func1\", \"func2\"],\n"
                + "    \"unsupported\": [\"func3\", \"func4\"]\n"
                + "  }\n"
                + "}";
        FunctionPushDownRule bothListsRule = FunctionPushDownRule.create("unknown", bothListsJson);

        // Only supported functions return true
        Assertions.assertTrue(bothListsRule.canPushDown("func1"));
        Assertions.assertTrue(bothListsRule.canPushDown("func2"));

        // Functions in unsupported list should be denied (not in supported list)
        Assertions.assertFalse(bothListsRule.canPushDown("func3"));
        Assertions.assertFalse(bothListsRule.canPushDown("func4"));

        // Other functions should be denied (not in supported list)
        Assertions.assertFalse(bothListsRule.canPushDown("func5"));
        Assertions.assertFalse(bothListsRule.canPushDown("other_func"));

        // Test case 3: MySQL with custom supported functions
        String mysqlSupportedJson = "{\n"
                + "  \"pushdown\": {\n"
                + "    \"supported\": [\"date_trunc\", \"money_format\"]\n"
                + "  }\n"
                + "}";
        FunctionPushDownRule mysqlSupportedRule = FunctionPushDownRule.create("mysql", mysqlSupportedJson);

        // Only supported functions return true (overrides default MySQL unsupported functions)
        Assertions.assertTrue(mysqlSupportedRule.canPushDown("date_trunc"));
        Assertions.assertTrue(mysqlSupportedRule.canPushDown("money_format"));

        // Other functions should be denied
        Assertions.assertFalse(mysqlSupportedRule.canPushDown("negative"));
        Assertions.assertFalse(mysqlSupportedRule.canPushDown("sum"));
        Assertions.assertFalse(mysqlSupportedRule.canPushDown("count"));
    }
}
