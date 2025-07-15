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
import org.apache.doris.datasource.ExternalFunctionRules.FunctionRewriteRules;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ExternalFunctionRewriteRulesTest {

    @Test
    public void testFunctionRewriteRuleCreateWithMysqlDataSource() {
        // Test MySQL datasource with default rewrite rules
        FunctionRewriteRules rule = FunctionRewriteRules.create("mysql", null);

        // MySQL has default rewrite rules
        Assertions.assertEquals("ifnull", rule.rewriteFunction("nvl"));
        Assertions.assertEquals("date", rule.rewriteFunction("to_date"));

        // Test case sensitivity - original function names should be used as-is
        Assertions.assertEquals("NVL", rule.rewriteFunction("NVL"));
        Assertions.assertEquals("To_Date", rule.rewriteFunction("To_Date"));

        // Functions not in rewrite map should return original name
        Assertions.assertEquals("sum", rule.rewriteFunction("sum"));
        Assertions.assertEquals("count", rule.rewriteFunction("count"));
        Assertions.assertEquals("unknown_func", rule.rewriteFunction("unknown_func"));
    }

    @Test
    public void testFunctionRewriteRuleCreateWithClickHouseDataSource() {
        // Test ClickHouse datasource with default rewrite rules
        FunctionRewriteRules rule = FunctionRewriteRules.create("clickhouse", null);

        // ClickHouse has default rewrite rules
        Assertions.assertEquals("FROM_UNIXTIME", rule.rewriteFunction("from_unixtime"));
        Assertions.assertEquals("toUnixTimestamp", rule.rewriteFunction("unix_timestamp"));

        // Test case sensitivity
        Assertions.assertEquals("FROM_UNIXTIME", rule.rewriteFunction("FROM_UNIXTIME"));
        Assertions.assertEquals("Unix_Timestamp", rule.rewriteFunction("Unix_Timestamp"));

        // Functions not in rewrite map should return original name
        Assertions.assertEquals("sum", rule.rewriteFunction("sum"));
        Assertions.assertEquals("count", rule.rewriteFunction("count"));
        Assertions.assertEquals("unknown_func", rule.rewriteFunction("unknown_func"));
    }

    @Test
    public void testFunctionRewriteRuleCreateWithOracleDataSource() {
        // Test Oracle datasource with default rewrite rules
        FunctionRewriteRules rule = FunctionRewriteRules.create("oracle", null);

        // Oracle has default rewrite rules
        Assertions.assertEquals("nvl", rule.rewriteFunction("ifnull"));

        // Test case sensitivity
        Assertions.assertEquals("IFNULL", rule.rewriteFunction("IFNULL"));
        Assertions.assertEquals("IfNull", rule.rewriteFunction("IfNull"));

        // Functions not in rewrite map should return original name
        Assertions.assertEquals("sum", rule.rewriteFunction("sum"));
        Assertions.assertEquals("count", rule.rewriteFunction("count"));
        Assertions.assertEquals("unknown_func", rule.rewriteFunction("unknown_func"));
    }

    @Test
    public void testFunctionRewriteRuleCreateWithUnknownDataSource() {
        // Test unknown datasource should have no default rewrite rules
        FunctionRewriteRules rule = FunctionRewriteRules.create("unknown", null);

        // All functions should return original name (no rewrite rules)
        Assertions.assertEquals("any_function", rule.rewriteFunction("any_function"));
        Assertions.assertEquals("sum", rule.rewriteFunction("sum"));
        Assertions.assertEquals("count", rule.rewriteFunction("count"));
        Assertions.assertEquals("nvl", rule.rewriteFunction("nvl"));
        Assertions.assertEquals("ifnull", rule.rewriteFunction("ifnull"));
    }

    @Test
    public void testFunctionRewriteRuleCreateWithValidCustomRules() {
        // Test custom rewrite rules
        String jsonRules = "{\n"
                + "  \"rewrite\": {\n"
                + "    \"old_func1\": \"new_func1\",\n"
                + "    \"Old_Func2\": \"New_Func2\",\n"
                + "    \"CUSTOM_FUNC\": \"custom_replacement\"\n"
                + "  }\n"
                + "}";

        FunctionRewriteRules rule = FunctionRewriteRules.create("mysql", jsonRules);

        // Custom rewrite rules should work
        Assertions.assertEquals("new_func1", rule.rewriteFunction("old_func1"));
        Assertions.assertEquals("New_Func2", rule.rewriteFunction("Old_Func2"));
        Assertions.assertEquals("custom_replacement", rule.rewriteFunction("CUSTOM_FUNC"));

        // Default MySQL rewrite rules should still work
        Assertions.assertEquals("ifnull", rule.rewriteFunction("nvl"));
        Assertions.assertEquals("date", rule.rewriteFunction("to_date"));

        // Functions not in any rewrite map should return original name
        Assertions.assertEquals("other_function", rule.rewriteFunction("other_function"));
        Assertions.assertEquals("sum", rule.rewriteFunction("sum"));
    }

    @Test
    public void testFunctionRewriteRuleCreateWithEmptyCustomRules() {
        // Test empty custom rewrite rules
        String jsonRules = "{\n"
                + "  \"rewrite\": {}\n"
                + "}";

        FunctionRewriteRules rule = FunctionRewriteRules.create("mysql", jsonRules);

        // Default MySQL rewrite rules should still work
        Assertions.assertEquals("ifnull", rule.rewriteFunction("nvl"));
        Assertions.assertEquals("date", rule.rewriteFunction("to_date"));

        // Other functions should return original name
        Assertions.assertEquals("other_function", rule.rewriteFunction("other_function"));
    }

    @Test
    public void testFunctionRewriteRuleCreateWithNullCustomRules() {
        // Test null rewrite section
        String jsonRules = "{\n"
                + "  \"rewrite\": null\n"
                + "}";

        FunctionRewriteRules rule = FunctionRewriteRules.create("mysql", jsonRules);

        // Default MySQL rewrite rules should still work
        Assertions.assertEquals("ifnull", rule.rewriteFunction("nvl"));
        Assertions.assertEquals("date", rule.rewriteFunction("to_date"));

        // Other functions should return original name
        Assertions.assertEquals("other_function", rule.rewriteFunction("other_function"));
    }

    @Test
    public void testFunctionRewriteRuleCreateWithNullRewriteMap() {
        // Test null rewrite map - this test is no longer relevant with the new format
        // Since rewrite is now directly the map, we just test with null rewrite
        String jsonRules = "{\n"
                + "  \"rewrite\": null\n"
                + "}";

        FunctionRewriteRules rule = FunctionRewriteRules.create("mysql", jsonRules);

        // Default MySQL rewrite rules should still work
        Assertions.assertEquals("ifnull", rule.rewriteFunction("nvl"));
        Assertions.assertEquals("date", rule.rewriteFunction("to_date"));

        // Other functions should return original name
        Assertions.assertEquals("other_function", rule.rewriteFunction("other_function"));
    }

    @Test
    public void testFunctionRewriteRuleCreateWithInvalidJson() {
        // Test invalid JSON should not throw exception but return default rule
        String invalidJson = "{ invalid json }";

        FunctionRewriteRules rule = FunctionRewriteRules.create("mysql", invalidJson);

        // Should still have default MySQL rewrite rules
        Assertions.assertEquals("ifnull", rule.rewriteFunction("nvl"));
        Assertions.assertEquals("date", rule.rewriteFunction("to_date"));

        // Other functions should return original name
        Assertions.assertEquals("other_function", rule.rewriteFunction("other_function"));
    }

    @Test
    public void testFunctionRewriteRuleCreateWithEmptyJsonRules() {
        // Test empty string rules
        FunctionRewriteRules rule = FunctionRewriteRules.create("mysql", "");

        // Should only have default MySQL rewrite rules
        Assertions.assertEquals("ifnull", rule.rewriteFunction("nvl"));
        Assertions.assertEquals("date", rule.rewriteFunction("to_date"));
        Assertions.assertEquals("other_function", rule.rewriteFunction("other_function"));
    }

    @Test
    public void testFunctionRewriteRuleCreateCaseInsensitiveDataSource() {
        // Test case insensitivity for datasource names
        FunctionRewriteRules mysqlRule = FunctionRewriteRules.create("MYSQL", null);
        FunctionRewriteRules clickhouseRule = FunctionRewriteRules.create("ClickHouse", null);
        FunctionRewriteRules oracleRule = FunctionRewriteRules.create("Oracle", null);

        // Should apply correct default rules regardless of case
        Assertions.assertEquals("ifnull", mysqlRule.rewriteFunction("nvl"));
        Assertions.assertEquals("FROM_UNIXTIME", clickhouseRule.rewriteFunction("from_unixtime"));
        Assertions.assertEquals("nvl", oracleRule.rewriteFunction("ifnull"));
    }

    @Test
    public void testFunctionRewriteRuleRewriteFunction() {
        // Test the rewriteFunction logic with different scenarios

        // Test with custom rewrite rules
        String jsonRules = "{\n"
                + "  \"rewrite\": {\n"
                + "    \"func1\": \"replacement1\",\n"
                + "    \"func2\": \"replacement2\"\n"
                + "  }\n"
                + "}";

        FunctionRewriteRules rule = FunctionRewriteRules.create("unknown", jsonRules);

        // Functions in rewrite map should be replaced
        Assertions.assertEquals("replacement1", rule.rewriteFunction("func1"));
        Assertions.assertEquals("replacement2", rule.rewriteFunction("func2"));

        // Functions not in rewrite map should return original name
        Assertions.assertEquals("func3", rule.rewriteFunction("func3"));
        Assertions.assertEquals("unknown_func", rule.rewriteFunction("unknown_func"));

        // Test with null function name
        Assertions.assertEquals(null, rule.rewriteFunction(null));

        // Test with empty function name
        Assertions.assertEquals("", rule.rewriteFunction(""));
    }

    @Test
    public void testFunctionRewriteRuleCheck() throws DdlException {
        // Test valid JSON rewrite rules
        String validJson = "{\n"
                + "  \"rewrite\": {\n"
                + "    \"func1\": \"replacement1\",\n"
                + "    \"func2\": \"replacement2\"\n"
                + "  }\n"
                + "}";

        // Should not throw exception
        Assertions.assertDoesNotThrow(() -> {
            FunctionRewriteRules.check(validJson);
        });
    }

    @Test
    public void testFunctionRewriteRuleCheckWithInvalidJson() {
        // Test invalid JSON rules
        String invalidJson = "{ invalid json }";

        // Should throw DdlException
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            FunctionRewriteRules.check(invalidJson);
        });

        Assertions.assertTrue(exception.getMessage().contains("Failed to parse rewrite rules"));
    }

    @Test
    public void testFunctionRewriteRuleCheckWithEmptyJson() throws DdlException {
        // Test empty JSON
        String emptyJson = "{}";

        // Should not throw exception
        Assertions.assertDoesNotThrow(() -> {
            FunctionRewriteRules.check(emptyJson);
        });
    }

    @Test
    public void testFunctionRewriteRuleCheckWithNullJson() {
        // Test null JSON
        String nullJson = null;

        // Should throw DdlException due to new null check
        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            FunctionRewriteRules.check(nullJson);
        });
        Assertions.assertTrue(exception.getMessage().contains("Failed to parse rewrite rules"));
    }

    @Test
    public void testFunctionRewriteRuleCheckWithMalformedJson() {
        // Test various malformed JSON strings
        String[] malformedJsons = {
                "{ \"rewrite\": }",                    // Missing value
                "{ \"rewrite\": { } }", // Missing object - this is actually valid now
                "{ \"rewrite\" { } }", // Missing colon
                "{ \"rewrite\": {\"func1\": \"replacement1\",} }", // Trailing comma
                "{ \"rewrite\": {\"func1\" \"replacement1\"} }" // Missing colon
        };

        for (String malformedJson : malformedJsons) {
            // Skip the second case as it's now valid
            if (malformedJson.equals("{ \"rewrite\": { } }")) {
                continue;
            }

            DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
                FunctionRewriteRules.check(malformedJson);
            });

            Assertions.assertTrue(exception.getMessage().contains("Failed to parse rewrite rules"));
        }
    }

    @Test
    public void testFunctionRewriteRuleCheckWithEmptyFunctionNames() {
        // Test empty function names in rewrite rules should throw exception
        String emptyKeyJson = "{\n"
                + "  \"rewrite\": {\n"
                + "    \"\": \"replacement1\",\n"
                + "    \"func2\": \"replacement2\"\n"
                + "  }\n"
                + "}";

        DdlException exception1 = Assertions.assertThrows(DdlException.class, () -> {
            FunctionRewriteRules.check(emptyKeyJson);
        });
        Assertions.assertTrue(exception1.getMessage().contains("Failed to parse rewrite rules"));

        String emptyValueJson = "{\n"
                + "  \"rewrite\": {\n"
                + "    \"func1\": \"\",\n"
                + "    \"func2\": \"replacement2\"\n"
                + "  }\n"
                + "}";

        DdlException exception2 = Assertions.assertThrows(DdlException.class, () -> {
            FunctionRewriteRules.check(emptyValueJson);
        });
        Assertions.assertTrue(exception2.getMessage().contains("Failed to parse rewrite rules"));
    }

    @Test
    public void testFunctionRewriteRuleCheckWithNullFunctionNames() {
        // Test null function names in rewrite rules should throw exception
        // Note: JSON parsing will not allow null keys, but null values are possible
        String nullValueJson = "{\n"
                + "  \"rewrite\": {\n"
                + "    \"func1\": null,\n"
                + "    \"func2\": \"replacement2\"\n"
                + "  }\n"
                + "}";

        DdlException exception = Assertions.assertThrows(DdlException.class, () -> {
            FunctionRewriteRules.check(nullValueJson);
        });
        Assertions.assertTrue(exception.getMessage().contains("Failed to parse rewrite rules"));
    }

    @Test
    public void testFunctionRewriteRuleWithComplexCustomRules() {
        // Test complex custom rewrite rules that override and extend default rules
        String complexJson = "{\n"
                + "  \"rewrite\": {\n"
                + "    \"nvl\": \"custom_nvl\",\n"
                + "    \"custom_func1\": \"transformed_func1\",\n"
                + "    \"old_function\": \"new_function\"\n"
                + "  }\n"
                + "}";

        // Test with MySQL (has default rewrite rules)
        FunctionRewriteRules mysqlRule = FunctionRewriteRules.create("mysql", complexJson);

        // Custom rewrite rules should override default rules
        Assertions.assertEquals("custom_nvl", mysqlRule.rewriteFunction("nvl")); // overridden
        Assertions.assertEquals("transformed_func1", mysqlRule.rewriteFunction("custom_func1")); // custom
        Assertions.assertEquals("new_function", mysqlRule.rewriteFunction("old_function")); // custom

        // Default MySQL rewrite rules that are not overridden should still work
        Assertions.assertEquals("date", mysqlRule.rewriteFunction("to_date")); // default

        // Functions not in any rewrite map should return original name
        Assertions.assertEquals("sum", mysqlRule.rewriteFunction("sum"));
        Assertions.assertEquals("count", mysqlRule.rewriteFunction("count"));
    }

    @Test
    public void testFunctionRewriteRuleCreateWithMultipleDataSources() {
        // Test that different datasources have different default rules
        FunctionRewriteRules mysqlRule = FunctionRewriteRules.create("mysql", null);
        FunctionRewriteRules clickhouseRule = FunctionRewriteRules.create("clickhouse", null);
        FunctionRewriteRules oracleRule = FunctionRewriteRules.create("oracle", null);

        // Same function should be rewritten differently for different datasources
        Assertions.assertEquals("ifnull", mysqlRule.rewriteFunction("nvl")); // MySQL: nvl -> ifnull
        Assertions.assertEquals("nvl", clickhouseRule.rewriteFunction("nvl")); // ClickHouse: no rewrite
        Assertions.assertEquals("nvl", oracleRule.rewriteFunction("nvl")); // Oracle: no rewrite

        Assertions.assertEquals("ifnull", mysqlRule.rewriteFunction("ifnull")); // MySQL: no rewrite
        Assertions.assertEquals("ifnull", clickhouseRule.rewriteFunction("ifnull")); // ClickHouse: no rewrite
        Assertions.assertEquals("nvl", oracleRule.rewriteFunction("ifnull")); // Oracle: ifnull -> nvl

        Assertions.assertEquals("FROM_UNIXTIME",
                clickhouseRule.rewriteFunction("from_unixtime")); // ClickHouse specific
        Assertions.assertEquals("from_unixtime", mysqlRule.rewriteFunction("from_unixtime")); // No rewrite in MySQL
        Assertions.assertEquals("from_unixtime", oracleRule.rewriteFunction("from_unixtime")); // No rewrite in Oracle
    }

    @Test
    public void testFunctionRewriteRuleRewriteFunctionEdgeCases() {
        // Test edge cases for rewriteFunction method
        String jsonRules = "{\n"
                + "  \"rewrite\": {\n"
                + "    \"normal_func\": \"replaced_func\",\n"
                + "    \"UPPER_CASE\": \"lower_case\",\n"
                + "    \"Mixed_Case\": \"another_case\"\n"
                + "  }\n"
                + "}";

        FunctionRewriteRules rule = FunctionRewriteRules.create("unknown", jsonRules);

        // Test exact matches
        Assertions.assertEquals("replaced_func", rule.rewriteFunction("normal_func"));
        Assertions.assertEquals("lower_case", rule.rewriteFunction("UPPER_CASE"));
        Assertions.assertEquals("another_case", rule.rewriteFunction("Mixed_Case"));

        // Test case sensitivity - should not match different cases
        Assertions.assertEquals("Normal_Func", rule.rewriteFunction("Normal_Func")); // different case
        Assertions.assertEquals("upper_case", rule.rewriteFunction("upper_case")); // different case
        Assertions.assertEquals("mixed_case", rule.rewriteFunction("mixed_case")); // different case

        // Test special characters
        Assertions.assertEquals("func_with_underscore", rule.rewriteFunction("func_with_underscore"));
        Assertions.assertEquals("func123", rule.rewriteFunction("func123"));
        Assertions.assertEquals("func-with-dash", rule.rewriteFunction("func-with-dash"));
    }
}
