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

package org.apache.doris.cloud;

import org.apache.doris.cloud.OnTablesFilter.TableFilterRule;
import org.apache.doris.cloud.OnTablesFilter.TableFilterRule.RuleType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Tests for {@link OnTablesFilter}: glob compilation, INCLUDE/EXCLUDE semantics,
 * edge cases for wildcards and regex metacharacters.
 */
public class OnTablesFilterTest {

    private OnTablesFilter buildFilter(TableFilterRule... rules) {
        return new OnTablesFilter(Arrays.asList(rules));
    }

    private TableFilterRule inc(String pattern) {
        return new TableFilterRule(RuleType.INCLUDE, pattern);
    }

    private TableFilterRule exc(String pattern) {
        return new TableFilterRule(RuleType.EXCLUDE, pattern);
    }

    // ===== Glob matching semantics =====

    @Test
    public void testGlobWildcards() {
        // '*' matches any characters, '?' matches exactly one character
        OnTablesFilter filter = buildFilter(inc("db?.tbl_*"));
        Assertions.assertTrue(filter.shouldWarmUp("db1", "tbl_orders"));
        Assertions.assertTrue(filter.shouldWarmUp("dbA", "tbl_"));
        Assertions.assertFalse(filter.shouldWarmUp("db12", "tbl_x"));  // '?' must match exactly one char
        Assertions.assertFalse(filter.shouldWarmUp("db", "tbl_x"));    // '?' requires one char, not zero
        Assertions.assertFalse(filter.shouldWarmUp("db1", "orders"));   // prefix must match
    }

    @Test
    public void testDotIsLiteral() {
        // '.' is a regex metachar but in glob it should be literal
        TableFilterRule rule = inc("ods.tbl");
        Assertions.assertTrue(rule.matches("ods.tbl"));
        Assertions.assertFalse(rule.matches("odsXtbl"));  // '.' must not match arbitrary char
    }

    @Test
    public void testRegexMetacharsEscaped() {
        // All regex metacharacters should be treated as literals in glob
        OnTablesFilter filter = buildFilter(inc("db(1).tbl[2]"));
        Assertions.assertTrue(filter.shouldWarmUp("db(1)", "tbl[2]"));
        Assertions.assertFalse(filter.shouldWarmUp("db1", "tbl2"));
    }

    // ===== INCLUDE / EXCLUDE semantics =====

    @Test
    public void testIncludeOnlyMatchesTargetDb() {
        OnTablesFilter filter = buildFilter(inc("ods.*"));
        Assertions.assertTrue(filter.shouldWarmUp("ods", "orders"));
        Assertions.assertTrue(filter.shouldWarmUp("ods", "users"));
        Assertions.assertFalse(filter.shouldWarmUp("dw", "orders"));
    }

    @Test
    public void testExcludeOverridesInclude() {
        OnTablesFilter filter = buildFilter(inc("ods.*"), exc("ods.tmp_*"));
        Assertions.assertTrue(filter.shouldWarmUp("ods", "orders"));
        Assertions.assertFalse(filter.shouldWarmUp("ods", "tmp_staging"));
        Assertions.assertFalse(filter.shouldWarmUp("dw", "orders"));  // not included
    }

    @Test
    public void testMultipleIncludesFormUnion() {
        OnTablesFilter filter = buildFilter(inc("ods.*"), inc("dw.*"));
        Assertions.assertTrue(filter.shouldWarmUp("ods", "orders"));
        Assertions.assertTrue(filter.shouldWarmUp("dw", "fact_sales"));
        Assertions.assertFalse(filter.shouldWarmUp("staging", "temp"));
    }

    @Test
    public void testExcludeOnlyNeverMatches() {
        // No INCLUDE rules means nothing is included, regardless of EXCLUDE
        OnTablesFilter filter = buildFilter(exc("ods.tmp_*"));
        Assertions.assertFalse(filter.shouldWarmUp("ods", "orders"));
        Assertions.assertFalse(filter.shouldWarmUp("ods", "tmp_staging"));
    }

    @Test
    public void testEmptyRulesNeverMatches() {
        OnTablesFilter filter = new OnTablesFilter(Collections.emptyList());
        Assertions.assertFalse(filter.shouldWarmUp("any", "table"));
    }

    // ===== Typical user scenario: multiple databases + selective exclusion =====

    @Test
    public void testComplexScenario() {
        // Include everything in ods and dw, but exclude all tmp tables and a specific table
        OnTablesFilter filter = buildFilter(
                inc("ods.*"), inc("dw.*"),
                exc("*.tmp_*"), exc("dw.secret_report"));
        Assertions.assertTrue(filter.shouldWarmUp("ods", "orders"));
        Assertions.assertTrue(filter.shouldWarmUp("dw", "fact_sales"));
        Assertions.assertFalse(filter.shouldWarmUp("ods", "tmp_staging"));
        Assertions.assertFalse(filter.shouldWarmUp("dw", "tmp_data"));
        Assertions.assertFalse(filter.shouldWarmUp("dw", "secret_report"));
        Assertions.assertFalse(filter.shouldWarmUp("staging", "anything"));
    }

    // ===== Rule partitioning =====

    @Test
    public void testGetRulesPartition() {
        OnTablesFilter filter = buildFilter(inc("ods.*"), exc("ods.tmp_*"), inc("dw.*"));
        Assertions.assertEquals(2, filter.getIncludeRules().size());
        Assertions.assertEquals(1, filter.getExcludeRules().size());
        Assertions.assertEquals(3, filter.getAllRules().size());
    }
}
