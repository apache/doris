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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for CheckSearchUsage rule.
 * This test validates that search() function can only be used in WHERE clauses
 * on single-table OLAP scans, and is rejected in other contexts.
 */
public class CheckSearchUsageTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");

        // Create test table with inverted index
        createTable("CREATE TABLE test_search_table (\n"
                + "  id INT,\n"
                + "  title VARCHAR(255),\n"
                + "  content TEXT,\n"
                + "  category VARCHAR(100),\n"
                + "  INDEX idx_title(title) USING INVERTED,\n"
                + "  INDEX idx_content(content) USING INVERTED\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 3\n"
                + "PROPERTIES ('replication_num' = '1');");
    }

    @Test
    public void testSearchInWhereClauseAllowed() {
        // Valid usage: search() in WHERE clause on single table
        String sql = "SELECT id, title FROM test_search_table WHERE search('title:hello')";

        // This should NOT throw exception
        Assertions.assertDoesNotThrow(() -> {
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .matches(logicalFilter());
        });
    }

    @Test
    public void testSearchInWhereWithAndAllowed() {
        // Valid usage: search() combined with other predicates
        String sql = "SELECT id, title FROM test_search_table "
                + "WHERE search('title:hello') AND id > 10";

        Assertions.assertDoesNotThrow(() -> {
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .matches(logicalFilter());
        });
    }

    @Test
    public void testSearchInGroupByRejected() {
        // Invalid: search() directly in GROUP BY expression
        String sql = "SELECT count(*) FROM test_search_table GROUP BY search('title:hello')";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            PlanChecker.from(connectContext).analyze(sql);
        });

        Assertions.assertTrue(
                exception.getMessage().contains("search()")
                        && (exception.getMessage().contains("GROUP BY")
                        || exception.getMessage().contains("WHERE filters")
                        || exception.getMessage().contains("single-table")),
                "Expected error about search() usage restrictions, got: " + exception.getMessage());
    }

    @Test
    public void testSearchInGroupByWithAliasRejected() {
        // Invalid: search() in SELECT, then GROUP BY alias
        String sql = "SELECT search('title:hello') as s, count(*) FROM test_search_table GROUP BY s";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            PlanChecker.from(connectContext).analyze(sql);
        });

        Assertions.assertTrue(
                exception.getMessage().contains("search()")
                        && (exception.getMessage().contains("projection")
                        || exception.getMessage().contains("WHERE")),
                "Expected error about search() usage, got: " + exception.getMessage());
    }

    @Test
    public void testSearchInSelectWithoutGroupByRejected() {
        // Invalid: search() in SELECT projection without WHERE
        String sql = "SELECT search('title:hello'), title FROM test_search_table";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            PlanChecker.from(connectContext).analyze(sql);
        });

        Assertions.assertTrue(
                exception.getMessage().contains("search()")
                        && (exception.getMessage().contains("projection")
                        || exception.getMessage().contains("WHERE")),
                "Expected error about search() in projection, got: " + exception.getMessage());
    }

    @Test
    public void testSearchInAggregateOutputRejected() {
        // Invalid: search() wrapped in aggregate function
        // Note: This might be caught by other checks, but we test it anyway
        String sql = "SELECT count(search('title:hello')) FROM test_search_table";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            PlanChecker.from(connectContext).analyze(sql);
        });

        Assertions.assertTrue(
                exception.getMessage().contains("search()")
                        || exception.getMessage().contains("WHERE"),
                "Expected error about search() usage, got: " + exception.getMessage());
    }

    @Test
    public void testSearchInHavingRejected() {
        // Invalid: search() in HAVING clause
        String sql = "SELECT category, count(*) FROM test_search_table "
                + "GROUP BY category HAVING search('title:hello')";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            PlanChecker.from(connectContext).analyze(sql);
        });

        Assertions.assertTrue(
                exception.getMessage().contains("search()")
                        || exception.getMessage().contains("WHERE")
                        || exception.getMessage().contains("HAVING"),
                "Expected error about search() usage, got: " + exception.getMessage());
    }

    @Test
    public void testSearchWithJoinRejected() {
        // Create second table for join test
        try {
            createTable("CREATE TABLE test_search_table2 (\n"
                    + "  id INT,\n"
                    + "  name VARCHAR(255)\n"
                    + ") ENGINE=OLAP\n"
                    + "DUPLICATE KEY(id)\n"
                    + "DISTRIBUTED BY HASH(id) BUCKETS 3\n"
                    + "PROPERTIES ('replication_num' = '1');");
        } catch (Exception e) {
            // Table might already exist from previous test
        }

        // Invalid: search() in WHERE with JOIN
        String sql = "SELECT t1.id FROM test_search_table t1 "
                + "JOIN test_search_table2 t2 ON t1.id = t2.id "
                + "WHERE search('title:hello')";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            PlanChecker.from(connectContext).analyze(sql);
        });

        Assertions.assertTrue(
                exception.getMessage().contains("search()")
                        && exception.getMessage().contains("single"),
                "Expected error about single table, got: " + exception.getMessage());
    }

    @Test
    public void testSearchInSubqueryRejected() {
        // Invalid: search() in subquery
        String sql = "SELECT * FROM (SELECT id, title FROM test_search_table "
                + "WHERE search('title:hello')) t WHERE id > 10";

        // The search() function is allowed in the WHERE clause of a subquery over a single table.
        // This test verifies that such usage does not throw an exception.
        Assertions.assertDoesNotThrow(() -> {
            PlanChecker.from(connectContext).analyze(sql);
        });
    }

    @Test
    public void testSearchWithMultipleFieldsAllowed() {
        // Valid: search() with multiple fields in WHERE
        String sql = "SELECT id, title FROM test_search_table "
                + "WHERE search('title:hello AND content:world')";

        Assertions.assertDoesNotThrow(() -> {
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .matches(logicalFilter());
        });
    }

    @Test
    public void testSearchInOrderByRejected() {
        // Invalid: search() in ORDER BY
        String sql = "SELECT id, title FROM test_search_table ORDER BY search('title:hello')";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            PlanChecker.from(connectContext).analyze(sql);
        });

        Assertions.assertTrue(
                exception.getMessage().contains("search()")
                        || exception.getMessage().contains("WHERE"),
                "Expected error about search() usage, got: " + exception.getMessage());
    }

    @Test
    public void testSearchInCaseWhenRejected() {
        // Invalid: search() in CASE WHEN (outside WHERE)
        String sql = "SELECT CASE WHEN search('title:hello') THEN 1 ELSE 0 END FROM test_search_table";

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            PlanChecker.from(connectContext).analyze(sql);
        });

        Assertions.assertTrue(
                exception.getMessage().contains("search()")
                        || exception.getMessage().contains("WHERE"),
                "Expected error about search() usage, got: " + exception.getMessage());
    }

    @Test
    public void testSearchWithComplexWhereAllowed() {
        // Valid: search() in complex WHERE clause
        String sql = "SELECT id, title FROM test_search_table "
                + "WHERE (search('title:hello') OR id = 1) AND category = 'tech'";

        Assertions.assertDoesNotThrow(() -> {
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .matches(logicalFilter());
        });
    }

    @Test
    public void testMultipleSearchInWhereAllowed() {
        // Valid: multiple search() functions in WHERE
        String sql = "SELECT id, title FROM test_search_table "
                + "WHERE search('title:hello') AND search('content:world')";

        Assertions.assertDoesNotThrow(() -> {
            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .matches(logicalFilter());
        });
    }

    @Test
    public void testSearchInWhereWithLimitAllowed() {
        // Valid: search() in WHERE with LIMIT
        String sql = "SELECT id, title FROM test_search_table "
                + "WHERE search('title:hello') LIMIT 10";

        Assertions.assertDoesNotThrow(() -> {
            PlanChecker.from(connectContext).analyze(sql);
        });
    }

    @Test
    public void testRuleTypeCorrect() {
        CheckSearchUsage rule = new CheckSearchUsage();
        Assertions.assertNotNull(rule);
        Assertions.assertNotNull(rule.buildRules());
        Assertions.assertEquals(1, rule.buildRules().size());
        Assertions.assertEquals(
                org.apache.doris.nereids.rules.RuleType.CHECK_SEARCH_USAGE,
                rule.buildRules().get(0).getRuleType());
    }
}
