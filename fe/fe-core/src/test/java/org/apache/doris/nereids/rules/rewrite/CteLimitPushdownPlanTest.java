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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

/**
 * Planner-level tests for CTE limit pushdown.
 */
class CteLimitPushdownPlanTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");

        createTable("CREATE TABLE cte_limit_pushdown_t (\n"
                + "  k1 int NULL,\n"
                + "  k2 int NULL\n"
                + ") ENGINE=OLAP\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  \"replication_allocation\" = \"tag.location.default: 1\"\n"
                + ");");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        StatementScopeIdGenerator.clear();
    }

    @Test
    void testPushLimitWithOffsetToProducer() {
        String sql = "WITH cte AS (SELECT k1, k2 FROM cte_limit_pushdown_t) "
                + "(SELECT * FROM cte LIMIT 10 OFFSET 5) "
                + "UNION ALL "
                + "(SELECT * FROM cte LIMIT 3)";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalCTEProducer(
                        logicalLimit().when(limit -> limit.getLimit() == 15 && limit.getOffset() == 0)));
    }

    @Test
    void testPushLimitBeforeProducerOutputPruning() {
        String sql = "WITH cte AS (SELECT k1, k2 FROM cte_limit_pushdown_t) "
                + "(SELECT k1 FROM cte LIMIT 7) "
                + "UNION ALL "
                + "(SELECT k1 FROM cte LIMIT 3)";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalCTEProducer(
                        logicalLimit().when(limit -> limit.getLimit() == 7 && limit.getOffset() == 0)));
    }

    @Test
    void testPushMaxLimitForAllLimitedConsumers() {
        String sql = "WITH cte AS (SELECT k1, k2 FROM cte_limit_pushdown_t) "
                + "(SELECT * FROM cte LIMIT 10 OFFSET 5) "
                + "UNION ALL "
                + "(SELECT * FROM cte LIMIT 20)";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalCTEProducer(
                        logicalLimit().when(limit -> limit.getLimit() == 20 && limit.getOffset() == 0)));
    }

    @Test
    void testSkipProducerLimitWhenAnyConsumerNeedsFullRows() {
        String sql = "WITH cte AS (SELECT k1, k2 FROM cte_limit_pushdown_t) "
                + "(SELECT * FROM cte LIMIT 10) "
                + "UNION ALL "
                + "(SELECT * FROM cte)";

        assertNoProducerLimit(sql);
    }

    @Test
    void testSkipProducerLimitWhenLimitIsAboveFilter() {
        String sql = "WITH cte AS (SELECT k1, k2 FROM cte_limit_pushdown_t) "
                + "(SELECT * FROM cte WHERE k1 > 1 LIMIT 10) "
                + "UNION ALL "
                + "(SELECT * FROM cte LIMIT 3)";

        assertNoProducerLimit(sql);
    }

    @Test
    void testSkipProducerLimitWhenConsumerUsesTopN() {
        String sql = "WITH cte AS (SELECT k1, k2 FROM cte_limit_pushdown_t) "
                + "(SELECT * FROM (SELECT * FROM cte ORDER BY k1 LIMIT 10) topn_branch) "
                + "UNION ALL "
                + "(SELECT * FROM cte LIMIT 3)";

        assertNoProducerLimit(sql);
    }

    @Test
    void testSkipProducerLimitWhenLimitIsAboveJoin() {
        String sql = "WITH cte AS (SELECT k1, k2 FROM cte_limit_pushdown_t) "
                + "(SELECT c.k1, c.k2 FROM cte c "
                + "JOIN cte_limit_pushdown_t t ON c.k1 = t.k1 LIMIT 10) "
                + "UNION ALL "
                + "(SELECT * FROM cte LIMIT 3)";

        assertNoProducerLimit(sql);
    }

    @Test
    void testSkipProducerLimitWhenLimitIsAboveAggregate() {
        String sql = "WITH cte AS (SELECT k1, k2 FROM cte_limit_pushdown_t) "
                + "(SELECT k1, COUNT(*) FROM cte GROUP BY k1 LIMIT 10) "
                + "UNION ALL "
                + "(SELECT * FROM cte LIMIT 3)";

        assertNoProducerLimit(sql);
    }

    @Test
    void testSkipProducerLimitWhenLimitIsAboveWindow() {
        String sql = "WITH cte AS (SELECT k1, k2 FROM cte_limit_pushdown_t) "
                + "(SELECT k1, rn FROM ("
                + "SELECT k1, ROW_NUMBER() OVER (ORDER BY k1) AS rn FROM cte"
                + ") window_branch LIMIT 10) "
                + "UNION ALL "
                + "(SELECT * FROM cte LIMIT 3)";

        assertNoProducerLimit(sql);
    }

    private void assertNoProducerLimit(String sql) {
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalCTEProducer())
                .nonMatch(logicalCTEProducer(logicalLimit()))
                .nonMatch(logicalCTEProducer(logicalLimit(logicalProject())))
                .nonMatch(logicalCTEProducer(logicalProject(logicalLimit())));
    }
}
