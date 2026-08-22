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

import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EliminateJoinByConstantOneRowRelationTest extends SqlTestBase {

    private boolean savedEnableRule;

    @BeforeEach
    void enableRule() {
        savedEnableRule = connectContext.getSessionVariable().enableEliminateJoinByConstantOneRowRelation;
        connectContext.getSessionVariable().enableEliminateJoinByConstantOneRowRelation = true;
    }

    @AfterEach
    void restoreRule() {
        connectContext.getSessionVariable().enableEliminateJoinByConstantOneRowRelation = savedEnableRule;
    }

    @Test
    void testInnerJoinWithConstantOneRowRelationEliminated() {
        String sql = "WITH tc AS (SELECT '2026-01-01' AS s, '2026-08-21' AS e) "
                + "SELECT t.id FROM T1 t "
                + "INNER JOIN tc ON CAST(t.id AS VARCHAR) BETWEEN tc.s AND tc.e";

        LogicalPlan analyzed = (LogicalPlan) PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        assertNoJoin(analyzed);
    }

    @Test
    void testCrossJoinWithConstantOneRowRelationEliminated() {
        String sql = "WITH tc AS (SELECT '2026-01-01' AS s, '2026-08-21' AS e) "
                + "SELECT t.id FROM T1 t "
                + "CROSS JOIN tc "
                + "WHERE CAST(t.id AS VARCHAR) BETWEEN tc.s AND tc.e";

        LogicalPlan analyzed = (LogicalPlan) PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        assertNoJoin(analyzed);
    }

    @Test
    void testConstantOneRowRelationOnLeftSideAlsoEliminated() {
        String sql = "WITH tc AS (SELECT '2026-01-01' AS s) "
                + "SELECT t.id FROM tc INNER JOIN T1 t ON CAST(t.id AS VARCHAR) >= tc.s";

        LogicalPlan analyzed = (LogicalPlan) PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getPlan();

        assertNoJoin(analyzed);
    }

    @Test
    void testLeftOuterJoinNotBrokenByThisRule() {
        String sql = "WITH tc AS (SELECT '2026-01-01' AS s) "
                + "SELECT t.id FROM T1 t LEFT OUTER JOIN tc ON CAST(t.id AS VARCHAR) >= tc.s";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite();
    }

    @Test
    void testJoinWithTableSideNotRewrittenByThisRule() {
        String sql = "SELECT a.id FROM T1 a INNER JOIN T2 b ON a.id = b.id";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin());
    }

    private static void assertNoJoin(LogicalPlan plan) {
        plan.foreach(p -> {
            if (p instanceof LogicalJoin) {
                throw new AssertionError(
                        "Expected no LogicalJoin after rule, but found one.\nplan:\n" + plan.treeString());
            }
        });
    }
}
