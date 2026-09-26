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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Search;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Regression test for SEARCH under a materialized CTE.
 *
 * <p>RewriteSearchToSlots is the only rule that turns the raw Search scalar function into a
 * SearchExpression with bound slot children. The whole-tree instance of the rule is configured
 * with notTraverseChildrenOf(LogicalCTEAnchor), so a SEARCH that ends up under a materialized CTE
 * (or in a main body below a hoisted anchor) used to keep the raw Search function. It was then
 * translated to FunctionSearch and BE rejected it with "only inverted index queries are supported".
 */
public class SearchCteRewriteTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        // The tables below stay empty, so keep the OlapScan instead of replacing it by LogicalEmptyRelation.
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createTable("CREATE TABLE t_search (\n"
                + "  id INT,\n"
                + "  title VARCHAR(255),\n"
                + "  INDEX idx_title(title) USING INVERTED\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1');");
        createTable("CREATE TABLE t_dim (\n"
                + "  id INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1');");
    }

    private static List<Expression> allExpressions(Plan plan) {
        List<Expression> result = new ArrayList<>(plan.getExpressions());
        for (Plan child : plan.children()) {
            result.addAll(allExpressions(child));
        }
        return result;
    }

    private static long countSearchExpression(Plan plan) {
        return allExpressions(plan).stream()
                .filter(expr -> expr.anyMatch(e -> e instanceof SearchExpression))
                .count();
    }

    private static long countRawSearch(Plan plan) {
        return allExpressions(plan).stream()
                .filter(expr -> expr.anyMatch(e -> e instanceof Search))
                .count();
    }

    private void assertSearchRewritten(Plan plan) {
        Assertions.assertEquals(1, countSearchExpression(plan),
                "SEARCH must be rewritten to SearchExpression, plan:\n" + plan.treeString());
        Assertions.assertEquals(0, countRawSearch(plan),
                "raw Search scalar function must not survive in the plan:\n" + plan.treeString());
    }

    @Test
    public void testSearchInsideMaterializedCte() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("WITH c AS (SELECT id FROM t_search WHERE search('title:hello'))\n"
                        + "SELECT a.id FROM c a JOIN c b ON a.id = b.id")
                .rewrite().getPlan();
        assertSearchRewritten(plan);
    }

    @Test
    public void testSearchInMaterializedCteWithPreMvRecord() {
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze("WITH c AS (SELECT id FROM t_search WHERE search('title:hello'))\n"
                        + "SELECT a.id FROM c a JOIN c b ON a.id = b.id");
        // Force RecordPlanForMvPreRewrite, which runs a temporary RewriteCteChildren pass and may
        // leave its rewritten CTE producers/consumers in the shared StatementContext.
        checker.getCascadesContext().getStatementContext().setForceRecordTmpPlan(true);
        assertSearchRewritten(checker.rewrite().getPlan());
    }

    @Test
    public void testSearchInInlinedCteWithMaterializedSibling() {
        // c1 is referenced once and inlined, c3 is referenced twice and materialized. PullUpCteAnchor
        // hoists c3's anchor above the whole plan, so the inlined SEARCH ends up below an anchor.
        Plan plan = PlanChecker.from(connectContext)
                .analyze("WITH c1 AS (SELECT id FROM t_search WHERE search('title:hello')),\n"
                        + "     c3 AS (SELECT id FROM t_dim)\n"
                        + "SELECT a.id FROM c1 a JOIN c3 x ON a.id = x.id JOIN c3 y ON a.id = y.id")
                .rewrite().getPlan();
        assertSearchRewritten(plan);
    }
}
