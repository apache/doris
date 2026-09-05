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

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

public class CTEInlineTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTable("CREATE TABLE cte_inline_tbl (\n"
                + "  id int NULL,\n"
                + "  val int NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES (\"replication_num\" = \"1\")");
        createTable("CREATE TABLE T1 (\n"
                + "  id bigint NULL,\n"
                + "  score bigint NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES (\"replication_num\" = \"1\")");
    }

    @Test
    public void recCteInline() {
        String sql = new StringBuilder().append("with recursive t1 as (\n").append("    select\n")
                .append("        1 as c1,\n").append("        1 as c2\n").append("),\n").append("t2 as (\n")
                .append("    select\n").append("        2 as c1,\n").append("        2 as c2\n").append("),\n")
                .append("t3 as (\n").append("    select\n").append("        3 as c1,\n").append("        3 as c2\n")
                .append("),\n").append("xx as (\n").append("    select\n").append("        c1,\n")
                .append("        c2\n").append("    from\n").append("        t1\n").append("    union\n")
                .append("    select\n").append("        t2.c1,\n").append("        t2.c2\n").append("    from\n")
                .append("        t2,\n").append("        xx\n").append("    where\n").append("        t2.c1 = xx.c1\n")
                .append("),\n").append("yy as (\n").append("    select\n").append("        c1,\n")
                .append("        c2\n").append("    from\n").append("        t3\n").append("    union\n")
                .append("    select\n").append("        t3.c1,\n").append("        t3.c2\n").append("    from\n")
                .append("        t3,\n").append("        yy,\n").append("        xx\n").append("    where\n")
                .append("        t3.c1 = yy.c1\n").append("        and t3.c2 = xx.c1\n").append(")\n")
                .append("select\n").append("    *\n").append("from\n").append("    yy y1,\n").append("    yy y2;")
                .toString();
        LogicalPlan unboundPlan = new NereidsParser().parseSingle(sql);
        StatementContext statementContext = new StatementContext(connectContext,
                new OriginStatement(sql, 0));
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        planner.planWithLock(unboundPlan, PhysicalProperties.ANY,
                ExplainCommand.ExplainLevel.REWRITTEN_PLAN);
        MemoTestUtils.initMemoAndValidState(planner.getCascadesContext());
        PlanChecker.from(planner.getCascadesContext()).matches(
                this.logicalRecursiveUnion(
                        any(
                        ),
                        logicalRecursiveUnionProducer(
                                logicalProject(
                                        logicalJoin(
                                                any(),
                                                logicalProject(
                                                        logicalFilter(
                                                                logicalRecursiveUnion().when(cte -> cte.getCteName().equals("xx"))
                                                        )
                                                )
                                        )
                                )
                        )
                ).when(cte -> cte.getCteName().equals("yy"))
        );
    }

    @Test
    public void refreshCteConsumersAfterNormalizeEliminatesEmptyBranch() {
        int oldCteInlineMode = connectContext.getSessionVariable().cteInlineMode;
        int oldInlineCteReferencedThreshold = connectContext.getSessionVariable().inlineCTEReferencedThreshold;
        connectContext.getSessionVariable().cteInlineMode = 0;
        connectContext.getSessionVariable().inlineCTEReferencedThreshold = 1;
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "with cte as (select id, val from cte_inline_tbl) "
                + "select * from cte where id = 1 "
                + "union all select * from cte where id = 2 "
                + "union all select * from cte where 1 = 0";
        try {
            PlanChecker.from(connectContext).checkPlannerResult(sql, planner -> {
                Map<CTEId, Set<LogicalCTEConsumer>> consumers =
                        planner.getCascadesContext().getStatementContext().getCteIdToConsumers();
                Assertions.assertEquals(1, consumers.size());
                Assertions.assertEquals(2, consumers.values().iterator().next().size());
            });
        } finally {
            connectContext.getSessionVariable().cteInlineMode = oldCteInlineMode;
            connectContext.getSessionVariable().inlineCTEReferencedThreshold = oldInlineCteReferencedThreshold;
            connectContext.getSessionVariable().setDisableNereidsRules("");
        }
    }

    @Test
    public void testConstantOneRowCteAlwaysInlined() {
        String sql = "WITH c AS (SELECT 1 AS a, 'x' AS b) "
                + "SELECT * FROM c c1 JOIN c c2 ON c1.a = c2.a";

        LogicalPlan analyzed = (LogicalPlan) PlanChecker.from(connectContext)
                .analyze(sql)
                .applyCustom(new PullUpCteAnchor())
                .applyCustom(new CTEInline())
                .getPlan();

        assertNoCteNodes(analyzed);
    }

    @Test
    public void testConstantOneRowCteWithManyConsumersInlined() {
        String sql = "WITH consts AS ("
                + "  SELECT '2026-01-01' AS day_start, '2026-08-17' AS day_end, "
                + "         '2025-01-01' AS tq_start,  '2025-08-17' AS tq_end)"
                + "SELECT * FROM consts c1, consts c2, consts c3, consts c4, "
                + "              consts c5, consts c6, consts c7, consts c8";

        LogicalPlan analyzed = (LogicalPlan) PlanChecker.from(connectContext)
                .analyze(sql)
                .applyCustom(new PullUpCteAnchor())
                .applyCustom(new CTEInline())
                .getPlan();

        assertNoCteNodes(analyzed);
    }

    @Test
    public void testTableBackedCteNotForcedInline() {
        String sql = "WITH t AS (SELECT id, score FROM T1) "
                + "SELECT * FROM t x JOIN t y ON x.id = y.id";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyCustom(new PullUpCteAnchor())
                .applyCustom(new CTEInline())
                .matches(logicalCTEAnchor());
    }

    @Test
    public void testNonDeterministicOneRowCteNotForcedInline() {
        String sql = "WITH r AS (SELECT RANDOM() AS x) "
                + "SELECT * FROM r r1 JOIN r r2 ON r1.x = r2.x";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyCustom(new PullUpCteAnchor())
                .applyCustom(new CTEInline())
                .matches(logicalCTEAnchor());
    }

    @Test
    public void testFoldableExpressionOneRowCteInlined() {
        String sql = "WITH c AS ("
                + "  SELECT CONCAT(SUBSTR('2026年', 1, 4), '-01-01') AS day_start,"
                + "         DATE_FORMAT(CURRENT_DATE(), '%Y-%m-%d') AS day_end,"
                + "         CAST(SUBSTR('2026年', 1, 4) AS INT) - 1 AS prev_year)"
                + "SELECT * FROM c c1 JOIN c c2 ON c1.day_start = c2.day_start "
                + "                  JOIN c c3 ON c1.day_start = c3.day_start";

        LogicalPlan analyzed = (LogicalPlan) PlanChecker.from(connectContext)
                .analyze(sql)
                .applyCustom(new PullUpCteAnchor())
                .applyCustom(new CTEInline())
                .getPlan();

        assertNoCteNodes(analyzed);
    }

    @Test
    public void testChainedConstantCtesAllInlined() {
        String sql = "WITH t_const AS ("
                + "  SELECT '2026-01-01' AS s, '2026-08-21' AS e, CURRENT_DATE() AS today), "
                + "t_filter AS ("
                + "  SELECT s AS day_start, IF(e = DATE_FORMAT(today, '%Y-%m-%d'), e, s) AS day_end"
                + "  FROM t_const), "
                + "t_range AS ("
                + "  SELECT CONCAT(day_start, '_', day_end) AS token FROM t_filter) "
                + "SELECT * FROM t_range r1 JOIN t_range r2 ON r1.token = r2.token"
                + "                        JOIN t_range r3 ON r1.token = r3.token";

        LogicalPlan analyzed = (LogicalPlan) PlanChecker.from(connectContext)
                .analyze(sql)
                .applyCustom(new PullUpCteAnchor())
                .applyCustom(new CTEInline())
                .getPlan();

        assertNoCteNodes(analyzed);
    }

    @Test
    public void testRuntimeOnlyConstantCteNotForcedInline() {
        String sql = "WITH s AS (SELECT sleep(0) AS x) "
                + "SELECT * FROM s s1 JOIN s s2 ON s1.x = s2.x";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyCustom(new PullUpCteAnchor())
                .applyCustom(new CTEInline())
                .matches(logicalCTEAnchor());
    }

    @Test
    public void testSubqueryInProducerNotForcedInline() {
        String sql = "WITH c AS ("
                + "  SELECT '2026-01-01' AS s, (SELECT MAX(id) FROM T1) AS e) "
                + "SELECT * FROM c c1 JOIN c c2 ON c1.s = c2.s JOIN c c3 ON c1.s = c3.s";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyCustom(new PullUpCteAnchor())
                .applyCustom(new CTEInline())
                .matches(logicalCTEAnchor());
    }

    private static void assertNoCteNodes(LogicalPlan plan) {
        plan.foreach(p -> {
            if (p instanceof LogicalCTEAnchor || p instanceof LogicalCTEProducer) {
                throw new AssertionError(
                        "Expected all CTE nodes to be inlined, but found: " + p.getClass().getSimpleName()
                        + "\nplan:\n" + plan.treeString());
            }
        });
    }
}
