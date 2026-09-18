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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
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
    public void inlineTransitiveRecursiveDependencies() {
        boolean oldEnableCteMaterialize = connectContext.getSessionVariable().enableCTEMaterialize;
        int oldCteInlineMode = connectContext.getSessionVariable().cteInlineMode;
        int oldInlineCteReferencedThreshold = connectContext.getSessionVariable().inlineCTEReferencedThreshold;
        connectContext.getSessionVariable().enableCTEMaterialize = true;
        connectContext.getSessionVariable().cteInlineMode = 0;
        connectContext.getSessionVariable().inlineCTEReferencedThreshold = 1;
        try {
            for (String input : new String[] {"base", "middle"}) {
                String sql = "with recursive "
                        + "base as (select 1 as src, 2 as dst union all select 2, 3), "
                        + "middle as (select * from base union all select * from base), "
                        + "edges as (select * from " + input + " union all select * from " + input + "), "
                        + "ordinary as (select id from cte_inline_tbl), "
                        + "r1(n) as (select 1 union all "
                        + "select e.dst from r1 r join edges e on r.n = e.src), "
                        + "r2(n) as (select 2 union all "
                        + "select e.dst from r2 r join edges e on r.n = e.src) "
                        + "select r1.n, r2.n from r1 join r2 on r1.n = r2.n "
                        + "join ordinary a on a.id = r1.n join ordinary b on b.id = r2.n";
                LogicalPlan unboundPlan = new NereidsParser().parseSingle(sql);
                NereidsPlanner planner = new NereidsPlanner(new StatementContext(connectContext,
                        new OriginStatement(sql, 0)));
                planner.planWithLock(unboundPlan, PhysicalProperties.ANY,
                        ExplainCommand.ExplainLevel.REWRITTEN_PLAN);
                List<LogicalCTEConsumer> consumers = planner.getRewrittenPlan()
                        .collectToList(p -> p instanceof LogicalCTEConsumer);
                Assertions.assertEquals(2, consumers.size());
                Assertions.assertTrue(consumers.stream().allMatch(c -> c.getName().equals("ordinary")));
            }
        } finally {
            connectContext.getSessionVariable().enableCTEMaterialize = oldEnableCteMaterialize;
            connectContext.getSessionVariable().cteInlineMode = oldCteInlineMode;
            connectContext.getSessionVariable().inlineCTEReferencedThreshold = oldInlineCteReferencedThreshold;
        }
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
    public void mustInlineVolatileCteLiveReference() {
        // u is referenced by the recursive child, so it must be inlined, but uuid() can not be inlined
        String sql = "with recursive "
                + "u as (select uuid() as v), "
                + "r(n) as (select cast(1 as int) union all "
                + "select cast(n + 1 as int) from r join u u1 on true join u u2 on true "
                + "where n < 2 and u1.v = u2.v) "
                + "select n from r order by n";
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> planRecursiveCte(sql), "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("inline is blocked"));
    }

    @Test
    public void mustInlineTransitiveVolatileCteLiveReference() {
        // u is only referenced by v, but v is referenced by the recursive child, so u must be inlined too
        String sql = "with recursive "
                + "u as (select random() as x), "
                + "v as (select x from u), "
                + "r(n) as (select cast(1 as int) union all "
                + "select cast(n + 1 as int) from r join v on true where n < 2) "
                + "select n from r order by n";
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> planRecursiveCte(sql), "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("inline is blocked"));
    }

    @Test
    public void mustInlineVolatileCteUsedByAnchorOnly() {
        // the anchor child is executed once, so u may stay materialized even if it contains uuid()
        String sql = "with recursive "
                + "u as (select uuid() as v), "
                + "r(n) as (select cast(1 as int) from u union all "
                + "select cast(n + 1 as int) from r where n < 3) "
                + "select n from r order by n";
        planRecursiveCte(sql);
    }

    @Test
    public void mustInlineVolatileCteReferenceRemovedAsDeadCode() {
        // the reference of u is eliminated together with the dead branch, so nothing has to be inlined
        String sql = "with recursive "
                + "u as (select uuid() as v), "
                + "r(n) as (select cast(1 as int) union all "
                + "select cast(n + 1 as int) from r join u on true where n < 3 and false) "
                + "select n from r order by n";
        planRecursiveCte(sql);
    }

    @Test
    public void mustInlineVolatileCteFromSubQuery() {
        // u is consumed inside an exists subquery of the recursive child, it must be inlined as well
        String sql = "with recursive "
                + "u as (select uuid() as v), "
                + "r(n) as (select cast(1 as int) union all "
                + "select cast(n + 1 as int) from r "
                + "where n < 2 and exists (select 1 from u where u.v is not null)) "
                + "select n from r order by n";
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> planRecursiveCte(sql), "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("inline is blocked"));
    }

    @Test
    public void mustInlineVolatileCteInsideNestedCte() {
        // the recursive child consumes a nested cte which consumes u, so u has to be inlined as well
        String sql = "with recursive "
                + "u as (select uuid() as v), "
                + "r(n) as (select cast(1 as int) union all "
                + "select cast(n + 1 as int) from r join "
                + "(with w as (select v from u) select count(*) as c from w) t on t.c = 1 where n < 2) "
                + "select n from r order by n";
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> planRecursiveCte(sql), "Not throw expected exception.");
        Assertions.assertTrue(exception.getMessage().contains("inline is blocked"));
    }

    @Test
    public void mustInlineVolatileCteWithUnusedOutput() {
        // the recursive child only uses k, so uuid() is pruned from the inlined copy
        String sql = "with recursive "
                + "u as (select 1 as k, uuid() as v), "
                + "r(n) as (select cast(1 as int) union all "
                + "select cast(n + u.k as int) from r join u on true where n < 3) "
                + "select n from r order by n";
        planRecursiveCte(sql);
    }

    @Test
    public void mustInlineVolatileCteWithMixedConsumers() {
        // the outer consumer needs v, the recursive child only needs k: the volatile producer stays
        // materialized for the outer consumer while a pruned copy is inlined into the recursive child
        String sql = "with recursive "
                + "u as (select 1 as k, uuid() as v), "
                + "r(n) as (select cast(1 as int) union all "
                + "select cast(n + u.k as int) from r join u on true where n < 3) "
                + "select r.n from r, u where u.v is not null order by r.n";
        planRecursiveCte(sql);
    }

    @Test
    public void mustInlineVolatileCteWithEmptyAnchor() {
        // the anchor is empty, the recursive side is never executed, so nothing must be inlined
        String sql = "with recursive "
                + "u as (select uuid() as v), "
                + "r(n) as (select cast(1 as int) where false union all "
                + "select cast(n + 1 as int) from r join u on true where n < 3) "
                + "select n from r order by n";
        planRecursiveCte(sql);
    }

    private void planRecursiveCte(String sql) {
        LogicalPlan unboundPlan = new NereidsParser().parseSingle(sql);
        NereidsPlanner planner = new NereidsPlanner(new StatementContext(connectContext,
                new OriginStatement(sql, 0)));
        planner.planWithLock(unboundPlan, PhysicalProperties.ANY,
                ExplainCommand.ExplainLevel.REWRITTEN_PLAN);
    }
}
