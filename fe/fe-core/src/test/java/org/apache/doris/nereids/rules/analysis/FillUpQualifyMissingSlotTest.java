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
import org.apache.doris.nereids.rules.rewrite.PullUpProjectUnderApply;
import org.apache.doris.nereids.rules.rewrite.UnCorrelatedApplyFilter;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class FillUpQualifyMissingSlotTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");

        createTables(
                "CREATE TABLE test.o (\n"
                        + "    k INT,\n"
                        + "    flag INT,\n"
                        + "    h INT\n"
                        + ")\n"
                        + "DUPLICATE KEY (k)\n"
                        + "DISTRIBUTED BY HASH (k) BUCKETS 3\n"
                        + "PROPERTIES(\n"
                        + "    'replication_num' = '1'\n"
                        + ");",
                "CREATE TABLE test.i (\n"
                        + "    k INT,\n"
                        + "    not_grouped INT\n"
                        + ")\n"
                        + "DUPLICATE KEY (k)\n"
                        + "DISTRIBUTED BY HASH (k) BUCKETS 3\n"
                        + "PROPERTIES(\n"
                        + "    'replication_num' = '1'\n"
                        + ");"
        );
    }

    private LogicalApply findApply(Plan plan) {
        List<LogicalApply> applies = plan.collectToList(LogicalApply.class::isInstance)
                .stream().map(node -> (LogicalApply) node).collect(Collectors.toList());
        Assertions.assertEquals(1, applies.size(),
                "expected exactly one LogicalApply in plan:\n" + plan.treeString());
        return applies.get(0);
    }

    private Set<Slot> getCorrelationFilterInputSlots(LogicalApply apply) {
        Optional<Expression> filter = apply.getCorrelationFilter();
        Assertions.assertTrue(filter.isPresent(),
                "apply should record a correlation filter, plan:\n" + apply.treeString());
        Set<Expression> conjuncts = ExpressionUtils.extractConjunctionToSet(filter.get());
        return conjuncts.stream().flatMap(e -> e.getInputSlots().stream()).collect(Collectors.toSet());
    }

    private boolean containsSlotNamed(Set<Slot> slots, String name) {
        return slots.stream().anyMatch(s -> s.getName().equals(name));
    }

    /**
     * qualify -> having -> agg where both the having and the qualify reference correlated outer
     * columns. The window expression in qualify is extracted into a project above the having during
     * NormalizeAggregate; the having's correlated predicate must be conjoined into the qualify so it
     * stays above that window project and is still collected into the apply during unnesting.
     */
    @Test
    public void testCorrelatedQualifyAndHaving() {
        String sql = "SELECT o.k\n"
                + "FROM o\n"
                + "WHERE EXISTS (\n"
                + "  SELECT i.k\n"
                + "  FROM i\n"
                + "  GROUP BY i.k\n"
                + "  HAVING o.h = 1\n"
                + "  QUALIFY row_number() OVER (ORDER BY i.k) = 1 AND o.flag = 1\n"
                + ")\n"
                + "ORDER BY o.k";
        Plan plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyBottomUp(new UnCorrelatedApplyFilter())
                .getPlan();
        LogicalApply apply = findApply(plan);
        Set<Slot> slots = getCorrelationFilterInputSlots(apply);
        // both the qualify correlation (o.flag) and the having correlation (o.h) must be collected
        Assertions.assertTrue(containsSlotNamed(slots, "flag"),
                "correlation filter should reference o.flag, plan:\n" + apply.treeString());
        Assertions.assertTrue(containsSlotNamed(slots, "h"),
                "correlation filter should reference o.h, plan:\n" + apply.treeString());
    }

    /**
     * qualify -> project where the qualify references a project alias (f) whose producer is a
     * correlated outer column (o.flag). The alias-producer dependency must be resolved so the
     * correlation slot is still collected into the apply even though the window expression in the
     * project blocks filter pushdown.
     */
    @Test
    public void testCorrelatedQualifyWithAlias() {
        String sql = "SELECT o.k\n"
                + "FROM o\n"
                + "WHERE EXISTS (\n"
                + "  SELECT i.k, o.flag AS f, row_number() OVER (ORDER BY i.k) AS rn\n"
                + "  FROM i\n"
                + "  QUALIFY rn = 1 AND f = 1\n"
                + ")\n"
                + "ORDER BY o.k";
        Plan plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyBottomUp(new UnCorrelatedApplyFilter())
                .getPlan();
        LogicalApply apply = findApply(plan);
        Set<Slot> slots = getCorrelationFilterInputSlots(apply);
        Assertions.assertTrue(containsSlotNamed(slots, "flag"),
                "correlation filter should reference o.flag (resolved from alias f), plan:\n"
                        + apply.treeString());
    }

    /**
     * A having predicate that mixes outer correlated slots with aggregate results cannot be
     * moved above the window project (it depends on the aggregate rows), and the window project
     * would prevent subquery unnesting from collecting its correlation. Such a shape must be
     * rejected during analysis instead of silently dropping the predicate.
     */
    @Test
    public void testMixedCorrelatedHavingRejected() {
        String sql = "SELECT o.k\n"
                + "FROM o\n"
                + "WHERE EXISTS (\n"
                + "  SELECT i.k\n"
                + "  FROM i\n"
                + "  GROUP BY i.k\n"
                + "  HAVING o.h = sum(i.k)\n"
                + "  QUALIFY row_number() OVER (ORDER BY i.k) = 1 AND o.flag = 1\n"
                + ")\n"
                + "ORDER BY o.k";
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql));
        Assertions.assertTrue(exception.getMessage().contains("not supported"),
                "unexpected exception message: " + exception.getMessage());
    }

    /**
     * qualify -> having -> project where the HAVING references a project alias (f) whose producer
     * is a correlated outer column (o.flag) consumed ONLY by HAVING (not by qualify). The having
     * conjuncts must take part in the alias classification so `HAVING f = 1` is rewritten to
     * `HAVING o.flag = 1`, and the correlation is still collected into the apply even though the
     * window-bearing project blocks filter pushdown.
     */
    @Test
    public void testCorrelatedHavingWithAlias() {
        String sql = "SELECT o.k\n"
                + "FROM o\n"
                + "WHERE EXISTS (\n"
                + "  SELECT i.k, o.flag AS f, row_number() OVER (ORDER BY i.k) AS rn\n"
                + "  FROM i\n"
                + "  HAVING f = 1\n"
                + "  QUALIFY rn = 1\n"
                + ")\n"
                + "ORDER BY o.k";
        Plan plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new PullUpProjectUnderApply())
                .applyBottomUp(new UnCorrelatedApplyFilter())
                .getPlan();
        LogicalApply apply = findApply(plan);
        Set<Slot> slots = getCorrelationFilterInputSlots(apply);
        Assertions.assertTrue(containsSlotNamed(slots, "flag"),
                "correlation filter should reference o.flag (resolved from alias f in HAVING), plan:\n"
                        + apply.treeString());
    }

    /**
     * An aggregate output alias consumed ONLY by HAVING that only depends on outer correlated
     * columns cannot be produced by the aggregate, so the shape is rejected: the having conjuncts
     * take part in the aggregate-output classification too.
     */
    @Test
    public void testCorrelatedHavingAliasGroupedRejected() {
        connectContext.getSessionVariable().setSqlMode(SqlModeHelper.MODE_DEFAULT);
        String sql = "SELECT o.k\n"
                + "FROM o\n"
                + "WHERE EXISTS (\n"
                + "  SELECT i.k, o.flag AS f, row_number() OVER (ORDER BY i.k) AS rn\n"
                + "  FROM i\n"
                + "  GROUP BY i.k\n"
                + "  HAVING f = 1\n"
                + "  QUALIFY rn = 1\n"
                + ")\n"
                + "ORDER BY o.k";
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql));
        Assertions.assertTrue(exception.getMessage().contains("only depends on outer correlated columns"),
                "unexpected exception message: " + exception.getMessage());
    }
}
