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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.SessionVarGuardExpr;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Repeat.RepeatType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Every row-set / grouping affecting position of a materialized view plan must be scanned for a cache guard:
 * a guard there means the materialized rows or groups were derived with the creation session semantics, so a
 * query in another session can never compensate them.
 */
public class MaterializedViewCacheGuardTest {
    private static final SessionVarGuardExpr CACHE_GUARD = new SessionVarGuardExpr(
            new DateTrunc(new SlotReference("ts", TimeStampTzType.of(6)), new VarcharLiteral("day")),
            ImmutableMap.of("time_zone", "+08:00"), true);

    private static LogicalOlapScan scan(String name) {
        return PlanConstructor.newLogicalOlapScan(name.equals("l") ? 0 : 1, name, 0);
    }

    @Test
    public void testFilterPredicateGuardIsRowAffecting() {
        Assertions.assertTrue(AbstractMaterializedViewRule.containsCacheGuardInRowAffectingExpressions(
                new LogicalFilter<>(Sets.newHashSet(CACHE_GUARD), scan("l"))));
        Assertions.assertFalse(AbstractMaterializedViewRule.containsCacheGuardInRowAffectingExpressions(
                new LogicalFilter<>(Sets.newHashSet(new SlotReference("id", IntegerType.INSTANCE)), scan("l"))));
    }

    /**
     * HAVING is a separate filter node above the aggregation: a guarded predicate there decides which groups
     * survive, so it must reject the rewrite just like a WHERE predicate does.
     */
    @Test
    public void testHavingPredicateGuardIsRowAffecting() {
        LogicalHaving<Plan> having = new LogicalHaving<>(Sets.newHashSet(CACHE_GUARD), scan("l"));
        Assertions.assertEquals(1, having.getConjuncts().size());
        Assertions.assertTrue(AbstractMaterializedViewRule.containsCacheGuardInRowAffectingExpressions(having));
        Assertions.assertFalse(AbstractMaterializedViewRule.containsCacheGuardInRowAffectingExpressions(
                new LogicalHaving<>(Sets.newHashSet(new SlotReference("id", IntegerType.INSTANCE)), scan("l"))));
    }

    /**
     * The grouping sets of a repeat own the group-by keys, and they are not always projected, so the guard
     * has to be found on the repeat node itself.
     */
    @Test
    public void testRepeatGroupingSetGuardIsRowAffecting() {
        LogicalRepeat<Plan> repeat = new LogicalRepeat<>(
                ImmutableList.of(
                        ImmutableList.of(CACHE_GUARD),
                        ImmutableList.of()),
                ImmutableList.of(new Alias(new SlotReference("GROUPING_ID", IntegerType.INSTANCE), "GROUPING_ID")),
                RepeatType.GROUPING_SETS, scan("l"));
        Assertions.assertTrue(AbstractMaterializedViewRule.containsCacheGuardInRowAffectingExpressions(repeat));
        LogicalRepeat<Plan> unguarded = new LogicalRepeat<>(
                ImmutableList.of(
                        ImmutableList.of(new SlotReference("ts", TimeStampTzType.of(6))),
                        ImmutableList.of()),
                ImmutableList.of(new Alias(new SlotReference("GROUPING_ID", IntegerType.INSTANCE), "GROUPING_ID")),
                RepeatType.GROUPING_SETS, scan("l"));
        Assertions.assertFalse(AbstractMaterializedViewRule.containsCacheGuardInRowAffectingExpressions(unguarded));
    }

    /**
     * A mark join conjunct produces the three-valued mark that becomes part of the row, so a guarded one is
     * as row affecting as a hash or other join conjunct.
     */
    @Test
    public void testMarkJoinConjunctGuardIsRowAffecting() {
        LogicalJoin<Plan, Plan> join = new LogicalJoin<>(JoinType.LEFT_SEMI_JOIN,
                ImmutableList.of(), ImmutableList.of(), ImmutableList.of(CACHE_GUARD),
                new DistributeHint(DistributeType.NONE), Optional.empty(), scan("l"), scan("r"),
                new JoinReorderContext());
        Assertions.assertEquals(1, join.getMarkJoinConjuncts().size());
        Assertions.assertTrue(AbstractMaterializedViewRule.containsCacheGuardInRowAffectingExpressions(join));
        LogicalJoin<Plan, Plan> unguarded = new LogicalJoin<>(JoinType.LEFT_SEMI_JOIN,
                ImmutableList.of(new SlotReference("id", IntegerType.INSTANCE)), ImmutableList.of(),
                ImmutableList.of(new SlotReference("id", IntegerType.INSTANCE)),
                new DistributeHint(DistributeType.NONE), Optional.empty(), scan("l"), scan("r"),
                new JoinReorderContext());
        Assertions.assertFalse(AbstractMaterializedViewRule.containsCacheGuardInRowAffectingExpressions(unguarded));
    }

    /**
     * A predicate that only names a slot produced by a guarded project alias is row affecting too: the
     * creation-session semantics live in the producer expression, not in the predicate. Without following the
     * slot lineage, an MV that materializes only a count over such a filter would look unguarded, because no
     * guarded scan column is mapped for the query to read.
     */
    @Test
    public void testGuardedProjectAliasUsedByRowFilterIsRowAffecting() {
        Alias guardedAlias = new Alias(CACHE_GUARD, "d");
        LogicalProject<Plan> project = new LogicalProject<>(
                ImmutableList.of(guardedAlias, new SlotReference("ts", TimeStampTzType.of(6))), scan("l"));
        LogicalFilter<Plan> filter = new LogicalFilter<>(
                // d > '2023-12-31 00:00:00': the predicate only references the produced slot
                Sets.newHashSet(new GreaterThan(guardedAlias.toSlot(), new VarcharLiteral("2023-12-31 00:00:00"))),
                project);
        Assertions.assertTrue(AbstractMaterializedViewRule.containsCacheGuardInRowAffectingExpressions(filter));

        // the same shape without a guard in the producer stays rewriteable
        Alias unguardedAlias = new Alias(
                new DateTrunc(new SlotReference("ts", TimeStampTzType.of(6)), new VarcharLiteral("day")), "d");
        LogicalProject<Plan> unguardedProject = new LogicalProject<>(
                ImmutableList.of(unguardedAlias, new SlotReference("ts", TimeStampTzType.of(6))), scan("l"));
        LogicalFilter<Plan> unguardedFilter = new LogicalFilter<>(
                Sets.newHashSet(new GreaterThan(unguardedAlias.toSlot(),
                        new VarcharLiteral("2023-12-31 00:00:00"))),
                unguardedProject);
        Assertions.assertFalse(
                AbstractMaterializedViewRule.containsCacheGuardInRowAffectingExpressions(unguardedFilter));
    }

    /**
     * The lineage is followed transitively, so a predicate over a slot that an intermediate project renames
     * still reaches the guarded expression below it - which is what nested subqueries in a definition look
     * like.
     */
    @Test
    public void testGuardedProducerIsFoundThroughNestedProject() {
        Alias guardedAlias = new Alias(CACHE_GUARD, "d");
        LogicalProject<Plan> inner = new LogicalProject<>(
                ImmutableList.of(guardedAlias, new SlotReference("ts", TimeStampTzType.of(6))), scan("l"));
        Alias renamed = new Alias(guardedAlias.toSlot(), "d2");
        LogicalProject<Plan> outer = new LogicalProject<>(ImmutableList.of(renamed), inner);
        LogicalFilter<Plan> filter = new LogicalFilter<>(
                Sets.newHashSet(new GreaterThan(renamed.toSlot(), new VarcharLiteral("2023-12-31 00:00:00"))),
                outer);
        Assertions.assertTrue(AbstractMaterializedViewRule.containsCacheGuardInRowAffectingExpressions(filter));
    }
}
