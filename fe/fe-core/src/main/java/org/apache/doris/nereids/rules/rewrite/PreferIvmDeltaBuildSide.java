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

import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.model.ColumnStatistic;
import org.apache.doris.statistics.model.Statistics;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Prefer the IVM delta as the build input when statistics cannot guide join reordering. */
public class PreferIvmDeltaBuildSide extends DefaultPlanRewriter<Void> {
    private static final Logger LOG = LogManager.getLogger(PreferIvmDeltaBuildSide.class);

    private final StatementContext statementContext;
    private final SessionVariable sessionVariable;
    private final StatsCalculator statsCalculator;
    private final Map<Plan, Boolean> hasDelta = new IdentityHashMap<>();
    private final Map<Plan, InputEstimate> estimates = new IdentityHashMap<>();

    private PreferIvmDeltaBuildSide(CascadesContext context) {
        statementContext = context.getStatementContext();
        sessionVariable = context.getConnectContext().getSessionVariable();
        statsCalculator = new StatsCalculator(context);
    }

    /** Run on the final logical plan, after the statement's join reorder decision. */
    public static Plan rewrite(Plan plan, CascadesContext context) {
        StatementContext statementContext = context.getStatementContext();
        if (!context.getConnectContext().getSessionVariable().isDisableJoinReorder()
                || context.isLeadingDisableJoinReorder()
                || context.getAndCacheDisableRules().get(RuleType.PREFER_IVM_DELTA_BUILD_SIDE.type())
                || !statementContext.getIvmRewriteContext().filter(IvmRewriteContext::isIncrementalRefresh).isPresent()
                || statementContext.hasSuccessfulJoinOrderHint()) {
            return plan;
        }
        return plan.accept(new PreferIvmDeltaBuildSide(context), null);
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Void unused) {
        LogicalJoin<?, ?> rewritten = (LogicalJoin<?, ?>) visitChildren(this, join, unused);
        // Leave joins reading a materialized CTE in their current direction.
        if ((!rewritten.getJoinType().isLeftOuterJoin() && !rewritten.getJoinType().isLeftAntiJoin())
                || shouldKeepJoinDirection(rewritten)
                || rewritten.containsType(LogicalCTEConsumer.class)
                || !hasDelta(rewritten.left()) || hasDelta(rewritten.right())
                || !shouldUseFallback(rewritten)) {
            return rewritten;
        }

        LogicalJoin<?, ?> swapped = rewritten.swap();
        return rewritten.getJoinType().isLeftOuterJoin()
                ? new LogicalProject<>(ImmutableList.copyOf(rewritten.getOutput()),
                        JoinUtils.adjustJoinConjunctsNullable(swapped))
                : swapped;
    }

    private static boolean shouldKeepJoinDirection(LogicalJoin<?, ?> join) {
        return join.isMarkJoin() || JoinUtils.shouldNestedLoopJoin(join) || join.hasDistributeHint()
                || join.isLeadingJoin() || join.getJoinReorderContext().isSaltJoinGenerated();
    }

    private boolean hasDelta(Plan plan) {
        Boolean cached = hasDelta.get(plan);
        if (cached != null) {
            return cached;
        }
        boolean found = plan instanceof CatalogRelation
                ? statementContext.isIvmDeltaScan(((CatalogRelation) plan).getRelationId())
                : plan.children().stream().anyMatch(this::hasDelta);
        hasDelta.put(plan, found);
        return found;
    }

    private boolean shouldUseFallback(LogicalJoin<?, ?> join) {
        InputEstimate probe = estimate(join.left());
        InputEstimate build = estimate(join.right());
        if (probe == InputEstimate.UNSUPPORTED || build == InputEstimate.UNSUPPORTED
                || (probe.reliable && build.reliable)) {
            return false;
        }
        return build.stats == null || !JoinUtils.checkBroadcastJoinStats(sessionVariable, build.stats.getRowCount(),
                build.stats.computeSize(join.right().getOutput()));
    }

    private InputEstimate estimate(Plan plan) {
        InputEstimate cached = estimates.get(plan);
        if (cached != null) {
            return cached;
        }
        InputEstimate estimate;
        try {
            estimate = estimateInput(plan);
        } catch (Exception e) {
            if (SessionVariable.isFeDebug()) {
                throw e;
            }
            // The normal Memo stats derivation can recover from an estimation failure.
            LOG.warn("Failed to estimate an IVM join input, preserving the join direction", e);
            estimate = InputEstimate.UNSUPPORTED;
        }
        estimates.put(plan, estimate);
        return estimate;
    }

    private InputEstimate estimateInput(Plan plan) {
        if (plan instanceof LogicalOlapScan) {
            return statsCalculator.estimateIvmInputScan((LogicalOlapScan) plan)
                    .map(InputEstimate::new).orElse(InputEstimate.UNKNOWN);
        }
        if (plan instanceof LogicalEmptyRelation) {
            return new InputEstimate(statsCalculator.computeEmptyRelation((LogicalEmptyRelation) plan));
        }
        boolean unionAll = plan instanceof LogicalUnion && ((LogicalUnion) plan).getQualifier() == Qualifier.ALL;
        if (!(plan instanceof LogicalProject || plan instanceof LogicalFilter || plan instanceof LogicalAggregate
                || plan instanceof LogicalJoin || plan instanceof LogicalLimit || unionAll)) {
            return InputEstimate.UNSUPPORTED;
        }

        List<InputEstimate> children = plan.children().stream().map(this::estimate).collect(Collectors.toList());
        if (children.contains(InputEstimate.UNSUPPORTED)) {
            return InputEstimate.UNSUPPORTED;
        }
        if (plan instanceof LogicalProject || plan instanceof LogicalLimit || plan instanceof LogicalFilter) {
            return estimateUnaryInput(plan, children.get(0));
        }
        if (children.stream().anyMatch(child -> !child.reliable)) {
            return InputEstimate.UNKNOWN;
        }
        if (unionAll) {
            return new InputEstimate(statsCalculator.computeUnion((LogicalUnion) plan,
                    children.stream().map(child -> child.stats).collect(Collectors.toList())));
        }
        Statistics first = children.get(0).stats;
        if (plan instanceof LogicalAggregate) {
            LogicalAggregate<?> aggregate = (LogicalAggregate<?>) plan;
            return PlanUtils.fastGetInputSlots(aggregate.getGroupByExpressions()).stream()
                    .allMatch(slot -> isKnown(slot, first))
                    ? new InputEstimate(statsCalculator.computeAggregate(aggregate, first)) : InputEstimate.UNKNOWN;
        }
        Statistics second = children.get(1).stats;
        return plan.getInputSlots().stream().allMatch(slot -> isKnown(slot, first) || isKnown(slot, second))
                ? new InputEstimate(statsCalculator.computeJoin((LogicalJoin<?, ?>) plan, first, second))
                : InputEstimate.UNKNOWN;
    }

    private InputEstimate estimateUnaryInput(Plan plan, InputEstimate input) {
        if (input.stats == null) {
            return InputEstimate.UNKNOWN;
        }
        if (plan instanceof LogicalProject) {
            return new InputEstimate(statsCalculator.computeProject((LogicalProject<?>) plan, input.stats),
                    input.reliable);
        }
        if (plan instanceof LogicalLimit) {
            return new InputEstimate(statsCalculator.computeLimit((LogicalLimit<?>) plan, input.stats),
                    input.reliable);
        }
        if (input.reliable && plan.getInputSlots().stream().allMatch(slot -> isKnown(slot, input.stats))) {
            return new InputEstimate(statsCalculator.computeFilter((LogicalFilter<?>) plan, input.stats));
        }
        // An unestimable filter can only shrink its input. Keep that bound for broadcast eligibility.
        return new InputEstimate(input.stats, false);
    }

    private static boolean isKnown(Slot slot, Statistics stats) {
        ColumnStatistic column = stats.findColumnStatistics(slot);
        return column != null && !column.isUnKnown;
    }

    private static class InputEstimate {
        private static final InputEstimate UNKNOWN = new InputEstimate(null, false);
        private static final InputEstimate UNSUPPORTED = new InputEstimate(null, false);

        private final Statistics stats;
        private final boolean reliable;

        private InputEstimate(Statistics stats) {
            this(stats, true);
        }

        private InputEstimate(Statistics stats, boolean reliable) {
            this.stats = stats;
            this.reliable = reliable;
        }
    }
}
