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
import org.apache.doris.nereids.hint.OrderedHint;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

/** Prefer the IVM delta as the build input when statistics cannot guide join reordering. */
public class PreferIvmDeltaBuildSide extends DefaultPlanRewriter<Void> {
    private final StatementContext statementContext;
    private final StatsCalculator statsCalculator;
    private final Map<CTEId, LogicalCTEProducer<?>> producers = new HashMap<>();
    private final Map<RelationId, Boolean> hasUnreliableStats = new HashMap<>();

    private PreferIvmDeltaBuildSide(Plan plan, CascadesContext context) {
        statementContext = context.getStatementContext();
        statsCalculator = new StatsCalculator(context);
        for (LogicalCTEProducer<?> producer : plan.<LogicalCTEProducer<?>>collectToList(
                LogicalCTEProducer.class::isInstance)) {
            producers.put(producer.getCteId(), producer);
        }
    }

    /** Run on the final logical plan, after the statement's join reorder decision. */
    public static Plan rewrite(Plan plan, CascadesContext context) {
        StatementContext statementContext = context.getStatementContext();
        if (!context.getConnectContext().getSessionVariable().isDisableJoinReorder()
                || context.isLeadingDisableJoinReorder()
                || context.getAndCacheDisableRules().get(RuleType.PREFER_IVM_DELTA_BUILD_SIDE.type())
                || !statementContext.getIvmRewriteContext().filter(IvmRewriteContext::isIncrementalRefresh).isPresent()
                || statementContext.getHints().stream().anyMatch(hint -> hint instanceof OrderedHint
                        && hint.isSuccess())) {
            return plan;
        }
        return plan.accept(new PreferIvmDeltaBuildSide(plan, context), null);
    }

    @Override
    public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Void unused) {
        LogicalJoin<?, ?> rewritten = (LogicalJoin<?, ?>) visitChildren(this, join, unused);
        if ((!rewritten.getJoinType().isLeftOuterJoin() && !rewritten.getJoinType().isLeftAntiJoin())
                || shouldKeepJoinDirection(rewritten)
                || !anyInputScanMatches(rewritten.left(), scan -> statementContext.isIvmDeltaScan(scan.getRelationId()))
                || anyInputScanMatches(rewritten.right(), scan -> statementContext.isIvmDeltaScan(scan.getRelationId()))
                || !anyInputScanMatches(rewritten, this::hasUnreliableStats)) {
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

    private boolean hasUnreliableStats(CatalogRelation scan) {
        return hasUnreliableStats.computeIfAbsent(scan.getRelationId(),
                id -> statsCalculator.hasUnreliableScanStats(scan));
    }

    private boolean anyInputScanMatches(Plan plan, Predicate<CatalogRelation> test) {
        if (plan instanceof CatalogRelation) {
            return test.test((CatalogRelation) plan);
        }
        if (plan instanceof LogicalCTEConsumer) {
            LogicalCTEProducer<?> producer = producers.get(((LogicalCTEConsumer) plan).getCteId());
            return producer != null && anyInputScanMatches(producer.child(), test);
        }
        if (plan instanceof LogicalCTEAnchor) {
            return anyInputScanMatches(plan.child(1), test);
        }
        return plan.children().stream().anyMatch(child -> anyInputScanMatches(child, test));
    }
}
