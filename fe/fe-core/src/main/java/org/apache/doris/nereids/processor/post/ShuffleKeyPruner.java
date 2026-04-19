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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.ShuffleKeyPruneUtils;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalBlackholeSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalConnectorTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDeferMaterializeResultSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDictionarySink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHiveTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIcebergTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalMaxComputeTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRecursiveUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnary;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Post-process shuffle key pruning on final physical plan. {@link PruneCtx#allowShuffleKeyPrune} marks
 * subtrees where shuffle key pruning is allowed; when false, pruning is skipped (e.g. join/setop alignment,
 * sinks).
 */
public class ShuffleKeyPruner extends PlanPostProcessor {

    @Override
    public Plan processRoot(Plan plan, CascadesContext ctx) {
        if (!ctx.getConnectContext().getSessionVariable().enableShuffleKeyPrune) {
            return plan;
        }
        Plan newPlan = plan.accept(new ShuffleKeyPruneRewriter(), new PruneCtx(true, ctx));
        if (newPlan == plan) {
            return plan;
        }
        return newPlan.accept(RecomputePhysicalPropertiesPostProcessor.INSTANCE, ctx);
    }

    private static final class PruneCtx {
        /** When true, shuffle key pruning may run on descendants; when false, skip pruning. */
        final boolean allowShuffleKeyPrune;
        final CascadesContext cascadesContext;

        private PruneCtx(boolean allowShuffleKeyPrune, CascadesContext cascadesContext) {
            this.allowShuffleKeyPrune = allowShuffleKeyPrune;
            this.cascadesContext = cascadesContext;
        }

        PruneCtx withAllowShuffleKeyPrune(boolean allowShuffleKeyPrune) {
            return new PruneCtx(allowShuffleKeyPrune, cascadesContext);
        }
    }

    private static class ShuffleKeyPruneRewriter extends DefaultPlanRewriter<PruneCtx> {
        @Override
        public Plan visitPhysicalDistribute(PhysicalDistribute<? extends Plan> distribute, PruneCtx ctx) {
            return rewriteUnary(distribute, ctx.withAllowShuffleKeyPrune(true));
        }

        @Override
        public Plan visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> join, PruneCtx ctx) {
            Pair<Boolean, Boolean> lr = deriveHashJoinChildAllowShuffleKeyPrune(join, ctx.allowShuffleKeyPrune);
            PhysicalHashJoin<? extends Plan, ? extends Plan> current = rewriteChildren(join,
                    ImmutableList.of(ctx.withAllowShuffleKeyPrune(lr.first),
                            ctx.withAllowShuffleKeyPrune(lr.second)));
            if (ctx.allowShuffleKeyPrune) {
                return maybePruneShuffleJoin(current, ctx.cascadesContext).orElse(current);
            }
            return current;
        }

        @Override
        public Plan visitPhysicalHashAggregate(PhysicalHashAggregate<? extends Plan> agg, PruneCtx ctx) {
            PhysicalHashAggregate<? extends Plan> current;
            if (agg.getAggPhase().isLocal()) {
                current = rewriteUnary(agg, ctx.withAllowShuffleKeyPrune(true));
            } else {
                current = rewriteUnary(agg, ctx);
            }
            if (ctx.allowShuffleKeyPrune && agg.getAggPhase().isGlobal()) {
                return tryPruneGlobalAgg(current, ctx.cascadesContext);
            }
            return current;
        }

        @Override
        public Plan visitPhysicalCTEAnchor(PhysicalCTEAnchor<? extends Plan, ? extends Plan> anchor, PruneCtx ctx) {
            return rewriteChildren(anchor, ImmutableList.of(ctx.withAllowShuffleKeyPrune(true), ctx));
        }

        @Override
        public Plan visitPhysicalWindow(PhysicalWindow<? extends Plan> window, PruneCtx ctx) {
            return rewriteUnary(window, ctx);
        }

        @Override
        public Plan visitPhysicalSetOperation(PhysicalSetOperation setOperation, PruneCtx ctx) {
            return visitChildren(this, setOperation, ctx.withAllowShuffleKeyPrune(false));
        }

        @Override
        public Plan visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows, PruneCtx ctx) {
            return rewriteUnary(assertNumRows, ctx.withAllowShuffleKeyPrune(false));
        }

        @Override
        public Plan visitPhysicalLimit(PhysicalLimit<? extends Plan> limit, PruneCtx ctx) {
            boolean childAllowShuffleKeyPrune = !limit.isGlobal();
            return rewriteUnary(limit, ctx.withAllowShuffleKeyPrune(childAllowShuffleKeyPrune));
        }

        @Override
        public Plan visitAbstractPhysicalSort(AbstractPhysicalSort<? extends Plan> sort, PruneCtx ctx) {
            boolean childAllowShuffleKeyPrune = sort.getSortPhase().isLocal();
            return rewriteUnary(sort, ctx.withAllowShuffleKeyPrune(childAllowShuffleKeyPrune));

        }

        @Override
        public Plan visitPhysicalNestedLoopJoin(
                PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin, PruneCtx ctx) {
            boolean leftAllowShuffleKeyPrune;
            if (nestedLoopJoin.getJoinType().isCrossJoin() || nestedLoopJoin.getJoinType().isInnerJoin()
                    || nestedLoopJoin.getJoinType().isLeftJoin()) {
                leftAllowShuffleKeyPrune = true;
            } else {
                leftAllowShuffleKeyPrune = false;
            }
            return rewriteChildren(nestedLoopJoin,
                    ImmutableList.of(ctx.withAllowShuffleKeyPrune(leftAllowShuffleKeyPrune),
                    ctx.withAllowShuffleKeyPrune(false)));
        }

        @Override
        public Plan visitPhysicalPartitionTopN(PhysicalPartitionTopN<? extends Plan> partitionTopN, PruneCtx ctx) {
            if (partitionTopN.getPhase().isTwoPhaseLocal()) {
                return rewriteUnary(partitionTopN, ctx.withAllowShuffleKeyPrune(true));
            } else {
                return rewriteUnary(partitionTopN, ctx);
            }
        }

        @Override
        public Plan visitPhysicalRecursiveUnion(
                PhysicalRecursiveUnion<? extends Plan, ? extends Plan> recursiveUnion, PruneCtx ctx) {
            return rewriteChildren(recursiveUnion, ImmutableList.of(ctx.withAllowShuffleKeyPrune(false),
                    ctx.withAllowShuffleKeyPrune(false)));
        }

        @Override
        public Plan visitPhysicalCTEProducer(PhysicalCTEProducer<? extends Plan> producer, PruneCtx ctx) {
            return rewriteUnary(producer, ctx.withAllowShuffleKeyPrune(true));
        }

        @Override
        public Plan visitPhysicalGenerate(PhysicalGenerate<? extends Plan> generate, PruneCtx ctx) {
            return rewriteUnary(generate, ctx.withAllowShuffleKeyPrune(true));
        }

        @Override
        public Plan visitPhysicalBlackholeSink(PhysicalBlackholeSink<? extends Plan> sink, PruneCtx ctx) {
            return rewriteUnary(sink, ctx.withAllowShuffleKeyPrune(true));
        }

        @Override
        public Plan visitPhysicalRepeat(PhysicalRepeat<? extends Plan> repeat, PruneCtx ctx) {
            return rewriteUnary(repeat, ctx.withAllowShuffleKeyPrune(true));
        }

        @Override
        public Plan visitPhysicalOlapTableSink(PhysicalOlapTableSink<? extends Plan> sink, PruneCtx ctx) {
            boolean childAllowShuffleKeyPrune;
            if (ctx.cascadesContext.getConnectContext() != null
                    && !ctx.cascadesContext.getConnectContext().getSessionVariable().enableStrictConsistencyDml) {
                childAllowShuffleKeyPrune = true;
            } else {
                childAllowShuffleKeyPrune = sink.getRequirePhysicalProperties().equals(PhysicalProperties.ANY);
            }
            return rewriteUnary(sink, ctx.withAllowShuffleKeyPrune(childAllowShuffleKeyPrune));
        }

        @Override
        public Plan visitPhysicalResultSink(PhysicalResultSink<? extends Plan> sink, PruneCtx ctx) {
            boolean childAllowShuffleKeyPrune = false;
            if (ctx.cascadesContext.getConnectContext() != null
                    && ctx.cascadesContext.getConnectContext().getSessionVariable().enableParallelResultSink()
                    && !ctx.cascadesContext.getStatementContext().isShortCircuitQuery()) {
                childAllowShuffleKeyPrune = true;
            }
            return rewriteUnary(sink, ctx.withAllowShuffleKeyPrune(childAllowShuffleKeyPrune));
        }

        @Override
        public Plan visitPhysicalHiveTableSink(PhysicalHiveTableSink<? extends Plan> hiveTableSink, PruneCtx ctx) {
            boolean childAllowShuffleKeyPrune;
            if (ctx.cascadesContext.getConnectContext() != null
                    && !ctx.cascadesContext.getConnectContext().getSessionVariable().enableStrictConsistencyDml) {
                childAllowShuffleKeyPrune = true;
            } else {
                childAllowShuffleKeyPrune =
                        hiveTableSink.getRequirePhysicalProperties().equals(PhysicalProperties.ANY);
            }
            return rewriteUnary(hiveTableSink, ctx.withAllowShuffleKeyPrune(childAllowShuffleKeyPrune));
        }

        @Override
        public Plan visitPhysicalIcebergTableSink(
                PhysicalIcebergTableSink<? extends Plan> icebergTableSink, PruneCtx ctx) {
            boolean childAllowShuffleKeyPrune;
            if (ctx.cascadesContext.getConnectContext() != null
                    && !ctx.cascadesContext.getConnectContext().getSessionVariable().enableStrictConsistencyDml) {
                childAllowShuffleKeyPrune = true;
            } else {
                childAllowShuffleKeyPrune =
                        icebergTableSink.getRequirePhysicalProperties().equals(PhysicalProperties.ANY);
            }
            return rewriteUnary(icebergTableSink, ctx.withAllowShuffleKeyPrune(childAllowShuffleKeyPrune));
        }

        @Override
        public Plan visitPhysicalMaxComputeTableSink(
                PhysicalMaxComputeTableSink<? extends Plan> mcTableSink, PruneCtx ctx) {
            boolean childAllowShuffleKeyPrune;
            if (ctx.cascadesContext.getConnectContext() != null
                    && !ctx.cascadesContext.getConnectContext().getSessionVariable().enableStrictConsistencyDml) {
                childAllowShuffleKeyPrune = true;
            } else {
                childAllowShuffleKeyPrune = mcTableSink.getRequirePhysicalProperties().equals(
                        PhysicalProperties.ANY);
            }
            return rewriteUnary(mcTableSink, ctx.withAllowShuffleKeyPrune(childAllowShuffleKeyPrune));
        }

        @Override
        public Plan visitPhysicalConnectorTableSink(
                PhysicalConnectorTableSink<? extends Plan> connectorSink, PruneCtx ctx) {
            boolean childAllowShuffleKeyPrune;
            if (connectorSink.getRequirePhysicalProperties().equals(PhysicalProperties.ANY)) {
                childAllowShuffleKeyPrune = true;
            } else {
                childAllowShuffleKeyPrune = false;
            }
            return rewriteUnary(connectorSink, ctx.withAllowShuffleKeyPrune(childAllowShuffleKeyPrune));
        }

        @Override
        public Plan visitPhysicalTVFTableSink(
                PhysicalTVFTableSink<? extends Plan> tvfTableSink, PruneCtx ctx) {
            return rewriteUnary(tvfTableSink, ctx.withAllowShuffleKeyPrune(false));
        }

        @Override
        public Plan visitPhysicalDictionarySink(PhysicalDictionarySink<? extends Plan> dictionarySink,
                PruneCtx ctx) {
            boolean childAllowShuffleKeyPrune = dictionarySink.getRequirePhysicalProperties()
                    .equals(PhysicalProperties.ANY);
            return rewriteUnary(dictionarySink, ctx.withAllowShuffleKeyPrune(childAllowShuffleKeyPrune));
        }

        @Override
        public Plan visitPhysicalDeferMaterializeResultSink(
                PhysicalDeferMaterializeResultSink<? extends Plan> sink,
                PruneCtx ctx) {
            return rewriteUnary(sink, ctx.withAllowShuffleKeyPrune(false));
        }

        private <P extends PhysicalUnary<?>> P rewriteUnary(P plan, PruneCtx ctx) {
            Plan oldChild = plan.child();
            Plan newChild = oldChild.accept(this, ctx);
            if (newChild == oldChild) {
                return plan;
            }
            AbstractPhysicalPlan rewritten = (AbstractPhysicalPlan) plan.withChildren(ImmutableList.of(newChild));
            return (P) rewritten.copyStatsAndGroupIdFrom((AbstractPhysicalPlan) plan);
        }

        private <P extends Plan> P rewriteChildren(P plan, List<PruneCtx> context) {
            ImmutableList.Builder<Plan> newChildren = ImmutableList.builderWithExpectedSize(plan.arity());
            boolean hasNewChildren = false;
            for (int i = 0; i < plan.arity(); ++i) {
                Plan child = plan.child(i);
                Plan newChild = child.accept(this, context.get(i));
                if (newChild != child) {
                    hasNewChildren = true;
                }
                newChildren.add(newChild);
            }

            if (hasNewChildren) {
                P originPlan = plan;
                plan = (P) plan.withChildren(newChildren.build());
                plan = (P) ((AbstractPhysicalPlan) plan).copyStatsAndGroupIdFrom((AbstractPhysicalPlan) originPlan);
            }
            return plan;
        }
    }

    private static Pair<Boolean, Boolean> deriveHashJoinChildAllowShuffleKeyPrune(
            PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            boolean parentAllowShuffleKeyPrune) {
        if (join.isBroadCastJoin()) {
            return Pair.of(parentAllowShuffleKeyPrune, false);
        }
        if (join.shuffleType() == org.apache.doris.nereids.trees.plans.algebra.ShuffleType.shuffle) {
            return Pair.of(false, false);
        }
        return Pair.of(false, false);
    }

    private static Optional<PhysicalHashJoin<? extends Plan, ? extends Plan>> maybePruneShuffleJoin(
            PhysicalHashJoin<? extends Plan, ? extends Plan> join, CascadesContext cascadesContext) {
        if (join.isMarkJoin() && join.getHashJoinConjuncts().isEmpty()) {
            return Optional.empty();
        }
        if (join.isBroadCastJoin()) {
            return Optional.empty();
        }
        Optional<PhysicalDistribute<Plan>> leftDistOpt = findHashDistributeUnderJoinChild(join.left());
        Optional<PhysicalDistribute<Plan>> rightDistOpt = findHashDistributeUnderJoinChild(join.right());
        if (!leftDistOpt.isPresent() || !rightDistOpt.isPresent()) {
            return Optional.empty();
        }
        if (join.getDistributeHint().getSkewInfo() != null) {
            return Optional.empty();
        }
        PhysicalDistribute<Plan> leftDist = leftDistOpt.get();
        PhysicalDistribute<Plan> rightDist = rightDistOpt.get();
        if (!(leftDist.getDistributionSpec() instanceof DistributionSpecHash)
                || !(rightDist.getDistributionSpec() instanceof DistributionSpecHash)) {
            return Optional.empty();
        }
        Statistics leftStats = statisticsForShuffleKeyPruneBelowDistribute(leftDist);
        Statistics rightStats = statisticsForShuffleKeyPruneBelowDistribute(rightDist);
        DistributionSpecHash leftSpec = (DistributionSpecHash) leftDist.getDistributionSpec();
        DistributionSpecHash rightSpec = (DistributionSpecHash) rightDist.getDistributionSpec();
        Optional<Pair<List<ExprId>, List<ExprId>>> optimal =
                ShuffleKeyPruneUtils.tryFindOptimalShuffleKeyForJoinWithDistributeColumns(
                        cascadesContext.getConnectContext(),
                        leftDist.getOrderedShuffledSlots(), rightDist.getOrderedShuffledSlots(),
                        leftSpec.getOrderedShuffledColumns(), rightSpec.getOrderedShuffledColumns(),
                        leftStats, rightStats);
        if (!optimal.isPresent()) {
            return Optional.empty();
        }
        Pair<List<ExprId>, List<ExprId>> keys = optimal.get();
        DistributionSpecHash newLeftSpec = leftSpec.withShuffleExprs(keys.first);
        DistributionSpecHash newRightSpec = rightSpec.withShuffleExprs(keys.second);
        Plan rebuiltLeftDist = rebuildDistribute(leftDist, newLeftSpec, leftDist.child());
        Plan rebuiltRightDist = rebuildDistribute(rightDist, newRightSpec, rightDist.child());
        Plan replacedLeft = replaceDistributeUnderJoinChild(join.left(), rebuiltLeftDist);
        Plan replacedRight = replaceDistributeUnderJoinChild(join.right(), rebuiltRightDist);
        PhysicalHashJoin<Plan, Plan> rewritten =
                asHashJoin(join.withChildren(ImmutableList.of(replacedLeft, replacedRight)));
        return Optional.of((PhysicalHashJoin<? extends Plan, ? extends Plan>) rewritten.copyStatsAndGroupIdFrom(join));
    }

    /**
     * Join child is either {@link PhysicalDistribute}, or {@link PhysicalHashAggregate} (GLOBAL) whose
     * child is {@link PhysicalDistribute}, optionally wrapped by {@link PhysicalProject} and/or
     * {@link PhysicalFilter}; otherwise empty.
     */
    static Optional<PhysicalDistribute<Plan>> findHashDistributeUnderJoinChild(Plan joinChild) {
        if (joinChild instanceof PhysicalDistribute) {
            return Optional.of((PhysicalDistribute<Plan>) joinChild);
        }
        if (joinChild instanceof PhysicalProject || joinChild instanceof PhysicalFilter) {
            return findHashDistributeUnderJoinChild(joinChild.child(0));
        }
        if (joinChild instanceof PhysicalHashAggregate) {
            PhysicalHashAggregate<?> agg = (PhysicalHashAggregate<?>) joinChild;
            if (agg.getAggPhase().isGlobal() && agg.child() instanceof PhysicalDistribute) {
                return Optional.of((PhysicalDistribute<Plan>) agg.child());
            }
        }
        return Optional.empty();
    }

    /**
     * Replaces the target {@link PhysicalDistribute} under a join child with {@code newDistributeRoot}
     * (typically from {@link #rebuildDistribute}). If the join child is the distribute itself, returns
     * {@code newDistributeRoot}; if the join child is GLOBAL agg over that distribute, returns agg with
     * updated child. {@link PhysicalProject} / {@link PhysicalFilter} wrappers are preserved.
     */
    static Plan replaceDistributeUnderJoinChild(Plan joinChild, Plan newDistributeRoot) {
        if (joinChild instanceof PhysicalDistribute) {
            return newDistributeRoot;
        }
        if (joinChild instanceof PhysicalProject) {
            PhysicalProject<?> project = (PhysicalProject<?>) joinChild;
            Plan innerNew = replaceDistributeUnderJoinChild(project.child(), newDistributeRoot);
            if (innerNew == project.child()) {
                return project;
            }
            PhysicalProject<Plan> rewritten = (PhysicalProject<Plan>) project.withChildren(
                    ImmutableList.of(innerNew));
            return rewritten.copyStatsAndGroupIdFrom((AbstractPhysicalPlan) project);
        }
        if (joinChild instanceof PhysicalFilter) {
            PhysicalFilter<?> filter = (PhysicalFilter<?>) joinChild;
            Plan innerNew = replaceDistributeUnderJoinChild(filter.child(), newDistributeRoot);
            if (innerNew == filter.child()) {
                return filter;
            }
            PhysicalFilter<Plan> rewritten = (PhysicalFilter<Plan>) filter.withChildren(
                    ImmutableList.of(innerNew));
            return rewritten.copyStatsAndGroupIdFrom((AbstractPhysicalPlan) filter);
        }
        PhysicalHashAggregate<?> agg = (PhysicalHashAggregate<?>) joinChild;
        PhysicalHashAggregate<Plan> rewritten = (PhysicalHashAggregate<Plan>) agg.withChildren(
                ImmutableList.of(newDistributeRoot));
        return rewritten.copyStatsAndGroupIdFrom((AbstractPhysicalPlan) agg);
    }

    /**
     * Statistics below a shuffle {@link PhysicalDistribute}, aligned with {@link #tryPruneGlobalAgg} for
     * column balance / NDV when the child is local agg or otherwise.
     */
    private static Statistics statisticsForShuffleKeyPruneBelowDistribute(PhysicalDistribute<Plan> dist) {
        if (dist.child() instanceof PhysicalHashAggregate) {
            PhysicalHashAggregate<Plan> childAgg = (PhysicalHashAggregate<Plan>) dist.child();
            if (childAgg.getAggPhase().isLocal()) {
                return childAgg.child().getStats();
            } else {
                return childAgg.getStats();
            }
        } else {
            return dist.child().getStats();
        }
    }

    private static PhysicalHashAggregate<? extends Plan> tryPruneGlobalAgg(PhysicalHashAggregate<? extends Plan> agg,
            CascadesContext cascadesContext) {
        if (!(agg.child() instanceof PhysicalDistribute)) {
            return agg;
        }
        PhysicalDistribute<Plan> dist = (PhysicalDistribute<Plan>) agg.child();
        if (!(dist.getDistributionSpec() instanceof DistributionSpecHash)) {
            return agg;
        }
        if (agg.hasSourceRepeat()) {
            return agg;
        }
        DistributionSpecHash hashSpec = (DistributionSpecHash) dist.getDistributionSpec();
        Statistics childStats = statisticsForShuffleKeyPruneBelowDistribute(dist);

        List<Expression> evalExprs;
        if (agg.getPartitionExpressions().isPresent() && !agg.getPartitionExpressions().get().isEmpty()) {
            evalExprs = agg.getPartitionExpressions().get();
        } else {
            evalExprs = agg.getGroupByExpressions();
        }
        if (evalExprs.isEmpty()) {
            return agg;
        }
        List<ExprId> shuffleExprIds = hashSpec.getOrderedShuffledColumns();
        if (shuffleExprIds.size() < 2) {
            return agg;
        }
        List<Expression> shuffleExprs = new ArrayList<>(hashSpec.getOrderedShuffledColumns().size());
        Map<ExprId, Slot> exprIdSlotMap = new HashMap<>();
        for (Slot slot : agg.getOutput()) {
            exprIdSlotMap.put(slot.getExprId(), slot);
        }
        for (ExprId exprId : hashSpec.getOrderedShuffledColumns()) {
            if (!exprIdSlotMap.containsKey(exprId)) {
                return agg;
            }
            shuffleExprs.add(exprIdSlotMap.get(exprId));
        }

        Optional<List<Expression>> best = ShuffleKeyPruneUtils.selectBestShuffleKeyForAgg(agg, shuffleExprs,
                childStats, cascadesContext.getConnectContext());
        if (!best.isPresent()) {
            return agg;
        }
        List<ExprId> newIds = best.get().stream()
                .filter(SlotReference.class::isInstance)
                .map(SlotReference.class::cast)
                .map(SlotReference::getExprId)
                .collect(Collectors.toList());
        if (newIds.size() != best.get().size() || newIds.size() >= hashSpec.getOrderedShuffledColumns().size()) {
            return agg;
        }
        DistributionSpecHash newSpec = hashSpec.withShuffleExprs(newIds);
        Plan replaced = rebuildDistribute(dist, newSpec, dist.child());
        PhysicalHashAggregate<Plan> rewritten = asAgg(agg.withChildren(ImmutableList.of(replaced)));
        return (PhysicalHashAggregate<Plan>) rewritten.copyStatsAndGroupIdFrom(agg);
    }

    private static DistributionSpecHash sliceHashSpec(DistributionSpecHash origin, List<ExprId> newOrderedKeys) {
        return new DistributionSpecHash(newOrderedKeys, origin.getShuffleType(),
                origin.getTableId(), origin.getSelectedIndexId(), origin.getPartitionIds());
    }

    private static PhysicalDistribute<Plan> rebuildDistribute(PhysicalDistribute<Plan> origin,
            DistributionSpecHash newHashSpec, Plan newChild) {
        PhysicalProperties props = PhysicalProperties.createHash(newHashSpec)
                .withOrderSpec(origin.getPhysicalProperties().getOrderSpec());
        return AbstractPlan.copyWithSameId(origin,
                () -> new PhysicalDistribute<>(newHashSpec, origin.getGroupExpression(),
                        origin.getLogicalProperties(), props, origin.getStats(), newChild));
    }

    @SuppressWarnings("unchecked")
    private static PhysicalHashJoin<Plan, Plan> asHashJoin(Plan join) {
        return (PhysicalHashJoin<Plan, Plan>) join;
    }

    @SuppressWarnings("unchecked")
    private static PhysicalHashAggregate<Plan> asAgg(Plan agg) {
        return (PhysicalHashAggregate<Plan>) agg;
    }
}
