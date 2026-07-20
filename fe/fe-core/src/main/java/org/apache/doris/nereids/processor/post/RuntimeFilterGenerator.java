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
import org.apache.doris.nereids.stats.ExpressionEstimation;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.RuntimeFilterTypeHelper;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * generate runtime filter
 */
public class RuntimeFilterGenerator extends PlanPostProcessor {

    public static final ImmutableSet<JoinType> DENIED_JOIN_TYPES = ImmutableSet.of(
            JoinType.LEFT_ANTI_JOIN,
            JoinType.FULL_OUTER_JOIN,
            JoinType.LEFT_OUTER_JOIN,
            JoinType.ASOF_LEFT_OUTER_JOIN,
            JoinType.NULL_AWARE_LEFT_ANTI_JOIN
    );

    private static final Logger LOG = LogManager.getLogger(RuntimeFilterGenerator.class);

    private static final Set<Class<? extends PhysicalPlan>> SPJ_PLAN = ImmutableSet.of(
            PhysicalRelation.class,
            PhysicalProject.class,
            PhysicalFilter.class,
            PhysicalDistribute.class,
            PhysicalHashJoin.class
    );

    public RuntimeFilterGenerator() {
    }

    @Override
    public Plan processRoot(Plan plan, CascadesContext ctx) {
        if (!plan.containsType(Join.class) && !plan.containsType(SetOperation.class)) {
            return plan;
        }

        Plan result = plan.accept(this, ctx);
        // try to push rf inside CTEProducer
        // collect cteProducers
        RuntimeFilterContext rfCtx = ctx.getRuntimeFilterContext();
        Map<CTEId, PhysicalCTEProducer> cteProducerMap = plan.collect(PhysicalCTEProducer.class::isInstance)
                .stream().collect(Collectors.toMap(p -> ((PhysicalCTEProducer) p).getCteId(),
                        p -> (PhysicalCTEProducer) p));
        // collect cteConsumers which are RF targets
        Map<CTEId, Set<PhysicalCTEConsumer>> cteIdToConsumersWithRF = Maps.newHashMap();
        Map<PhysicalCTEConsumer, Set<RuntimeFilter>> consumerToRFs = Maps.newHashMap();
        Map<PhysicalCTEConsumer, Set<Expression>> consumerToSrcExpression = Maps.newHashMap();
        List<RuntimeFilter> allRFs = rfCtx.getNereidsRuntimeFilter();
        for (RuntimeFilter rf : allRFs) {
            PhysicalRelation rel = rf.getTargetScan();
            if (rel instanceof PhysicalCTEConsumer) {
                PhysicalCTEConsumer consumer = (PhysicalCTEConsumer) rel;
                CTEId cteId = consumer.getCteId();
                cteIdToConsumersWithRF.computeIfAbsent(cteId, key -> Sets.newHashSet()).add(consumer);
                consumerToRFs.computeIfAbsent(consumer, key -> Sets.newHashSet()).add(rf);
                consumerToSrcExpression.computeIfAbsent(consumer, key -> Sets.newHashSet())
                        .add(rf.getSrcExpr());
            }
        }
        for (CTEId cteId : cteIdToConsumersWithRF.keySet()) {
            // if any consumer does not have RF, RF cannot be pushed down.
            // cteIdToConsumersWithRF.get(cteId).size() can not be 1, o.w. this cte will be inlined.
            if (ctx.getCteIdToConsumers().get(cteId).size() == cteIdToConsumersWithRF.get(cteId).size()
                        && cteIdToConsumersWithRF.get(cteId).size() >= 2) {
                // check if there is a common srcExpr among all the consumers
                Set<PhysicalCTEConsumer> consumers = cteIdToConsumersWithRF.get(cteId);
                PhysicalCTEConsumer consumer0 = consumers.iterator().next();
                Set<Expression> candidateSrcExpressions = consumerToSrcExpression.get(consumer0);
                for (PhysicalCTEConsumer currentConsumer : consumers) {
                    Set<Expression> srcExpressionsOnCurrentConsumer = consumerToSrcExpression.get(currentConsumer);
                    candidateSrcExpressions.retainAll(srcExpressionsOnCurrentConsumer);
                    if (candidateSrcExpressions.isEmpty()) {
                        break;
                    }
                }
                if (!candidateSrcExpressions.isEmpty()) {
                    // find RFs to push down
                    for (Expression srcExpr : candidateSrcExpressions) {
                        List<RuntimeFilter> rfsToPushDown = Lists.newArrayList();
                        for (PhysicalCTEConsumer consumer : cteIdToConsumersWithRF.get(cteId)) {
                            for (RuntimeFilter rf : consumerToRFs.get(consumer)) {
                                if (rf.getSrcExpr().equals(srcExpr)) {
                                    rfsToPushDown.add(rf);
                                }
                            }
                        }
                        if (rfsToPushDown.isEmpty()) {
                            break;
                        }
                        if (!canPushDownRuntimeFiltersIntoCTEProducer(rfsToPushDown, cteId)) {
                            continue;
                        }

                        // the most right deep buildNode from rfsToPushDown is used as buildNode for pushDown rf
                        // since the srcExpr are the same, all buildNodes of rfToPushDown are in the same tree path
                        // the longest ancestors means its corresponding rf build node is the most right deep one.
                        List<RuntimeFilter> rightDeepRfs = Lists.newArrayList();
                        List<Plan> rightDeepAncestors = rfsToPushDown.get(0).getBuilderNode().getAncestors();
                        int rightDeepAncestorsSize = rightDeepAncestors.size();
                        RuntimeFilter leftTop = rfsToPushDown.get(0);
                        int leftTopAncestorsSize = rightDeepAncestorsSize;
                        for (RuntimeFilter rf : rfsToPushDown) {
                            List<Plan> ancestors = rf.getBuilderNode().getAncestors();
                            int currentAncestorsSize = ancestors.size();
                            if (currentAncestorsSize >= rightDeepAncestorsSize) {
                                if (currentAncestorsSize == rightDeepAncestorsSize) {
                                    rightDeepRfs.add(rf);
                                } else {
                                    rightDeepAncestorsSize = currentAncestorsSize;
                                    rightDeepAncestors = ancestors;
                                    rightDeepRfs.clear();
                                    rightDeepRfs.add(rf);
                                }
                            }
                            if (currentAncestorsSize < leftTopAncestorsSize) {
                                leftTopAncestorsSize = currentAncestorsSize;
                                leftTop = rf;
                            }
                        }
                        Preconditions.checkArgument(rightDeepAncestors.contains(leftTop.getBuilderNode()));
                        // check nodes between right deep and left top are SPJ and not denied join and not mark join
                        boolean valid = true;
                        for (Plan cursor : rightDeepAncestors) {
                            if (cursor.equals(leftTop.getBuilderNode())) {
                                break;
                            }
                            // valid = valid && SPJ_PLAN.contains(cursor.getClass());
                            if (cursor instanceof AbstractPhysicalJoin) {
                                AbstractPhysicalJoin cursorJoin = (AbstractPhysicalJoin) cursor;
                                valid = (!RuntimeFilterGenerator.DENIED_JOIN_TYPES
                                        .contains(cursorJoin.getJoinType())
                                        || cursorJoin.isMarkJoin()) && valid;
                            }
                            if (!valid) {
                                break;
                            }
                        }

                        if (!valid) {
                            break;
                        }

                        for (RuntimeFilter rfToPush : rightDeepRfs) {
                            Expression rightDeepTargetExpressionOnCTE = null;
                            PhysicalRelation rel = rfToPush.getTargetScan();
                            if (rel instanceof PhysicalCTEConsumer
                                    && ((PhysicalCTEConsumer) rel).getCteId().equals(cteId)) {
                                rightDeepTargetExpressionOnCTE = rfToPush.getTargetExpression();
                            }

                            boolean pushedDown = doPushDownIntoCTEProducerInternal(
                                    rfToPush,
                                    rightDeepTargetExpressionOnCTE,
                                    rfCtx,
                                    cteProducerMap.get(cteId)
                            );
                            if (pushedDown) {
                                rfCtx.removeFilter(
                                        rfToPush,
                                        rightDeepTargetExpressionOnCTE.getInputSlotExprIds().iterator().next());
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    /**
     * the runtime filter generator run at the phase of post process and plan translation of nereids planner.
     * post process:
     * first step: if encounter supported join type, generate nereids runtime filter for all the hash conjunctions
     * and make association from exprId of the target slot references to the runtime filter. or delete the runtime
     * filter whose target slot reference is one of the output slot references of the left child of the physical join as
     * the runtime filter.
     * second step: if encounter project, collect the association of its child and it for pushing down through
     * the project node.
     * plan translation:
     * third step: generate nereids runtime filter target at scan node fragment.
     * forth step: generate legacy runtime filter target and runtime filter at hash join node fragment.
     * NOTICE: bottom-up travel the plan tree!!!
     */
    // TODO: current support inner join, cross join, right outer join, and will support more join type.
    @Override
    public PhysicalPlan visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            CascadesContext context) {
        join.right().accept(this, context);
        join.left().accept(this, context);
        if (RuntimeFilterGenerator.DENIED_JOIN_TYPES.contains(join.getJoinType()) || join.isMarkJoin()) {
            // do not generate RF on this join
            return join;
        }
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        List<TRuntimeFilterType> legalTypes = RuntimeFilterTypeHelper.getSupportedRuntimeFilterTypes().stream()
                .filter(type -> ctx.getSessionVariable().allowedRuntimeFilterType(type))
                .collect(Collectors.toList());

        List<Expression> hashJoinConjuncts = join.getHashJoinConjuncts();

        for (int i = 0; i < hashJoinConjuncts.size(); i++) {
            EqualPredicate equalTo = JoinUtils.swapEqualToForChildrenOrder(
                    (EqualPredicate) hashJoinConjuncts.get(i), join.left().getOutputSet());
            if (isUniqueValueEqualTo(join, equalTo)) {
                continue;
            }
            for (TRuntimeFilterType type : legalTypes) {
                long buildSideNdv = getBuildSideNdv(join, equalTo);
                if (equalTo.left().getInputSlots().size() == 1) {
                    RuntimeFilterPushDownVisitor.PushDownContext pushDownContext =
                            RuntimeFilterPushDownVisitor.PushDownContext.createPushDownContext(
                                    ctx, join, equalTo.right(), equalTo.left(),
                                    type, context.getStatementContext().isHasUnknownColStats(),
                                    buildSideNdv, i);
                    // pushDownContext is not valid, if the target is an agg result.
                    // Currently, we only apply RF on PhysicalScan. So skip this rf.
                    // example: (select sum(x) as s from A) T join B on T.s=B.s
                    if (pushDownContext.isValid()) {
                        join.accept(new RuntimeFilterPushDownVisitor(), pushDownContext);
                    }
                }
            }
        }

        // Decoupled RF: for each equi-conjunct, check if the probe-side expression originates
        // from a descendant join's build side. If so, generate an RF produced by that descendant
        // join and pushed down into this join's build subtree.
        if (ctx.getSessionVariable().enableDecoupledRuntimeFilter) {
            generateDecoupledRuntimeFilters(join, hashJoinConjuncts, legalTypes, ctx, context);
        }

        return join;
    }

    /**
     *
     * T1 join T1 on T1.a=T2.a where T1.a=1 and T2.a=1
     * if T1.a = T2.a, no need to generate RF by "T1.a=T2.a"
     *
     * T1 join T1 on T1.a=T2.a where T1.a in (1, 2) and T2.a in (1, 2)
     * in above case RF: T2.a->T1.a is generated, because the limitation of
     * const propagation
     */
    private boolean isUniqueValueEqualTo(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
                                         EqualPredicate equalTo) {
        if (equalTo.left() instanceof Slot && equalTo.right() instanceof Slot) {
            Optional<Expression> leftValue = join.left().getLogicalProperties()
                    .getTrait().getUniformValue((Slot) equalTo.left());
            Optional<Expression> rightValue = join.right().getLogicalProperties()
                    .getTrait().getUniformValue((Slot) equalTo.right());
            if (leftValue != null && rightValue != null) {
                if (leftValue.isPresent() && rightValue.isPresent()) {
                    if (leftValue.get().equals(rightValue.get())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Generate decoupled runtime filters for the given join.
     *
     * For each equi-conjunct left=probe, right=build:
     *   Walk the probe subtree to find a descendant join whose build side contains
     *   the probe expression. If found, create an RF produced by that descendant join,
     *   pushed down into this join's build subtree to reach the target scan.
     *
     * Decision logic to avoid circular wait between standard and decoupled RFs:
     * - Stats known: if decoupled_ndv / standard_ndv < threshold → prefer decoupled (remove standard)
     *                else → keep both, decoupled is non-blocking (wait_time=0)
     * - Stats unknown: if probe subtree has filters but build subtree doesn't → prefer decoupled
     *                  else → keep both, decoupled is non-blocking
     */
    private void generateDecoupledRuntimeFilters(
            PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            List<Expression> hashJoinConjuncts,
            List<TRuntimeFilterType> legalTypes,
            RuntimeFilterContext ctx,
            CascadesContext context) {
        // Only generate decoupled RFs for INNER/CROSS joins. For SEMI/ANTI/OUTER joins,
        // the standard RF has specialized semantics and a reverse-direction decoupled RF
        // may interfere or be semantically incorrect.
        if (!join.getJoinType().isInnerJoin() && !join.getJoinType().isCrossJoin()) {
            return;
        }
        double ndvRatioThreshold = ctx.getSessionVariable().decoupledRfNdvRatioThreshold;

        for (int i = 0; i < hashJoinConjuncts.size(); i++) {
            EqualPredicate equalTo = JoinUtils.swapEqualToForChildrenOrder(
                    (EqualPredicate) hashJoinConjuncts.get(i), join.left().getOutputSet());
            Expression probeExpr = equalTo.left();
            Expression buildExpr = equalTo.right();
            if (buildExpr.getInputSlots().size() != 1) {
                continue;
            }
            Pair<AbstractPhysicalJoin<?, ?>, Expression> result =
                    findBuilderForDecoupledRf(probeExpr, join.left());
            if (result == null) {
                continue;
            }
            AbstractPhysicalJoin<?, ?> decoupledBuilder = result.first;
            Expression resolvedSrcExpr = result.second;

            long decoupledNdv = getDecoupledBuildSideNdv(decoupledBuilder, resolvedSrcExpr);
            // Use strict NDV (returns -1 for unknown) for the preference comparison,
            // to avoid biased ratio when one side has real NDV and the other uses rowCount fallback.
            long strictDecoupledNdv = getStrictNdv(decoupledBuilder.right(), resolvedSrcExpr);
            long strictStandardNdv = getStrictNdv(join.right(), equalTo.right());

            // Prune decoupled RFs that won't be selective enough when stats are unknown.
            // With known stats: always create the decoupled RF — shouldPreferDecoupledRf()
            // decides below whether to prefer it or keep both with decoupled non-blocking.
            // Without stats: if the builder's build side has no filter predicates,
            // it likely outputs all distinct values -> non-selective.
            if (!(strictDecoupledNdv > 0 && strictStandardNdv > 0)
                    && !hasFilterInSubtree(decoupledBuilder.right())) {
                continue;
            }

            boolean preferDecoupled = shouldPreferDecoupledRf(
                    strictDecoupledNdv, strictStandardNdv, ndvRatioThreshold,
                    decoupledBuilder, join);

            for (TRuntimeFilterType type : legalTypes) {
                RuntimeFilterPushDownVisitor.PushDownContext pushDownContext =
                        RuntimeFilterPushDownVisitor.PushDownContext.createPushDownContext(
                                ctx, decoupledBuilder, resolvedSrcExpr, buildExpr, type,
                                context.getStatementContext().isHasUnknownColStats(),
                                decoupledNdv, -1 /*sentinel: decoupled RF*/);
                boolean decoupledRfPushed = false;
                if (pushDownContext.isValid()) {
                    decoupledRfPushed = join.right().accept(
                            new RuntimeFilterPushDownVisitor(), pushDownContext);
                }
                if (decoupledRfPushed) {
                    if (preferDecoupled) {
                        // When preferring decoupled RF, make the standard RF non-blocking.
                        // This preserves both filters while prioritizing the decoupled RF
                        // (which blocks). The standard RF still applies if it arrives in
                        // time (with wait_time=0).
                        markStandardRfAsNonBlocking(ctx, join, equalTo.right(), type, i);
                    } else {
                        markDecoupledRfAsNonBlocking(ctx, pushDownContext);
                    }
                }
            }
        }
    }

    /**
     * Decide whether decoupled RF should replace the standard RF.
     *
     * @return true if decoupled RF is preferred (standard RF should be removed);
     *         false means keep both but decoupled RF will be non-blocking.
     */
    private boolean shouldPreferDecoupledRf(
            long decoupledNdv, long standardNdv, double ndvRatioThreshold,
            AbstractPhysicalJoin<?, ?> decoupledBuilder,
            PhysicalHashJoin<? extends Plan, ? extends Plan> conditionJoin) {
        boolean statsKnown = decoupledNdv > 0 && standardNdv > 0;
        if (statsKnown) {
            double ratio = (double) decoupledNdv / standardNdv;
            return ratio < ndvRatioThreshold;
        }
        // Unknown stats: use filter-presence heuristic.
        // The decoupled RF's source is in the probe subtree (via decoupledBuilder),
        // the standard RF's source is in conditionJoin's build subtree.
        // If probe source has filters but build source doesn't → decoupled RF is more selective.
        boolean probeHasFilter = hasFilterInSubtree(decoupledBuilder.right());
        boolean buildHasFilter = hasFilterInSubtree(conditionJoin.right());
        return probeHasFilter && !buildHasFilter;
    }

    /**
     * Check if the given subtree contains a PhysicalFilter with visible predicates.
     * Walks through Project, Filter, Distribute, and Join nodes.
     */
    private boolean hasFilterInSubtree(Plan subtree) {
        if (subtree instanceof PhysicalFilter) {
            PhysicalFilter<?> filter = (PhysicalFilter<?>) subtree;
            for (Expression expr : filter.getExpressions()) {
                for (Slot slot : expr.getInputSlots()) {
                    if (slot instanceof SlotReference) {
                        SlotReference slotRef = (SlotReference) slot;
                        if (!slotRef.getOriginalColumn().isPresent()
                                || slotRef.getOriginalColumn().get().isVisible()) {
                            return true;
                        }
                    }
                }
            }
        }
        for (Plan child : subtree.children()) {
            if (hasFilterInSubtree(child)) {
                return true;
            }
        }
        return false;
    }

    /**
     * After push-down, find the newly created decoupled RF and mark it as non-blocking.
     * The decoupled RF was just pushed down and registered in targetExprIdToFilter.
     * We identify it by exprOrder == -1 and the matching builderNode.
     */
    private void markDecoupledRfAsNonBlocking(
            RuntimeFilterContext ctx,
            RuntimeFilterPushDownVisitor.PushDownContext pushDownContext) {
        for (List<RuntimeFilter> filters : ctx.getTargetExprIdToFilter().values()) {
            for (RuntimeFilter rf : filters) {
                if (rf.getExprOrder() == -1
                        && rf.getBuilderNode().equals(pushDownContext.builderNode)
                        && rf.getSrcExpr().equals(pushDownContext.srcExpr)
                        && rf.getType().equals(pushDownContext.type)) {
                    rf.setNonBlocking(true);
                }
            }
        }
    }

    private void markStandardRfAsNonBlocking(
            RuntimeFilterContext ctx,
            PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            Expression srcExpr,
            TRuntimeFilterType type,
            int exprOrder) {
        for (RuntimeFilter rf : ctx.getNereidsRuntimeFilter()) {
            if (rf.getExprOrder() == exprOrder
                    && rf.getBuilderNode().equals(join)
                    && rf.getSrcExpr().equals(srcExpr)
                    && rf.getType().equals(type)) {
                rf.setNonBlocking(true);
            }
        }
    }

    /**
     * Walk the probe subtree to find a descendant join whose build side (right child)
     * contains all input slots of the given expression.
     *
     * @return Pair of (builderNode, resolvedSrcExpr) where resolvedSrcExpr is the
     *         expression rewritten through any intermediate Projects, or null if not found.
     */
    private Pair<AbstractPhysicalJoin<?, ?>, Expression> findBuilderForDecoupledRf(
            Expression expr, Plan subtree) {
        //Constraints:
        //   - Only INNER/CROSS joins on the path (OUTER/ANTI/SEMI block traversal)
        //   - Expr must come from a join's build side (not a leaf scan directly)
        if (subtree instanceof AbstractPhysicalJoin) {
            AbstractPhysicalJoin<?, ?> join = (AbstractPhysicalJoin<?, ?>) subtree;
            if (join.getJoinType() != JoinType.INNER_JOIN
                    && join.getJoinType() != JoinType.CROSS_JOIN) {
                return null;
            }
            if (join.isMarkJoin()) {
                return null;
            }
            if (join.right().getOutputSet().containsAll(expr.getInputSlots())) {
                // Try to find a deeper builder within the build subtree.
                // A deeper builder produces the bloom filter earlier because it's built
                // from a smaller, more selective dataset (e.g., a filtered dimension table).
                Pair<AbstractPhysicalJoin<?, ?>, Expression> deeper =
                        tryFindDeeperBuilder(expr, join.right());
                return deeper != null ? deeper : Pair.of(join, expr);
            }
            if (join.left().getOutputSet().containsAll(expr.getInputSlots())) {
                return findBuilderForDecoupledRf(expr, join.left());
            }
        } else if (subtree instanceof PhysicalProject) {
            PhysicalProject<?> project = (PhysicalProject<?>) subtree;
            Map<Slot, Expression> replaceMap = ExpressionUtils.generateReplaceMap(project.getProjects());
            Expression rewritten = expr.rewriteDownShortCircuit(e -> replaceMap.getOrDefault(e, e));
            if (rewritten.getInputSlots().size() == 1) {
                return findBuilderForDecoupledRf(rewritten, project.child());
            }
        } else if (subtree instanceof PhysicalDistribute || subtree instanceof PhysicalFilter) {
            // Transparent operators: pass through without expression rewriting
            return findBuilderForDecoupledRf(expr, subtree.child(0));
        }
        return null;
    }

    /**
     * Dive into the build subtree to find a deeper join that can serve as the decoupled RF
     * builder. When expr is on the probe side of an inner join and an equi-condition maps it
     * to the build side, the inner join is a better builder — its bloom filter is produced
     * earlier (when the smaller build-side hash table is ready) rather than waiting for the
     * entire join result.
     *
     * Example: plan is orders ⋈ (lineitem ⋈ part[filter]) with expr=l_partkey.
     *   l_partkey is on the probe side of lineitem ⋈ part, equi-cond l_partkey = p_partkey
     *   maps to p_partkey on part (build side). Returning (lineitem ⋈ part, p_partkey) means
     *   the bloom filter is built from part's hash table, ready immediately after part scan.
     */
    private Pair<AbstractPhysicalJoin<?, ?>, Expression> tryFindDeeperBuilder(
            Expression expr, Plan subtree) {
        if (subtree instanceof AbstractPhysicalJoin) {
            AbstractPhysicalJoin<?, ?> join = (AbstractPhysicalJoin<?, ?>) subtree;
            if (join.getJoinType() != JoinType.INNER_JOIN
                    && join.getJoinType() != JoinType.CROSS_JOIN) {
                return null;
            }
            if (join.isMarkJoin()) {
                return null;
            }
            if (join.right().getOutputSet().containsAll(expr.getInputSlots())) {
                // expr is already on the build side; try to go even deeper
                Pair<AbstractPhysicalJoin<?, ?>, Expression> deeper =
                        tryFindDeeperBuilder(expr, join.right());
                return deeper != null ? deeper : Pair.of(join, expr);
            }
            if (join.left().getOutputSet().containsAll(expr.getInputSlots())) {
                // expr is on the probe side; check equi-conditions for equivalent build expr
                Expression buildEquiv = findEquivalentBuildExpr(expr, join);
                if (buildEquiv != null) {
                    Pair<AbstractPhysicalJoin<?, ?>, Expression> deeper =
                            tryFindDeeperBuilder(buildEquiv, join.right());
                    return deeper != null ? deeper : Pair.of(join, buildEquiv);
                }
                // No equi-condition maps expr to build side; try through probe side
                return tryFindDeeperBuilder(expr, join.left());
            }
        } else if (subtree instanceof PhysicalProject) {
            PhysicalProject<?> project = (PhysicalProject<?>) subtree;
            Map<Slot, Expression> replaceMap = ExpressionUtils.generateReplaceMap(project.getProjects());
            Expression rewritten = expr.rewriteDownShortCircuit(e -> replaceMap.getOrDefault(e, e));
            if (rewritten.getInputSlots().size() == 1) {
                return tryFindDeeperBuilder(rewritten, project.child());
            }
        } else if (subtree instanceof PhysicalDistribute || subtree instanceof PhysicalFilter) {
            return tryFindDeeperBuilder(expr, subtree.child(0));
        }
        return null;
    }

    /**
     * Find an equivalent build-side expression through a join's equi-conditions.
     * For example, if expr is l_partkey (probe) and join has l_partkey = p_partkey,
     * returns p_partkey (build side).
     */
    private Expression findEquivalentBuildExpr(Expression expr, AbstractPhysicalJoin<?, ?> join) {
        Set<Slot> exprSlots = expr.getInputSlots();
        Set<Slot> leftOutputSet = join.left().getOutputSet();
        for (Expression condition : join.getHashJoinConjuncts()) {
            if (condition instanceof EqualPredicate) {
                EqualPredicate eq = JoinUtils.swapEqualToForChildrenOrder(
                        (EqualPredicate) condition, leftOutputSet);
                if (eq.left().getInputSlots().equals(exprSlots)
                        && eq.right().getInputSlots().size() == 1) {
                    // Rewrite the full expression shape by replacing the equivalent slot,
                    // rather than returning only the right-side slot. This preserves the
                    // expression structure through descendant joins. For example, when
                    // expr = t1.k + 1 and the equi-cond is t1.k = t2.k, we return
                    // t2.k + 1 instead of just t2.k.
                    Map<Expression, Expression> replaceMap = new HashMap<>();
                    replaceMap.put(eq.left(), eq.right());
                    return expr.rewriteDownShortCircuit(e -> replaceMap.getOrDefault(e, e));
                }
            }
        }
        return null;
    }

    /**
     * Get NDV estimate for the decoupled RF's source expression on the builder's build side.
     * Used for bloom filter sizing — returns rowCount fallback when column stats are unknown.
     */
    private long getDecoupledBuildSideNdv(AbstractPhysicalJoin<?, ?> builder, Expression srcExpr) {
        AbstractPlan right = (AbstractPlan) builder.right();
        if (right.getStats() == null) {
            return -1L;
        }
        ExpressionEstimation estimator = new ExpressionEstimation();
        ColumnStatistic colStats = srcExpr.accept(estimator, right.getStats());
        return colStats.isUnKnown
                ? Math.max(1, (long) right.getStats().getRowCount()) : Math.max(1, (long) colStats.ndv);
    }

    /**
     * Get strict NDV estimate for a plan's expression — returns -1 when column stats are unknown.
     * Used only for NDV ratio comparison in decoupled RF decision, where rowCount fallback
     * would introduce bias (rowCount != NDV).
     */
    private long getStrictNdv(Plan planNode, Expression expr) {
        AbstractPlan plan = (AbstractPlan) planNode;
        if (plan.getStats() == null) {
            return -1L;
        }
        ExpressionEstimation estimator = new ExpressionEstimation();
        ColumnStatistic colStats = expr.accept(estimator, plan.getStats());
        return colStats.isUnKnown ? -1L : Math.max(1, (long) colStats.ndv);
    }

    @Override
    public PhysicalCTEConsumer visitPhysicalCTEConsumer(PhysicalCTEConsumer scan, CascadesContext context) {
        return scan;
    }

    /**
     * A join B on B.x < A.x
     * transform B.x < A.x to A.x > B.x,
     * otherwise return null
     */
    private ComparisonPredicate normalizeNonEqual(AbstractPhysicalJoin<? extends Plan, ? extends Plan> join,
                                                  Expression expr) {
        if (!(expr instanceof ComparisonPredicate)) {
            return null;
        }
        if (!(expr instanceof LessThan) && !(expr instanceof LessThanEqual)
                && !(expr instanceof GreaterThanEqual) && !(expr instanceof GreaterThan)) {
            return null;
        }
        if (!(expr.child(0) instanceof SlotReference)) {
            return null;
        }
        if (!(expr.child(1) instanceof SlotReference)) {
            return null;
        }
        if (! join.left().getOutput().contains(expr.child(0))
                || ! join.right().getOutput().contains(expr.child(1))) {
            if (join.left().getOutput().contains(expr.child(1))
                    && join.right().getOutput().contains(expr.child(0))) {
                return ((ComparisonPredicate) expr).commute();
            }
        } else {
            return (ComparisonPredicate) expr;
        }
        return null;
    }

    private TMinMaxRuntimeFilterType getMinMaxType(ComparisonPredicate compare) {
        if (compare instanceof LessThan || compare instanceof LessThanEqual) {
            return TMinMaxRuntimeFilterType.MAX;
        }
        if (compare instanceof GreaterThan || compare instanceof GreaterThanEqual) {
            return TMinMaxRuntimeFilterType.MIN;
        }
        return TMinMaxRuntimeFilterType.MIN_MAX;
    }

    /**
     * A join B on A.x < B.y
     * min-max filter (A.x < N, N=max(B.y)) could be applied to A.x
     */
    private void generateMinMaxRuntimeFilter(AbstractPhysicalJoin<? extends Plan, ? extends Plan> join,
                                                   RuntimeFilterContext ctx) {
        int hashCondionSize = join.getHashJoinConjuncts().size();
        for (int idx = 0; idx < join.getOtherJoinConjuncts().size(); idx++) {
            int exprOrder = idx + hashCondionSize;
            Expression expr = join.getOtherJoinConjuncts().get(exprOrder);
            ComparisonPredicate compare = normalizeNonEqual(join, expr);
            if (compare != null) {
                if (compare.child(0).getInputSlots().size() != 1) {
                    continue;
                }
                RuntimeFilterPushDownVisitor.PushDownContext pushDownContext =
                        RuntimeFilterPushDownVisitor.PushDownContext.createPushDownContext(
                                ctx, join, compare.child(1), compare.child(0),
                                TRuntimeFilterType.MIN_MAX, getMinMaxType(compare),
                                false, -1, exprOrder);
                if (pushDownContext.isValid()) {
                    join.accept(new RuntimeFilterPushDownVisitor(), pushDownContext);
                }
            }
        }
    }

    @Override
    public PhysicalPlan visitPhysicalNestedLoopJoin(PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> join,
            CascadesContext context) {
        // TODO: we need to support all type join
        join.right().accept(this, context);
        join.left().accept(this, context);
        if (RuntimeFilterGenerator.DENIED_JOIN_TYPES.contains(join.getJoinType()) || join.isMarkJoin()) {
            // do not generate RF on this join
            return join;
        }
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();

        if (ctx.getSessionVariable().allowedRuntimeFilterType(TRuntimeFilterType.MIN_MAX)) {
            generateMinMaxRuntimeFilter(join, ctx);
        }

        return join;
    }

    @Override
    public PhysicalPlan visitPhysicalProject(PhysicalProject<? extends Plan> project, CascadesContext context) {
        project.child().accept(this, context);
        return project;
    }

    @Override
    public Plan visitPhysicalOneRowRelation(PhysicalOneRowRelation oneRowRelation, CascadesContext context) {
        return oneRowRelation;
    }

    @Override
    public PhysicalRelation visitPhysicalRelation(PhysicalRelation relation, CascadesContext context) {
        return relation;
    }

    @Override
    public PhysicalSetOperation visitPhysicalSetOperation(PhysicalSetOperation setOperation, CascadesContext context) {
        setOperation.children().forEach(child -> child.accept(this, context));
        return setOperation;
    }

    @Override
    public Plan visitPhysicalIntersect(PhysicalIntersect intersect, CascadesContext context) {
        visitPhysicalSetOperation(intersect, context);
        generateRuntimeFilterForSetOperation(intersect, context);
        return intersect;
    }

    @Override
    public Plan visitPhysicalExcept(PhysicalExcept except, CascadesContext context) {
        visitPhysicalSetOperation(except, context);
        generateRuntimeFilterForSetOperation(except, context);
        return except;
    }

    private void generateRuntimeFilterForSetOperation(PhysicalSetOperation setOp, CascadesContext context) {
        AbstractPlan child0 = (AbstractPlan) setOp.child(0);
        if (child0.getStats() == null
                || ConnectContext.get() == null
                || ConnectContext.get().getSessionVariable() == null
                || child0.getStats().getRowCount()
                    >= ConnectContext.get().getSessionVariable().runtimeFilterMaxBuildRowCount) {
            return;
        }
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        List<TRuntimeFilterType> legalTypes = RuntimeFilterTypeHelper.getSupportedRuntimeFilterTypes().stream()
                .filter(type -> ctx.getSessionVariable().allowedRuntimeFilterType(type))
                .collect(Collectors.toList());

        boolean hasUnknownColStats = context.getStatementContext().isHasUnknownColStats();
        RuntimeFilterPushDownVisitor pushDownVisitor = new RuntimeFilterPushDownVisitor();
        for (int slotIdx : chooseSourceSlotsForSetOp(setOp)) {
            Expression sourceExpression = setOp.getRegularChildrenOutputs().get(0).get(slotIdx);
            long buildNdvOrRowCount = computeBuildNdvOrRowCount(child0, sourceExpression);
            for (int childId = 1; childId < setOp.children().size(); childId++) {
                Expression targetExpression = setOp.getRegularChildrenOutputs().get(childId).get(slotIdx);
                for (TRuntimeFilterType type : legalTypes) {
                    RuntimeFilterPushDownVisitor.PushDownContext pushDownContext =
                            RuntimeFilterPushDownVisitor.PushDownContext.createPushDownContext(
                                    ctx, setOp, sourceExpression, targetExpression,
                                    type, hasUnknownColStats, buildNdvOrRowCount, slotIdx);
                    if (pushDownContext.isValid()) {
                        setOp.child(childId).accept(pushDownVisitor, pushDownContext);
                    }
                }
            }
        }
    }

    private List<Integer> chooseSourceSlotsForSetOp(PhysicalSetOperation setOp) {
        List<Slot> output = setOp.getOutput();
        for (int i = 0; i < output.size(); i++) {
            if (!output.get(i).getDataType().isOnlyMetricType()
                    && !setOp.getLogicalProperties().getTrait().getUniformValue(output.get(i)).isPresent()) {
                return ImmutableList.of(i);
            }
        }
        return ImmutableList.of();
    }

    private long computeBuildNdvOrRowCount(AbstractPlan child0, Expression sourceExpression) {
        Statistics stats = child0.getStats();
        if (stats == null) {
            return -1L;
        }
        long buildNdvOrRowCount = (long) stats.getRowCount();
        ColumnStatistic colStats = stats.findColumnStatistics(sourceExpression);
        if (colStats != null && !colStats.isUnKnown) {
            buildNdvOrRowCount = Math.max(1, (long) colStats.ndv);
        }
        return buildNdvOrRowCount;
    }

    // runtime filter build side ndv
    private long getBuildSideNdv(AbstractPhysicalJoin<? extends Plan, ? extends Plan> join,
                                 ComparisonPredicate compare) {
        AbstractPlan right = (AbstractPlan) join.right();
        //make ut test friendly
        if (right.getStats() == null) {
            return -1L;
        }
        ExpressionEstimation estimator = new ExpressionEstimation();
        ColumnStatistic buildColStats = compare.right().accept(estimator, right.getStats());
        return buildColStats.isUnKnown
                ? Math.max(1, (long) right.getStats().getRowCount()) : Math.max(1, (long) buildColStats.ndv);
    }

    public static Slot checkTargetChild(Expression leftChild) {
        Expression expression = ExpressionUtils.getSingleNumericSlotOrExpressionCoveredByCast(leftChild);
        return expression instanceof Slot ? ((Slot) expression) : null;
    }

    /**
     * Check whether runtime filters on CTE consumers can be pushed into their shared CTE producer.
     */
    @VisibleForTesting
    public static boolean canPushDownRuntimeFiltersIntoCTEProducer(
            List<RuntimeFilter> rfsToPushDown, CTEId cteId) {
        if (rfsToPushDown.isEmpty()) {
            LOG.warn("Skip pushing runtime filters into CTE producer because no runtime filters exist for cteId: {}",
                    cteId);
            return false;
        }
        Set<Expression> producerTargetExpressions = rfsToPushDown.stream()
                .map(rf -> getProducerTargetExpression(rf, cteId))
                .collect(Collectors.toSet());
        return producerTargetExpressions.size() == 1;
    }

    private static Expression getProducerTargetExpression(RuntimeFilter rf, CTEId cteId) {
        PhysicalRelation rel = rf.getTargetScan();
        Preconditions.checkArgument(rel instanceof PhysicalCTEConsumer
                && ((PhysicalCTEConsumer) rel).getCteId().equals(cteId));
        PhysicalCTEConsumer consumer = (PhysicalCTEConsumer) rel;
        Map<Expression, Expression> replaceMap = Maps.newHashMap();
        for (Slot slot : rf.getTargetExpression().getInputSlots()) {
            replaceMap.put(slot, consumer.getProducerSlot(slot));
        }
        return ExpressionUtils.replace(rf.getTargetExpression(), replaceMap);
    }

    private boolean doPushDownIntoCTEProducerInternal(RuntimeFilter rf, Expression targetExpression,
                                                    RuntimeFilterContext ctx, PhysicalCTEProducer cteProducer) {
        PhysicalPlan inputPlanNode = (PhysicalPlan) cteProducer.child(0);
        Slot unwrappedSlot = checkTargetChild(targetExpression);
        if (unwrappedSlot == null) {
            return false;
        }
        // Find the CTE consumer that owns this target
        PhysicalCTEConsumer cteConsumer = null;
        Slot consumerSlot = null;
        PhysicalRelation rel = rf.getTargetScan();
        if (rel instanceof PhysicalCTEConsumer) {
            PhysicalCTEConsumer candidate = (PhysicalCTEConsumer) rel;
            if (candidate.getCteId().equals(cteProducer.getCteId())) {
                cteConsumer = candidate;
                consumerSlot = rf.getTargetSlot();
            }
        }
        if (cteConsumer == null || consumerSlot == null) {
            return false;
        }
        // Map consumer slot to producer slot
        Slot producerSlot = cteConsumer.getProducerSlot(consumerSlot);
        Expression producerTargetExpression = getProducerTargetExpression(rf, cteProducer.getCteId());
        Slot producerTargetSlot = checkTargetChild(producerTargetExpression);
        if (!producerSlot.equals(producerTargetSlot)) {
            return false;
        }
        if (!checkCanPushDownIntoBasicTable(inputPlanNode)) {
            return false;
        }
        // Use the PushDownVisitor to push inside the CTE producer subtree
        RuntimeFilterPushDownVisitor.PushDownContext pushDownContext =
                RuntimeFilterPushDownVisitor.PushDownContext.createPushDownContext(
                        ctx, rf.getBuilderNode(), rf.getSrcExpr(), producerTargetExpression,
                        rf.getType(), rf.gettMinMaxType(),
                        !rf.isBloomFilterSizeCalculatedByNdv(), rf.getBuildSideNdv(), rf.getExprOrder());
        if (pushDownContext.isValid()) {
            return inputPlanNode.accept(new RuntimeFilterPushDownVisitor(), pushDownContext);
        }
        return false;
    }

    /**
     * Check runtime filter push down relation related pre-conditions.
     */
    public static boolean checkPushDownPreconditionsForRelation(PhysicalPlan root, PhysicalRelation relation) {
        Preconditions.checkState(relation != null, "relation is null");
        if (!relation.canPushDownRuntimeFilter()) {
            return false;
        }
        Set<PhysicalRelation> relations = new HashSet<>();
        RuntimeFilterGenerator.getAllScanInfo(root, relations);
        return relations.contains(relation);
    }

    private boolean checkCanPushDownIntoBasicTable(PhysicalPlan root) {
        // only support spj currently
        List<PhysicalPlan> plans = Lists.newArrayList();
        plans.addAll(root.collect(PhysicalPlan.class::isInstance));
        return plans.stream().allMatch(p -> SPJ_PLAN.stream().anyMatch(c -> c.isInstance(p)));
    }

    /**
     * Get all relation node from current root plan.
     */
    public static void getAllScanInfo(Plan root, Set<PhysicalRelation> scans) {
        if (root instanceof PhysicalRelation) {
            scans.add((PhysicalRelation) root);
            // if (root instanceof PhysicalLazyMaterializeOlapScan) {
            //     scans.add(((PhysicalLazyMaterializeOlapScan) root).getScan());
            // } else {
            //     scans.add((PhysicalRelation) root);
            // }
        } else {
            for (Plan child : root.children()) {
                getAllScanInfo(child, scans);
            }
        }
    }
}
