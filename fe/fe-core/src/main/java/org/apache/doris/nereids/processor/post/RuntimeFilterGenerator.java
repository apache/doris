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
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapContains;
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
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeOlapScan;
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
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Arrays;
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
        List<TRuntimeFilterType> legalTypes = Arrays.stream(TRuntimeFilterType.values())
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
                //bitmap rf is generated by nested loop join.
                if (type == TRuntimeFilterType.BITMAP) {
                    continue;
                }
                long buildSideNdv = getBuildSideNdv(join, equalTo);
                if (equalTo.left().getInputSlots().size() == 1) {
                    RuntimeFilterPushDownVisitor.PushDownContext pushDownContext =
                            RuntimeFilterPushDownVisitor.PushDownContext.createPushDownContext(
                                    ctx, join, equalTo.right(), equalTo.left(),
                                    type, false, context.getStatementContext().isHasUnknownColStats(),
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

    @Override
    public PhysicalCTEConsumer visitPhysicalCTEConsumer(PhysicalCTEConsumer scan, CascadesContext context) {
        return scan;
    }

    private void generateBitMapRuntimeFilterForNLJ(PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> join,
                                                   RuntimeFilterContext ctx) {
        if (join.getJoinType() != JoinType.LEFT_SEMI_JOIN && join.getJoinType() != JoinType.CROSS_JOIN) {
            return;
        }
        List<Slot> leftSlots = join.left().getOutput();
        List<Slot> rightSlots = join.right().getOutput();
        List<Expression> bitmapRuntimeFilterConditions = JoinUtils.extractBitmapRuntimeFilterConditions(leftSlots,
                rightSlots, join.getOtherJoinConjuncts());
        if (!JoinUtils.extractExpressionForHashTable(leftSlots, rightSlots, join.getOtherJoinConjuncts())
                .first.isEmpty()) {
            return;
        }
        int bitmapRFCount = bitmapRuntimeFilterConditions.size();
        for (int i = 0; i < bitmapRFCount; i++) {
            Expression bitmapRuntimeFilterCondition = bitmapRuntimeFilterConditions.get(i);
            boolean isNot = bitmapRuntimeFilterCondition instanceof Not;
            BitmapContains bitmapContains;
            if (bitmapRuntimeFilterCondition instanceof Not) {
                bitmapContains = (BitmapContains) bitmapRuntimeFilterCondition.child(0);
            } else {
                bitmapContains = (BitmapContains) bitmapRuntimeFilterCondition;
            }
            RuntimeFilterPushDownVisitor.PushDownContext pushDownContext =
                    RuntimeFilterPushDownVisitor.PushDownContext.createPushDownContext(
                            ctx, join, bitmapContains.child(0), bitmapContains.child(1),
                            TRuntimeFilterType.BITMAP, isNot, false, -1, i);
            if (pushDownContext.isValid()) {
                join.accept(new RuntimeFilterPushDownVisitor(), pushDownContext);
            }
        }
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
                                TRuntimeFilterType.MIN_MAX, false, getMinMaxType(compare),
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

        if (ctx.getSessionVariable().allowedRuntimeFilterType(TRuntimeFilterType.BITMAP)) {
            generateBitMapRuntimeFilterForNLJ(join, ctx);
        }

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
    public Plan visitPhysicalLazyMaterializeOlapScan(PhysicalLazyMaterializeOlapScan scan, CascadesContext context) {
        return scan;
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
        List<TRuntimeFilterType> legalTypes = Arrays.stream(TRuntimeFilterType.values())
                .filter(type -> ctx.getSessionVariable().allowedRuntimeFilterType(type))
                .filter(type -> type != TRuntimeFilterType.BITMAP)
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
                                    type, false, hasUnknownColStats, buildNdvOrRowCount, slotIdx);
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
        if (producerSlot == null) {
            return false;
        }
        if (!checkCanPushDownIntoBasicTable(inputPlanNode)) {
            return false;
        }
        // Use the PushDownVisitor to push inside the CTE producer subtree
        RuntimeFilterPushDownVisitor.PushDownContext pushDownContext =
                RuntimeFilterPushDownVisitor.PushDownContext.createPushDownContext(
                        ctx, rf.getBuilderNode(), rf.getSrcExpr(), producerSlot,
                        rf.getType(), rf.isBitmapFilterNotIn(), rf.gettMinMaxType(),
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
