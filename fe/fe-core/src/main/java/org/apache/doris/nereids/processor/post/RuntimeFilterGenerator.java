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

import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.stats.ExpressionEstimation;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapContains;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSchemaScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
            JoinType.NULL_AWARE_LEFT_ANTI_JOIN
    );

    private static final Set<Class<? extends PhysicalPlan>> SPJ_PLAN = ImmutableSet.of(
            PhysicalRelation.class,
            PhysicalProject.class,
            PhysicalFilter.class,
            PhysicalDistribute.class,
            PhysicalHashJoin.class
    );

    private final IdGenerator<RuntimeFilterId> generator = RuntimeFilterId.createGenerator();

    @Override
    public Plan processRoot(Plan plan, CascadesContext ctx) {
        Plan result = plan.accept(this, ctx);
        // cte rf
        RuntimeFilterContext rfCtx = ctx.getRuntimeFilterContext();
        int cteCount = rfCtx.getProcessedCTE().size();
        if (cteCount != 0) {
            Map<CTEId, Set<PhysicalCTEConsumer>> cteIdToConsumersWithRF = Maps.newHashMap();
            Map<CTEId, List<RuntimeFilter>> cteToRFsMap = Maps.newHashMap();
            Map<PhysicalCTEConsumer, Set<RuntimeFilter>> consumerToRFs = Maps.newHashMap();
            Map<PhysicalCTEConsumer, Set<Expression>> consumerToSrcExpression = Maps.newHashMap();
            List<RuntimeFilter> allRFs = rfCtx.getNereidsRuntimeFilter();
            for (RuntimeFilter rf : allRFs) {
                for (PhysicalRelation rel : rf.getTargetScans()) {
                    if (rel instanceof PhysicalCTEConsumer) {
                        PhysicalCTEConsumer consumer = (PhysicalCTEConsumer) rel;
                        CTEId cteId = consumer.getCteId();
                        cteToRFsMap.computeIfAbsent(cteId, key -> Lists.newArrayList()).add(rf);
                        cteIdToConsumersWithRF.computeIfAbsent(cteId, key -> Sets.newHashSet()).add(consumer);
                        consumerToRFs.computeIfAbsent(consumer, key -> Sets.newHashSet()).add(rf);
                        consumerToSrcExpression.computeIfAbsent(consumer, key -> Sets.newHashSet())
                                .add(rf.getSrcExpr());
                    }
                }
            }
            for (CTEId cteId : rfCtx.getCteProduceMap().keySet()) {
                // if any consumer does not have RF, RF cannot be pushed down.
                // cteIdToConsumersWithRF.get(cteId).size() can not be 1, o.w. this cte will be inlined.
                if (cteIdToConsumersWithRF.get(cteId) != null
                        && ctx.getCteIdToConsumers().get(cteId).size() == cteIdToConsumersWithRF.get(cteId).size()
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
                                int targetCount = rfToPush.getTargetExpressions().size();
                                for (int i = 0; i < targetCount; i++) {
                                    PhysicalRelation rel = rfToPush.getTargetScans().get(i);
                                    if (rel instanceof PhysicalCTEConsumer
                                            && ((PhysicalCTEConsumer) rel).getCteId().equals(cteId)) {
                                        rightDeepTargetExpressionOnCTE = rfToPush.getTargetExpressions().get(i);
                                        break;
                                    }
                                }

                                boolean pushedDown = doPushDownIntoCTEProducerInternal(
                                        rfToPush,
                                        rightDeepTargetExpressionOnCTE,
                                        rfCtx,
                                        rfCtx.getCteProduceMap().get(cteId)
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
                .filter(type -> (type.getValue() & ctx.getSessionVariable().getRuntimeFilterType()) > 0)
                .collect(Collectors.toList());

        List<Expression> hashJoinConjuncts = join.getHashJoinConjuncts().stream().collect(Collectors.toList());
        boolean buildSideContainsConsumer = hasCTEConsumerDescendant((PhysicalPlan) join.right());
        for (int i = 0; i < hashJoinConjuncts.size(); i++) {
            EqualPredicate equalTo = JoinUtils.swapEqualToForChildrenOrder(
                    (EqualPredicate) hashJoinConjuncts.get(i), join.left().getOutputSet());
            for (TRuntimeFilterType type : legalTypes) {
                //bitmap rf is generated by nested loop join.
                if (type == TRuntimeFilterType.BITMAP) {
                    continue;
                }
                long buildSideNdv = getBuildSideNdv(join, equalTo);
                Pair<PhysicalRelation, Slot> pair = ctx.getAliasTransferMap().get(equalTo.right());
                // CteConsumer is not allowed to generate RF in order to avoid RF cycle.
                if ((pair == null && buildSideContainsConsumer)
                        || (pair != null && pair.first instanceof PhysicalCTEConsumer)) {
                    continue;
                }
                if (equalTo.left().getInputSlots().size() == 1) {
                    RuntimeFilterPushDownVisitor.PushDownContext pushDownContext =
                            RuntimeFilterPushDownVisitor.PushDownContext.createPushDownContextForHashJoin(
                                    equalTo.right(), equalTo.left(), ctx, generator, type, join,
                                    context.getStatementContext().isHasUnknownColStats(), buildSideNdv, i);
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

    @Override
    public PhysicalCTEConsumer visitPhysicalCTEConsumer(PhysicalCTEConsumer scan, CascadesContext context) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        scan.getOutput().forEach(slot -> ctx.aliasTransferMapPut(slot, Pair.of(scan, slot)));
        return scan;
    }

    @Override
    public PhysicalCTEProducer<? extends Plan> visitPhysicalCTEProducer(PhysicalCTEProducer<? extends Plan> producer,
            CascadesContext context) {
        CTEId cteId = producer.getCteId();
        context.getRuntimeFilterContext().getCteProduceMap().put(cteId, producer);
        Set<CTEId> processedCTE = context.getRuntimeFilterContext().getProcessedCTE();
        if (!processedCTE.contains(cteId)) {
            PhysicalPlan inputPlanNode = (PhysicalPlan) producer.child(0);
            inputPlanNode.accept(this, context);
            processedCTE.add(cteId);
        }
        return producer;
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
                    RuntimeFilterPushDownVisitor.PushDownContext.createPushDownContextForBitMapFilter(
                            bitmapContains.child(0), bitmapContains.child(1), ctx, generator, join,
                            -1, i, isNot);
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
                Slot unwrappedSlot = checkTargetChild(compare.child(0));
                if (unwrappedSlot == null) {
                    continue;
                }
                Pair<PhysicalRelation, Slot> pair = ctx.getAliasTransferPair(unwrappedSlot);
                if (pair == null) {
                    continue;
                }
                Slot olapScanSlot = pair.second;
                PhysicalRelation scan = pair.first;
                Preconditions.checkState(olapScanSlot != null && scan != null);
                RuntimeFilterPushDownVisitor.PushDownContext pushDownContext =
                        RuntimeFilterPushDownVisitor.PushDownContext.createPushDownContextForNljMinMaxFilter(
                                compare.child(1), compare.child(0), ctx, generator, join,
                                exprOrder, getMinMaxType(compare));
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

        if ((ctx.getSessionVariable().getRuntimeFilterType() & TRuntimeFilterType.BITMAP.getValue()) != 0) {
            generateBitMapRuntimeFilterForNLJ(join, ctx);
        }

        if ((ctx.getSessionVariable().getRuntimeFilterType() & TRuntimeFilterType.MIN_MAX.getValue()) != 0) {
            generateMinMaxRuntimeFilter(join, ctx);
        }

        return join;
    }

    @Override
    public PhysicalPlan visitPhysicalProject(PhysicalProject<? extends Plan> project, CascadesContext context) {
        project.child().accept(this, context);
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        // change key when encounter alias.
        // TODO: same action will be taken for set operation
        for (Expression expression : project.getProjects()) {
            if (expression.children().isEmpty()) {
                continue;
            }
            Expression expr = ExpressionUtils.getSingleNumericSlotOrExpressionCoveredByCast(expression.child(0));
            if (expr instanceof NamedExpression
                    && ctx.aliasTransferMapContains((NamedExpression) expr)) {
                if (expression instanceof Alias) {
                    Alias alias = ((Alias) expression);
                    ctx.aliasTransferMapPut(alias.toSlot(), ctx.getAliasTransferPair((NamedExpression) expr));
                }
            }
        }
        return project;
    }

    @Override
    public Plan visitPhysicalOneRowRelation(PhysicalOneRowRelation oneRowRelation, CascadesContext context) {
        // TODO: OneRowRelation will be translated to union. Union node cannot apply runtime filter now
        //  so, just return itself now, until runtime filter could apply on any node.
        return oneRowRelation;
    }

    @Override
    public PhysicalRelation visitPhysicalRelation(PhysicalRelation relation, CascadesContext context) {
        if (relation instanceof PhysicalSchemaScan) {
            return relation;
        }
        // add all the slots in map.
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        relation.getOutput().forEach(slot -> ctx.aliasTransferMapPut(slot, Pair.of(relation, slot)));
        return relation;
    }

    @Override
    public PhysicalSetOperation visitPhysicalSetOperation(PhysicalSetOperation setOperation, CascadesContext context) {
        setOperation.children().forEach(child -> child.accept(this, context));
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        if (!setOperation.getRegularChildrenOutputs().isEmpty()) {
            // example: RegularChildrenOutputs is empty
            // "select 1 a, 2 b union all select 3, 4 union all select 10 e, 20 f;"
            for (int i = 0; i < setOperation.getOutput().size(); i++) {
                Pair childSlotPair = ctx.getAliasTransferPair(setOperation.getRegularChildOutput(0).get(i));
                if (childSlotPair != null) {
                    ctx.aliasTransferMapPut(setOperation.getOutput().get(i), childSlotPair);
                }
            }
        }
        return setOperation;
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
        // aliasTransMap doesn't contain the key, means that the path from the scan to the join
        // contains join with denied join type. for example: a left join b on a.id = b.id
        if (!checkProbeSlot(ctx, unwrappedSlot)) {
            return false;
        }
        Slot consumerOutputSlot = ctx.getAliasTransferPair(unwrappedSlot).second;
        PhysicalRelation cteNode = ctx.getAliasTransferPair(unwrappedSlot).first;
        long buildSideNdv = rf.getBuildSideNdv();
        if (!(cteNode instanceof PhysicalCTEConsumer) || !(inputPlanNode instanceof PhysicalProject)) {
            return false;
        }
        Slot cteSlot = ((PhysicalCTEConsumer) cteNode).getProducerSlot(consumerOutputSlot);

        PhysicalProject<Plan> project = (PhysicalProject<Plan>) inputPlanNode;
        NamedExpression targetExpr = null;
        for (NamedExpression ne : project.getProjects()) {
            if (cteSlot.getExprId().equals(ne.getExprId())) {
                targetExpr = ne;
                break;
            }
        }
        Preconditions.checkState(targetExpr != null,
                "cannot find runtime filter cte.target: "
                        + cteSlot + "in project " + project.toString());
        if (targetExpr instanceof SlotReference && checkCanPushDownIntoBasicTable(project)) {
            Map<Slot, PhysicalRelation> pushDownBasicTableInfos = getPushDownBasicTablesInfos(project,
                    (SlotReference) targetExpr, ctx);
            if (!pushDownBasicTableInfos.isEmpty()) {
                List<Slot> targetList = new ArrayList<>();
                List<Expression> targetExpressions = new ArrayList<>();
                List<PhysicalRelation> targetNodes = new ArrayList<>();
                for (Map.Entry<Slot, PhysicalRelation> entry : pushDownBasicTableInfos.entrySet()) {
                    Slot targetSlot = entry.getKey();
                    PhysicalRelation scan = entry.getValue();
                    if (!RuntimeFilterGenerator.checkPushDownPreconditionsForRelation(project, scan)) {
                        continue;
                    }
                    targetList.add(targetSlot);
                    targetExpressions.add(targetSlot);
                    targetNodes.add(scan);
                    ctx.addJoinToTargetMap(rf.getBuilderNode(), targetSlot.getExprId());
                    ctx.setTargetsOnScanNode(scan, targetSlot);
                }
                if (targetList.isEmpty()) {
                    return false;
                }
                RuntimeFilter filter = new RuntimeFilter(generator.getNextId(),
                        rf.getSrcExpr(), targetList, targetExpressions, rf.getType(), rf.getExprOrder(),
                        rf.getBuilderNode(), buildSideNdv, rf.isBloomFilterSizeCalculatedByNdv(),
                        rf.gettMinMaxType(), cteNode);
                targetNodes.forEach(node -> node.addAppliedRuntimeFilter(filter));
                for (Slot slot : targetList) {
                    ctx.setTargetExprIdToFilter(slot.getExprId(), filter);
                }
                ctx.setRuntimeFilterIdentityToFilter(rf.getSrcExpr(), rf.getType(), rf.getBuilderNode(), filter);
                return true;
            }
        }
        return false;
    }

    /**
     * check if slot is in ctx.aliasTransferMap
     */
    public static boolean checkProbeSlot(RuntimeFilterContext ctx, Slot slot) {
        if (slot == null || !ctx.aliasTransferMapContains(slot)) {
            return false;
        }
        return true;
    }

    /**
     * Check runtime filter push down relation related pre-conditions.
     */
    public static boolean checkPushDownPreconditionsForRelation(PhysicalPlan root, PhysicalRelation relation) {
        Preconditions.checkState(relation != null, "relation is null");
        // check if the relation supports runtime filter push down
        if (!relation.canPushDownRuntimeFilter()) {
            return false;
        }
        // check if the plan root can cover the push down candidate relation
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

    private Map<Slot, PhysicalRelation> getPushDownBasicTablesInfos(PhysicalPlan root, SlotReference slot,
            RuntimeFilterContext ctx) {
        Map<Slot, PhysicalRelation> basicTableInfos = new HashMap<>();
        Set<PhysicalHashJoin> joins = new HashSet<>();
        ExprId exprId = slot.getExprId();
        if (ctx.getAliasTransferPair(slot) != null) {
            basicTableInfos.put(slot, ctx.getAliasTransferPair(slot).first);
        }
        // try to find propagation condition from join
        getAllJoinInfo(root, joins);
        for (PhysicalHashJoin join : joins) {
            List<Expression> conditions = join.getHashJoinConjuncts();
            for (Expression equalTo : conditions) {
                if (equalTo instanceof EqualTo) {
                    SlotReference leftSlot = (SlotReference) ((EqualTo) equalTo).left();
                    SlotReference rightSlot = (SlotReference) ((EqualTo) equalTo).right();
                    if (leftSlot.getExprId() == exprId && ctx.getAliasTransferPair(rightSlot) != null) {
                        PhysicalRelation rightTable = ctx.getAliasTransferPair(rightSlot).first;
                        if (rightTable != null) {
                            basicTableInfos.put(rightSlot, rightTable);
                        }
                    } else if (rightSlot.getExprId() == exprId && ctx.getAliasTransferPair(leftSlot) != null) {
                        PhysicalRelation leftTable = ctx.getAliasTransferPair(leftSlot).first;
                        if (leftTable != null) {
                            basicTableInfos.put(leftSlot, leftTable);
                        }
                    }
                }
            }
        }
        return basicTableInfos;
    }

    private void getAllJoinInfo(PhysicalPlan root, Set<PhysicalHashJoin> joins) {
        if (root instanceof PhysicalHashJoin) {
            joins.add((PhysicalHashJoin) root);
        } else {
            for (Object child : root.children()) {
                getAllJoinInfo((PhysicalPlan) child, joins);
            }
        }
    }

    /**
     * Get all relation node from current root plan.
     */
    public static void getAllScanInfo(Plan root, Set<PhysicalRelation> scans) {
        if (root instanceof PhysicalRelation) {
            scans.add((PhysicalRelation) root);
        } else {
            for (Plan child : root.children()) {
                getAllScanInfo(child, scans);
            }
        }
    }

    /**
     * Check whether plan root contains cte consumer descendant.
     */
    public static boolean hasCTEConsumerDescendant(PhysicalPlan root) {
        if (root instanceof PhysicalCTEConsumer) {
            return true;
        } else if (root.children().size() == 1) {
            return hasCTEConsumerDescendant((PhysicalPlan) root.child(0));
        } else {
            for (Object child : root.children()) {
                if (hasCTEConsumerDescendant((PhysicalPlan) child)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Check whether runtime filter target is remote or local
     */
    public static boolean hasRemoteTarget(AbstractPlan join, AbstractPlan scan) {
        if (scan instanceof PhysicalCTEConsumer) {
            return true;
        } else {
            Preconditions.checkArgument(join.getMutableState(AbstractPlan.FRAGMENT_ID).isPresent(),
                    "cannot find fragment id for Join node");
            Preconditions.checkArgument(scan.getMutableState(AbstractPlan.FRAGMENT_ID).isPresent(),
                    "cannot find fragment id for scan node");
            return join.getMutableState(AbstractPlan.FRAGMENT_ID).get()
                    != scan.getMutableState(AbstractPlan.FRAGMENT_ID).get();
        }
    }
}
