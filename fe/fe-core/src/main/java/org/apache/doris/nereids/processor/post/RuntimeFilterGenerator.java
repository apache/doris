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
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
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
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWindow;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.thrift.TMinMaxRuntimeFilterType;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        join.right().accept(this, context);
        join.left().accept(this, context);
        if (RuntimeFilterGenerator.DENIED_JOIN_TYPES.contains(join.getJoinType()) || join.isMarkJoin()) {
            join.right().getOutput().forEach(slot ->
                    context.getRuntimeFilterContext().getAliasTransferMap().remove(slot));
        }
        collectPushDownCTEInfos(join, context);
        if (!getPushDownCTECandidates(ctx).isEmpty()) {
            pushDownRuntimeFilterIntoCTE(ctx);
        } else {
            pushDownRuntimeFilterCommon(join, context);
        }
        return join;
    }

    @Override
    public PhysicalCTEConsumer visitPhysicalCTEConsumer(PhysicalCTEConsumer scan, CascadesContext context) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        scan.getOutput().forEach(slot -> ctx.getAliasTransferMap().put(slot, Pair.of(scan, slot)));
        return scan;
    }

    @Override
    public PhysicalCTEProducer visitPhysicalCTEProducer(PhysicalCTEProducer producer, CascadesContext context) {
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
        Map<NamedExpression, Pair<PhysicalRelation, Slot>> aliasTransferMap = ctx.getAliasTransferMap();
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
            TRuntimeFilterType type = TRuntimeFilterType.BITMAP;
            Set<Slot> targetSlots = bitmapContains.child(1).getInputSlots();
            for (Slot targetSlot : targetSlots) {
                if (!checkPushDownPreconditionsForJoin(join, ctx, targetSlot)) {
                    continue;
                }
                Slot scanSlot = aliasTransferMap.get(targetSlot).second;
                PhysicalRelation scan = aliasTransferMap.get(targetSlot).first;
                RuntimeFilter filter = new RuntimeFilter(generator.getNextId(),
                        bitmapContains.child(0), ImmutableList.of(scanSlot),
                        ImmutableList.of(bitmapContains.child(1)), type, i, join, isNot, -1L);
                scan.addAppliedRuntimeFilter(filter);
                ctx.addJoinToTargetMap(join, scanSlot.getExprId());
                ctx.setTargetExprIdToFilter(scanSlot.getExprId(), filter);
                ctx.setTargetsOnScanNode(aliasTransferMap.get(targetSlot).first,
                        scanSlot);
                join.addBitmapRuntimeFilterCondition(bitmapRuntimeFilterCondition);
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
        Map<NamedExpression, Pair<PhysicalRelation, Slot>> aliasTransferMap = ctx.getAliasTransferMap();
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
                Pair<PhysicalRelation, Slot> pair = aliasTransferMap.get(unwrappedSlot);
                if (pair == null) {
                    continue;
                }
                Slot olapScanSlot = pair.second;
                PhysicalRelation scan = pair.first;
                Preconditions.checkState(olapScanSlot != null && scan != null);
                long buildSideNdv = getBuildSideNdv(join, compare);
                RuntimeFilter filter = new RuntimeFilter(generator.getNextId(),
                        compare.child(1), ImmutableList.of(olapScanSlot), ImmutableList.of(olapScanSlot),
                        TRuntimeFilterType.MIN_MAX, exprOrder, join, true, buildSideNdv,
                        getMinMaxType(compare));
                scan.addAppliedRuntimeFilter(filter);
                ctx.addJoinToTargetMap(join, olapScanSlot.getExprId());
                ctx.setTargetExprIdToFilter(olapScanSlot.getExprId(), filter);
                ctx.setTargetsOnScanNode(scan, olapScanSlot);
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
            join.right().getOutput().forEach(slot ->
                    context.getRuntimeFilterContext().getAliasTransferMap().remove(slot));
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
        Map<NamedExpression, Pair<PhysicalRelation, Slot>> aliasTransferMap
                = context.getRuntimeFilterContext().getAliasTransferMap();
        // change key when encounter alias.
        // TODO: same action will be taken for set operation
        for (Expression expression : project.getProjects()) {
            if (expression.children().isEmpty()) {
                continue;
            }
            Expression expr = ExpressionUtils.getExpressionCoveredByCast(expression.child(0));
            if (expr instanceof NamedExpression && aliasTransferMap.containsKey((NamedExpression) expr)) {
                if (expression instanceof Alias) {
                    Alias alias = ((Alias) expression);
                    aliasTransferMap.put(alias.toSlot(), aliasTransferMap.get(expr));
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
        // add all the slots in map.
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        relation.getOutput().forEach(slot -> ctx.getAliasTransferMap().put(slot, Pair.of(relation, slot)));
        return relation;
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
        Expression expression = ExpressionUtils.getExpressionCoveredByCast(leftChild);
        return expression instanceof Slot ? ((Slot) expression) : null;
    }

    private void pushDownRuntimeFilterCommon(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            CascadesContext context) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        List<TRuntimeFilterType> legalTypes = Arrays.stream(TRuntimeFilterType.values())
                .filter(type -> (type.getValue() & ctx.getSessionVariable().getRuntimeFilterType()) > 0)
                .collect(Collectors.toList());
        List<EqualTo> hashJoinConjuncts = join.getEqualToConjuncts();
        for (int i = 0; i < hashJoinConjuncts.size(); i++) {
            EqualTo equalTo = ((EqualTo) JoinUtils.swapEqualToForChildrenOrder(
                    hashJoinConjuncts.get(i), join.left().getOutputSet()));
            for (TRuntimeFilterType type : legalTypes) {
                //bitmap rf is generated by nested loop join.
                if (type == TRuntimeFilterType.BITMAP) {
                    continue;
                }
                long buildSideNdv = getBuildSideNdv(join, equalTo);
                join.pushDownRuntimeFilter(context, generator, join, equalTo.right(),
                        equalTo.left(), type, buildSideNdv, i);
            }
        }
    }

    private void collectPushDownCTEInfos(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            CascadesContext context) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        Set<CTEId> cteIds = new HashSet<>();
        PhysicalPlan leftChild = (PhysicalPlan) join.left();
        PhysicalPlan rightChild = (PhysicalPlan) join.right();

        Preconditions.checkState(leftChild != null && rightChild != null);

        boolean leftHasCTE = hasCTEConsumerUnderJoin(leftChild, cteIds);
        boolean rightHasCTE = hasCTEConsumerUnderJoin(rightChild, cteIds);
        // only support single cte in join currently
        if ((leftHasCTE && !rightHasCTE) || (!leftHasCTE && rightHasCTE)) {
            for (CTEId id : cteIds) {
                if (ctx.getCteToJoinsMap().get(id) == null) {
                    Set<PhysicalHashJoin> newJoin = new HashSet<>();
                    newJoin.add(join);
                    ctx.getCteToJoinsMap().put(id, newJoin);
                } else {
                    ctx.getCteToJoinsMap().get(id).add(join);
                }
            }
        }
        if (!ctx.getCteToJoinsMap().isEmpty()) {
            analyzeRuntimeFilterPushDownIntoCTEInfos(join, context);
        }
    }

    private List<CTEId> getPushDownCTECandidates(RuntimeFilterContext ctx) {
        List<CTEId> candidates = new ArrayList<>();
        Map<PhysicalCTEProducer, Map<EqualTo, PhysicalHashJoin>> cteRFPushDownMap = ctx.getCteRFPushDownMap();
        for (Map.Entry<PhysicalCTEProducer, Map<EqualTo, PhysicalHashJoin>> entry : cteRFPushDownMap.entrySet()) {
            CTEId cteId = entry.getKey().getCteId();
            if (ctx.getPushedDownCTE().contains(cteId)) {
                continue;
            }
            candidates.add(cteId);
        }
        return candidates;
    }

    private boolean hasCTEConsumerUnderJoin(PhysicalPlan root, Set<CTEId> cteIds) {
        if (root instanceof PhysicalCTEConsumer) {
            cteIds.add(((PhysicalCTEConsumer) root).getCteId());
            return true;
        } else if (root.children().size() != 1) {
            // only collect cte in one side
            return false;
        } else if (root instanceof PhysicalDistribute
                || root instanceof PhysicalFilter
                || root instanceof PhysicalProject) {
            // only collect cte as single child node under join
            return hasCTEConsumerUnderJoin((PhysicalPlan) root.child(0), cteIds);
        } else {
            return false;
        }
    }

    private void analyzeRuntimeFilterPushDownIntoCTEInfos(PhysicalHashJoin<? extends Plan, ? extends Plan> curJoin,
            CascadesContext context) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        Map<CTEId, Set<PhysicalHashJoin>> cteToJoinsMap = ctx.getCteToJoinsMap();
        for (Map.Entry<CTEId, Set<PhysicalHashJoin>> entry : cteToJoinsMap.entrySet()) {
            CTEId cteId = entry.getKey();
            Set<PhysicalHashJoin> joinSet = entry.getValue();
            if (joinSet.contains(curJoin)) {
                // skip current join
                continue;
            }
            Set<LogicalCTEConsumer> cteSet = context.getCteIdToConsumers().get(cteId);
            Preconditions.checkState(!cteSet.isEmpty());
            String cteName = cteSet.iterator().next().getName();
            // preconditions for rf pushing into cte producer:
            // multiple joins whose join condition is on the same cte's column of the same cte
            // the other side of these join conditions are the same column of the same table, or
            // they in the same equal sets, such as under an equal join condition
            // case 1: two joins with t1.c1 = cte1_consumer1.c1 and t1.c1 = cte1_consumer2.c1 conditions
            //         rf of t1.c1 can be pushed down into cte1 producer.
            // ----------------------hashJoin(t1.c1 = cte2_consumer1.c1)
            // ----------------------------CteConsumer[cteId= ( CTEId#1=] )
            // ----------------------------PhysicalOlapScan[t1]
            // ----------------------hashJoin(t1.c1 = cte2_consumer2.c1)
            // ----------------------------CteConsumer[cteId= ( CTEId#1=] )
            // ----------------------------PhysicalOlapScan[t1]
            // case 2: two joins with t1.c1 = cte2_consumer1.c1 and t2.c2 = cte2_consumer2.c1 and another equal join
            //         condition t1.c1 = t2.c2, which means t1.c1 and t2.c2 are in the same equal set.
            //         rf of t1.c1 and t2.c2 can be pushed down into cte2 producer.
            // --------------------hashJoin(t1.c1 = t2.c2)
            // ----------------------hashJoin(t2.c2 = cte2_consumer1.c1)
            // ----------------------------CteConsumer[cteId= ( CTEId#1=] )
            // ----------------------------PhysicalOlapScan[t2]
            // ----------------------hashJoin(t1.c1 = cte2_consumer2.c1)
            // ----------------------------CteConsumer[cteId= ( CTEId#1=] )
            // ----------------------------PhysicalOlapScan[t1]
            if (joinSet.size() != cteSet.size()) {
                continue;
            }
            List<EqualTo> equalTos = new ArrayList<>();
            Map<EqualTo, PhysicalHashJoin> equalCondToJoinMap = new LinkedHashMap<>();
            for (PhysicalHashJoin join : joinSet) {
                // precondition:
                // 1. no non-equal join condition
                // 2. only equalTo and slotReference both sides
                // 3. only support one join condition (will be refined further)
                if (join.getOtherJoinConjuncts().size() > 1
                        || join.getHashJoinConjuncts().size() != 1
                        || !(join.getHashJoinConjuncts().get(0) instanceof EqualTo)) {
                    break;
                } else {
                    EqualTo equalTo = join.getEqualToConjuncts().get(0);
                    equalTos.add(equalTo);
                    equalCondToJoinMap.put(equalTo, join);
                }
            }
            if (joinSet.size() == equalTos.size()) {
                int matchNum = 0;
                Set<String> cteNameSet = new HashSet<>();
                Set<SlotReference> anotherSideSlotSet = new HashSet<>();
                for (EqualTo equalTo : equalTos) {
                    SlotReference left = (SlotReference) equalTo.left();
                    SlotReference right = (SlotReference) equalTo.right();
                    if (left.getQualifier().size() == 1 && left.getQualifier().get(0).equals(cteName)) {
                        matchNum += 1;
                        anotherSideSlotSet.add(right);
                        cteNameSet.add(left.getQualifiedName());
                    } else if (right.getQualifier().size() == 1 && right.getQualifier().get(0).equals(cteName)) {
                        matchNum += 1;
                        anotherSideSlotSet.add(left);
                        cteNameSet.add(right.getQualifiedName());
                    }
                }
                if (matchNum == equalTos.size() && cteNameSet.size() == 1) {
                    // means all join condition points to the same cte on the same cte column.
                    // collect the other side columns besides cte column side.
                    Preconditions.checkState(equalTos.size() == equalCondToJoinMap.size(),
                            "equalTos.size() != equalCondToJoinMap.size()");

                    PhysicalCTEProducer cteProducer = context.getRuntimeFilterContext().getCteProduceMap().get(cteId);
                    if (anotherSideSlotSet.size() == 1) {
                        // meet requirement for pushing down into cte producer
                        ctx.getCteRFPushDownMap().put(cteProducer, equalCondToJoinMap);
                    } else {
                        // check further whether the join upper side can bring equal set, which
                        // indicating actually the same runtime filter build side
                        // see above case 2 for reference
                        boolean inSameEqualSet = false;
                        for (EqualTo e : curJoin.getEqualToConjuncts()) {
                            if (e instanceof EqualTo) {
                                SlotReference oneSide = (SlotReference) e.left();
                                SlotReference anotherSide = (SlotReference) e.right();
                                if (anotherSideSlotSet.contains(oneSide) && anotherSideSlotSet.contains(anotherSide)) {
                                    inSameEqualSet = true;
                                    break;
                                }
                            }
                        }
                        if (inSameEqualSet) {
                            ctx.getCteRFPushDownMap().put(cteProducer, equalCondToJoinMap);
                        }
                    }
                }
            }
        }
    }

    private void pushDownRuntimeFilterIntoCTE(RuntimeFilterContext ctx) {
        Map<PhysicalCTEProducer, Map<EqualTo, PhysicalHashJoin>> cteRFPushDownMap = ctx.getCteRFPushDownMap();
        for (Map.Entry<PhysicalCTEProducer, Map<EqualTo, PhysicalHashJoin>> entry : cteRFPushDownMap.entrySet()) {
            PhysicalCTEProducer cteProducer = entry.getKey();
            Preconditions.checkState(cteProducer != null);
            if (ctx.getPushedDownCTE().contains(cteProducer.getCteId())) {
                continue;
            }
            Map<EqualTo, PhysicalHashJoin> equalCondToJoinMap = entry.getValue();
            for (Map.Entry<EqualTo, PhysicalHashJoin> innerEntry : equalCondToJoinMap.entrySet()) {
                EqualTo equalTo = innerEntry.getKey();
                PhysicalHashJoin<? extends Plan, ? extends Plan> join = innerEntry.getValue();
                Preconditions.checkState(join != null);
                TRuntimeFilterType type = TRuntimeFilterType.IN_OR_BLOOM;
                if (ctx.getSessionVariable().getEnablePipelineEngine()) {
                    type = TRuntimeFilterType.BLOOM;
                }
                EqualTo newEqualTo = ((EqualTo) JoinUtils.swapEqualToForChildrenOrder(
                        equalTo, join.child(0).getOutputSet()));
                doPushDownIntoCTEProducerInternal(join, ctx, newEqualTo, type, cteProducer);
            }
            ctx.getPushedDownCTE().add(cteProducer.getCteId());
        }
    }

    private void doPushDownIntoCTEProducerInternal(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            RuntimeFilterContext ctx, EqualTo equalTo, TRuntimeFilterType type, PhysicalCTEProducer cteProducer) {
        Map<NamedExpression, Pair<PhysicalRelation, Slot>> aliasTransferMap = ctx.getAliasTransferMap();
        PhysicalPlan inputPlanNode = (PhysicalPlan) cteProducer.child(0);
        Slot unwrappedSlot = checkTargetChild(equalTo.left());
        // aliasTransMap doesn't contain the key, means that the path from the scan to the join
        // contains join with denied join type. for example: a left join b on a.id = b.id
        if (!checkPushDownPreconditionsForJoin(join, ctx, unwrappedSlot)) {
            return;
        }
        Slot cteSlot = aliasTransferMap.get(unwrappedSlot).second;
        PhysicalRelation cteNode = aliasTransferMap.get(unwrappedSlot).first;
        long buildSideNdv = getBuildSideNdv(join, equalTo);
        if (cteNode instanceof PhysicalCTEConsumer && inputPlanNode instanceof PhysicalProject) {
            PhysicalProject project = (PhysicalProject) inputPlanNode;
            NamedExpression targetExpr = null;
            for (Object column : project.getProjects()) {
                NamedExpression alias = (NamedExpression) column;
                if (cteSlot.getName().equals(alias.getName())) {
                    targetExpr = alias;
                    break;
                }
            }
            Preconditions.checkState(targetExpr != null);
            if (!(targetExpr instanceof SlotReference)) {
                // if not SlotReference, skip the push down
                return;
            } else if (!checkCanPushDownIntoBasicTable(project)) {
                return;
            } else {
                Map<Slot, PhysicalRelation> pushDownBasicTableInfos = getPushDownBasicTablesInfos(project,
                        (SlotReference) targetExpr, aliasTransferMap);
                if (!pushDownBasicTableInfos.isEmpty()) {
                    List<Slot> targetList = new ArrayList<>();
                    List<PhysicalRelation> targetNodes = new ArrayList<>();
                    for (Map.Entry<Slot, PhysicalRelation> entry : pushDownBasicTableInfos.entrySet()) {
                        Slot targetSlot = entry.getKey();
                        PhysicalRelation scan = entry.getValue();
                        if (!RuntimeFilterGenerator.checkPushDownPreconditionsForRelation(project, scan)) {
                            continue;
                        }
                        targetList.add(targetSlot);
                        targetNodes.add(scan);
                        ctx.addJoinToTargetMap(join, targetSlot.getExprId());
                        ctx.setTargetsOnScanNode(scan, targetSlot);
                    }
                    // build multi-target runtime filter
                    // since always on different join, set the expr_order as 0
                    RuntimeFilter filter = new RuntimeFilter(generator.getNextId(),
                            equalTo.right(), targetList, type, 0, join, buildSideNdv);
                    targetNodes.forEach(node -> node.addAppliedRuntimeFilter(filter));
                    for (Slot slot : targetList) {
                        ctx.setTargetExprIdToFilter(slot.getExprId(), filter);
                    }
                    ctx.setRuntimeFilterIdentityToFilter(equalTo.right(), type, join, filter);
                }
            }
        }
    }

    @Override
    public PhysicalPlan visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, CascadesContext context) {
        topN.child().accept(this, context);
        PhysicalPlan child = (PhysicalPlan) topN.child();
        for (Slot slot : child.getOutput()) {
            context.getRuntimeFilterContext().getAliasTransferMap().remove(slot);
        }
        return topN;
    }

    @Override
    public PhysicalPlan visitPhysicalWindow(PhysicalWindow<? extends Plan> window, CascadesContext context) {
        window.child().accept(this, context);
        Set<SlotReference> commonPartitionKeys = window.getCommonPartitionKeyFromWindowExpressions();
        window.child().getOutput().stream().filter(slot -> !commonPartitionKeys.contains(slot)).forEach(
                slot -> context.getRuntimeFilterContext().getAliasTransferMap().remove(slot)
        );
        return window;
    }

    /**
     * Check runtime filter push down pre-conditions, such as builder side join type, etc.
     */
    public static boolean checkPushDownPreconditionsForJoin(AbstractPhysicalJoin physicalJoin,
                                                       RuntimeFilterContext ctx, Slot slot) {
        Map<NamedExpression, Pair<PhysicalRelation, Slot>> aliasTransferMap = ctx.getAliasTransferMap();
        if (slot == null || !aliasTransferMap.containsKey(slot)) {
            return false;
        } else if (DENIED_JOIN_TYPES.contains(physicalJoin.getJoinType()) || physicalJoin.isMarkJoin()) {
            return false;
        } else {
            return true;
        }
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
            Map<NamedExpression, Pair<PhysicalRelation, Slot>> aliasTransferMap) {
        Map<Slot, PhysicalRelation> basicTableInfos = new HashMap<>();
        Set<PhysicalHashJoin> joins = new HashSet<>();
        ExprId exprId = slot.getExprId();
        if (aliasTransferMap.get(slot) != null) {
            basicTableInfos.put(slot, aliasTransferMap.get(slot).first);
        }
        // try to find propagation condition from join
        getAllJoinInfo(root, joins);
        for (PhysicalHashJoin join : joins) {
            List<Expression> conditions = join.getHashJoinConjuncts();
            for (Expression equalTo : conditions) {
                if (equalTo instanceof EqualTo) {
                    SlotReference leftSlot = (SlotReference) ((EqualTo) equalTo).left();
                    SlotReference rightSlot = (SlotReference) ((EqualTo) equalTo).right();
                    if (leftSlot.getExprId() == exprId && aliasTransferMap.get(rightSlot) != null) {
                        PhysicalRelation rightTable = aliasTransferMap.get(rightSlot).first;
                        if (rightTable != null) {
                            basicTableInfos.put(rightSlot, rightTable);
                        }
                    } else if (rightSlot.getExprId() == exprId && aliasTransferMap.get(leftSlot) != null) {
                        PhysicalRelation leftTable = aliasTransferMap.get(leftSlot).first;
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
    public static void getAllScanInfo(PhysicalPlan root, Set<PhysicalRelation> scans) {
        if (root instanceof PhysicalRelation) {
            scans.add((PhysicalRelation) root);
        } else {
            for (Object child : root.children()) {
                getAllScanInfo((PhysicalPlan) child, scans);
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
