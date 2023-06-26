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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapContains;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * generate runtime filter
 */
public class RuntimeFilterGenerator extends PlanPostProcessor {

    private static final ImmutableSet<JoinType> DENIED_JOIN_TYPES = ImmutableSet.of(
            JoinType.LEFT_ANTI_JOIN,
            JoinType.FULL_OUTER_JOIN,
            JoinType.LEFT_OUTER_JOIN,
            JoinType.NULL_AWARE_LEFT_ANTI_JOIN
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
     * third step: generate nereids runtime filter target at olap scan node fragment.
     * forth step: generate legacy runtime filter target and runtime filter at hash join node fragment.
     * NOTICE: bottom-up travel the plan tree!!!
     */
    // TODO: current support inner join, cross join, right outer join, and will support more join type.
    @Override
    public PhysicalPlan visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            CascadesContext context) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        Map<NamedExpression, Pair<PhysicalRelation, Slot>> aliasTransferMap = ctx.getAliasTransferMap();
        join.right().accept(this, context);
        join.left().accept(this, context);
        if (DENIED_JOIN_TYPES.contains(join.getJoinType()) || join.isMarkJoin()) {
            Set<Slot> slots = join.getOutputSet();
            slots.forEach(aliasTransferMap::remove);
        } else {
            List<TRuntimeFilterType> legalTypes = Arrays.stream(TRuntimeFilterType.values())
                    .filter(type -> (type.getValue() & ctx.getSessionVariable().getRuntimeFilterType()) > 0)
                    .collect(Collectors.toList());
            // TODO: some complex situation cannot be handled now, see testPushDownThroughJoin.
            //   we will support it in later version.
            for (int i = 0; i < join.getHashJoinConjuncts().size(); i++) {
                EqualTo equalTo = ((EqualTo) JoinUtils.swapEqualToForChildrenOrder(
                        (EqualTo) join.getHashJoinConjuncts().get(i), join.left().getOutputSet()));
                for (TRuntimeFilterType type : legalTypes) {
                    //bitmap rf is generated by nested loop join.
                    if (type == TRuntimeFilterType.BITMAP) {
                        continue;
                    }
                    if (join.left() instanceof PhysicalUnion
                            || join.left() instanceof PhysicalIntersect
                            || join.left() instanceof PhysicalExcept) {
                        List<Slot> targetList = new ArrayList<>();
                        int projIndex = -1;
                        for (int j = 0; j < join.left().children().size(); j++) {
                            PhysicalPlan child = (PhysicalPlan) join.left().child(j);
                            if (child instanceof PhysicalProject) {
                                PhysicalProject project = (PhysicalProject) child;
                                Slot leftSlot = checkTargetChild(equalTo.left());
                                if (leftSlot == null) {
                                    break;
                                }
                                for (int k = 0; projIndex < 0 && k < project.getProjects().size(); k++) {
                                    NamedExpression expr = (NamedExpression) project.getProjects().get(k);
                                    if (expr.getName().equals(leftSlot.getName())) {
                                        projIndex = k;
                                        break;
                                    }
                                }
                                Preconditions.checkState(projIndex >= 0
                                        && projIndex < project.getProjects().size());

                                NamedExpression targetExpr = (NamedExpression) project.getProjects().get(projIndex);

                                SlotReference origSlot = null;
                                if (targetExpr instanceof Alias) {
                                    origSlot = (SlotReference) targetExpr.child(0);
                                } else {
                                    origSlot = (SlotReference) targetExpr;
                                }
                                Slot olapScanSlot = aliasTransferMap.get(origSlot).second;
                                PhysicalRelation scan = aliasTransferMap.get(origSlot).first;
                                if (type == TRuntimeFilterType.IN_OR_BLOOM
                                        && ctx.getSessionVariable().enablePipelineEngine()
                                        && hasRemoteTarget(join, scan)) {
                                    type = TRuntimeFilterType.BLOOM;
                                }
                                targetList.add(olapScanSlot);
                                ctx.addJoinToTargetMap(join, olapScanSlot.getExprId());
                                ctx.setTargetsOnScanNode(aliasTransferMap.get(origSlot).first.getId(), olapScanSlot);
                            }
                        }
                        if (!targetList.isEmpty()) {
                            long buildSideNdv = getBuildSideNdv(join, equalTo);
                            RuntimeFilter filter = new RuntimeFilter(generator.getNextId(),
                                    equalTo.right(), targetList, type, i, join, buildSideNdv);
                            for (int j = 0; j < targetList.size(); j++) {
                                ctx.setTargetExprIdToFilter(targetList.get(j).getExprId(), filter);
                            }
                        }
                    } else {
                        // currently, we can ensure children in the two side are corresponding to the equal_to's.
                        // so right maybe an expression and left is a slot
                        Slot unwrappedSlot = checkTargetChild(equalTo.left());
                        // aliasTransMap doesn't contain the key, means that the path from the olap scan to the join
                        // contains join with denied join type. for example: a left join b on a.id = b.id
                        if (unwrappedSlot == null || !aliasTransferMap.containsKey(unwrappedSlot)) {
                            continue;
                        }
                        Slot olapScanSlot = aliasTransferMap.get(unwrappedSlot).second;
                        PhysicalRelation scan = aliasTransferMap.get(unwrappedSlot).first;
                        // in-filter is not friendly to pipeline
                        if (type == TRuntimeFilterType.IN_OR_BLOOM
                                && ctx.getSessionVariable().enablePipelineEngine()
                                && hasRemoteTarget(join, scan)) {
                            type = TRuntimeFilterType.BLOOM;
                        }
                        long buildSideNdv = getBuildSideNdv(join, equalTo);
                        RuntimeFilter filter = new RuntimeFilter(generator.getNextId(),
                                equalTo.right(), ImmutableList.of(olapScanSlot), type, i, join, buildSideNdv);
                        ctx.addJoinToTargetMap(join, olapScanSlot.getExprId());
                        ctx.setTargetExprIdToFilter(olapScanSlot.getExprId(), filter);
                        ctx.setTargetsOnScanNode(aliasTransferMap.get(unwrappedSlot).first.getId(), olapScanSlot);
                    }
                }
            }
        }
        return join;
    }

    private boolean hasRemoteTarget(AbstractPlan join, AbstractPlan scan) {
        Preconditions.checkArgument(join.getMutableState(AbstractPlan.FRAGMENT_ID).isPresent(),
                "cannot find fragment id for Join node");
        Preconditions.checkArgument(scan.getMutableState(AbstractPlan.FRAGMENT_ID).isPresent(),
                "cannot find fragment id for scan node");
        return join.getMutableState(AbstractPlan.FRAGMENT_ID).get()
                != scan.getMutableState(AbstractPlan.FRAGMENT_ID).get();
    }

    private long getBuildSideNdv(PhysicalHashJoin<? extends Plan, ? extends Plan> join, EqualTo equalTo) {
        AbstractPlan right = (AbstractPlan) join.right();
        //make ut test friendly
        if (right.getStats() == null) {
            return -1L;
        }
        ExpressionEstimation estimator = new ExpressionEstimation();
        ColumnStatistic buildColStats = equalTo.right().accept(estimator, right.getStats());
        return buildColStats.isUnKnown ? -1 : Math.max(1, (long) buildColStats.ndv);
    }

    @Override
    public PhysicalPlan visitPhysicalNestedLoopJoin(PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> join,
            CascadesContext context) {
        // TODO: we need to support all type join
        join.right().accept(this, context);
        join.left().accept(this, context);
        if (join.getJoinType() != JoinType.LEFT_SEMI_JOIN && join.getJoinType() != JoinType.CROSS_JOIN) {
            return join;
        }
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        Map<NamedExpression, Pair<PhysicalRelation, Slot>> aliasTransferMap = ctx.getAliasTransferMap();

        if ((ctx.getSessionVariable().getRuntimeFilterType() & TRuntimeFilterType.BITMAP.getValue()) == 0) {
            //only generate BITMAP filter for nested loop join
            return join;
        }
        List<Slot> leftSlots = join.left().getOutput();
        List<Slot> rightSlots = join.right().getOutput();
        List<Expression> bitmapRuntimeFilterConditions = JoinUtils.extractBitmapRuntimeFilterConditions(leftSlots,
                rightSlots, join.getOtherJoinConjuncts());
        if (!JoinUtils.extractExpressionForHashTable(leftSlots, rightSlots, join.getOtherJoinConjuncts())
                .first.isEmpty()) {
            return join;
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
                if (targetSlot != null && aliasTransferMap.containsKey(targetSlot)) {
                    Slot olapScanSlot = aliasTransferMap.get(targetSlot).second;
                    RuntimeFilter filter = new RuntimeFilter(generator.getNextId(),
                            bitmapContains.child(0), ImmutableList.of(olapScanSlot),
                            ImmutableList.of(bitmapContains.child(1)), type, i, join, isNot, -1L);
                    ctx.addJoinToTargetMap(join, olapScanSlot.getExprId());
                    ctx.setTargetExprIdToFilter(olapScanSlot.getExprId(), filter);
                    ctx.setTargetsOnScanNode(aliasTransferMap.get(targetSlot).first.getId(),
                            olapScanSlot);
                    join.addBitmapRuntimeFilterCondition(bitmapRuntimeFilterCondition);
                }
            }
        }
        return join;
    }

    @Override
    public PhysicalPlan visitPhysicalProject(PhysicalProject<? extends Plan> project, CascadesContext context) {
        project.child().accept(this, context);
        Map<NamedExpression, Pair<PhysicalRelation, Slot>> aliasTransferMap
                = context.getRuntimeFilterContext().getAliasTransferMap();
        // change key when encounter alias.
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
    public PhysicalRelation visitPhysicalScan(PhysicalRelation scan, CascadesContext context) {
        // add all the slots in map.
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        scan.getOutput().forEach(slot -> ctx.getAliasTransferMap().put(slot, Pair.of(scan, slot)));
        return scan;
    }

    private static Slot checkTargetChild(Expression leftChild) {
        Expression expression = ExpressionUtils.getExpressionCoveredByCast(leftChild);
        return expression instanceof Slot ? ((Slot) expression) : null;
    }
}
