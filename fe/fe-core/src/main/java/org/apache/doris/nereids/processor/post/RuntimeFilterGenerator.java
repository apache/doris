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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapContains;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * generate runtime filter
 */
public class RuntimeFilterGenerator extends PlanPostProcessor {
    private static final ImmutableSet<JoinType> deniedJoinType = ImmutableSet.of(
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
     */
    // TODO: current support inner join, cross join, right outer join, and will support more join type.
    @Override
    public PhysicalPlan visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> join,
            CascadesContext context) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        Map<NamedExpression, Pair<RelationId, Slot>> aliasTransferMap = ctx.getAliasTransferMap();
        join.right().accept(this, context);
        join.left().accept(this, context);
        if (deniedJoinType.contains(join.getJoinType()) || join.isMarkJoin()) {
            // copy to avoid bug when next call of getOutputSet()
            Set<Slot> slots = join.getOutputSet();
            slots.forEach(aliasTransferMap::remove);
        } else {
            List<TRuntimeFilterType> legalTypes = Arrays.stream(TRuntimeFilterType.values())
                    .filter(type -> (type.getValue() & ctx.getSessionVariable().getRuntimeFilterType()) > 0)
                    .collect(Collectors.toList());
            // TODO: some complex situation cannot be handled now, see testPushDownThroughJoin.
            // TODO: we will support it in later version.
            for (int i = 0; i < join.getHashJoinConjuncts().size(); i++) {
                EqualTo equalTo = ((EqualTo) JoinUtils.swapEqualToForChildrenOrder(
                        (EqualTo) join.getHashJoinConjuncts().get(i), join.left().getOutputSet()));
                for (TRuntimeFilterType type : legalTypes) {
                    //bitmap rf is generated by nested loop join.
                    if (type == TRuntimeFilterType.BITMAP) {
                        continue;
                    }
                    // currently, we can ensure children in the two side are corresponding to the equal_to's.
                    // so right maybe an expression and left is a slot or cast(slot)
                    Slot unwrappedSlot = checkTargetChild(equalTo.left());
                    // aliasTransMap doesn't contain the key, means that the path from the olap scan to the join
                    // contains join with denied join type. for example: a left join b on a.id = b.id
                    if (unwrappedSlot == null || !aliasTransferMap.containsKey(unwrappedSlot)) {
                        continue;
                    }
                    Slot olapScanSlot = aliasTransferMap.get(unwrappedSlot).second;
                    RuntimeFilter filter = new RuntimeFilter(generator.getNextId(),
                            equalTo.right(), olapScanSlot, type, i, join);
                    ctx.addJoinToTargetMap(join, olapScanSlot.getExprId());
                    ctx.setTargetExprIdToFilter(olapScanSlot.getExprId(), filter);
                    ctx.setTargetsOnScanNode(aliasTransferMap.get(unwrappedSlot).first, olapScanSlot);
                }
            }
        }
        return join;
    }

    @Override
    public PhysicalPlan visitPhysicalNestedLoopJoin(PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> join,
            CascadesContext context) {
        if (join.getJoinType() != JoinType.LEFT_SEMI_JOIN && join.getJoinType() != JoinType.CROSS_JOIN) {
            return join;
        }
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        Map<NamedExpression, Pair<RelationId, Slot>> aliasTransferMap = ctx.getAliasTransferMap();
        join.right().accept(this, context);
        join.left().accept(this, context);

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
            BitmapContains bitmapContains = null;
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
                            bitmapContains.child(0), olapScanSlot,
                            bitmapContains.child(1), type, i, join, isNot);
                    ctx.addJoinToTargetMap(join, olapScanSlot.getExprId());
                    ctx.setTargetExprIdToFilter(olapScanSlot.getExprId(), filter);
                    ctx.setTargetsOnScanNode(aliasTransferMap.get(targetSlot).first, olapScanSlot);
                    join.addBitmapRuntimeFilterCondition(bitmapRuntimeFilterCondition);
                }
            }
        }
        return join;
    }

    @Override
    public PhysicalPlan visitPhysicalProject(PhysicalProject<? extends Plan> project, CascadesContext context) {
        project.child().accept(this, context);
        Map<NamedExpression, Pair<RelationId, Slot>> aliasTransferMap
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
                    aliasTransferMap.put(alias.toSlot(), aliasTransferMap.remove(expr));
                }
            }
        }
        return project;
    }

    @Override
    public PhysicalRelation visitPhysicalScan(PhysicalRelation scan, CascadesContext context) {
        // add all the slots in map.
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        scan.getOutput().forEach(slot -> ctx.getAliasTransferMap().put(slot, Pair.of(scan.getId(), slot)));
        return scan;
    }

    @Override
    public PhysicalStorageLayerAggregate visitPhysicalStorageLayerAggregate(
            PhysicalStorageLayerAggregate storageLayerAggregate, CascadesContext context) {
        storageLayerAggregate.getRelation().accept(this, context);
        return storageLayerAggregate;
    }

    private static Slot checkTargetChild(Expression leftChild) {
        Expression expression = ExpressionUtils.getExpressionCoveredByCast(leftChild);
        return expression instanceof Slot ? ((Slot) expression) : null;
    }
}
