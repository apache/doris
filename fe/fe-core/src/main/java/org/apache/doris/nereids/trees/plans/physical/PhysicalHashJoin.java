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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.processor.post.RuntimeFilterGenerator;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Physical hash join plan.
 */
public class PhysicalHashJoin<
        LEFT_CHILD_TYPE extends Plan,
        RIGHT_CHILD_TYPE extends Plan>
        extends AbstractPhysicalJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {

    public PhysicalHashJoin(
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            JoinHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            LogicalProperties logicalProperties,
            LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild) {
        this(joinType, hashJoinConjuncts, otherJoinConjuncts, hint, markJoinSlotReference,
                Optional.empty(), logicalProperties, leftChild, rightChild);
    }

    /**
     * Constructor of PhysicalHashJoinNode.
     *
     * @param joinType Which join type, left semi join, inner join...
     * @param hashJoinConjuncts conjunct list could use for build hash table in hash join
     */
    public PhysicalHashJoin(
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            JoinHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.PHYSICAL_HASH_JOIN, joinType, hashJoinConjuncts, otherJoinConjuncts, hint, markJoinSlotReference,
                groupExpression, logicalProperties, leftChild, rightChild);
    }

    private PhysicalHashJoin(
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            JoinHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties,
            Statistics statistics,
            LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.PHYSICAL_HASH_JOIN, joinType, hashJoinConjuncts, otherJoinConjuncts, hint, markJoinSlotReference,
                groupExpression, logicalProperties, physicalProperties, statistics, leftChild, rightChild);
    }

    /**
     * Get all used slots from hashJoinConjuncts of join.
     * Return pair of left used slots and right used slots.
     */
    public Pair<List<ExprId>, List<ExprId>> getHashConjunctsExprIds() {
        List<ExprId> exprIds1 = Lists.newArrayListWithCapacity(hashJoinConjuncts.size());
        List<ExprId> exprIds2 = Lists.newArrayListWithCapacity(hashJoinConjuncts.size());

        Set<ExprId> leftExprIds = left().getOutputExprIdSet();
        Set<ExprId> rightExprIds = right().getOutputExprIdSet();

        for (Expression expr : hashJoinConjuncts) {
            expr.getInputSlotExprIds().forEach(exprId -> {
                if (leftExprIds.contains(exprId)) {
                    exprIds1.add(exprId);
                } else if (rightExprIds.contains(exprId)) {
                    exprIds2.add(exprId);
                } else {
                    throw new RuntimeException("Could not generate valid equal on clause slot pairs for join");
                }
            });
        }
        return Pair.of(exprIds1, exprIds2);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalHashJoin(this, context);
    }

    @Override
    public String toString() {
        List<Object> args = Lists.newArrayList("type", joinType,
                "hashJoinCondition", hashJoinConjuncts,
                "otherJoinCondition", otherJoinConjuncts,
                "stats", statistics,
                "fr", getMutableState(AbstractPlan.FRAGMENT_ID));
        if (markJoinSlotReference.isPresent()) {
            args.add("isMarkJoin");
            args.add("true");
        }
        if (markJoinSlotReference.isPresent()) {
            args.add("MarkJoinSlotReference");
            args.add(markJoinSlotReference.get());
        }
        if (hint != JoinHint.NONE) {
            args.add("hint");
            args.add(hint);
        }
        if (!runtimeFilters.isEmpty()) {
            args.add("runtimeFilters");
            args.add(runtimeFilters.stream().map(rf -> rf.toString() + " ").collect(Collectors.toList()));
        }
        return Utils.toSqlString("PhysicalHashJoin[" + id.asInt() + "]" + getGroupIdWithPrefix(),
                args.toArray());
    }

    @Override
    public PhysicalHashJoin<Plan, Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        PhysicalHashJoin newJoin = new PhysicalHashJoin<>(joinType, hashJoinConjuncts,
                otherJoinConjuncts, hint, markJoinSlotReference,
                Optional.empty(), getLogicalProperties(), physicalProperties, statistics,
                children.get(0), children.get(1));
        if (groupExpression.isPresent()) {
            newJoin.setMutableState(MutableState.KEY_GROUP, groupExpression.get().getOwnerGroup().getGroupId().asInt());
        }
        return newJoin;
    }

    @Override
    public PhysicalHashJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> withGroupExpression(
            Optional<GroupExpression> groupExpression) {
        return new PhysicalHashJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint, markJoinSlotReference,
                groupExpression, getLogicalProperties(), left(), right());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new PhysicalHashJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint, markJoinSlotReference,
                groupExpression, logicalProperties.get(), children.get(0), children.get(1));
    }

    public PhysicalHashJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalHashJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint, markJoinSlotReference,
                groupExpression, getLogicalProperties(), physicalProperties, statistics, left(), right());
    }

    @Override
    public boolean pushDownRuntimeFilter(CascadesContext context, IdGenerator<RuntimeFilterId> generator,
                                         AbstractPhysicalJoin builderNode, Expression srcExpr, Expression probeExpr,
                                         TRuntimeFilterType type, long buildSideNdv, int exprOrder) {
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();
        Map<NamedExpression, Pair<PhysicalRelation, Slot>> aliasTransferMap = ctx.getAliasTransferMap();

        // if rf built between plan nodes containing cte both, for example both src slot and target slot are from cte,
        // or two sub-queries both containing cte, disable this rf since this kind of cross-cte rf will make one side
        // of cte to wait for a long time until another side cte consumer finished, which will make the rf into
        // not ready state.
        AbstractPhysicalPlan builderLeftNode = (AbstractPhysicalPlan) builderNode.child(0);
        AbstractPhysicalPlan builderRightNode = (AbstractPhysicalPlan) builderNode.child(1);
        Preconditions.checkState(builderLeftNode != null && builderRightNode != null,
                "builder join node child node is null");
        if (RuntimeFilterGenerator.hasCTEConsumerDescendant(builderLeftNode)
                && RuntimeFilterGenerator.hasCTEConsumerDescendant(builderRightNode)) {
            return false;
        }

        boolean pushedDown = false;
        AbstractPhysicalPlan leftNode = (AbstractPhysicalPlan) child(0);
        AbstractPhysicalPlan rightNode = (AbstractPhysicalPlan) child(1);
        Preconditions.checkState(leftNode != null && rightNode != null,
                "join child node is null");

        pushedDown |= leftNode.pushDownRuntimeFilter(context, generator, builderNode,
                srcExpr, probeExpr, type, buildSideNdv, exprOrder);
        pushedDown |= rightNode.pushDownRuntimeFilter(context, generator, builderNode,
                srcExpr, probeExpr, type, buildSideNdv, exprOrder);

        // currently, we can ensure children in the two side are corresponding to the equal_to's.
        // so right maybe an expression and left is a slot
        Slot probeSlot = RuntimeFilterGenerator.checkTargetChild(probeExpr);

        // aliasTransMap doesn't contain the key, means that the path from the olap scan to the join
        // contains join with denied join type. for example: a left join b on a.id = b.id
        if (!RuntimeFilterGenerator.checkPushDownPreconditions(builderNode, ctx, probeSlot)) {
            return false;
        }
        Slot olapScanSlot = aliasTransferMap.get(probeSlot).second;
        PhysicalRelation scan = aliasTransferMap.get(probeSlot).first;
        Preconditions.checkState(olapScanSlot != null && scan != null);
        if (!RuntimeFilterGenerator.isCoveredByPlanNode(this, scan)) {
            return false;
        }

        // TODO: if can't push down into join's chidren, try to
        // find possible chance in upper layer
        if (pushedDown) {
            return true;
        }

        // in-filter is not friendly to pipeline
        if (type == TRuntimeFilterType.IN_OR_BLOOM
                && ctx.getSessionVariable().getEnablePipelineEngine()
                && RuntimeFilterGenerator.hasRemoteTarget(this, scan)) {
            type = TRuntimeFilterType.BLOOM;
        }
        RuntimeFilter filter = new RuntimeFilter(generator.getNextId(),
                srcExpr, ImmutableList.of(olapScanSlot), type, exprOrder, this, buildSideNdv);
        ctx.addJoinToTargetMap(this, olapScanSlot.getExprId());
        ctx.setTargetExprIdToFilter(olapScanSlot.getExprId(), filter);
        ctx.setTargetsOnScanNode(aliasTransferMap.get(probeSlot).first.getRelationId(), olapScanSlot);
        return true;
    }

    private class ExprComparator implements Comparator<Expression> {
        @Override
        public int compare(Expression e1, Expression e2) {
            List<ExprId> ids1 = e1.getInputSlotExprIds()
                    .stream().sorted(Comparator.comparing(ExprId::asInt))
                    .collect(Collectors.toList());
            List<ExprId> ids2 = e2.getInputSlotExprIds()
                    .stream().sorted(Comparator.comparing(ExprId::asInt))
                    .collect(Collectors.toList());
            if (ids1.size() > ids2.size()) {
                return 1;
            } else if (ids1.size() < ids2.size()) {
                return -1;
            } else {
                for (int i = 0; i < ids1.size(); i++) {
                    if (ids1.get(i).asInt() > ids2.get(i).asInt()) {
                        return 1;
                    } else if (ids1.get(i).asInt() < ids2.get(i).asInt()) {
                        return -1;
                    }
                }
                return 0;
            }
        }
    }

    @Override
    public String shapeInfo() {
        StringBuilder builder = new StringBuilder();
        builder.append("hashJoin[").append(joinType).append("]");
        // print sorted hash conjuncts for plan check
        hashJoinConjuncts.stream().sorted(new ExprComparator()).forEach(expr -> {
            builder.append(expr.shapeInfo());
        });
        otherJoinConjuncts.stream().sorted(new ExprComparator()).forEach(expr -> {
            builder.append(expr.shapeInfo());
        });
        return builder.toString();
    }

    @Override
    public PhysicalHashJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalHashJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint, markJoinSlotReference,
                groupExpression, null, physicalProperties, statistics, left(), right());
    }
}
