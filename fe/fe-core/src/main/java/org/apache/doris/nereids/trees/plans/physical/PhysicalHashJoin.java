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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
            AbstractPhysicalJoin<?, ?> builderNode, Expression srcExpr, Expression probeExpr,
            TRuntimeFilterType type, long buildSideNdv, int exprOrder) {
        if (RuntimeFilterGenerator.DENIED_JOIN_TYPES.contains(getJoinType()) || isMarkJoin()) {
            if (builderNode instanceof PhysicalHashJoin) {
                PhysicalHashJoin<?, ?> builderJion = (PhysicalHashJoin<?, ?>) builderNode;
                if (builderJion == this) {
                    return false;
                }
            }
        }
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

        Set<Expression> probExprList = Sets.newHashSet(probeExpr);
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().expandRuntimeFilterByInnerJoin) {
            if (!this.equals(builderNode) && this.getJoinType() == JoinType.INNER_JOIN) {
                for (Expression expr : this.getHashJoinConjuncts()) {
                    EqualTo equalTo = (EqualTo) expr;
                    if (probeExpr.equals(equalTo.left())) {
                        probExprList.add(equalTo.right());
                    } else if (probeExpr.equals(equalTo.right())) {
                        probExprList.add(equalTo.left());
                    }
                }
                probExprList.remove(srcExpr);
            }
        }
        for (Expression prob : probExprList) {
            pushedDown |= leftNode.pushDownRuntimeFilter(context, generator, builderNode,
                    srcExpr, prob, type, buildSideNdv, exprOrder);
            pushedDown |= rightNode.pushDownRuntimeFilter(context, generator, builderNode,
                    srcExpr, prob, type, buildSideNdv, exprOrder);
        }

        // currently, we can ensure children in the two side are corresponding to the equal_to's.
        // so right maybe an expression and left is a slot
        Slot probeSlot = RuntimeFilterGenerator.checkTargetChild(probeExpr);

        // aliasTransMap doesn't contain the key, means that the path from the olap scan to the join
        // contains join with denied join type. for example: a left join b on a.id = b.id
        if (!RuntimeFilterGenerator.checkPushDownPreconditionsForJoin(builderNode, ctx, probeSlot)) {
            return false;
        }
        PhysicalRelation scan = aliasTransferMap.get(probeSlot).first;
        if (!RuntimeFilterGenerator.checkPushDownPreconditionsForRelation(this, scan)) {
            return false;
        }

        return pushedDown;
    }

    public Set<Slot> getConditionSlot() {
        return Stream.concat(hashJoinConjuncts.stream(), otherJoinConjuncts.stream())
                .flatMap(expr -> expr.getInputSlots().stream()).collect(ImmutableSet.toImmutableSet());
    }

    @Override
    public String shapeInfo() {
        StringBuilder builder = new StringBuilder();
        builder.append("hashJoin[").append(joinType).append("]");
        // print sorted hash conjuncts for plan check
        builder.append(hashJoinConjuncts.stream().map(conjunct -> conjunct.shapeInfo())
                .sorted().collect(Collectors.joining(" and ", " hashCondition=(", ")")));
        builder.append(otherJoinConjuncts.stream().map(cond -> cond.shapeInfo())
                .sorted().collect(Collectors.joining(" and ", "otherCondition=(", ")")));
        return builder.toString();
    }

    @Override
    public PhysicalHashJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalHashJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts, hint, markJoinSlotReference,
                groupExpression, null, physicalProperties, statistics, left(), right());
    }
}
