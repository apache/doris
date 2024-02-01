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
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.processor.post.RuntimeFilterContext;
import org.apache.doris.nereids.processor.post.RuntimeFilterGenerator;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.EqualPredicate;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
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
            DistributeHint hint,
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
            DistributeHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        this(joinType, hashJoinConjuncts, otherJoinConjuncts, ExpressionUtils.EMPTY_CONDITION, hint,
                markJoinSlotReference, groupExpression, logicalProperties, null, null, leftChild,
                rightChild);
    }

    public PhysicalHashJoin(
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            List<Expression> markJoinConjuncts,
            DistributeHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            LogicalProperties logicalProperties,
            LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild) {
        this(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinConjuncts, hint, markJoinSlotReference,
                Optional.empty(), logicalProperties, null, null, leftChild, rightChild);
    }

    private PhysicalHashJoin(
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            List<Expression> markJoinConjuncts,
            DistributeHint hint,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties,
            Statistics statistics,
            LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.PHYSICAL_HASH_JOIN, joinType, hashJoinConjuncts, otherJoinConjuncts,
                markJoinConjuncts, hint, markJoinSlotReference, groupExpression, logicalProperties,
                physicalProperties, statistics, leftChild, rightChild);
    }

    /**
     * Get all used slots from hashJoinConjuncts of join.
     * Return pair of left used slots and right used slots.
     */
    public Pair<List<ExprId>, List<ExprId>> getHashConjunctsExprIds() {
        // TODO this function is only called by addShuffleJoinRequestProperty
        //  currently standalone mark join can only allow broadcast( we can remove this limitation after implement
        //  something like nullaware shuffle to broadcast nulls to all instances
        //  mark join with non-empty hash join conjuncts allow shuffle join by hash join conjuncts
        Preconditions.checkState(!(isMarkJoin() && hashJoinConjuncts.isEmpty()),
                "shouldn't call mark join's getHashConjunctsExprIds method for standalone mark join");
        int size = hashJoinConjuncts.size();

        List<ExprId> exprIds1 = new ArrayList<>(size);
        List<ExprId> exprIds2 = new ArrayList<>(size);

        Set<ExprId> leftExprIds = left().getOutputExprIdSet();
        Set<ExprId> rightExprIds = right().getOutputExprIdSet();

        for (Expression expr : hashJoinConjuncts) {
            for (ExprId exprId : expr.getInputSlotExprIds()) {
                if (leftExprIds.contains(exprId)) {
                    exprIds1.add(exprId);
                } else if (rightExprIds.contains(exprId)) {
                    exprIds2.add(exprId);
                } else {
                    throw new RuntimeException("Invalid ExprId found: " + exprId
                            + ". Cannot generate valid equal on clause slot pairs for join.");
                }
            }
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
                otherJoinConjuncts, markJoinConjuncts, hint, markJoinSlotReference,
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
        return new PhysicalHashJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts,
                markJoinConjuncts, hint, markJoinSlotReference, groupExpression,
                getLogicalProperties(), null, null, left(), right());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new PhysicalHashJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts,
                markJoinConjuncts, hint, markJoinSlotReference, groupExpression,
                logicalProperties.get(), null, null, children.get(0), children.get(1));
    }

    public PhysicalHashJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalHashJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts,
                markJoinConjuncts, hint, markJoinSlotReference, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, left(), right());
    }

    @Override
    public boolean pushDownRuntimeFilter(CascadesContext context, IdGenerator<RuntimeFilterId> generator,
            AbstractPhysicalJoin<?, ?> builderNode, Expression srcExpr, Expression probeExpr,
            TRuntimeFilterType type, long buildSideNdv, int exprOrder) {
        // currently, mark join doesn't support RF, so markJoinConjuncts is not processed here
        if (RuntimeFilterGenerator.DENIED_JOIN_TYPES.contains(getJoinType()) || isMarkJoin()) {
            if (builderNode instanceof PhysicalHashJoin) {
                PhysicalHashJoin<?, ?> builderJoin = (PhysicalHashJoin<?, ?>) builderNode;
                if (builderJoin == this) {
                    return false;
                }
            }
        }
        RuntimeFilterContext ctx = context.getRuntimeFilterContext();

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
        Pair<PhysicalRelation, Slot> pair = ctx.getAliasTransferMap().get(probeExpr);
        PhysicalRelation target1 = (pair == null) ? null : pair.first;
        PhysicalRelation target2 = null;
        pair = ctx.getAliasTransferMap().get(srcExpr);
        PhysicalRelation srcNode = (pair == null) ? null : pair.first;
        if (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().expandRuntimeFilterByInnerJoin) {
            if (!this.equals(builderNode) && this.getJoinType() == JoinType.INNER_JOIN) {
                for (Expression expr : this.getHashJoinConjuncts()) {
                    EqualPredicate equalTo = (EqualPredicate) expr;
                    if (probeExpr.equals(equalTo.left())) {
                        probExprList.add(equalTo.right());
                        pair = ctx.getAliasTransferMap().get(equalTo.right());
                        target2 = (pair == null) ? null : pair.first;
                    } else if (probeExpr.equals(equalTo.right())) {
                        probExprList.add(equalTo.left());
                        pair = ctx.getAliasTransferMap().get(equalTo.left());
                        target2 = (pair == null) ? null : pair.first;
                    }
                    if (target2 != null) {
                        ctx.getExpandedRF().add(
                            new RuntimeFilterContext.ExpandRF(this, srcNode, target1, target2, equalTo));
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

        return pushedDown;
    }

    @Override
    public String shapeInfo() {
        StringBuilder builder = new StringBuilder();
        builder.append("hashJoin[").append(joinType).append("]");
        // print sorted hash conjuncts for plan check
        builder.append(hashJoinConjuncts.stream().map(conjunct -> conjunct.shapeInfo())
                .sorted().collect(Collectors.joining(" and ", " hashCondition=(", ")")));
        builder.append(otherJoinConjuncts.stream().map(cond -> cond.shapeInfo())
                .sorted().collect(Collectors.joining(" and ", " otherCondition=(", ")")));
        if (!markJoinConjuncts.isEmpty()) {
            builder.append(markJoinConjuncts.stream().map(cond -> cond.shapeInfo()).sorted()
                    .collect(Collectors.joining(" and ", " markCondition=(", ")")));
        }
        if (!runtimeFilters.isEmpty()) {
            builder.append(" build RFs:").append(runtimeFilters.stream()
                    .map(rf -> rf.shapeInfo()).collect(Collectors.joining(";")));
        }
        return builder.toString();
    }

    @Override
    public PhysicalHashJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalHashJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts,
                markJoinConjuncts, hint, markJoinSlotReference, groupExpression, null,
                physicalProperties, statistics, left(), right());
    }
}
