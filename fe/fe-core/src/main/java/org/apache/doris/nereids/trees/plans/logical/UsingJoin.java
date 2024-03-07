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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.BlockFuncDepsPropagation;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;
import java.util.Optional;

/**
 * select col1 from t1 join t2 using(col1);
 */
public class UsingJoin<LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
        extends LogicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> implements Join, BlockFuncDepsPropagation {

    private final JoinType joinType;
    private final ImmutableList<Expression> otherJoinConjuncts;
    private final ImmutableList<Expression> hashJoinConjuncts;
    private final DistributeHint hint;

    public UsingJoin(JoinType joinType, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild,
            List<Expression> expressions, List<Expression> hashJoinConjuncts,
            DistributeHint hint) {
        this(joinType, leftChild, rightChild, expressions,
                hashJoinConjuncts, Optional.empty(), Optional.empty(), hint);
    }

    /**
     * Constructor.
     */
    public UsingJoin(JoinType joinType, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild,
            List<Expression> expressions, List<Expression> hashJoinConjuncts, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            DistributeHint hint) {
        super(PlanType.LOGICAL_USING_JOIN, groupExpression, logicalProperties, leftChild, rightChild);
        this.joinType = joinType;
        this.otherJoinConjuncts = ImmutableList.copyOf(expressions);
        this.hashJoinConjuncts = ImmutableList.copyOf(hashJoinConjuncts);
        this.hint = hint;
    }

    @Override
    public List<Slot> computeOutput() {

        List<Slot> newLeftOutput = left().getOutput().stream().map(o -> o.withNullable(true))
                .collect(ImmutableList.toImmutableList());

        List<Slot> newRightOutput = right().getOutput().stream().map(o -> o.withNullable(true))
                .collect(ImmutableList.toImmutableList());

        switch (joinType) {
            case LEFT_SEMI_JOIN:
            case LEFT_ANTI_JOIN:
                return ImmutableList.copyOf(left().getOutput());
            case RIGHT_SEMI_JOIN:
            case RIGHT_ANTI_JOIN:
                return ImmutableList.copyOf(right().getOutput());
            case LEFT_OUTER_JOIN:
                return ImmutableList.<Slot>builder()
                        .addAll(left().getOutput())
                        .addAll(newRightOutput)
                        .build();
            case RIGHT_OUTER_JOIN:
                return ImmutableList.<Slot>builder()
                        .addAll(newLeftOutput)
                        .addAll(right().getOutput())
                        .build();
            case FULL_OUTER_JOIN:
                return ImmutableList.<Slot>builder()
                        .addAll(newLeftOutput)
                        .addAll(newRightOutput)
                        .build();
            default:
                return ImmutableList.<Slot>builder()
                        .addAll(left().getOutput())
                        .addAll(right().getOutput())
                        .build();
        }
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new UsingJoin(joinType, child(0), child(1), otherJoinConjuncts,
                hashJoinConjuncts, groupExpression, Optional.of(getLogicalProperties()), hint);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new UsingJoin(joinType, children.get(0), children.get(1), otherJoinConjuncts,
                hashJoinConjuncts, groupExpression, logicalProperties, hint);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new UsingJoin(joinType, children.get(0), children.get(1), otherJoinConjuncts,
                hashJoinConjuncts, groupExpression, Optional.of(getLogicalProperties()), hint);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visit(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return new Builder<Expression>()
                .addAll(hashJoinConjuncts)
                .addAll(otherJoinConjuncts)
                .build();
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public List<Expression> getOtherJoinConjuncts() {
        return otherJoinConjuncts;
    }

    public List<Expression> getHashJoinConjuncts() {
        return hashJoinConjuncts;
    }

    public DistributeHint getDistributeHint() {
        return hint;
    }

    public boolean isMarkJoin() {
        return false;
    }

    public Optional<MarkJoinSlotReference> getMarkJoinSlotReference() {
        return Optional.empty();
    }

    public List<Expression> getMarkJoinConjuncts() {
        return ExpressionUtils.EMPTY_CONDITION;
    }

    @Override
    public Optional<Expression> getOnClauseCondition() {
        return ExpressionUtils.optionalAnd(hashJoinConjuncts, otherJoinConjuncts);
    }

    @Override
    public boolean hasDistributeHint() {
        return hint != null;
    }
}
