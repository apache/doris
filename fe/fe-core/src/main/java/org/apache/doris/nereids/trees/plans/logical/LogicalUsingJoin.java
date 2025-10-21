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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.BlockFuncDepsPropagation;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * select col1 from t1 join t2 using(col1);
 */
public class LogicalUsingJoin<LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
        extends LogicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> implements BlockFuncDepsPropagation {

    private final JoinType joinType;
    private final ImmutableList<Expression> usingSlots;
    private final DistributeHint hint;

    public LogicalUsingJoin(JoinType joinType, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild,
            List<Expression> usingSlots, DistributeHint hint) {
        this(joinType, leftChild, rightChild, usingSlots, Optional.empty(), Optional.empty(), hint);
    }

    /**
     * Constructor.
     */
    public LogicalUsingJoin(JoinType joinType, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild,
            List<Expression> usingSlots, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, DistributeHint hint) {
        super(PlanType.LOGICAL_USING_JOIN, groupExpression, logicalProperties, leftChild, rightChild);
        this.joinType = joinType;
        this.usingSlots = ImmutableList.copyOf(usingSlots);
        this.hint = hint;
    }

    @Override
    public List<Slot> computeOutput() {
        return JoinUtils.getJoinOutput(joinType, left(), right());
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalUsingJoin<>(joinType, child(0), child(1),
                usingSlots, groupExpression, Optional.of(getLogicalProperties()), hint);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalUsingJoin<>(joinType, children.get(0), children.get(1),
                usingSlots, groupExpression, logicalProperties, hint);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new LogicalUsingJoin<>(joinType, children.get(0), children.get(1),
                usingSlots, groupExpression, Optional.of(getLogicalProperties()), hint);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visit(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return usingSlots;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public List<Expression> getUsingSlots() {
        return usingSlots;
    }

    public DistributeHint getDistributeHint() {
        return hint;
    }

    public List<Expression> getMarkJoinConjuncts() {
        return ExpressionUtils.EMPTY_CONDITION;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalUsingJoin<?, ?> usingJoin = (LogicalUsingJoin<?, ?>) o;
        return joinType == usingJoin.joinType
                && Objects.equals(usingSlots, usingJoin.usingSlots)
                && Objects.equals(hint, usingJoin.hint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(joinType, usingSlots, hint);
    }

    @Override
    public String toString() {
        List<Object> args = Lists.newArrayList(
                "type", joinType,
                "usingSlots", usingSlots,
                "stats", statistics);
        if (hint.distributeType != DistributeType.NONE) {
            args.add("hint");
            args.add(hint.getExplainString());
        }
        return Utils.toSqlStringSkipNull("UsingJoin[" + id.asInt() + "]", args.toArray());
    }
}
