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

package org.apache.doris.nereids.operators.plans.physical;

import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.JoinType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanOperatorVisitor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalBinaryPlan;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical hash join plan operator.
 */
public class PhysicalHashJoin extends PhysicalBinaryOperator {

    private final JoinType joinType;

    private final Optional<Expression> condition;

    /**
     * Constructor of PhysicalHashJoinNode.
     *
     * @param joinType Which join type, left semi join, inner join...
     * @param predicate join condition.
     */
    public PhysicalHashJoin(JoinType joinType, Optional<Expression> predicate) {
        super(OperatorType.PHYSICAL_HASH_JOIN);
        this.joinType = Objects.requireNonNull(joinType, "joinType can not be null");
        this.condition = Objects.requireNonNull(predicate, "predicate can not be null");
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Optional<Expression> getCondition() {
        return condition;
    }

    @Override
    public <R, C> R accept(PlanOperatorVisitor<R, C> visitor, Plan plan, C context) {
        return visitor.visitPhysicalHashJoin((PhysicalBinaryPlan<PhysicalHashJoin, Plan, Plan>) plan, context);
    }

    @Override
    public List<Expression> getExpressions() {
        return condition.<List<Expression>>map(ImmutableList::of).orElseGet(ImmutableList::of);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PhysicalHashJoin ([").append(joinType).append("]");
        if (condition.isPresent()) {
            sb.append(", [").append(condition.get()).append("]");
        }
        sb.append(")");
        return sb.toString();
    }
}
