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

import org.apache.doris.nereids.PlanContext;
import org.apache.doris.nereids.cost.CostEstimate;
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.JoinType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.Objects;
import java.util.Optional;

/**
 * Physical operator represents broadcast hash join.
 */
public class PhysicalBroadcastHashJoin<LEFT_INPUT_TYPE extends Plan, RIGHT_INPUT_TYPE extends Plan>
        extends PhysicalBinaryOperator<PhysicalBroadcastHashJoin<LEFT_INPUT_TYPE, RIGHT_INPUT_TYPE>,
            LEFT_INPUT_TYPE, RIGHT_INPUT_TYPE> {

    private final JoinType joinType;
    private final Optional<Expression> onClause;

    /**
     * Constructor for PhysicalBroadcastHashJoin.
     *
     * @param joinType logical join type in Nereids
     */
    public PhysicalBroadcastHashJoin(JoinType joinType) {
        this(joinType, Optional.empty());
    }

    /**
     * Constructor for PhysicalBroadcastHashJoin.
     *
     * @param joinType logical join type in Nereids
     * @param onClause on clause expression
     */
    public PhysicalBroadcastHashJoin(JoinType joinType, Optional<Expression> onClause) {
        super(OperatorType.PHYSICAL_BROADCAST_HASH_JOIN);
        this.joinType = Objects.requireNonNull(joinType, "joinType can not be null");
        this.onClause = Objects.requireNonNull(onClause, "onClause can not be null");
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public Optional<Expression> getOnClause() {
        return onClause;
    }

    @Override
    public CostEstimate calculateCost(PlanContext planCtx) {
        return null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Broadcast Hash Join (").append(joinType);
        if (onClause != null) {
            sb.append(", ").append(onClause);
        }
        return sb.append(")").toString();
    }
}
