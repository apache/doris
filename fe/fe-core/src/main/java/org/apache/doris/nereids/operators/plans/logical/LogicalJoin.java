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

package org.apache.doris.nereids.operators.plans.logical;

import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.JoinType;
import org.apache.doris.nereids.rules.exploration.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical join plan operator.
 */
public class LogicalJoin extends LogicalBinaryOperator {

    private final JoinType joinType;
    private final Optional<Expression> onClause;

    // Use for top-to-down join reorder
    private final JoinReorderContext joinReorderContext = new JoinReorderContext();

    /**
     * Constructor for LogicalJoinPlan.
     *
     * @param joinType logical type for join
     */
    public LogicalJoin(JoinType joinType) {
        this(joinType, Optional.empty());
    }

    /**
     * Constructor for LogicalJoinPlan.
     *
     * @param joinType logical type for join
     * @param onClause on clause for join node
     */
    public LogicalJoin(JoinType joinType, Optional<Expression> onClause) {
        super(OperatorType.LOGICAL_JOIN);
        this.joinType = Objects.requireNonNull(joinType, "joinType can not be null");
        this.onClause = Objects.requireNonNull(onClause, "onClause can not be null");
    }

    public Optional<Expression> getOnClause() {
        return onClause;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    @Override
    public List<Slot> computeOutput(Plan leftInput, Plan rightInput) {
        switch (joinType) {
            case LEFT_SEMI_JOIN:
                return ImmutableList.copyOf(leftInput.getOutput());
            case RIGHT_SEMI_JOIN:
                return ImmutableList.copyOf(rightInput.getOutput());
            default:
                return ImmutableList.<Slot>builder()
                        .addAll(leftInput.getOutput())
                        .addAll(rightInput.getOutput())
                        .build();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Join (").append(joinType);
        if (onClause != null) {
            sb.append(", ").append(onClause);
        }
        return sb.append(")").toString();
    }

    public JoinReorderContext getJoinReorderContext() {
        return joinReorderContext;
    }
}
