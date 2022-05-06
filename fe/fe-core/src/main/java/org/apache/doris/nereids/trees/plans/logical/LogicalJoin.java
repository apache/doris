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

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Logical join plan node.
 */
public class LogicalJoin extends LogicalBinary {
    private final JoinType joinType;
    private final Expression onClause;

    /**
     * Constructor for LogicalJoinPlan.
     *
     * @param joinType logical type for join
     * @param onClause on clause for join node
     * @param left left child of join node
     * @param right right child of join node
     */
    public LogicalJoin(JoinType joinType, Expression onClause, LogicalPlan left, LogicalPlan right) {
        super(NodeType.LOGICAL_JOIN, left, right);
        this.joinType = joinType;
        this.onClause = onClause;
    }

    public Expression getOnClause() {
        return onClause;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    @Override
    public List<Slot> getOutput() throws UnboundException {
        if (CollectionUtils.isEmpty(output)) {
            switch (joinType) {
                case LEFT_SEMI_JOIN:
                    output.addAll(left().getOutput());
                    break;
                case RIGHT_SEMI_JOIN:
                    output.addAll(right().getOutput());
                    break;
                default:
                    output.addAll(left().getOutput());
                    output.addAll(right().getOutput());
                    break;
            }
        }
        return output;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Join (").append(joinType);
        if (onClause != null) {
            sb.append(", ").append(onClause);
        }
        if (CollectionUtils.isNotEmpty(output)) {
            sb.append(", output: ").append(StringUtils.join(output, ", "));
        }
        return sb.append(")").toString();
    }
}
