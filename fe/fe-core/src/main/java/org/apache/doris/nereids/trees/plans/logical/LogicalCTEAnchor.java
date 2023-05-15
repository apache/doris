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

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * LogicalCTEAnchor
 */
public class LogicalCTEAnchor<
        LEFT_CHILD_TYPE extends Plan,
        RIGHT_CHILD_TYPE extends Plan> extends LogicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {

    private final int cteId;
    private final int consumeNum;

    private final List<Expression> projection;

    public LogicalCTEAnchor(LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild, int cteId) {
        this(Optional.empty(), Optional.empty(), leftChild, rightChild, cteId);
    }

    public LogicalCTEAnchor(Optional<GroupExpression> groupExpression,
                            Optional<LogicalProperties> logicalProperties,
                            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild, int cteId) {
        super(PlanType.LOGICAL_CTE_ANCHOR, groupExpression, logicalProperties, leftChild, rightChild);
        this.cteId = cteId;
        this.consumeNum = -1;
        this.projection = Collections.emptyList();
    }

    public LogicalCTEAnchor(
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild, int cteId,
            int consumeNum, List<Expression> projection) {
        this(Optional.empty(), Optional.empty(),
                leftChild, rightChild, cteId, consumeNum, projection);
    }

    public LogicalCTEAnchor(Optional<GroupExpression> groupExpression,
                            Optional<LogicalProperties> logicalProperties,
                            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild,
                            int cteId, int consumeNum, List<Expression> projection) {
        super(PlanType.LOGICAL_CTE_ANCHOR, groupExpression, logicalProperties, leftChild, rightChild);
        this.cteId = cteId;
        this.consumeNum = consumeNum;
        this.projection = projection;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new LogicalCTEAnchor<>(groupExpression, Optional.of(getLogicalProperties()),
                children.get(0), children.get(1), cteId);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visit(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return child(1).getExpressions();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalCTEAnchor<>(groupExpression, Optional.of(getLogicalProperties()), left(), right(), cteId);
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalCTEAnchor<>(groupExpression, logicalProperties, left(), right(), cteId);
    }

    @Override
    public List<Slot> computeOutput() {
        return right().getOutput();
    }

    public int getCteId() {
        return cteId;
    }

    public int getConsumeNum() {
        return consumeNum;
    }

    public List<Expression> getProjection() {
        return projection;
    }

    @Override
    public String toString() {
        return String.format("LOGICAL_CTE_ANCHOR#%d", cteId);
    }
}
