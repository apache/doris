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
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.PropagateFuncDeps;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * LogicalCTEAnchor
 */
public class LogicalCTEAnchor<LEFT_CHILD_TYPE extends Plan,
        RIGHT_CHILD_TYPE extends Plan> extends LogicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> implements
        PropagateFuncDeps {

    private final CTEId cteId;

    public LogicalCTEAnchor(CTEId cteId, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        this(cteId, Optional.empty(), Optional.empty(), leftChild, rightChild);
    }

    public LogicalCTEAnchor(CTEId cteId, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.LOGICAL_CTE_ANCHOR, groupExpression, logicalProperties, leftChild, rightChild);
        this.cteId = cteId;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new LogicalCTEAnchor<>(cteId, children.get(0), children.get(1));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalCTEAnchor(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalCTEAnchor<>(cteId, groupExpression, Optional.of(getLogicalProperties()), left(), right());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalCTEAnchor<>(cteId, groupExpression, logicalProperties, children.get(0), children.get(1));
    }

    @Override
    public List<Slot> computeOutput() {
        return right().getOutput();
    }

    public CTEId getCteId() {
        return cteId;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalCteAnchor[" + id.asInt() + "]",
                "cteId", cteId);
    }
}
