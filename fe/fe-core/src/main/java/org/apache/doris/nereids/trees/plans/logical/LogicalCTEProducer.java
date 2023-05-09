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

import java.util.List;
import java.util.Optional;

public class LogicalCTEProducer<CHILD_TYPE extends Plan>
        extends LogicalUnary<CHILD_TYPE> {

    private final long cteId;

    public LogicalCTEProducer(CHILD_TYPE child, long cteId) {
        super(PlanType.LOGICAL_CTE_PRODUCER, child);
        this.cteId = cteId;
    }

    public LogicalCTEProducer(PlanType type, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child, long cteId) {
        super(type, groupExpression, logicalProperties, child);
        this.cteId = cteId;
    }

    public long getCteId() {
        return cteId;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new LogicalCTEProducer<>(type, groupExpression, Optional.of(getLogicalProperties()), children.get(0),
                cteId);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visit(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return child().getExpressions();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalCTEProducer<>(type, groupExpression, Optional.of(getLogicalProperties()), child(), cteId);
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalCTEProducer<>(type, groupExpression, logicalProperties, child(), cteId);
    }

    @Override
    public List<Slot> computeOutput() {
        return child().computeOutput();
    }
}
