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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

public class LogicalCTERelation extends LogicalLeaf {

    private final LogicalPlan childPlan;

    private final int cteId;

    public LogicalCTERelation(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, LogicalPlan childPlan, int cteId) {
        super(PlanType.LOGICAL_CTE_RELATION, groupExpression, logicalProperties);
        this.childPlan = childPlan;
        this.cteId = cteId;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visit(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalCTERelation(groupExpression, Optional.of(getLogicalProperties()), childPlan, cteId);
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalCTERelation(groupExpression, logicalProperties, childPlan, cteId);
    }

    @Override
    public List<Slot> computeOutput() {
        return childPlan.computeOutput();
    }

    public LogicalPlan getChildPlan() {
        return childPlan;
    }

    public int getCteId() {
        return cteId;
    }

}
