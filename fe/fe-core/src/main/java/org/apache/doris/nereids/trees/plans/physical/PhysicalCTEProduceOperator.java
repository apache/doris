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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical CTE producer operator.
 */
public class PhysicalCTEProduceOperator<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> {
    private final int cteId;

    public PhysicalCTEProduceOperator(int cteId, LogicalProperties logicalProperties, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_CTE_PRODUCE, Optional.empty(), logicalProperties, child);
        this.cteId = cteId;
    }

    public PhysicalCTEProduceOperator(int cteId, Optional<GroupExpression> groupExpression,
                          LogicalProperties logicalProperties, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_CTE_PRODUCE, groupExpression, logicalProperties, child);
        this.cteId = cteId;
    }

    public PhysicalCTEProduceOperator(int cteId, Optional<GroupExpression> groupExpression,
                          LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
                          Statistics statistics, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_CTE_PRODUCE, groupExpression, logicalProperties, physicalProperties, statistics, child);
        this.cteId = cteId;
    }

    public int getCteId() {
        return cteId;
    }

    // TODO
    @Override
    public List<Expression> getExpressions() {
        List<Expression> output = new ArrayList<>();
        child().getExpressions().stream().map(e -> output.add(e));
        return output;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalCTEProduceOperator that = (PhysicalCTEProduceOperator) o;
        return Objects.equals(cteId, that.cteId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cteId);
    }

    @Override
    public String toString() {
        return "PhysicalCTEProduceOperator{"
                + "cteId='"
                + cteId
                + '\''
                + '}';
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalCTEProduce(this, context);
    }

    @Override
    public PhysicalCTEProduceOperator<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalCTEProduceOperator<>(cteId, getLogicalProperties(), children.get(0));
    }

    @Override
    public PhysicalCTEProduceOperator<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalCTEProduceOperator<>(cteId, groupExpression, getLogicalProperties(), child());
    }

    @Override
    public PhysicalCTEProduceOperator<CHILD_TYPE> withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalCTEProduceOperator<>(cteId, Optional.empty(), logicalProperties.get(), child());
    }

    @Override
    public PhysicalCTEProduceOperator<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
                                                                     Statistics statistics) {
        return new PhysicalCTEProduceOperator<>(cteId, groupExpression, getLogicalProperties(), physicalProperties,
            statistics, child());
    }

    @Override
    public String shapeInfo() {
        StringBuilder builder = new StringBuilder();
        builder.append("cte producer(" + cteId + ")");
        return builder.toString();
    }
}
