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
 * Physical CTE anchor operator.
 */
public class PhysicalCTEAnchorOperator<
        LEFT_CHILD_TYPE extends Plan,
        RIGHT_CHILD_TYPE extends Plan>
        extends PhysicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {
    private final int cteId;
    private final int consumeNum;
    private final List<Expression> projection;

    public PhysicalCTEAnchorOperator(int cteId, int consumeNum, List<Expression> projection,
                                     LogicalProperties logicalProperties,
                                     LEFT_CHILD_TYPE leftChild,
                                     RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.PHYSICAL_CTE_ANCHOR, Optional.empty(),
                logicalProperties, leftChild, rightChild);
        this.cteId = cteId;
        this.consumeNum = consumeNum;
        this.projection = projection;
    }

    public PhysicalCTEAnchorOperator(int cteId, int consumeNum, List<Expression> projection,
                                     Optional<GroupExpression> groupExpression,
                                      LogicalProperties logicalProperties,
                                     LEFT_CHILD_TYPE leftChild,
                                     RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.PHYSICAL_CTE_ANCHOR, groupExpression, logicalProperties, leftChild, rightChild);
        this.cteId = cteId;
        this.consumeNum = consumeNum;
        this.projection = projection;
    }

    public PhysicalCTEAnchorOperator(int cteId, int consumeNum, List<Expression> projection,
                                     Optional<GroupExpression> groupExpression,
                                      LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
                                      Statistics statistics, LEFT_CHILD_TYPE leftChild,
                                     RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.PHYSICAL_CTE_ANCHOR, groupExpression, logicalProperties, physicalProperties, statistics,
                leftChild, rightChild);
        this.cteId = cteId;
        this.consumeNum = consumeNum;
        this.projection = projection;
    }

    public int getCteId() {
        return cteId;
    }

    public int getConsumeNum() {
        return consumeNum;
    }

    @Override
    public List<Expression> getExpressions() {
        return new ArrayList<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalCTEAnchorOperator that = (PhysicalCTEAnchorOperator) o;
        return Objects.equals(cteId, that.cteId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), cteId);
    }

    @Override
    public String toString() {
        return "PhysicalCTEAnchorOperator{"
                + "cteId='"
                + cteId
                + '\''
                + '}';
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalCTEAnchor(this, context);
    }

    @Override
    public PhysicalCTEAnchorOperator<Plan, Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new PhysicalCTEAnchorOperator<>(cteId, consumeNum, projection,
            getLogicalProperties(), children.get(0), children.get(1));
    }

    @Override
    public PhysicalCTEAnchorOperator<Plan, Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalCTEAnchorOperator<>(cteId, consumeNum, projection, groupExpression, getLogicalProperties(),
            child(0), child(1));
    }

    @Override
    public PhysicalCTEAnchorOperator<Plan, Plan> withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalCTEAnchorOperator<>(cteId, consumeNum, projection,
            Optional.empty(), logicalProperties.get(), child(0), child(1));
    }

    @Override
    public PhysicalCTEAnchorOperator<Plan, Plan> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
                                                                                 Statistics statistics) {
        return new PhysicalCTEAnchorOperator<>(cteId, consumeNum, projection, groupExpression,
            getLogicalProperties(), physicalProperties,
            statistics, child(0), child(1));
    }

    @Override
    public String shapeInfo() {
        StringBuilder builder = new StringBuilder();
        builder.append("cte Anchor(" + cteId + ")");
        return builder.toString();
    }
}
