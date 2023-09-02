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
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical CTE anchor.
 */
public class PhysicalCTEAnchor<
        LEFT_CHILD_TYPE extends Plan,
        RIGHT_CHILD_TYPE extends Plan>
        extends PhysicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {
    private final CTEId cteId;

    public PhysicalCTEAnchor(CTEId cteId, LogicalProperties logicalProperties,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        this(cteId, Optional.empty(), logicalProperties, leftChild, rightChild);
    }

    public PhysicalCTEAnchor(CTEId cteId, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        this(cteId, groupExpression, logicalProperties, null, null, leftChild, rightChild);
    }

    public PhysicalCTEAnchor(CTEId cteId, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            Statistics statistics, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.PHYSICAL_CTE_ANCHOR, groupExpression, logicalProperties, physicalProperties, statistics,
                leftChild, rightChild);
        this.cteId = cteId;
    }

    public CTEId getCteId() {
        return cteId;
    }

    @Override
    public List<Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalCTEAnchor<?, ?> that = (PhysicalCTEAnchor<?, ?>) o;
        return cteId.equals(that.cteId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cteId);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalCTEAnchor", "cteId", cteId);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalCTEAnchor(this, context);
    }

    @Override
    public PhysicalCTEAnchor<Plan, Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new PhysicalCTEAnchor<>(cteId, groupExpression, getLogicalProperties(), physicalProperties,
                statistics, children.get(0), children.get(1));
    }

    @Override
    public PhysicalCTEAnchor<Plan, Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalCTEAnchor<>(cteId, groupExpression, getLogicalProperties(), child(0), child(1));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new PhysicalCTEAnchor<>(cteId, groupExpression, logicalProperties.get(), children.get(0),
                children.get(1));
    }

    @Override
    public PhysicalCTEAnchor<Plan, Plan> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            Statistics statistics) {
        return new PhysicalCTEAnchor<>(cteId, groupExpression, getLogicalProperties(), physicalProperties,
                statistics, child(0), child(1));
    }

    @Override
    public String shapeInfo() {
        return Utils.toSqlString("PhysicalCteAnchor",
                "cteId", cteId);
    }

    @Override
    public List<Slot> computeOutput() {
        return right().getOutput();
    }

    @Override
    public PhysicalCTEAnchor<Plan, Plan> resetLogicalProperties() {
        return new PhysicalCTEAnchor<>(cteId, groupExpression, null, physicalProperties,
                statistics, child(0), child(1));
    }
}
