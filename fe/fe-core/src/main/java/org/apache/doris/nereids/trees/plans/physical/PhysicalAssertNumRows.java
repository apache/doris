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
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Physical assertNumRows.
 */
public class PhysicalAssertNumRows<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> {
    private final AssertNumRowsElement assertNumRowsElement;

    public PhysicalAssertNumRows(AssertNumRowsElement assertNumRowsElement,
            LogicalProperties logicalProperties, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_ASSERT_NUM_ROWS, Optional.empty(), logicalProperties, child);
        this.assertNumRowsElement = assertNumRowsElement;
    }

    public PhysicalAssertNumRows(AssertNumRowsElement assertNumRowsElement, Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties, PhysicalProperties physicalProperties,
            StatsDeriveResult statsDeriveResult, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_ASSERT_NUM_ROWS, groupExpression, logicalProperties, physicalProperties,
                statsDeriveResult, child);
        this.assertNumRowsElement = assertNumRowsElement;
    }

    @Override
    public List<Slot> computeOutput() {
        return ImmutableList.<Slot>builder()
                .addAll(child().getOutput())
                .build();
    }

    public AssertNumRowsElement getAssertNumRowsElement() {
        return assertNumRowsElement;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalAssertNumRows",
                "assertNumRowsElement", assertNumRowsElement);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PhysicalAssertNumRows that = (PhysicalAssertNumRows) o;
        return assertNumRowsElement.equals(that.assertNumRowsElement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(assertNumRowsElement);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of(assertNumRowsElement);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalAssertNumRows(this, context);
    }

    @Override
    public PhysicalAssertNumRows<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalAssertNumRows<>(assertNumRowsElement, getLogicalProperties(), children.get(0));
    }

    @Override
    public PhysicalAssertNumRows<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalAssertNumRows<>(assertNumRowsElement, groupExpression,
                getLogicalProperties(), physicalProperties, statsDeriveResult, child());
    }

    @Override
    public PhysicalAssertNumRows<CHILD_TYPE> withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalAssertNumRows<>(assertNumRowsElement, Optional.empty(),
                logicalProperties.get(), physicalProperties, statsDeriveResult, child());
    }

    @Override
    public PhysicalAssertNumRows<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            StatsDeriveResult statsDeriveResult) {
        return new PhysicalAssertNumRows<>(assertNumRowsElement, Optional.empty(),
                getLogicalProperties(), physicalProperties, statsDeriveResult, child());
    }
}
