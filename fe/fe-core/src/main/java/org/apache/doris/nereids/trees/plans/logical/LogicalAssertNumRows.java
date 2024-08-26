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
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.DataTrait.Builder;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement;
import org.apache.doris.nereids.trees.expressions.AssertNumRowsElement.Assertion;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Assert num rows node is used to determine whether the number of rows is less than desired num of rows.
 * The rows are the result of subqueryString.
 * If the number of rows is more than the desired num of rows, the query will be cancelled.
 * The cancelled reason will be reported by Backend and displayed back to the user.
 */
public class LogicalAssertNumRows<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> {

    private final AssertNumRowsElement assertNumRowsElement;

    public LogicalAssertNumRows(AssertNumRowsElement assertNumRowsElement, CHILD_TYPE child) {
        this(assertNumRowsElement, Optional.empty(), Optional.empty(), child);
    }

    public LogicalAssertNumRows(AssertNumRowsElement assertNumRowsElement, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_ASSERT_NUM_ROWS, groupExpression, logicalProperties, child);
        this.assertNumRowsElement = Objects.requireNonNull(assertNumRowsElement,
                "assertNumRowsElement can not be null");
    }

    public AssertNumRowsElement getAssertNumRowsElement() {
        return assertNumRowsElement;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalAssertNumRows",
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
        LogicalAssertNumRows that = (LogicalAssertNumRows) o;
        return assertNumRowsElement.equals(that.assertNumRowsElement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(assertNumRowsElement);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalAssertNumRows(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public LogicalUnary<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalAssertNumRows<>(assertNumRowsElement, children.get(0));
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalAssertNumRows<>(assertNumRowsElement,
                groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalAssertNumRows<>(assertNumRowsElement, groupExpression, logicalProperties, children.get(0));
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput().stream().map(o -> o.withNullable(true)).collect(Collectors.toList());
    }

    @Override
    public void computeUnique(Builder builder) {
        if (assertNumRowsElement.getDesiredNumOfRows() == 1
                && (assertNumRowsElement.getAssertion() == Assertion.EQ
                || assertNumRowsElement.getAssertion() == Assertion.LT
                || assertNumRowsElement.getAssertion() == Assertion.LE)) {
            getOutput().forEach(builder::addUniqueSlot);
        }
    }

    @Override
    public void computeUniform(Builder builder) {
        if (assertNumRowsElement.getDesiredNumOfRows() == 1
                && (assertNumRowsElement.getAssertion() == Assertion.EQ
                || assertNumRowsElement.getAssertion() == Assertion.LT
                || assertNumRowsElement.getAssertion() == Assertion.LE)) {
            getOutput().forEach(builder::addUniformSlot);
        }
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        builder.addEqualSet(child().getLogicalProperties().getTrait());
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        builder.addFuncDepsDG(child().getLogicalProperties().getTrait());
    }
}
