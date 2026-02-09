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
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * LogicalRecursiveUnionProducer is sentinel plan for must_shuffle
 */
public class LogicalRecursiveUnionProducer<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> {
    private final String cteName;

    public LogicalRecursiveUnionProducer(String cteName, CHILD_TYPE child) {
        this(cteName, Optional.empty(), Optional.empty(), child);
    }

    public LogicalRecursiveUnionProducer(String cteName, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        this(cteName, groupExpression, logicalProperties, ImmutableList.of(child));
    }

    public LogicalRecursiveUnionProducer(String cteName, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> child) {
        super(PlanType.LOGICAL_RECURSIVE_CTE_RECURSIVE_CHILD, groupExpression, logicalProperties, child);
        this.cteName = cteName;
    }

    public String getCteName() {
        return cteName;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        return new LogicalRecursiveUnionProducer<>(cteName, Optional.empty(), Optional.empty(), children);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalRecursiveUnionProducer(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalRecursiveUnionProducer<>(cteName, groupExpression,
                Optional.of(getLogicalProperties()), children);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalRecursiveUnionProducer<>(cteName, groupExpression, logicalProperties, children);
    }

    @Override
    public String toString() {
        return Utils.toSqlStringSkipNull("LogicalRecursiveUnionProducer",
                "cteName", cteName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalRecursiveUnionProducer that = (LogicalRecursiveUnionProducer) o;
        return cteName.equals(that.cteName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cteName);
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {

    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {

    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {

    }

    @Override
    public void computeFd(DataTrait.Builder builder) {

    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }
}
