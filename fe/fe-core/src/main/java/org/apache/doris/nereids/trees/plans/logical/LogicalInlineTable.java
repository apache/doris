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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.BlockFuncDepsPropagation;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * represent value list such as values(1), (2), (3) will generate LogicalInlineTable((1), (2), (3)).
 */
public class LogicalInlineTable extends LogicalLeaf implements BlockFuncDepsPropagation {

    private final List<List<NamedExpression>> constantExprsList;

    public LogicalInlineTable(List<List<NamedExpression>> constantExprsList) {
        this(constantExprsList, Optional.empty(), Optional.empty());
    }

    public LogicalInlineTable(List<List<NamedExpression>> constantExprsList,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties) {
        super(PlanType.LOGICAL_INLINE_TABLE, groupExpression, logicalProperties);
        this.constantExprsList = ImmutableList.copyOf(
                Objects.requireNonNull(constantExprsList, "constantExprsList should not be null"));
    }

    public List<List<NamedExpression>> getConstantExprsList() {
        return constantExprsList;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalInlineTable(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return constantExprsList.stream().flatMap(List::stream).collect(ImmutableList.toImmutableList());
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return null;
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return null;
    }

    @Override
    public List<Slot> computeOutput() {
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
        LogicalInlineTable that = (LogicalInlineTable) o;
        return Objects.equals(constantExprsList, that.constantExprsList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(constantExprsList);
    }
}
