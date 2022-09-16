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
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical Having plan
 * @param <CHILD_TYPE> Types which inherit from {@link Plan}
 */
public class LogicalHaving<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> implements Filter {

    private final Expression predicates;

    public LogicalHaving(Expression predicates, CHILD_TYPE child) {
        this(predicates, Optional.empty(), Optional.empty(), child);
    }

    public LogicalHaving(Expression predicates, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_HAVING, groupExpression, logicalProperties, child);
        this.predicates = Objects.requireNonNull(predicates, "predicates can not be null");
    }

    @Override
    public Expression getPredicates() {
        return predicates;
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalHaving<>(predicates, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalHaving((LogicalHaving<Plan>) this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of(predicates);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalHaving<>(predicates, groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalHaving<>(predicates, Optional.empty(), logicalProperties, child());
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(predicates);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof LogicalHaving) || getClass() != object.getClass()) {
            return false;
        }
        LogicalHaving other = (LogicalHaving) object;
        return predicates.equals(other.predicates);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalHaving", "predicates", predicates);
    }
}
