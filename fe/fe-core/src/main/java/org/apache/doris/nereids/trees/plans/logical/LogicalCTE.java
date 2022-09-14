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
import org.apache.doris.nereids.trees.expressions.WithClause;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical Node for CTE
 */
public class LogicalCTE<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> {

    private final List<WithClause> withQueries;

    public LogicalCTE(List<WithClause> withQueries, CHILD_TYPE child) {
        this(withQueries, Optional.empty(), Optional.empty(), child);
    }

    public LogicalCTE(List<WithClause> withQueries, Optional<GroupExpression> groupExpression,
                                Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_CTE, groupExpression, logicalProperties, child);
        this.withQueries = withQueries;
    }

    public List<WithClause> getWithQueries() {
        return withQueries;
    }

    @Override
    public List<Slot> computeOutput() {
        return null;
    }

    @Override
    public String toString() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalCTE that = (LogicalCTE) o;
        return withQueries.equals(that.withQueries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(withQueries);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(withQueries.size() > 0);
        return new LogicalCTE<>(withQueries, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalCTE(this, context);
    }

    @Override
    public List<Expression> getExpressions() {
        return Collections.emptyList();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalCTE<>(withQueries, groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalCTE<>(withQueries, Optional.empty(), logicalProperties, child());
    }
}
