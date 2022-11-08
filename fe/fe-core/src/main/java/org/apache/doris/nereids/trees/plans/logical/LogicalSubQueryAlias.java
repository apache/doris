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
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The node of logical plan for sub query and alias
 *
 * @param <CHILD_TYPE> param
 */
public class LogicalSubQueryAlias<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> {
    private final String alias;

    private final Optional<List<String>> columnAliases;

    public LogicalSubQueryAlias(String tableAlias, CHILD_TYPE child) {
        this(tableAlias, Optional.empty(), Optional.empty(), Optional.empty(), child);
    }

    public LogicalSubQueryAlias(String tableAlias, Optional<List<String>> columnAliases, CHILD_TYPE child) {
        this(tableAlias, columnAliases, Optional.empty(), Optional.empty(), child);
    }

    public LogicalSubQueryAlias(String tableAlias, Optional<List<String>> columnAliases,
                                Optional<GroupExpression> groupExpression,
                                Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_SUBQUERY_ALIAS, groupExpression, logicalProperties, child);
        this.alias = tableAlias;
        this.columnAliases = columnAliases;
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput().stream()
                .map(slot -> slot.withQualifier(ImmutableList.of(alias)))
                .collect(Collectors.toList());
    }

    public String getAlias() {
        return alias;
    }

    public Optional<List<String>> getColumnAliases() {
        return columnAliases;
    }

    @Override
    public String toString() {
        if (columnAliases.isPresent()) {
            return Utils.toSqlString("LogicalSubQueryAlias",
                "alias", alias,
                "columnAliases", StringUtils.join(columnAliases.get(), ",")
            );
        }
        return Utils.toSqlString("LogicalSubQueryAlias",
                "alias", alias
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalSubQueryAlias that = (LogicalSubQueryAlias) o;
        return alias.equals(that.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alias);
    }

    @Override
    public Plan withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalSubQueryAlias<>(alias, children.get(0));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitSubQueryAlias((LogicalSubQueryAlias<Plan>) this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalSubQueryAlias<>(alias, columnAliases, groupExpression,
                Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalSubQueryAlias<>(alias, columnAliases, Optional.empty(),
                logicalProperties, child());
    }
}
