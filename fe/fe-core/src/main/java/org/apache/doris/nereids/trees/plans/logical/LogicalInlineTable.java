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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.BlockFuncDepsPropagation;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.InlineTable;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * represent value list such as values(1), (2), (3) will generate LogicalInlineTable((1), (2), (3)).
 */
public class LogicalInlineTable extends LogicalLeaf implements InlineTable, BlockFuncDepsPropagation {

    private final List<List<NamedExpression>> constantExprsList;

    public LogicalInlineTable(List<List<NamedExpression>> constantExprsList) {
        this(constantExprsList, Optional.empty(), Optional.empty());
    }

    /** LogicalInlineTable */
    public LogicalInlineTable(List<List<NamedExpression>> constantExprsList,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties) {
        super(PlanType.LOGICAL_INLINE_TABLE, groupExpression, logicalProperties);

        if (constantExprsList.isEmpty()) {
            throw new AnalysisException("constantExprsList should now be empty");
        }
        this.constantExprsList = Utils.fastToImmutableList(
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
        ImmutableList.Builder<Expression> expressions = ImmutableList.builderWithExpectedSize(
                constantExprsList.size() * constantExprsList.get(0).size());

        for (List<NamedExpression> namedExpressions : constantExprsList) {
            expressions.addAll(namedExpressions);
        }

        return expressions.build();
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalInlineTable(
                constantExprsList, groupExpression, Optional.of(getLogicalProperties())
        );
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        if (!children.isEmpty()) {
            throw new AnalysisException("children should not be empty");
        }
        return new LogicalInlineTable(constantExprsList, groupExpression, logicalProperties);
    }

    @Override
    public List<Slot> computeOutput() {
        int columnNum = constantExprsList.get(0).size();
        List<NamedExpression> firstRow = constantExprsList.get(0);
        ImmutableList.Builder<Slot> output = ImmutableList.builderWithExpectedSize(constantExprsList.size());
        for (int i = 0; i < columnNum; i++) {
            NamedExpression firstRowColumn = firstRow.get(i);
            boolean nullable = false;
            for (List<NamedExpression> row : constantExprsList) {
                if (row.get(i).nullable()) {
                    nullable = true;
                    break;
                }
            }
            output.add(new SlotReference(firstRowColumn.getName(), firstRowColumn.getDataType(), nullable,
                    false));
        }
        return output.build();
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

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalInlineTable[" + id.asInt() + "]",
                "rowNum", constantExprsList.size(),
                "constantExprsList", constantExprsList);
    }
}
