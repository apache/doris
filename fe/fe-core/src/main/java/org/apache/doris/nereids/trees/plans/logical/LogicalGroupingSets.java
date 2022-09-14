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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * Grouping sets.
 */
public class LogicalGroupingSets<CHILD_TYPE extends Plan> extends LogicalRepeat<CHILD_TYPE> {

    public LogicalGroupingSets(List<List<Expression>> groupingSets,
            List<NamedExpression> outputExpressions,
            CHILD_TYPE child) {
        this(groupingSets, extractDistinctGroupByExpressions(groupingSets), outputExpressions,
                Optional.empty(), Optional.empty(), child);
    }

    public LogicalGroupingSets(List<List<Expression>> groupingSets,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            List<GroupingSetShape> groupingSetIdSlots,
            CHILD_TYPE child) {
        this(groupingSets, groupByExpressions, outputExpressions, groupingSetIdSlots,
                Optional.empty(), Optional.empty(), child);
    }

    public LogicalGroupingSets(List<List<Expression>> groupingSets,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_GROUPING_SETS, groupingSets, groupByExpressions, outputExpressions,
                groupExpression, logicalProperties, child);
    }

    public LogicalGroupingSets(
            List<List<Expression>> groupingSets,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            List<GroupingSetShape> groupingSetIdSlots,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_GROUPING_SETS, groupingSets, groupByExpressions,
                outputExpressions, groupingSetIdSlots,
                groupExpression, logicalProperties, child);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalGroupingSets",
                "outputExpressions", outputExpressions,
                "grouping sets", groupingSets,
                "groupByExpressions", groupByExpressions,
                "groupingSetIdSlots", groupingSetShapes
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalGroupingSets(this, context);
    }

    @Override
    public LogicalGroupingSets<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalGroupingSets<>(groupingSets, groupByExpressions,
                outputExpressions, groupingSetShapes,
                children.get(0));
    }

    @Override
    public LogicalGroupingSets<Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalGroupingSets<>(groupingSets, groupByExpressions,
                outputExpressions, groupingSetShapes,
                groupExpression, Optional.of(getLogicalProperties()), children.get(0));
    }

    @Override
    public LogicalGroupingSets<Plan> withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalGroupingSets<>(groupingSets, groupByExpressions,
                outputExpressions, groupingSetShapes,
                Optional.empty(), logicalProperties, children.get(0));
    }

    @Override
    public LogicalGroupingSets<Plan> replace(List<List<Expression>> groupByExprList,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressionList,
            List<GroupingSetShape> groupingSetIdSlots) {
        return new LogicalGroupingSets<>(groupByExprList, groupByExpressions,
                outputExpressionList, groupingSetIdSlots, child());
    }

    @Override
    public LogicalGroupingSets<Plan> replaceWithChild(List<List<Expression>> groupByExprList,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressionList,
            List<GroupingSetShape> groupingSetIdSlots,
            Plan child) {
        return new LogicalGroupingSets<>(groupByExprList, groupByExpressions,
                outputExpressionList, groupingSetIdSlots, child);
    }

    public static List<Expression> extractDistinctGroupByExpressions(List<List<Expression>> groupingSets) {
        return groupingSets.stream()
                .flatMap(List::stream)
                .distinct()
                .collect(ImmutableList.toImmutableList());
    }
}
