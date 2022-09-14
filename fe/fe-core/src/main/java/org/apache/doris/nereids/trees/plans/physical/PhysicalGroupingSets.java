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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.GroupingSetShape;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * Physical GroupingSets.
 */
public class PhysicalGroupingSets<CHILD_TYPE extends Plan> extends PhysicalRepeat<CHILD_TYPE> {

    public PhysicalGroupingSets(List<Expression> groupingByExpressions,
            List<NamedExpression> outputExpressions,
            List<GroupingSetShape> groupingSetIdSlots,
            LogicalProperties logicalProperties, CHILD_TYPE child) {
        this(groupingByExpressions, outputExpressions, groupingSetIdSlots,
                Optional.empty(), logicalProperties, child);
    }

    /**
     * initial construction method.
     */
    public PhysicalGroupingSets(List<Expression> groupingByExpressions,
            List<NamedExpression> outputExpressions,
            List<GroupingSetShape> groupingSetIdSlots,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            CHILD_TYPE child) {
        super(PlanType.PHYSICAL_GROUPING_SETS, groupingByExpressions,
                outputExpressions, groupingSetIdSlots,
                groupExpression, logicalProperties, child);
    }

    /**
     * Constructor with all parameters.
     */
    public PhysicalGroupingSets(List<Expression> groupingByExpressions,
            List<NamedExpression> outputExpressions,
            List<GroupingSetShape> groupingSetIdSlots,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, StatsDeriveResult statsDeriveResult, CHILD_TYPE child) {
        super(PlanType.PHYSICAL_GROUPING_SETS, groupingByExpressions,
                outputExpressions, groupingSetIdSlots,
                groupExpression, logicalProperties, physicalProperties, statsDeriveResult, child);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalGroupingSets",
                "outputExpr", outputExpressions,
                "groupByExpressions", groupByExpressions
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalGroupingSets(this, context);
    }

    @Override
    public List<Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>()
                .addAll(getGroupByExpressions()).addAll(outputExpressions).build();
    }

    @Override
    public PhysicalGroupingSets<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new PhysicalGroupingSets<>(groupByExpressions, outputExpressions, groupingSetShapes,
                getLogicalProperties(), children.get(0));
    }

    @Override
    public PhysicalGroupingSets<CHILD_TYPE> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new PhysicalGroupingSets<>(groupByExpressions, outputExpressions, groupingSetShapes,
                groupExpression, getLogicalProperties(), child());
    }

    @Override
    public PhysicalGroupingSets<CHILD_TYPE> withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new PhysicalGroupingSets<>(groupByExpressions, outputExpressions, groupingSetShapes,
                Optional.empty(), logicalProperties.get(), child());
    }

    @Override
    public PhysicalGroupingSets<CHILD_TYPE> withPhysicalPropertiesAndStats(PhysicalProperties physicalProperties,
            StatsDeriveResult statsDeriveResult) {
        return new PhysicalGroupingSets<>(groupByExpressions, outputExpressions, groupingSetShapes,
                Optional.empty(), getLogicalProperties(), physicalProperties, statsDeriveResult, child());
    }
}
