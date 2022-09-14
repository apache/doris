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
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Repeat;
import org.apache.doris.nereids.trees.plans.logical.ExtraVirtualSlotShape;
import org.apache.doris.nereids.trees.plans.logical.GroupingSetShape;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * PhysicalRepeat.
 */
public abstract class PhysicalRepeat<CHILD_TYPE extends Plan>
        extends PhysicalUnary<CHILD_TYPE> implements Repeat {
    protected final List<Expression> groupByExpressions;
    protected final List<NamedExpression> outputExpressions;
    protected final List<GroupingSetShape> groupingSetShapes;

    /**
     * initial construction method.
     */
    public PhysicalRepeat(
            PlanType type,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            List<GroupingSetShape> groupingSetShapes,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            CHILD_TYPE child) {
        super(type, groupExpression, logicalProperties, child);
        this.groupByExpressions = ImmutableList.copyOf(groupByExpressions);
        this.outputExpressions = ImmutableList.copyOf(outputExpressions);
        this.groupingSetShapes = ImmutableList.copyOf(groupingSetShapes);
    }

    /**
     * Constructor with all parameters.
     */
    public PhysicalRepeat(
            PlanType type,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            List<GroupingSetShape> groupingSetShapes,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, StatsDeriveResult statsDeriveResult, CHILD_TYPE child) {
        super(type, groupExpression, logicalProperties, physicalProperties, statsDeriveResult, child);
        this.groupByExpressions = ImmutableList.copyOf(groupByExpressions);
        this.outputExpressions = ImmutableList.copyOf(outputExpressions);
        this.groupingSetShapes = ImmutableList.copyOf(groupingSetShapes);
    }

    public List<NamedExpression> getOutputExpressions() {
        return outputExpressions;
    }

    @Override
    public List<Expression> getGroupByExpressions() {
        return groupByExpressions;
    }

    public List<Expression> getNonVirtualGroupByExpressions() {
        return groupByExpressions.stream().filter(e -> !(e instanceof VirtualSlotReference))
                .collect(ImmutableList.toImmutableList());
    }

    public List<Expression> getVirtualGroupByExpressions() {
        return groupByExpressions.stream().filter(VirtualSlotReference.class::isInstance)
                .collect(ImmutableList.toImmutableList());
    }

    public List<GroupingSetShape> getGroupingSetShapes() {
        return groupingSetShapes;
    }

    public List<List<Long>> getVirtualSlotValues() {
        Builder<List<Long>> wholeVirtualSlotValues = ImmutableList.builder();
        groupingSetShapes.stream().forEach(e -> wholeVirtualSlotValues.add(e.toLongValue()));
        return wholeVirtualSlotValues.build();
    }

    public ExtraVirtualSlotShape getGroupSetShape() {
        return groupingSetShapes.stream()
                .filter(ExtraVirtualSlotShape.class::isInstance)
                .map(ExtraVirtualSlotShape.class::cast)
                .collect(Collectors.toList()).get(0);
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalRepeat",
                "groupByExpressions", groupByExpressions,
                "outputExpr", outputExpressions,
                "groupingSetIdSlot", groupingSetShapes
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalRepeat(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalRepeat that = (PhysicalRepeat) o;
        return Objects.equals(groupByExpressions, that.groupByExpressions)
                && Objects.equals(outputExpressions, that.outputExpressions)
                && Objects.equals(groupingSetShapes, that.groupingSetShapes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupByExpressions, outputExpressions, groupingSetShapes);
    }
}
