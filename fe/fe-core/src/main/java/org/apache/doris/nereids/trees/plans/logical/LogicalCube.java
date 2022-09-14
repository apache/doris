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
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Logical Cube.
 */
public class LogicalCube<CHILD_TYPE extends Plan> extends LogicalGroupBy<CHILD_TYPE> {

    public LogicalCube(List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions,
                Optional.empty(), Optional.empty(), child);
    }

    public LogicalCube(List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            List<BitSet> groupingIdList,
            Set<VirtualSlotReference> virtualSlotRefs,
            List<Expression> virtualGroupingExprs,
            List<List<Long>> groupingList,
            boolean isResolve,
            boolean changedOutput,
            boolean isNormalized,
            CHILD_TYPE child) {
        this(groupByExpressions, outputExpressions, groupingIdList,
                virtualSlotRefs, virtualGroupingExprs, groupingList, isResolve, changedOutput,
                isNormalized,
                Optional.empty(), Optional.empty(), child);
    }

    public LogicalCube(List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_GROUPING_SETS, groupByExpressions, outputExpressions,
                groupExpression, logicalProperties, child);
    }

    public LogicalCube(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            List<BitSet> groupingIdList,
            Set<VirtualSlotReference> virtualSlotRefs,
            List<Expression> virtualGroupingExprs,
            List<List<Long>> groupingList,
            boolean isResolve,
            boolean changedOutput,
            boolean isNormalized,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_GROUPING_SETS, groupByExpressions, outputExpressions, groupingIdList,
                virtualSlotRefs, virtualGroupingExprs, groupingList, isResolve, changedOutput,
                isNormalized,
                groupExpression, logicalProperties, child);
    }

    @Override
    public List<List<Expression>> getGroupingSets() {
        return new ArrayList<>();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalCube",
                "outputExpressions", outputExpressions,
                "original groupByExpressions", groupByExpressions,
                "groupByExpressions", getGroupByExpressions()
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalCube(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalCube that = (LogicalCube) o;
        return super.equals(that);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputExpressions);
    }

    @Override
    public List<Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>()
                .addAll(getGroupByExpressions()).addAll(outputExpressions).build();
    }

    @Override
    public List<Slot> computeOutput() {
        return new ImmutableList.Builder<Slot>()
                .addAll(child().getOutput())
                .addAll(virtualGroupingExprs.stream()
                        .filter(VirtualSlotReference.class::isInstance)
                        .map(VirtualSlotReference.class::cast).collect(Collectors.toList()))
                .build();
    }

    @Override
    public LogicalCube<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalCube<>(groupByExpressions, outputExpressions,
                groupingIdList, virtualSlotRefs, virtualGroupingExprs, groupingList,
                isResolved, changedOutput, isNormalized,
                children.get(0));
    }

    @Override
    public LogicalCube<Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalCube<>(groupByExpressions, outputExpressions,
                groupingIdList, virtualSlotRefs, virtualGroupingExprs, groupingList,
                isResolved, changedOutput, isNormalized,
                groupExpression, Optional.of(getLogicalProperties()), children.get(0));
    }

    @Override
    public LogicalCube<Plan> withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalCube<>(groupByExpressions, outputExpressions,
                groupingIdList, virtualSlotRefs, virtualGroupingExprs, groupingList,
                isResolved, changedOutput, isNormalized,
                Optional.empty(), logicalProperties, children.get(0));
    }

    @Override
    public LogicalCube<Plan> replace(List<List<Expression>> groupByExprList,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressionList,
            List<BitSet> groupingIdList,
            Set<VirtualSlotReference> virtualSlotRefs,
            List<Expression> virtualGroupingExprs,
            List<List<Long>> groupingList,
            boolean isResolved,
            boolean changedOutput,
            boolean isNormalized) {
        return new LogicalCube<>(groupByExpressions, outputExpressionList,
                groupingIdList, virtualSlotRefs, virtualGroupingExprs, groupingList,
                isResolved, changedOutput, isNormalized, child());
    }

    @Override
    public LogicalCube<Plan> replaceWithChild(List<List<Expression>> groupByExprList,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressionList,
            List<BitSet> groupingIdList,
            Set<VirtualSlotReference> virtualSlotRefs,
            List<Expression> virtualGroupingExprs,
            List<List<Long>> groupingList,
            boolean isResolved,
            boolean changedOutput,
            boolean isNormalized,
            Plan child) {
        return new LogicalCube<>(groupByExpressions, outputExpressionList,
                groupingIdList, virtualSlotRefs, virtualGroupingExprs, groupingList,
                isResolved, changedOutput, isNormalized, child);
    }

    @Override
    public List<BitSet> genGroupingIdList(List<Expression> groupingExprs, List<List<Expression>> groupByExpressions) {
        List<BitSet> groupingIdList = new ArrayList<>();
        // it will generate the full permutation bitmap of cube item
        // e.g. cube (k1,k2,k3) the result is ["{}", "{1}", "{0}", "{0, 1}", "{2}", "{1, 2}", "{0, 1, 2}",
        // "{0, 2}"]
        for (int i = 0; i < (1 << groupingExprs.size()); i++) {
            BitSet bitSet = new BitSet();
            for (int j = 0; j < groupingExprs.size(); j++) {
                if ((i & (1 << j)) > 0) {
                    bitSet.set(j, true);
                }
            }
            groupingIdList.add(bitSet);
        }
        return groupingIdList;
    }
}
