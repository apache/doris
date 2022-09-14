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
import org.apache.doris.nereids.trees.plans.algebra.GroupBy;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.StatsDeriveResult;

import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * PhysicalGroupBy.
 */
public abstract class PhysicalGroupBy<CHILD_TYPE extends Plan> extends PhysicalUnary<CHILD_TYPE> implements GroupBy {
    protected final List<Expression> groupByExpressions;
    protected final List<Expression> originGroupByExprs;
    protected final List<NamedExpression> outputExpressions;
    protected final Set<VirtualSlotReference> virtualSlotRefs;
    protected final List<BitSet> groupingIdList;
    protected final List<Expression> virtualGroupingExprs;
    protected final List<List<Long>> groupingList;

    /**
     * initial construction method.
     */
    public PhysicalGroupBy(
            PlanType type,
            List<Expression> groupingByExpressions,
            List<Expression> originGroupByExprs,
            List<NamedExpression> outputExpressions,
            List<BitSet> groupingIdList,
            Set<VirtualSlotReference> virtualSlotRefs,
            List<Expression> virtualGroupingExprs,
            List<List<Long>> groupingList,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            CHILD_TYPE child) {
        super(type, groupExpression, logicalProperties, child);
        this.groupByExpressions = Objects.requireNonNull(
                groupingByExpressions, "groupingByExpressions can not null");
        this.originGroupByExprs = Objects.requireNonNull(
                originGroupByExprs, "originGroupByExprs can not null");
        this.outputExpressions = Objects.requireNonNull(outputExpressions, "outputExpressions can not null");
        this.groupingIdList = Objects.requireNonNull(groupingIdList, "groupingIdList can not null");
        this.virtualSlotRefs = Objects.requireNonNull(virtualSlotRefs, "virtualSlotRefs can not null");
        this.virtualGroupingExprs = Objects.requireNonNull(virtualGroupingExprs, "virtualGroupingExprs can not null");
        this.groupingList = Objects.requireNonNull(groupingList, "groupingList can not be null");
    }

    /**
     * Constructor with all parameters.
     */
    public PhysicalGroupBy(
            PlanType type,
            List<Expression> groupingByExpressions,
            List<Expression> originGroupByExprs,
            List<NamedExpression> outputExpressions,
            List<BitSet> groupingIdList,
            Set<VirtualSlotReference> virtualSlotRefs,
            List<Expression> virtualGroupingExprs,
            List<List<Long>> groupingList,
            Optional<GroupExpression> groupExpression, LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties, StatsDeriveResult statsDeriveResult, CHILD_TYPE child) {
        super(type, groupExpression, logicalProperties, physicalProperties, statsDeriveResult, child);
        this.groupByExpressions = Objects.requireNonNull(
                groupingByExpressions, "groupingByExpressions can not null");
        this.originGroupByExprs = Objects.requireNonNull(
                originGroupByExprs, "originGroupByExprs can not null");
        this.outputExpressions = Objects.requireNonNull(outputExpressions, "outputExpressions can not null");
        this.groupingIdList = Objects.requireNonNull(groupingIdList, "groupingIdList can not null");
        this.virtualSlotRefs = Objects.requireNonNull(virtualSlotRefs, "virtualSlotRefs can not null");
        this.virtualGroupingExprs = Objects.requireNonNull(virtualGroupingExprs, "virtualGroupingExprs can not null");
        this.groupingList = Objects.requireNonNull(groupingList, "groupingList can not be null");
    }

    public List<NamedExpression> getOutputExpressions() {
        return outputExpressions;
    }

    public Set<VirtualSlotReference> getVirtualSlotRefs() {
        return virtualSlotRefs;
    }

    public List<Expression> getVirtualGroupingExprs() {
        return virtualGroupingExprs;
    }

    public List<BitSet> getGroupingIdList() {
        return groupingIdList;
    }

    public List<List<Long>> getGroupingList() {
        return groupingList;
    }

    public List<Expression> getOriginGroupByExprs() {
        return originGroupByExprs;
    }

    @Override
    public List<Expression> getGroupByExpressions() {
        return groupByExpressions;
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalGroupBy",
                "groupByExpressions", groupByExpressions,
                "originGroupByExprs", originGroupByExprs,
                "outputExpr", outputExpressions,
                "groupingIdList", groupingIdList,
                "virtualSlotRefs", virtualSlotRefs,
                "wholeGroupingExprs", virtualGroupingExprs,
                "groupingList", groupingList
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalGroupBy(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PhysicalGroupBy that = (PhysicalGroupBy) o;
        return Objects.equals(groupByExpressions, that.groupByExpressions)
                && Objects.equals(originGroupByExprs, that.originGroupByExprs)
                && Objects.equals(outputExpressions, that.outputExpressions)
                && Objects.equals(virtualSlotRefs, that.virtualSlotRefs)
                && Objects.equals(groupingIdList, that.groupingIdList)
                && Objects.equals(virtualGroupingExprs, that.virtualGroupingExprs)
                && Objects.equals(groupingList, that.groupingList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupByExpressions, originGroupByExprs, outputExpressions, virtualSlotRefs,
                groupingIdList, virtualGroupingExprs, groupingList);
    }
}
