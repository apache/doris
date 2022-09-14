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
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.GroupBy;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Logical Grouping sets.
 */
public abstract class LogicalGroupBy<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> implements GroupBy {

    public static final String COL_GROUPING_ID = "GROUPING_ID";
    public static final String GROUPING_PREFIX = "GROUPING_PREFIX_";
    protected final List<Expression> groupByExpressions;
    protected final List<NamedExpression> outputExpressions;
    protected final Set<VirtualSlotReference> virtualSlotRefs;
    protected final List<BitSet> groupingIdList;
    protected final List<Expression> virtualGroupingExprs;
    protected final List<List<Long>> groupingList;
    protected final boolean isResolved;
    protected final boolean changedOutput;
    protected final boolean isNormalized;

    /**
     * initial construction method.
     */
    public LogicalGroupBy(
            PlanType type,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            CHILD_TYPE child) {
        super(type, groupExpression, logicalProperties, child);
        this.groupByExpressions = ImmutableList.copyOf(groupByExpressions);
        this.outputExpressions = ImmutableList.copyOf(outputExpressions);
        this.groupingIdList = Lists.newArrayList();
        this.virtualSlotRefs = new LinkedHashSet<>();
        VirtualSlotReference virtualSlotReference = new VirtualSlotReference(
                COL_GROUPING_ID, BigIntType.INSTANCE, new ArrayList<>(), false);
        this.virtualSlotRefs.add(virtualSlotReference);
        this.virtualGroupingExprs = Lists.newArrayList();
        this.groupingList = Lists.newArrayList();
        this.isResolved = false;
        this.changedOutput = false;
        this.isNormalized = false;
    }

    /**
     * Constructor with all parameters.
     */
    public LogicalGroupBy(
            PlanType type,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            List<BitSet> groupingIdList,
            Set<VirtualSlotReference> virtualSlotRefs,
            List<Expression> virtualGroupingExprs,
            List<List<Long>> groupingList,
            boolean isResolved,
            boolean changedOutput,
            boolean isNormalized,
            Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            CHILD_TYPE child) {
        super(type, groupExpression, logicalProperties, child);
        this.groupByExpressions = ImmutableList.copyOf(groupByExpressions);
        this.outputExpressions = ImmutableList.copyOf(outputExpressions);
        this.groupingIdList = Objects.requireNonNull(groupingIdList, "groupingIdList can not null");
        this.virtualSlotRefs = Objects.requireNonNull(virtualSlotRefs, "virtualSlotRefs can not null");
        this.virtualGroupingExprs = Objects.requireNonNull(virtualGroupingExprs, "virtualGroupingExprs can not null");
        this.groupingList = Objects.requireNonNull(groupingList, "groupingList can not be null");
        this.isResolved = isResolved;
        this.changedOutput = changedOutput;
        this.isNormalized = isNormalized;
    }

    public abstract List<List<Expression>> getGroupingSets();

    public List<NamedExpression> getOutputExpressions() {
        return outputExpressions;
    }

    public List<Expression> getOriginalGroupByExpressions() {
        return groupByExpressions;
    }

    public List<Expression> getGroupByExpressions() {
        return new ImmutableList.Builder<Expression>()
                .addAll(getOriginalGroupByExpressions())
                .addAll(virtualGroupingExprs)
                .build();
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

    public boolean isResolved() {
        return isResolved;
    }

    public boolean hasChangedOutput() {
        return changedOutput;
    }

    public boolean isNormalized() {
        return isNormalized;
    }

    public static List<Expression> genOriginalGroupByExpressions(List<List<Expression>> groupingSets) {
        List<Expression> groupingExpression = new ArrayList<>();
        groupingSets.forEach(groupingExpression::addAll);
        return groupingExpression.stream().distinct().collect(ImmutableList.toImmutableList());
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalGroupBy",
                "outputExpr", outputExpressions,
                "groupingIdList", groupingIdList,
                "virtualSlotRefs", virtualSlotRefs,
                "virtualGroupingExprs", virtualGroupingExprs,
                "groupingList", groupingList
        );
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalGroupBy(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalGroupBy that = (LogicalGroupBy) o;
        return Objects.equals(groupByExpressions, that.groupByExpressions)
                && Objects.equals(outputExpressions, that.outputExpressions)
                && Objects.equals(virtualSlotRefs, that.virtualSlotRefs)
                && Objects.equals(groupingIdList, that.groupingIdList)
                && Objects.equals(virtualGroupingExprs, that.virtualGroupingExprs)
                && Objects.equals(groupingList, that.groupingList)
                && isResolved == that.isResolved
                && changedOutput == that.changedOutput
                && isNormalized == that.isNormalized;
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupByExpressions, outputExpressions, virtualSlotRefs,
                groupingIdList, virtualGroupingExprs, groupingList, isResolved, changedOutput, isNormalized);
    }

    @Override
    public List<Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>()
                .addAll(groupByExpressions)
                .addAll(outputExpressions)
                .addAll(virtualGroupingExprs)
                .build();
    }

    public abstract LogicalGroupBy<Plan> replace(List<List<Expression>> groupByExprList,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressionList,
            List<BitSet> groupingIdList,
            Set<VirtualSlotReference> virtualSlotRefs,
            List<Expression> virtualGroupingExprs,
            List<List<Long>> groupingList,
            boolean isResolved,
            boolean changedOutput,
            boolean isNormalized);

    public abstract LogicalGroupBy<Plan> replaceWithChild(List<List<Expression>> groupByExprList,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressionList,
            List<BitSet> groupingIdList,
            Set<VirtualSlotReference> virtualSlotRefs,
            List<Expression> virtualGroupingExprs,
            List<List<Long>> groupingList,
            boolean isResolved,
            boolean changedOutput,
            boolean isNormalized,
            Plan child);

    public abstract List<BitSet> genGroupingIdList(List<Expression> groupingExprs, List<List<Expression>> groupingSets);

    /**
     * Generate groupingList based on groupby information.
     */
    public List<List<Long>> genGroupingList(List<Expression> groupingExprs,
            List<Expression> virtualGroupingExprs, List<BitSet> groupingIdList,
            Set<VirtualSlotReference> newVirtualSlotRefs) {
        List<Expression> wholeGroupingExprs = Lists.newArrayList();
        wholeGroupingExprs.addAll(groupingExprs);
        wholeGroupingExprs.addAll(virtualGroupingExprs);
        BitSet bitSetAll = new BitSet();
        bitSetAll.set(0, groupingExprs.size(), true);
        List<List<Long>> groupingList = new ArrayList<>();
        newVirtualSlotRefs.forEach(slot -> {
            List<Long> glist = new ArrayList<>();
            groupingIdList.forEach(bitSet -> {
                long l = 0L;
                // for all column, using for group by
                if (slot.getName().equalsIgnoreCase(COL_GROUPING_ID)) {
                    BitSet newBitset = new BitSet();
                    for (int i = 0; i < bitSetAll.length(); ++i) {
                        newBitset.set(i, bitSet.get(bitSetAll.length() - i - 1));
                    }
                    newBitset.flip(0, bitSetAll.length());
                    newBitset.and(bitSetAll);
                    for (int i = 0; i < newBitset.length(); ++i) {
                        l += newBitset.get(i) ? (1L << i) : 0L;
                    }
                } else {
                    // for grouping[_id] functions
                    int slotSize = slot.getRealSlots().size();
                    for (int i = 0; i < slotSize; ++i) {
                        int j = wholeGroupingExprs.indexOf(slot.getRealSlots().get(i));
                        if (j < 0 || j >= bitSet.size()) {
                            throw new AnalysisException("Column" + slot.getName()
                                    + " in GROUP_ID() dose not exist in GROUP BY clause.");
                        }
                        l += bitSet.get(j) ? 0L : (1L << (slotSize - i - 1));
                    }
                }
                glist.add(l);
            });
            groupingList.add(glist);
        });
        return groupingList;
    }
}
