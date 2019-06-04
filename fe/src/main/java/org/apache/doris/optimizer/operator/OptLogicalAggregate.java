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

package org.apache.doris.optimizer.operator;

import com.google.common.base.Preconditions;
import org.apache.doris.optimizer.OptUtils;
import org.apache.doris.optimizer.base.*;
import org.apache.doris.optimizer.rule.OptRuleType;
import org.apache.doris.optimizer.stat.Statistics;
import org.apache.doris.optimizer.stat.StatisticsEstimator;

import java.util.BitSet;
import java.util.List;

// A GROUP BY and AGGREGATE operator, its structure looks like
// OptLogicalAggregate
// |--- OptLogical
// |--- OptItemProjectList
// |      |--- OptItemProjectElement
// |      |    |--- OptItem
// |      |--- OptItemProjectElement
// |      |    |--- OptItem
public class OptLogicalAggregate extends OptLogical {
    // If this operator will generate duplicate values for same group
    boolean generateDuplicate = false;
    private AggType aggType;
    private AggStage aggStage;
    private List<OptColumnRef> groupByColumns;
    // minimal grouping columns based on FD's
    private List<OptColumnRef> minimalGroupByColumns;
    // array of columns used in distinct qualified aggregates (DQA)
    // used only in the case of intermediate aggregates
    private List<OptColumnRef> dqaColumns;

    public OptLogicalAggregate() {
        super(OptOperatorType.OP_LOGICAL_AGGREGATE);
    }

    public OptLogicalAggregate(List<OptColumnRef> groupByColumns) {
        this(groupByColumns, AggType.GB_GLOBAL);
    }

    public OptLogicalAggregate(List<OptColumnRef> groupByColumns, AggType aggType) {
        super(OptOperatorType.OP_LOGICAL_AGGREGATE);
        this.aggType = aggType;
        this.aggStage = AggStage.OTHERS;
        this.groupByColumns = groupByColumns;
        if (aggType == AggType.GB_LOCAL) {
            this.generateDuplicate = true;
        }
    }

    public Statistics deriveStat(OptExpressionHandle expressionHandle, RequiredLogicalProperty property) {
        Preconditions.checkArgument(expressionHandle.getChildrenStatistics().size() == 1,
                "Aggregate has wrong number of children.");
        final OptColumnRefSet groupBy = new OptColumnRefSet(groupByColumns);
        return StatisticsEstimator.estimateAgg(groupBy, property, expressionHandle);
    }

    @Override
    public OptColumnRefSet requiredStatForChild(OptExpressionHandle expressionHandle,
                                                RequiredLogicalProperty property, int childIndex) {
        Preconditions.checkArgument(childIndex == 0, "The child can only be logical.");
        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(property.getColumns());
        columns.include(groupByColumns);

        if (expressionHandle.arity() == 2) {
            // Aggregation functions and other project list.
            final OptItemProperty itemProperty = expressionHandle.getChildItemProperty(1);
            columns.include(itemProperty.getUsedColumns());
        }

        final OptLogicalProperty logical = (OptLogicalProperty) expressionHandle.getChildProperty(childIndex);
        columns.intersects(logical.getOutputColumns());
        return columns;
    }

    //------------------------------------------------------------------------
    // Used to get operator's derived property
    //------------------------------------------------------------------------

    @Override
    public OptColumnRefSet getOutputColumns(OptExpressionHandle exprHandle) {
        Preconditions.checkArgument(exprHandle.arity() == 2
                || exprHandle.arity() == 1);
        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(groupByColumns);

        final OptLogicalProperty childProperty = exprHandle.getChildLogicalProperty(0);
        columns.include(childProperty.getOutputColumns());

        if (exprHandle.arity() == 2) {
            final OptItemProperty itemProperty = exprHandle.getChildItemProperty(1);
            columns.include(itemProperty.getGeneratedColumns());
        }
        return columns;
    }

    @Override
    public OptColumnRefSet getOuterColumns(OptExpressionHandle exprHandle) {
        OptColumnRefSet outerColumns = new OptColumnRefSet();
        outerColumns.include(groupByColumns);

        return getOuterColumns(exprHandle, outerColumns);
    }

    @Override
    public OptMaxcard getMaxcard(OptExpressionHandle exprHandle) {
        if (groupByColumns.isEmpty()) {
            return new OptMaxcard(1);
        }
        return new OptMaxcard();
    }

    @Override
    public BitSet getCandidateRulesForImplement() {
        final BitSet set = new BitSet();
        set.set(OptRuleType.RULE_IMP_AGG_TO_HASH_AGG.ordinal());
        return set;
    }

    //------------------------------------------------------------------------
    // Transformations
    //------------------------------------------------------------------------

    public List<OptColumnRef> getGroupByColumns() {
        return groupByColumns;
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode();
        for (OptColumnRef col : groupByColumns) {
            hash = OptUtils.combineHash(hash, col);
        }
        hash = OptUtils.combineHash(hash, aggType);
        return OptUtils.combineHash(hash, generateDuplicate);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof OptLogicalAggregate)) {
            return false;
        }
        OptLogicalAggregate rhs = (OptLogicalAggregate) obj;
        if (this == rhs) return true;
        return generateDuplicate == rhs.generateDuplicate &&
                aggType == rhs.aggType &&
                OptColumnRef.equals(groupByColumns, rhs.groupByColumns);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(type).append("(").append(aggType)
                .append("), GroupBy[").append(OptColumnRef.toString(groupByColumns)).append("]");
        return sb.toString();
    }

    public List<OptColumnRef> getGroupBy() { return groupByColumns; }
    public List<OptColumnRef> getDistinctColumns() { return dqaColumns; }
    public boolean isDuplicate() { return dqaColumns.isEmpty(); }
}
