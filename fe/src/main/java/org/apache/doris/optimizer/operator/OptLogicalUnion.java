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
import org.apache.doris.optimizer.base.OptColumnRefSet;
import org.apache.doris.optimizer.base.OptLogicalProperty;
import org.apache.doris.optimizer.base.RequiredLogicalProperty;
import org.apache.doris.optimizer.stat.Statistics;

import java.util.BitSet;

public class OptLogicalUnion extends OptLogical {
    private final boolean isUnionAll;
    private final OptColumnRefSet groupBy;

    public OptLogicalUnion(OptColumnRefSet groupBy, boolean isUnionAll) {
        super(OptOperatorType.OP_LOGICAL_UNION);
        this.isUnionAll = isUnionAll;
        this.groupBy = groupBy;
    }

    @Override
    public BitSet getCandidateRulesForImplement() {
        return null;
    }

    @Override
    public Statistics deriveStat(OptExpressionHandle expressionHandle, RequiredLogicalProperty property) {
        Preconditions.checkArgument(expressionHandle.getChildrenStatistics().size() == 2);
        final Statistics leftChild = expressionHandle.getChildrenStatistics().get(0);
        final Statistics rightChild = expressionHandle.getChildrenStatistics().get(1);
        if (leftChild.getStatColumns().cardinality() != rightChild.getStatColumns().cardinality()
                || !leftChild.getStatColumns().equals(rightChild.getStatColumns())) {
            Preconditions.checkState(false,
                    "Operator Union has wrong number children or it's " +
                            "children have different columns.");
        }
        final Statistics leftGroupByStats = estimateAgg(groupBy, property, leftChild);
        final Statistics rightGroupByStats = estimateAgg(groupBy, property, rightChild);
        return estimateUnion(groupBy, leftGroupByStats, rightGroupByStats);
    }

    protected Statistics estimateUnion(OptColumnRefSet groupBy, Statistics leftChild, Statistics rightChild) {
        final Statistics statistics = new Statistics();
        for (int id : groupBy.getColumnIds()) {
            statistics.addRow(id, estimateCardinalityWithCardinalities(
                    leftChild.getCardinality(id),
                    rightChild.getCardinality(id))
            );
        }
        statistics.setRowCount(estimateCardinalityWithRows(
                leftChild.getRowCount(), rightChild.getRowCount()));
        return statistics;
    }

    @Override
    public OptColumnRefSet requiredStatForChild(
            OptExpressionHandle expressionHandle,
            RequiredLogicalProperty property, int childIndex) {
        final OptLogicalProperty logicalProperty = expressionHandle.getChildLogicalProperty(childIndex);
        final OptColumnRefSet columns = new OptColumnRefSet();
        columns.include(groupBy);
        columns.intersects(logicalProperty.getOutputColumns());
        return columns;
    }

    @Override
    public OptColumnRefSet getOutputColumns(OptExpressionHandle exprHandle) {
        OptColumnRefSet columns = new OptColumnRefSet();
        for (int i = 0; i < exprHandle.arity(); ++i) {
            columns.include(exprHandle.getChildLogicalProperty(i).getOutputColumns());
        }
        return columns;
    }

    public boolean isUnionAll() {
        return this.isUnionAll;
    }

    public OptColumnRefSet getGroupBy() {
        return groupBy;
    }
}
