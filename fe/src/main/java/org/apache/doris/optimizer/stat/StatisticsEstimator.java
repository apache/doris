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

package org.apache.doris.optimizer.stat;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.doris.analysis.BaseTableRef;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.base.OptColumnRefSet;
import org.apache.doris.optimizer.base.RequiredLogicalProperty;
import org.apache.doris.optimizer.operator.*;

import java.util.List;

public class StatisticsEstimator {

    public static Statistics estimateAgg(
            OptColumnRefSet groupBy, RequiredLogicalProperty property, OptExpressionHandle exprHandle) {
        return estimateAgg(groupBy, property, exprHandle, 0);
    }

    private static Statistics estimateAgg(
            OptColumnRefSet groupBy, RequiredLogicalProperty property,
            OptExpressionHandle exprHandle, int childIndex) {
        final Statistics statistics = new Statistics();
        final Statistics childStatistcs = exprHandle.getChildrenStatistics().get(childIndex);

        long rowCount = 1;
        for (int id : groupBy.getColumnIds()) {
            rowCount *= childStatistcs.getCardinality(id);
        }
        statistics.setRowCount(rowCount);

        // TODO aggregate function's disticnt count now is replaced by it's param cardinality.
        for (int id : childStatistcs.getStatColumns().getColumnIds()) {
            statistics.addRow(id, childStatistcs.getCardinality(id));
        }
        return statistics;
    }

    public static Statistics estimateInnerJoin(OptExpressionHandle exprHandle) {
        return estimateJoin(exprHandle, false, false,
                false, false);
    }

    public static Statistics estimateJoin(OptExpressionHandle exprHandle,
            boolean isSemiJoin, boolean isAntiJoin, boolean isLeftOutJoin, boolean isFullOuterJoin) {
        final Statistics statistics = new Statistics();
        final Statistics outerChild = exprHandle.getChildrenStatistics().get(0);
        final Statistics innerChild = exprHandle.getChildrenStatistics().get(1);

        final OptExpression conjunct = exprHandle.getItemChild(2);
        double selectivity = guessSelectivity(conjunct);
        if (isAntiJoin) {
            selectivity = 1 - selectivity;
        }

        final double outerRowCount = outerChild.getRowCount();
        final double innerRowCount = innerChild.getRowCount();
        double rowCount = outerRowCount * innerRowCount * selectivity;
        if (isLeftOutJoin)  {
            rowCount += outerRowCount * (1 - selectivity);
        } else if (isFullOuterJoin) {
            rowCount += (innerRowCount + outerRowCount) * (1 - selectivity);
        }
        statistics.setRowCount((long)rowCount);

        for (int id : outerChild.getStatColumns().getColumnIds()) {
            statistics.addRow(id, getMax((long) ((double) outerChild.getCardinality(id) * selectivity)));
        }

        if (!isSemiJoin && !isAntiJoin) {
            for (int id : innerChild.getStatColumns().getColumnIds()) {
                statistics.addRow(id, getMax((long) ((double) outerChild.getCardinality(id) * selectivity)));
            }
        }
        return statistics;
    }

    public static Statistics estimateLeftSemiJoin(OptExpressionHandle exprHandle) {
        return estimateJoin(exprHandle, true, false,
                false, false);
    }

    public static Statistics estimateLeftAntiJoin(OptExpressionHandle exprHandle) {
        return estimateJoin(exprHandle, false, true,
                false, false);
    }

    public static Statistics estimateLeftOuterJoin(OptExpressionHandle exprHandle) {
        return estimateJoin(exprHandle, false, false,
                true, false);
    }

    public static Statistics estimateFullOuterJoin(OptExpressionHandle exprHandle) {
        return estimateJoin(exprHandle, false, false,
                false, true);
    }

    public static Statistics estimateLimit(
            RequiredLogicalProperty property, OptExpressionHandle exprHandle, long limit) {
        final Statistics childStatiscs = exprHandle.getChildrenStatistics().get(0);
        final Statistics limitStatistics = new Statistics();

        limitStatistics.setRowCount(limit);

        for (int id : childStatiscs.getStatColumns().getColumnIds()) {
            limitStatistics.addRow(id, Math.min(limit, childStatiscs.getCardinality(id)));
        }
        return childStatiscs;
    }

    public static Statistics estimateUnion(
            OptColumnRefSet groupBy, RequiredLogicalProperty property, OptExpressionHandle exprHandle) {
        final Statistics outerChild = estimateAgg(groupBy, property, exprHandle, 0);
        final Statistics innerChild = estimateAgg(groupBy, property, exprHandle, 1);
        final Statistics statistics = new Statistics();

        statistics.setRowCount(outerChild.getRowCount() + innerChild.getRowCount());

        for (int id : groupBy.getColumnIds()) {
            statistics.addRow(id,
                    outerChild.getCardinality(id) + innerChild.getCardinality(id));
        }
        return statistics;
    }

    public static Statistics estimateSelect(OptExpressionHandle exprHandle) {
        final Statistics childStatiscs = exprHandle.getChildrenStatistics().get(0);
        final Statistics statistics = new Statistics();
        double selectivity = 1.0;
        for (int i = 1; i < exprHandle.arity(); i++) {
            final OptExpression predicate = exprHandle.getItemChild(i);
            selectivity *= guessSelectivity(predicate);
        }

        statistics.setRowCount(getMax((long) (childStatiscs.getRowCount() * selectivity)));

        for (int id : childStatiscs.getStatColumns().getColumnIds()) {
            statistics.addRow(id, getMax((long) (childStatiscs.getCardinality(id) * selectivity)));
        }
        return statistics;
    }

    private static long getMax(long value) {
        return Math.max(value, 1);
    }

    public static Statistics estimateOlapScan(
            OlapTable olapTable, RequiredLogicalProperty property) {
        final Statistics statistics = new Statistics();
        for (int id : property.getColumns().getColumnIds()) {
            statistics.addRow(id, estimateCardinalityWithRows(olapTable.getRowCount()));
        }
        statistics.setRowCount(olapTable.getRowCount());
        return statistics;
    }

    public static Statistics estimateProject(
            RequiredLogicalProperty property, OptExpressionHandle exprHandle) {
        final Statistics childStatiscs = exprHandle.getChildrenStatistics().get(0);
        final Statistics statistics = new Statistics();
        statistics.setRowCount(childStatiscs.getRowCount());
        for (int id : childStatiscs.getStatColumns().getColumnIds()) {
            statistics.addRow(id, childStatiscs.getCardinality(id));
        }
        return statistics;
    }

    private static long estimateCardinalityWithRows(long c) {
        return c / 10;
    }

    private static double guessSelectivity(OptExpression expr) {
        double selectivity = 1.0;
        if (expr == null) {
            return selectivity;
        }

        final OptItem scalar = (OptItem) expr.getOp();
        if (scalar.isConstant()) {
            if (scalar.isAlwaysTrue()) {
                return selectivity;
            } else {
                return 0.0;
            }
        }

        final List<OptExpression> predicates = Lists.newArrayList();
        decomposeConjunction(expr, predicates);
        for (OptExpression optExpression : predicates) {
            if (optExpression.getOp().getType() == OptOperatorType.OP_ITEM_BINARY_PREDICATE) {
                final OptItemBinaryPredicate scalarBinary = (OptItemBinaryPredicate) expr.getOp();
                if (scalarBinary.getOp() == BinaryPredicate.Operator.EQ) {
                    selectivity *= 0.15;
                } else {
                    selectivity *= 0.5;
                }
            } else {
                selectivity *= 0.25;
            }
        }
        return 0;
    }

    private static void decomposeConjunction(OptExpression expr, List<OptExpression> result) {
        if (expr == null) {
            return;
        }

        if (expr.getOp().getType() == OptOperatorType.OP_ITEM_COMPOUND_PREDICATE) {
            final OptItemCompoundPredicate scalar = (OptItemCompoundPredicate) expr.getOp();
            if (scalar.getOp() == CompoundPredicate.Operator.AND) {
                for (OptExpression child : expr.getInputs()) {
                    decomposeConjunction(child, result);
                }
            }
        } else {
            result.add(expr);
        }
    }
}
