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
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.operator.*;

import java.util.List;

public class RowCountProvider {

    public static long getRowCount(OptExpression expr, StatisticsContext context) {
        final OptOperatorType type = expr.getOp().getType();
       if (type == OptOperatorType.OP_LOGICAL_JOIN) {
           return (long)getJoinRowCount(expr, context);
       } else if (type == OptOperatorType.OP_LOGICAL_SCAN) {
           return (long)getScanRowCount(expr);
       } else if (type == OptOperatorType.OP_LOGICAL_UNION) {

       } else if (type == OptOperatorType.OP_LOGICAL_AGGREGATE) {

       }
       return 0;
    }

    private static double getJoinRowCount(OptExpression expr, StatisticsContext context) {
        final Statistics firstChild = context.getChildrenStatistics().get(0);
        final Statistics secondChild = context.getChildrenStatistics().get(1);
        Preconditions.checkNotNull(firstChild);
        Preconditions.checkNotNull(secondChild);
        final double first = firstChild.getRowCount();
        final double second = firstChild.getRowCount();
        if (first <= 1.0 || second <= 1.0) {
            return Math.min(first, second);
        }
        double selectivity = 1.0;
        final OptExpression scalar = expr.getInput(2);
        if (scalar != null) {
            selectivity = guessSelectivity(scalar);
        }
        return first * second * selectivity;
    }

    private static double getScanRowCount(OptExpression expr) {
        final OptLogicalScan scan = (OptLogicalScan)expr.getOp();
        final double rowCount = scan.deriveStat(null, null).getRowCount();
        final OptExpression scalar = expr.getInput(0);
        double selectivity = 1.0;
        if (scalar != null) {
            selectivity = guessSelectivity(scalar);
        }
        return rowCount * selectivity;
    }

    private static double guessSelectivity(OptExpression expr) {
        double selectivity = 1.0;
        if (expr == null) {
            return selectivity;
        }

        final OptItem scalar = (OptItem)expr.getOp();
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
                final OptItemBinaryPredicate scalarBinary = (OptItemBinaryPredicate)expr.getOp();
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
            final OptItemCompoundPredicate scalar = (OptItemCompoundPredicate)expr.getOp();
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
