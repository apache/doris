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

package org.apache.doris.nereids.stats;

import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.statistics.ColumnStats;

import com.google.common.base.Preconditions;

import java.util.Map;

/**
 * Calculate selectivity of the filter.
 */
public class FilterSelectivityCalculator extends ExpressionVisitor<Double, Void> {

    private static double DEFAULT_SELECTIVITY = 0.1;

    private final Map<Slot, ColumnStats> slotRefToStats;

    public FilterSelectivityCalculator(Map<Slot, ColumnStats> slotRefToStats) {
        Preconditions.checkState(slotRefToStats != null);
        this.slotRefToStats = slotRefToStats;
    }

    /**
     * Do estimate.
     */
    public double estimate(Expression expression) {
        // For a comparison predicate, only when it's left side is a slot and right side is a literal, we would
        // consider is a valid predicate.
        if (expression instanceof ComparisonPredicate
                && !(expression.child(0) instanceof SlotReference
                && expression.child(1) instanceof Literal)) {
            return 1.0;
        }
        return expression.accept(this, null);
    }


    @Override
    public Double visit(Expression expr, Void context) {
        return DEFAULT_SELECTIVITY;
    }

    @Override
    public Double visitCompoundPredicate(CompoundPredicate compoundPredicate, Void context) {
        Expression leftExpr = compoundPredicate.child(0);
        Expression rightExpr = compoundPredicate.child(1);
        double leftSel = 1;
        double rightSel = 1;
        leftSel = estimate(leftExpr);
        rightSel = estimate(rightExpr);
        return compoundPredicate instanceof Or ? leftSel + rightSel - leftSel * rightSel : leftSel * rightSel;
    }

    @Override
    public Double visitComparisonPredicate(ComparisonPredicate cp, Void context) {
        return super.visitComparisonPredicate(cp, context);
    }

    // TODO: If right value greater than the max value or less than min value in column stats, return 0.0 .
    @Override
    public Double visitEqualTo(EqualTo equalTo, Void context) {
        SlotReference left = (SlotReference) equalTo.left();
        ColumnStats columnStats = slotRefToStats.get(left);
        if (columnStats == null) {
            return DEFAULT_SELECTIVITY;
        }
        long ndv = columnStats.getNdv();
        return ndv < 0 ? DEFAULT_SELECTIVITY : ndv == 0 ? 0 : 1.0 / columnStats.getNdv();
    }

    // TODO: Should consider the distribution of data.
}
