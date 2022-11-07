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
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.statistics.ColumnStat;

import com.google.common.base.Preconditions;

import java.util.Map;

/**
 * Calculate selectivity of the filter.
 */
public class FilterSelectivityCalculator extends ExpressionVisitor<Double, Void> {

    private static final double DEFAULT_EQUAL_SELECTIVITY = 0.3;
    private static final double DEFAULT_RANGE_SELECTIVITY = 0.8;
    private final Map<Slot, ColumnStat> slotRefToStats;

    public FilterSelectivityCalculator(Map<Slot, ColumnStat> slotRefToStats) {
        Preconditions.checkNotNull(slotRefToStats);
        this.slotRefToStats = slotRefToStats;
    }

    /**
     * Do estimate.
     */
    public double estimate(Expression expression) {
        //TODO: refine the selectivity by ndv.
        // T1.A = T2.B => selectivity = 1
        // T1.A + T1.B > 1 => selectivity = 1
        if (expression instanceof ComparisonPredicate
                && !(expression.getInputSlots().size() == 1)) {
            return 1.0;
        }
        //only calculate comparison in form of `slot comp literal`,
        //otherwise, use DEFAULT_RANGE_SELECTIVITY
        if (expression instanceof ComparisonPredicate
                && !(!expression.child(0).getInputSlots().isEmpty()
                && expression.child(0).getInputSlots().toArray()[0] instanceof SlotReference
                && expression.child(1) instanceof Literal)) {
            return DEFAULT_RANGE_SELECTIVITY;
        }
        return expression.accept(this, null);
    }

    @Override
    public Double visit(Expression expr, Void context) {
        return DEFAULT_RANGE_SELECTIVITY;
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
        //TODO: remove the assumption that fun(A) and A have the same stats
        SlotReference left = (SlotReference) equalTo.left().getInputSlots().toArray()[0];
        Literal literal = (Literal) equalTo.right();
        ColumnStat columnStats = slotRefToStats.get(left);
        if (columnStats == null) {
            throw new RuntimeException("FilterSelectivityCalculator - col stats not found: " + left);
        }
        ColumnStat newStats = new ColumnStat(1, columnStats.getAvgSizeByte(),
                columnStats.getMaxSizeByte(), 0,
                literal.getDouble(), literal.getDouble());
        newStats.setSelectivity(1.0 / columnStats.getNdv());
        slotRefToStats.put(left, newStats);
        double ndv = columnStats.getNdv();
        return ndv < 0 ? DEFAULT_EQUAL_SELECTIVITY : ndv == 0 ? 0 : 1.0 / columnStats.getNdv();
    }
    // TODO: Should consider the distribution of data.
}
