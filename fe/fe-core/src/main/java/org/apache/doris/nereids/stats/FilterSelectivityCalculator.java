// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.stats;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.ExpressionType;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.statistics.ColumnStats;
import org.apache.doris.statistics.StatsDeriveResult;
import org.apache.doris.statistics.TableStats;

import static org.apache.doris.nereids.trees.expressions.ExpressionType.OR;

import java.util.Map;


/**
 *
 */
public class FilterSelectivityCalculator extends DefaultExpressionVisitor<Double, Void> {

    private final Map<Slot, ColumnStats> slotRefToStatsMap;

    public FilterSelectivityCalculator(Map<Slot, ColumnStats> slotRefToStatsMap) {
        this.slotRefToStatsMap = slotRefToStatsMap;
    }

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
    public Double visitCompoundPredicate(CompoundPredicate compoundPredicate, Void context) {
        Expression leftExpr = compoundPredicate.child(0);
        Expression rightExpr = compoundPredicate.child(1);
        double leftSel = 1;
        double rightSel = 1;
        ExpressionType expressionType = compoundPredicate.getType();
        leftSel =  estimate(leftExpr);
        rightSel = estimate(rightExpr);
        return OR.equals(expressionType)? leftSel + rightSel - leftSel * rightSel : leftSel * rightSel;
    }

    @Override
    public Double visitComparisonPredicate(ComparisonPredicate cp, Void context) {
        return super.visitComparisonPredicate(cp, context);
    }

    @Override
    public Double visitEqualTo(EqualTo equalTo, Void context) {
        SlotReference left = (SlotReference) equalTo.left();
        ColumnStats columnStats = slotRefToStatsMap.get(left);
        Literal right = (Literal) equalTo.right();
        DataType dataType = right.getDataType();
    }

    @Override
    public Double visitGreaterThan(GreaterThan greaterThan, Void context) {
        return super.visitGreaterThan(greaterThan, context);
    }

    @Override
    public Double visitGreaterThanEqual(GreaterThanEqual greaterThanEqual, Void context) {
        return super.visitGreaterThanEqual(greaterThanEqual, context);
    }

    @Override
    public Double visitLessThan(LessThan lessThan, Void context) {
        return super.visitLessThan(lessThan, context);
    }

}
