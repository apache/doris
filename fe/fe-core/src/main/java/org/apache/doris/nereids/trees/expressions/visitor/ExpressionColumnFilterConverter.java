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

package org.apache.doris.nereids.trees.expressions.visitor;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.planner.PartitionColumnFilter;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * expression column filter converter
 */
public class ExpressionColumnFilterConverter
        extends DefaultExpressionVisitor<Expression, Void> {
    private final Map<String, PartitionColumnFilter> columnFilterMap;

    private static class FilterParam {
        public static LiteralExpr lowerBound;
        public static boolean lowerBoundInclusive;
        public static LiteralExpr upperBound;
        public static boolean upperBoundInclusive;
        public static org.apache.doris.analysis.InPredicate inPredicate;

        public static void setValues(LiteralExpr lowerBound, boolean lowerInclusive,
                LiteralExpr upperBound, boolean upperInclusive) {
            FilterParam.lowerBound = lowerBound;
            FilterParam.lowerBoundInclusive = lowerInclusive;
            FilterParam.upperBound = upperBound;
            FilterParam.upperBoundInclusive = upperInclusive;
        }

        public static void setInPredicate(org.apache.doris.analysis.InPredicate inPredicate) {
            FilterParam.inPredicate = inPredicate;
        }
    }

    public ExpressionColumnFilterConverter(Map<String, PartitionColumnFilter> filterMap) {
        this.columnFilterMap = filterMap;
    }

    public void convert(Expression expr) {
        expr.accept(this, null);
    }

    @Override
    public Expression visitComparisonPredicate(ComparisonPredicate predicate, Void unused) {
        if (predicate instanceof NullSafeEqual) {
            return null;
        }
        LiteralExpr literal = ((Literal) predicate.right()).toLegacyLiteral();
        if (predicate instanceof EqualTo) {
            FilterParam.setValues(literal, true, literal, true);
        } else if (predicate instanceof GreaterThan) {
            FilterParam.setValues(literal, false, null, false);
        } else if (predicate instanceof GreaterThanEqual) {
            FilterParam.setValues(literal, true, null, false);
        } else if (predicate instanceof LessThan) {
            FilterParam.setValues(null, false, literal, false);
        } else if (predicate instanceof LessThanEqual) {
            FilterParam.setValues(null, false, literal, true);
        }
        setOrUpdateFilter(((Slot) predicate.left()).getName());
        return null;
    }

    @Override
    public Expression visitInPredicate(InPredicate predicate, Void unused) {
        List<Expr> literals = predicate.getOptions().stream()
                .map(expr -> ((Expr) ((Literal) expr).toLegacyLiteral()))
                .collect(Collectors.toList());
        FilterParam.setInPredicate(new org.apache.doris.analysis.InPredicate(new SlotRef(null, ""), literals, false));
        setOrUpdateFilter(((Slot) predicate.getCompareExpr()).getName());
        return null;
    }

    @Override
    public Expression visitIsNull(IsNull predicate, Void unused) {
        FilterParam.setValues(new NullLiteral(), true, new NullLiteral(), true);
        setOrUpdateFilter(((Slot) predicate.child()).getName());
        return null;
    }

    private void setOrUpdateFilter(String columnName) {
        PartitionColumnFilter filter = columnFilterMap.computeIfAbsent(columnName,
                k -> new PartitionColumnFilter());
        if (FilterParam.lowerBound != null) {
            filter.setLowerBound(FilterParam.lowerBound, FilterParam.lowerBoundInclusive);
        }
        if (FilterParam.upperBound != null) {
            filter.setUpperBound(FilterParam.upperBound, FilterParam.upperBoundInclusive);
        }
        if (FilterParam.inPredicate != null) {
            if (filter.getInPredicate() == null) {
                filter.setInPredicate(FilterParam.inPredicate);
            } else {
                filter.getInPredicate().getChildren().addAll(FilterParam.inPredicate.getListChildren());
            }
        }
    }
}
