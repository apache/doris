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

    private class FilterParam {
        public LiteralExpr lowerBound = null;
        public boolean lowerBoundInclusive = false;
        public LiteralExpr upperBound = null;
        public boolean upperBoundInclusive = false;
        public org.apache.doris.analysis.InPredicate inPredicate = null;

        public void setValues(LiteralExpr lowerBound, boolean lowerInclusive,
                LiteralExpr upperBound, boolean upperInclusive) {
            this.lowerBound = lowerBound;
            this.lowerBoundInclusive = lowerInclusive;
            this.upperBound = upperBound;
            this.upperBoundInclusive = upperInclusive;
        }

        public void setInPredicate(org.apache.doris.analysis.InPredicate inPredicate) {
            this.inPredicate = inPredicate;
        }
    }

    private final FilterParam param = new FilterParam();

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
            param.setValues(literal, true, literal, true);
        } else if (predicate instanceof GreaterThan) {
            param.setValues(literal, false, null, false);
        } else if (predicate instanceof GreaterThanEqual) {
            param.setValues(literal, true, null, false);
        } else if (predicate instanceof LessThan) {
            param.setValues(null, false, literal, false);
        } else if (predicate instanceof LessThanEqual) {
            param.setValues(null, false, literal, true);
        }
        setOrUpdateFilter(((Slot) predicate.left()).getName());
        return null;
    }

    @Override
    public Expression visitInPredicate(InPredicate predicate, Void unused) {
        List<Expr> literals = predicate.getOptions().stream()
                .map(expr -> ((Expr) ((Literal) expr).toLegacyLiteral()))
                .collect(Collectors.toList());
        param.setInPredicate(new org.apache.doris.analysis.InPredicate(new SlotRef(null, ""), literals, false));
        setOrUpdateFilter(((Slot) predicate.getCompareExpr()).getName());
        return null;
    }

    @Override
    public Expression visitIsNull(IsNull predicate, Void unused) {
        param.setValues(new NullLiteral(), true, new NullLiteral(), true);
        setOrUpdateFilter(((Slot) predicate.child()).getName());
        return null;
    }

    private void setOrUpdateFilter(String columnName) {
        PartitionColumnFilter filter = columnFilterMap.computeIfAbsent(columnName,
                k -> new PartitionColumnFilter());
        if (param.lowerBound != null) {
            filter.setLowerBound(param.lowerBound, param.lowerBoundInclusive);
        }
        if (param.upperBound != null) {
            filter.setUpperBound(param.upperBound, param.upperBoundInclusive);
        }
        if (param.inPredicate != null) {
            if (filter.getInPredicate() == null) {
                filter.setInPredicate(param.inPredicate);
            } else {
                filter.getInPredicate().getChildren().addAll(param.inPredicate.getListChildren());
            }
        }
    }
}
