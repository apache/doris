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
        extends DefaultExpressionVisitor<Expression, Map<String, PartitionColumnFilter>> {
    public static ExpressionColumnFilterConverter INSTANCE = new ExpressionColumnFilterConverter();

    public static void convert(Expression expr, Map<String, PartitionColumnFilter> context) {
        expr.accept(INSTANCE, context);
    }

    @Override
    public Expression visitComparisonPredicate(ComparisonPredicate predicate,
            Map<String, PartitionColumnFilter> context) {
        if (predicate instanceof NullSafeEqual) {
            return null;
        }
        PartitionColumnFilter filter = new PartitionColumnFilter();
        LiteralExpr literal = ((Literal) predicate.right()).toLegacyLiteral();
        if (predicate instanceof EqualTo) {
            setFilter(filter, literal, true, literal, true);
        } else if (predicate instanceof GreaterThan) {
            setFilter(filter, literal, false, null, false);
        } else if (predicate instanceof GreaterThanEqual) {
            setFilter(filter, literal, true, null, false);
        } else if (predicate instanceof LessThan) {
            setFilter(filter, null, false, literal, false);
        } else if (predicate instanceof LessThanEqual) {
            setFilter(filter, null, false, literal, true);
        }
        context.put(((Slot) predicate.left()).getName(), filter);
        return null;
    }

    @Override
    public Expression visitInPredicate(InPredicate predicate, Map<String, PartitionColumnFilter> context) {
        PartitionColumnFilter filter = context.computeIfAbsent(((Slot) predicate.getCompareExpr()).getName(),
                k -> new PartitionColumnFilter());
        List<Expr> literals = predicate.getOptions().stream()
                .map(expr -> ((Expr) ((Literal) expr).toLegacyLiteral()))
                .collect(Collectors.toList());
        if (filter.getInPredicate() == null) {
            filter.setInPredicate(new org.apache.doris.analysis.InPredicate(new SlotRef(null, ""), literals, false));
        } else {
            filter.getInPredicate().getChildren().addAll(literals);
        }
        return null;
    }

    @Override
    public Expression visitIsNull(IsNull predicate, Map<String, PartitionColumnFilter> context) {
        PartitionColumnFilter filter = new PartitionColumnFilter();
        setFilter(filter, new NullLiteral(), true, new NullLiteral(), true);
        context.put(((Slot) predicate.child()).getName(), filter);
        return null;
    }

    private void setFilter(PartitionColumnFilter filter, LiteralExpr lowerBound, boolean lowerInclusive,
            LiteralExpr upperBound, boolean upperInclusive) {
        if (lowerBound != null) {
            filter.setLowerBound(lowerBound, lowerInclusive);
        }
        if (upperBound != null) {
            filter.setUpperBound(upperBound, upperInclusive);
        }
    }
}
