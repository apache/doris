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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Date;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.util.DateUtils;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 rewrite predicate for partition prune
 */
public class PredicateRewriteForPartitionPrune
        extends DefaultExpressionRewriter<PredicateRewriteForPartitionPrune.RewriteContext> {
    public static Expression rewrite(Expression expression,
                                     CascadesContext cascadesContext) {
        PredicateRewriteForPartitionPrune rewriter = new PredicateRewriteForPartitionPrune();
        return expression.accept(rewriter, RewriteContext.ALLOW_CAST_REWRITE);
    }

    // DTV2-to-TIMESTAMP_NS rewrites preserve the set of TRUE rows, but can turn NULL into FALSE.
    // That is sufficient for a positive filter and its AND/OR children, but not below expressions
    // such as IS NULL, NOT, or COALESCE that can observe or invert the three-valued result.
    @Override
    public Expression visit(Expression expression, RewriteContext context) {
        RewriteContext childrenContext = context.allowCastRewrite
                && (expression instanceof And || expression instanceof Or)
                ? context : RewriteContext.DISALLOW_CAST_REWRITE;
        return super.visit(expression, childrenContext);
    }

    /* F: a DateTime, DateTimeV2, or TimeStampNs column
     * Date(F) in (2020-01-02, 2020-01-01) =>
     *              (2020-01-01 24:00:00 >= F >= 2020-01-01 00:00:00)
     *              or (2020-01-02 24:00:00 >= F >= 2020-01-02 00:00:00)
     */
    @Override
    public Expression visitInPredicate(InPredicate in, RewriteContext context) {
        if (context.allowCastRewrite && isDateTimeV2ToTimeStampNsCast(in.getCompareExpr())) {
            Cast cast = (Cast) in.getCompareExpr();
            DateTimeV2Type sourceType = (DateTimeV2Type) cast.child().getDataType();
            List<Expression> children = new ArrayList<>();
            children.add(cast.child());
            for (Expression option : in.getOptions()) {
                if (!(option instanceof TimeStampNsLiteral)) {
                    return in;
                }
                TimeStampNsLiteral literal = (TimeStampNsLiteral) option;
                if (canRepresentExactly(literal, sourceType.getScale())) {
                    children.add(literal.roundFloorToDateTimeV2(sourceType.getScale()));
                }
            }
            return children.size() == 1 ? BooleanLiteral.FALSE
                    : in.withChildren(children);
        }
        if (in.getCompareExpr() instanceof Date) {
            Expression dateChild = in.getCompareExpr().child(0);
            boolean convertable = true;
            List<Expression> splitIn = new ArrayList<>();
            // V1
            if (dateChild.getDataType() instanceof DateTimeType) {
                for (Expression opt : in.getOptions()) {
                    if (opt instanceof DateLiteral) {
                        GreaterThanEqual ge = new GreaterThanEqual(dateChild, ((DateLiteral) opt).toBeginOfTheDay());
                        LessThanEqual le = new LessThanEqual(dateChild, ((DateLiteral) opt).toEndOfTheDay());
                        splitIn.add(new And(ge, le));
                    } else {
                        convertable = false;
                        break;
                    }
                }
                if (convertable) {
                    return ExpressionUtils.or(splitIn);
                }
            } else if (dateChild.getDataType() instanceof DateTimeV2Type) {
                // V2
                convertable = true;
                for (Expression opt : in.getOptions()) {
                    if (opt instanceof DateLiteral) {
                        GreaterThanEqual ge = new GreaterThanEqual(dateChild, ((DateV2Literal) opt).toBeginOfTheDay());
                        LessThanEqual le = new LessThanEqual(dateChild, ((DateV2Literal) opt).toEndOfTheDay());
                        splitIn.add(new And(ge, le));
                    } else {
                        convertable = false;
                        break;
                    }
                }
                if (convertable) {
                    return ExpressionUtils.or(splitIn);
                }
            } else if (dateChild.getDataType() instanceof TimeStampNsType) {
                for (Expression opt : in.getOptions()) {
                    Optional<Expression> range = timestampNsDateRange(dateChild, opt);
                    if (!range.isPresent()) {
                        convertable = false;
                        break;
                    }
                    splitIn.add(range.get());
                }
                if (convertable) {
                    return ExpressionUtils.or(splitIn);
                }
            }
        }
        return in;
    }

    @Override
    public Expression visitEqualTo(EqualTo equalTo, RewriteContext context) {
        if (context.allowCastRewrite && isDateTimeV2ToTimeStampNsCast(equalTo.left())
                && equalTo.right() instanceof TimeStampNsLiteral) {
            Cast cast = (Cast) equalTo.left();
            DateTimeV2Type sourceType = (DateTimeV2Type) cast.child().getDataType();
            TimeStampNsLiteral literal = (TimeStampNsLiteral) equalTo.right();
            return canRepresentExactly(literal, sourceType.getScale())
                    ? new EqualTo(cast.child(), literal.roundFloorToDateTimeV2(sourceType.getScale()))
                    : BooleanLiteral.FALSE;
        }
        if (equalTo.left() instanceof Date) {
            Expression dateChild = equalTo.left().child(0);
            if (dateChild.getDataType() instanceof TimeStampNsType) {
                return timestampNsDateRange(dateChild, equalTo.right()).orElse(equalTo);
            }
        }
        return equalTo;
    }

    private static boolean isDateTimeV2ToTimeStampNsCast(Expression expression) {
        return expression instanceof Cast
                && expression.getDataType() instanceof TimeStampNsType
                && expression.child(0).getDataType() instanceof DateTimeV2Type;
    }

    private static boolean canRepresentExactly(TimeStampNsLiteral literal, int scale) {
        long scaleFactor = (long) Math.pow(10, DateUtils.NANOSECOND_SCALE - scale);
        return literal.getNanoSecond() % scaleFactor == 0;
    }

    private static Optional<Expression> timestampNsDateRange(Expression dateChild, Expression option) {
        if (!(option instanceof DateV2Literal)) {
            return Optional.empty();
        }
        DateV2Literal date = (DateV2Literal) option;
        TimeStampNsLiteral minValue = TimeStampNsLiteral.getMinValue();
        TimeStampNsLiteral maxValue = TimeStampNsLiteral.getMaxValue();
        DateV2Literal minDate = new DateV2Literal(
                minValue.getYear(), minValue.getMonth(), minValue.getDay());
        DateV2Literal maxDate = new DateV2Literal(
                maxValue.getYear(), maxValue.getMonth(), maxValue.getDay());
        if (date.compareTo(minDate) < 0 || date.compareTo(maxDate) > 0) {
            return Optional.empty();
        }
        TimeStampNsLiteral begin = date.equals(minDate) ? minValue : new TimeStampNsLiteral(
                date.getYear(), date.getMonth(), date.getDay(), 0, 0, 0, 0);
        TimeStampNsLiteral end = date.equals(maxDate) ? maxValue : TimeStampNsLiteral.createEndOfDay(
                date.getYear(), date.getMonth(), date.getDay());
        return Optional.of(new And(new GreaterThanEqual(dateChild, begin),
                new LessThanEqual(dateChild, end)));
    }

    private static final class RewriteContext {
        private static final RewriteContext ALLOW_CAST_REWRITE = new RewriteContext(true);
        private static final RewriteContext DISALLOW_CAST_REWRITE = new RewriteContext(false);

        private final boolean allowCastRewrite;

        private RewriteContext(boolean allowCastRewrite) {
            this.allowCastRewrite = allowCastRewrite;
        }
    }

}
