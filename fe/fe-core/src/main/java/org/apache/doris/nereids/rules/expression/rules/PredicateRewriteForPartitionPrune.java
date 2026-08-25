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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Date;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.util.ExpressionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 rewrite predicate for partition prune
 */
public class PredicateRewriteForPartitionPrune
        extends DefaultExpressionRewriter<CascadesContext> {
    public static Expression rewrite(Expression expression,
                                     CascadesContext cascadesContext) {
        PredicateRewriteForPartitionPrune rewriter = new PredicateRewriteForPartitionPrune();
        return expression.accept(rewriter, cascadesContext);
    }

    /* F: a DateTime, DateTimeV2, or TimeStampNs column
     * Date(F) in (2020-01-02, 2020-01-01) =>
     *              (2020-01-01 24:00:00 >= F >= 2020-01-01 00:00:00)
     *              or (2020-01-02 24:00:00 >= F >= 2020-01-02 00:00:00)
     */
    @Override
    public Expression visitInPredicate(InPredicate in, CascadesContext context) {
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
    public Expression visitEqualTo(EqualTo equalTo, CascadesContext context) {
        if (equalTo.left() instanceof Date) {
            Expression dateChild = equalTo.left().child(0);
            if (dateChild.getDataType() instanceof TimeStampNsType) {
                return timestampNsDateRange(dateChild, equalTo.right()).orElse(equalTo);
            }
        }
        return equalTo;
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

}
