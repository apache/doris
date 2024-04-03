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

import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Date;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * F: a DateTime or DateTimeV2 column
 * Date(F) > 2020-01-01 => F > 2020-01-02 00:00:00
 * Date(F) >= 2020-01-01 => F > 2020-01-01 00:00:00
 *
 */
public class DateFunctionRewrite implements ExpressionPatternRuleFactory {
    public static DateFunctionRewrite INSTANCE = new DateFunctionRewrite();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(EqualTo.class).then(DateFunctionRewrite::rewriteEqualTo),
                matchesType(GreaterThan.class).then(DateFunctionRewrite::rewriteGreaterThan),
                matchesType(GreaterThanEqual.class).then(DateFunctionRewrite::rewriteGreaterThanEqual),
                matchesType(LessThan.class).then(DateFunctionRewrite::rewriteLessThan),
                matchesType(LessThanEqual.class).then(DateFunctionRewrite::rewriteLessThanEqual)
        );
    }

    private static Expression rewriteEqualTo(EqualTo equalTo) {
        if (equalTo.left() instanceof Date) {
            // V1
            if (equalTo.left().child(0).getDataType() instanceof DateTimeType
                    && equalTo.right() instanceof DateLiteral) {
                DateTimeLiteral lowerBound = ((DateLiteral) equalTo.right()).toBeginOfTheDay();
                DateTimeLiteral upperBound = ((DateLiteral) equalTo.right()).toEndOfTheDay();
                Expression newLeft = equalTo.left().child(0);
                return new And(new GreaterThanEqual(newLeft, lowerBound),
                        new LessThanEqual(newLeft, upperBound));
            }
            // V2
            if (equalTo.left().child(0).getDataType() instanceof DateTimeV2Type
                    && equalTo.right() instanceof DateV2Literal) {
                DateTimeV2Literal lowerBound = ((DateV2Literal) equalTo.right()).toBeginOfTheDay(
                        (DateTimeV2Type) equalTo.left().child(0).getDataType());
                DateTimeV2Literal upperBound = ((DateV2Literal) equalTo.right()).toEndOfTheDay(
                        (DateTimeV2Type) equalTo.left().child(0).getDataType());
                Expression newLeft = equalTo.left().child(0);
                return new And(new GreaterThanEqual(newLeft, lowerBound),
                        new LessThanEqual(newLeft, upperBound));
            }
        }
        return equalTo;
    }

    private static Expression rewriteGreaterThan(GreaterThan greaterThan) {
        if (greaterThan.left() instanceof Date) {
            // V1
            if (greaterThan.left().child(0).getDataType() instanceof DateTimeType
                    && greaterThan.right() instanceof DateLiteral) {
                DateTimeLiteral newLiteral = ((DateLiteral) greaterThan.right()).toBeginOfTomorrow();
                return new GreaterThanEqual(greaterThan.left().child(0), newLiteral);
            }

            // V2
            if (greaterThan.left().child(0).getDataType() instanceof DateTimeV2Type
                    && greaterThan.right() instanceof DateV2Literal) {
                DateTimeV2Literal newLiteral = ((DateV2Literal) greaterThan.right()).toBeginOfTomorrow();
                return new GreaterThanEqual(greaterThan.left().child(0), newLiteral);
            }
        }

        return greaterThan;
    }

    private static Expression rewriteGreaterThanEqual(GreaterThanEqual greaterThanEqual) {
        if (greaterThanEqual.left() instanceof Date) {
            // V1
            if (greaterThanEqual.left().child(0).getDataType() instanceof DateTimeType
                    && greaterThanEqual.right() instanceof DateLiteral) {
                DateTimeLiteral newLiteral = ((DateLiteral) greaterThanEqual.right()).toBeginOfTheDay();
                return new GreaterThanEqual(greaterThanEqual.left().child(0), newLiteral);
            }

            // V2
            if (greaterThanEqual.left().child(0).getDataType() instanceof DateTimeV2Type
                    && greaterThanEqual.right() instanceof DateV2Literal) {
                DateTimeV2Literal newLiteral = ((DateV2Literal) greaterThanEqual.right()).toBeginOfTheDay();
                return new GreaterThanEqual(greaterThanEqual.left().child(0), newLiteral);
            }
        }
        return greaterThanEqual;
    }

    private static Expression rewriteLessThan(LessThan lessThan) {
        if (lessThan.left() instanceof Date) {
            // V1
            if (lessThan.left().child(0).getDataType() instanceof DateTimeType
                    && lessThan.right() instanceof DateLiteral) {
                DateTimeLiteral newLiteral = ((DateLiteral) lessThan.right()).toBeginOfTheDay();
                return new LessThan(lessThan.left().child(0), newLiteral);
            }

            // V2
            if (lessThan.left().child(0).getDataType() instanceof DateTimeV2Type
                    && lessThan.right() instanceof DateV2Literal) {
                DateTimeV2Literal newLiteral = ((DateV2Literal) lessThan.right()).toBeginOfTheDay();
                return new LessThan(lessThan.left().child(0), newLiteral);
            }
        }
        return lessThan;
    }

    private static Expression rewriteLessThanEqual(LessThanEqual lessThanEqual) {
        if (lessThanEqual.left() instanceof Date) {
            // V1
            if (lessThanEqual.left().child(0).getDataType() instanceof DateTimeType
                    && lessThanEqual.right() instanceof DateLiteral) {
                DateTimeLiteral newLiteral = ((DateLiteral) lessThanEqual.right()).toEndOfTheDay();
                return new LessThanEqual(lessThanEqual.left().child(0), newLiteral);
            }

            // V2
            if (lessThanEqual.left().child(0).getDataType() instanceof DateTimeV2Type
                    && lessThanEqual.right() instanceof DateV2Literal) {
                DateTimeV2Literal newLiteral = ((DateV2Literal) lessThanEqual.right()).toEndOfTheDay(
                            (DateTimeV2Type) lessThanEqual.left().child(0).getDataType());
                return new LessThanEqual(lessThanEqual.left().child(0), newLiteral);
            }
        }
        return lessThanEqual;
    }
}
