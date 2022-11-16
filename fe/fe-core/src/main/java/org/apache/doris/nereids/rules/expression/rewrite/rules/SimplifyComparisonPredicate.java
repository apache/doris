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

package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.coercion.DateLikeType;

/**
 * simplify comparison
 * such as: cast(c1 as DateV2) >= DateV2Literal --> c1 >= DateLiteral
 */
public class SimplifyComparisonPredicate extends AbstractExpressionRewriteRule {

    public static SimplifyComparisonPredicate INSTANCE = new SimplifyComparisonPredicate();

    enum AdjustType {
        LOWER,
        UPPER,
        NONE
    }

    @Override
    public Expression visitComparisonPredicate(ComparisonPredicate cp, ExpressionRewriteContext context) {
        Expression left = rewrite(cp.left(), context);
        Expression right = rewrite(cp.right(), context);

        // date like type
        if (left.getDataType() instanceof DateLikeType && right.getDataType() instanceof DateLikeType) {
            return processDateLikeTypeCoercion(cp, left, right);
        }

        if (left != cp.left() || right != cp.right()) {
            return cp.withChildren(left, right);
        } else {
            return cp;
        }
    }

    private Expression processDateLikeTypeCoercion(ComparisonPredicate cp, Expression left, Expression right) {
        if (left instanceof DateLiteral) {
            cp = cp.commute();
            Expression temp = left;
            left = right;
            right = temp;
        }

        if (left instanceof Cast && right instanceof DateLiteral) {
            Cast cast = (Cast) left;
            if (cast.child().getDataType() instanceof DateTimeType) {
                if (right instanceof DateTimeV2Literal) {
                    left = cast.child();
                    right = migrateToDateTime((DateTimeV2Literal) right);
                }
            }
            // datetime to datev2
            if (cast.child().getDataType() instanceof DateType || cast.child().getDataType() instanceof DateV2Type) {
                if (right instanceof DateTimeLiteral) {
                    if (cannotAdjust((DateTimeLiteral) right, cp)) {
                        return cp;
                    }
                    AdjustType type = AdjustType.NONE;
                    if (cp instanceof GreaterThanEqual || cp instanceof LessThan) {
                        type = AdjustType.UPPER;
                    } else if (cp instanceof GreaterThan || cp instanceof LessThanEqual) {
                        type = AdjustType.LOWER;
                    }
                    right = migrateToDateV2((DateTimeLiteral) right, type);
                    if (cast.child().getDataType() instanceof DateV2Type) {
                        left = cast.child();
                    }
                }
            }

            // datev2 to date
            if (cast.child().getDataType() instanceof DateType) {
                if (right instanceof DateV2Literal) {
                    left = cast.child();
                    right = migrateToDate((DateV2Literal) right);
                }
            }
        }

        if (left != cp.left() || right != cp.right()) {
            return cp.withChildren(left, right);
        } else {
            return cp;
        }
    }

    private Expression migrateToDateTime(DateTimeV2Literal l) {
        return new DateTimeLiteral(l.getYear(), l.getMonth(), l.getDay(), l.getHour(), l.getMinute(), l.getSecond());
    }

    private boolean cannotAdjust(DateTimeLiteral l, ComparisonPredicate cp) {
        return cp instanceof EqualTo && (l.getHour() != 0 || l.getMinute() != 0 || l.getSecond() != 0);
    }

    private Expression migrateToDateV2(DateTimeLiteral l, AdjustType type) {
        DateV2Literal d = new DateV2Literal(l.getYear(), l.getMonth(), l.getDay());
        if (type == AdjustType.UPPER && (l.getHour() != 0 || l.getMinute() != 0 || l.getSecond() != 0)) {
            d = d.plusDays(1);
        }
        return d;
    }

    private Expression migrateToDate(DateV2Literal l) {
        return new DateLiteral(l.getYear(), l.getMonth(), l.getDay());
    }
}
