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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.expression.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NumericLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Optional;

/**
 * simplify comparison, not support large int.
 * such as: cast(c1 as DateV2) >= DateV2Literal --> c1 >= DateLiteral
 *          cast(c1 AS double) > 2.0 --> c1 >= 2 (c1 is integer like type)
 */
public class SimplifyComparisonPredicate extends AbstractExpressionRewriteRule implements ExpressionPatternRuleFactory {
    public static SimplifyComparisonPredicate INSTANCE = new SimplifyComparisonPredicate();

    enum AdjustType {
        LOWER,
        UPPER,
        NONE
    }

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(ComparisonPredicate.class).then(SimplifyComparisonPredicate::simplify)
        );
    }

    @Override
    public Expression visitComparisonPredicate(ComparisonPredicate cp, ExpressionRewriteContext context) {
        return simplify(cp);
    }

    /** simplify */
    public static Expression simplify(ComparisonPredicate cp) {
        if (cp.left() instanceof Literal && !(cp.right() instanceof Literal)) {
            cp = cp.commute();
        }

        Expression left = cp.left();
        Expression right = cp.right();

        Expression result;

        // process type coercion
        if (left.getDataType().isFloatLikeType() && right.getDataType().isFloatLikeType()) {
            result = processFloatLikeTypeCoercion(cp, left, right);
        } else if (left.getDataType() instanceof DecimalV3Type && right.getDataType() instanceof DecimalV3Type) {
            result = processDecimalV3TypeCoercion(cp, left, right);
        } else if (left.getDataType() instanceof DateLikeType && right.getDataType() instanceof DateLikeType) {
            result = processDateLikeTypeCoercion(cp, left, right);
        } else {
            result = cp;
        }

        if (result instanceof ComparisonPredicate && ((ComparisonPredicate) result).right() instanceof NumericLiteral) {
            ComparisonPredicate cmp = (ComparisonPredicate) result;
            result = processTypeRangeLimitComparison(cmp, cmp.left(), (NumericLiteral) cmp.right());
        }

        return result;
    }

    private static Expression processComparisonPredicateDateTimeV2Literal(
            ComparisonPredicate comparisonPredicate, Expression left, DateTimeV2Literal right) {
        DateTimeV2Type leftType = (DateTimeV2Type) left.getDataType();
        DateTimeV2Type rightType = right.getDataType();
        if (leftType.getScale() < rightType.getScale()) {
            int toScale = leftType.getScale();
            if (comparisonPredicate instanceof EqualTo) {
                long originValue = right.getMicroSecond();
                right = right.roundCeiling(toScale);
                if (right.getMicroSecond() == originValue) {
                    return comparisonPredicate.withChildren(left, right);
                } else {
                    // TODO: the ideal way is to return an If expr like:
                    // return new If(new IsNull(left), new NullLiteral(BooleanType.INSTANCE),
                    // BooleanLiteral.of(false));
                    // but current fold constant rule can't handle such complex expr with null literal
                    // before supporting complex conjuncts with null literal folding rules,
                    // we use a trick way like this:
                    return ExpressionUtils.falseOrNull(left);
                }
            } else if (comparisonPredicate instanceof NullSafeEqual) {
                long originValue = right.getMicroSecond();
                right = right.roundCeiling(toScale);
                if (right.getMicroSecond() == originValue) {
                    return comparisonPredicate.withChildren(left, right);
                } else {
                    return BooleanLiteral.of(false);
                }
            } else if (comparisonPredicate instanceof GreaterThan
                    || comparisonPredicate instanceof LessThanEqual) {
                return comparisonPredicate.withChildren(left, right.roundFloor(toScale));
            } else if (comparisonPredicate instanceof LessThan
                    || comparisonPredicate instanceof GreaterThanEqual) {
                return comparisonPredicate.withChildren(left, right.roundCeiling(toScale));
            }
        }
        return comparisonPredicate;
    }

    private static Expression processDateLikeTypeCoercion(ComparisonPredicate cp, Expression left, Expression right) {
        if (left instanceof Cast && right instanceof DateLiteral) {
            Cast cast = (Cast) left;
            if (cast.child().getDataType() instanceof DateTimeType) {
                if (right instanceof DateTimeV2Literal) {
                    left = cast.child();
                    right = migrateToDateTime((DateTimeV2Literal) right);
                }
            }
            if (cast.child().getDataType() instanceof DateTimeV2Type) {
                if (right instanceof DateTimeV2Literal) {
                    left = cast.child();
                    return processComparisonPredicateDateTimeV2Literal(cp, left, (DateTimeV2Literal) right);
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

    private static Expression processFloatLikeTypeCoercion(ComparisonPredicate comparisonPredicate,
            Expression left, Expression right) {
        if (left instanceof Cast && left.child(0).getDataType().isIntegerLikeType()
                && (right instanceof DoubleLiteral || right instanceof FloatLiteral)) {
            Cast cast = (Cast) left;
            left = cast.child();
            BigDecimal literal = new BigDecimal(((Literal) right).getStringValue());
            return processIntegerDecimalLiteralComparison(comparisonPredicate, left, literal);
        } else {
            return comparisonPredicate;
        }
    }

    private static Expression processDecimalV3TypeCoercion(ComparisonPredicate comparisonPredicate,
            Expression left, Expression right) {
        if (left instanceof Cast && right instanceof DecimalV3Literal) {
            Cast cast = (Cast) left;
            left = cast.child();
            DecimalV3Literal literal = (DecimalV3Literal) right;
            if (left.getDataType().isDecimalV3Type()) {
                DecimalV3Type leftType = (DecimalV3Type) left.getDataType();

                try {
                    BigDecimal trailingZerosValue = literal.getValue().stripTrailingZeros();
                    int literalScale = org.apache.doris.analysis.DecimalLiteral.getBigDecimalScale(trailingZerosValue);
                    int literalPrecision = org.apache.doris.analysis.DecimalLiteral
                            .getBigDecimalPrecision(trailingZerosValue);

                    // we have a column named col1 with type decimalv3(15, 2)
                    // and we have a comparison like col1 > 0.5 + 0.1
                    // suppose the result type of 0.5 + 0.1 is decimalv3(27, 9)
                    // then the col1 need to convert to decimalv3(27, 9) to match the precision of right hand
                    // then will have cast(col1 as decimalv3(27,9)) > 0.5 + 0.1,
                    // after fold constant, we have cast(col1 as decimalv3(27, 9)) > 0.6 (0.6 is decimalv3(27, 9))
                    // but 0.6 can be represented using decimalv3(15, 2)
                    // then simplify it from 'cast(col1 as decimalv3(27, 9)) > 0.6' (0.6 is decimalv3(27, 9))
                    // to 'col1 > 0.6' (0.6 is decimalv3(15, 2))
                    if (literalScale <= leftType.getScale() && literalPrecision - literalScale <= leftType.getRange()) {
                        trailingZerosValue = trailingZerosValue.setScale(leftType.getScale(), RoundingMode.UNNECESSARY);
                        Expression newLiteral = new DecimalV3Literal(
                                DecimalV3Type.createDecimalV3TypeLooseCheck(
                                        leftType.getPrecision(), leftType.getScale()),
                                trailingZerosValue);
                        return comparisonPredicate.withChildren(left, newLiteral);
                    }
                } catch (ArithmeticException e) {
                    // stripTrailingZeros and setScale may cause exception if overflow
                }

                DecimalV3Type literalType = (DecimalV3Type) literal.getDataType();
                if (leftType.getScale() < literalType.getScale()) {
                    int toScale = ((DecimalV3Type) left.getDataType()).getScale();
                    if (comparisonPredicate instanceof EqualTo) {
                        try {
                            return TypeCoercionUtils.processComparisonPredicate((ComparisonPredicate)
                                    comparisonPredicate.withChildren(left, new DecimalV3Literal(
                                            literal.getValue().setScale(toScale, RoundingMode.UNNECESSARY))));
                        } catch (ArithmeticException e) {
                            // TODO: the ideal way is to return an If expr like:
                            // return new If(new IsNull(left), new NullLiteral(BooleanType.INSTANCE),
                            // BooleanLiteral.of(false));
                            // but current fold constant rule can't handle such complex expr with null literal
                            // before supporting complex conjuncts with null literal folding rules,
                            // we use a trick way like this:
                            return ExpressionUtils.falseOrNull(left);
                        }
                    } else if (comparisonPredicate instanceof NullSafeEqual) {
                        try {
                            return TypeCoercionUtils.processComparisonPredicate((ComparisonPredicate)
                                    comparisonPredicate.withChildren(left, new DecimalV3Literal(
                                            literal.getValue().setScale(toScale, RoundingMode.UNNECESSARY))));
                        } catch (ArithmeticException e) {
                            return BooleanLiteral.of(false);
                        }
                    } else if (comparisonPredicate instanceof GreaterThan
                            || comparisonPredicate instanceof LessThanEqual) {
                        return TypeCoercionUtils.processComparisonPredicate((ComparisonPredicate)
                                comparisonPredicate.withChildren(left, literal.roundFloor(toScale)));
                    } else if (comparisonPredicate instanceof LessThan
                            || comparisonPredicate instanceof GreaterThanEqual) {
                        return TypeCoercionUtils.processComparisonPredicate((ComparisonPredicate)
                                comparisonPredicate.withChildren(left, literal.roundCeiling(toScale)));
                    }
                }
            } else if (left.getDataType().isIntegerLikeType()) {
                return processIntegerDecimalLiteralComparison(comparisonPredicate, left, literal.getValue());
            }
        }

        return comparisonPredicate;
    }

    private static Expression processIntegerDecimalLiteralComparison(
            ComparisonPredicate comparisonPredicate, Expression left, BigDecimal literal) {
        // we only process isIntegerLikeType, which are tinyint, smallint, int, bigint
        if (literal.compareTo(new BigDecimal(Long.MIN_VALUE)) >= 0
                && literal.compareTo(new BigDecimal(Long.MAX_VALUE)) <= 0) {
            literal = literal.stripTrailingZeros();
            if (literal.scale() > 0) {
                if (comparisonPredicate instanceof EqualTo) {
                    // TODO: the ideal way is to return an If expr like:
                    // return new If(new IsNull(left), new NullLiteral(BooleanType.INSTANCE),
                    // BooleanLiteral.of(false));
                    // but current fold constant rule can't handle such complex expr with null literal
                    // before supporting complex conjuncts with null literal folding rules,
                    // we use a trick way like this:
                    return ExpressionUtils.falseOrNull(left);
                } else if (comparisonPredicate instanceof NullSafeEqual) {
                    return BooleanLiteral.of(false);
                } else if (comparisonPredicate instanceof GreaterThan
                        || comparisonPredicate instanceof LessThanEqual) {
                    return TypeCoercionUtils
                            .processComparisonPredicate((ComparisonPredicate) comparisonPredicate
                                    .withChildren(left, convertDecimalToIntegerLikeLiteral(
                                            literal.setScale(0, RoundingMode.FLOOR))));
                } else if (comparisonPredicate instanceof LessThan
                        || comparisonPredicate instanceof GreaterThanEqual) {
                    return TypeCoercionUtils
                            .processComparisonPredicate((ComparisonPredicate) comparisonPredicate
                                    .withChildren(left, convertDecimalToIntegerLikeLiteral(
                                            literal.setScale(0, RoundingMode.CEILING))));
                }
            } else {
                return TypeCoercionUtils
                        .processComparisonPredicate((ComparisonPredicate) comparisonPredicate
                                .withChildren(left, convertDecimalToIntegerLikeLiteral(literal)));
            }
        }
        return comparisonPredicate;
    }

    private static Expression processTypeRangeLimitComparison(ComparisonPredicate cp, Expression left,
            NumericLiteral right) {
        BigDecimal typeMinValue = null;
        BigDecimal typeMaxValue = null;
        // cmp float like have lost precision, for example float.max_value + 0.01 still eval to float.max_value
        if (left.getDataType().isIntegerLikeType() || left.getDataType().isDecimalV3Type()) {
            Optional<Pair<BigDecimal, BigDecimal>> minMaxOpt =
                    TypeCoercionUtils.getDataTypeMinMaxValue(left.getDataType());
            if (minMaxOpt.isPresent()) {
                typeMinValue = minMaxOpt.get().first;
                typeMaxValue = minMaxOpt.get().second;
            }
        }

        // cast(child as dataType2) range should be:
        //  [ max(childDataType.min_value, dataType2.min_value), min(childDataType.max_value, dataType2.max_value)]
        if (left instanceof Cast) {
            left = ((Cast) left).child();
            if (left.getDataType().isIntegerLikeType() || left.getDataType().isDecimalV3Type()) {
                Optional<Pair<BigDecimal, BigDecimal>> minMaxOpt =
                        TypeCoercionUtils.getDataTypeMinMaxValue(left.getDataType());
                if (minMaxOpt.isPresent()) {
                    if (typeMinValue == null || typeMinValue.compareTo(minMaxOpt.get().first) < 0) {
                        typeMinValue = minMaxOpt.get().first;
                    }
                    if (typeMaxValue == null || typeMaxValue.compareTo(minMaxOpt.get().second) > 0) {
                        typeMaxValue = minMaxOpt.get().second;
                    }
                }
            }
        }

        if (typeMinValue == null || typeMaxValue == null) {
            return cp;
        }
        BigDecimal literal = new BigDecimal(right.getStringValue());
        int cmpMin = literal.compareTo(typeMinValue);
        int cmpMax = literal.compareTo(typeMaxValue);
        if (cp instanceof EqualTo) {
            if (cmpMin < 0 || cmpMax > 0) {
                return ExpressionUtils.falseOrNull(left);
            }
        } else if (cp instanceof NullSafeEqual) {
            if (cmpMin < 0 || cmpMax > 0) {
                return BooleanLiteral.of(false);
            }
        } else if (cp instanceof GreaterThan) {
            if (cmpMin < 0) {
                return ExpressionUtils.trueOrNull(left);
            }
            if (cmpMax >= 0) {
                return ExpressionUtils.falseOrNull(left);
            }
        } else if (cp instanceof GreaterThanEqual) {
            if (cmpMin <= 0) {
                return ExpressionUtils.trueOrNull(left);
            }
            if (cmpMax == 0) {
                return new EqualTo(cp.left(), cp.right());
            }
            if (cmpMax > 0) {
                return ExpressionUtils.falseOrNull(left);
            }
        } else if (cp instanceof LessThan) {
            if (cmpMin <= 0) {
                return ExpressionUtils.falseOrNull(left);
            }
            if (cmpMax > 0) {
                return ExpressionUtils.trueOrNull(left);
            }
        } else if (cp instanceof LessThanEqual) {
            if (cmpMin < 0) {
                return ExpressionUtils.falseOrNull(left);
            }
            if (cmpMin == 0) {
                return new EqualTo(cp.left(), cp.right());
            }
            if (cmpMax >= 0) {
                return ExpressionUtils.trueOrNull(left);
            }
        }
        return cp;
    }

    private static IntegerLikeLiteral convertDecimalToIntegerLikeLiteral(BigDecimal decimal) {
        Preconditions.checkArgument(decimal.scale() <= 0
                && decimal.compareTo(new BigDecimal(Long.MIN_VALUE)) >= 0
                && decimal.compareTo(new BigDecimal(Long.MAX_VALUE)) <= 0,
                "decimal literal must have 0 scale and in range [Long.MIN_VALUE, Long.MAX_VALUE]");
        long val = decimal.longValue();
        if (val >= Byte.MIN_VALUE && val <= Byte.MAX_VALUE) {
            return new TinyIntLiteral((byte) val);
        } else if (val >= Short.MIN_VALUE && val <= Short.MAX_VALUE) {
            return new SmallIntLiteral((short) val);
        } else if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE) {
            return new IntegerLiteral((int) val);
        } else {
            return new BigIntLiteral(val);
        }
    }

    private static Expression migrateToDateTime(DateTimeV2Literal l) {
        return new DateTimeLiteral(l.getYear(), l.getMonth(), l.getDay(), l.getHour(), l.getMinute(), l.getSecond());
    }

    private static boolean cannotAdjust(DateTimeLiteral l, ComparisonPredicate cp) {
        return cp instanceof EqualTo && (l.getHour() != 0 || l.getMinute() != 0 || l.getSecond() != 0);
    }

    private static Expression migrateToDateV2(DateTimeLiteral l, AdjustType type) {
        DateV2Literal d = new DateV2Literal(l.getYear(), l.getMonth(), l.getDay());
        if (type == AdjustType.UPPER && (l.getHour() != 0 || l.getMinute() != 0 || l.getSecond() != 0)) {
            return d.plusDays(1);
        } else {
            return d;
        }
    }

    private static Expression migrateToDate(DateV2Literal l) {
        return new DateLiteral(l.getYear(), l.getMonth(), l.getDay());
    }
}
