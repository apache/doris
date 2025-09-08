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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.rules.expression.ExpressionRuleType;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NonNullable;
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
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Optional;

/**
 * simplify comparison, not support large int.
 * such as: cast(c1 as DateV2) >= DateV2Literal --> c1 >= DateLiteral
 *          cast(c1 AS double) > 2.0 --> c1 >= 2 (c1 is integer like type)
 */
public class SimplifyComparisonPredicate implements ExpressionPatternRuleFactory {
    public static SimplifyComparisonPredicate INSTANCE = new SimplifyComparisonPredicate();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(ComparisonPredicate.class).then(SimplifyComparisonPredicate::simplify)
                        .toRule(ExpressionRuleType.SIMPLIFY_COMPARISON_PREDICATE)
        );
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
        if (left.getDataType().isIntegralType() && right.getDataType().isIntegralType()) {
            result = processIntegerLikeTypeCoercion(cp, left, right);
        } else if (left.getDataType().isFloatLikeType() && right.getDataType().isFloatLikeType()) {
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

    private static Expression processIntegerLikeTypeCoercion(ComparisonPredicate cp,
            Expression left, Expression right) {
        // Suppose a is integer type, for expression `a > 500000 + 100000`,
        // since right type is big int (int plus int is big int),
        // then will have cast(a as bigint) > cast(500000 + 100000).
        // After fold constant, will have cast(a as bigint) > big int(600000),
        // since 600000 can represent as an int type, will rewrite as a > int(600000).
        if (left instanceof Cast && left.getDataType().isIntegralType()
                && ((Cast) left).child().getDataType().isIntegralType()
                && right instanceof IntegerLikeLiteral) {
            DataType castDataType = left.getDataType();
            DataType childDataType = ((Cast) left).child().getDataType();
            boolean castDataTypeWider = false;
            for (DataType type : TypeCoercionUtils.NUMERIC_PRECEDENCE) {
                if (type.equals(childDataType)) {
                    break;
                }
                if (type.equals(castDataType)) {
                    castDataTypeWider = true;
                    break;
                }
            }
            if (castDataTypeWider) {
                Optional<Pair<BigDecimal, BigDecimal>> minMaxOpt =
                        TypeCoercionUtils.getDataTypeMinMaxValue(childDataType);
                if (minMaxOpt.isPresent()) {
                    BigDecimal childTypeMinValue = minMaxOpt.get().first;
                    BigDecimal childTypeMaxValue = minMaxOpt.get().second;
                    BigDecimal rightValue = ((IntegerLikeLiteral) right).getBigDecimalValue();
                    if (rightValue.compareTo(childTypeMinValue) >= 0 && rightValue.compareTo(childTypeMaxValue) <= 0) {
                        Expression newRight = null;
                        if (childDataType.equals(BigIntType.INSTANCE)) {
                            newRight = new BigIntLiteral(rightValue.longValue());
                        } else if (childDataType.equals(IntegerType.INSTANCE)) {
                            newRight = new IntegerLiteral(rightValue.intValue());
                        } else if (childDataType.equals(SmallIntType.INSTANCE)) {
                            newRight = new SmallIntLiteral(rightValue.shortValue());
                        } else if (childDataType.equals(TinyIntType.INSTANCE)) {
                            newRight = new TinyIntLiteral(rightValue.byteValue());
                        }
                        if (newRight != null) {
                            return cp.withChildren(((Cast) left).child(), newRight);
                        }
                    }
                }
            }
        }
        return cp;
    }

    private static Expression processDateLikeTypeCoercion(ComparisonPredicate cp, Expression left, Expression right) {
        if (left instanceof Cast && right instanceof DateLiteral
                && ((Cast) left).getDataType().equals(right.getDataType())) {
            Cast cast = (Cast) left;
            if (cast.child().getDataType() instanceof DateTimeType
                    || cast.child().getDataType() instanceof DateTimeV2Type) {
                // right is datetime
                if (right instanceof DateTimeV2Literal) {
                    return processDateTimeLikeComparisonPredicateDateTimeV2Literal(
                            cp, cast.child(), (DateTimeV2Literal) right);
                }
                // right is date, not datetime
                if (!(right instanceof DateTimeLiteral)) {
                    return processDateTimeLikeComparisonPredicateDateLiteral(
                            cp, cast.child(), (DateLiteral) right);
                }
            }

            // datetime to datev2
            if (cast.child().getDataType() instanceof DateType || cast.child().getDataType() instanceof DateV2Type) {
                return processDateLikeComparisonPredicateDateLiteral(cp, cast.child(), (DateLiteral) right);
            }
        }

        return cp;
    }

    // process cast(datetime as datetime) cmp datetime
    private static Expression processDateTimeLikeComparisonPredicateDateTimeV2Literal(
            ComparisonPredicate comparisonPredicate, Expression left, DateTimeV2Literal right) {
        DataType leftType = left.getDataType();
        if (!(leftType instanceof DateTimeType) && !(leftType instanceof DateTimeV2Type)) {
            return comparisonPredicate;
        }
        int toScale = leftType instanceof DateTimeV2Type ? ((DateTimeV2Type) leftType).getScale() : 0;
        DateTimeV2Type rightType = right.getDataType();
        if (toScale < rightType.getScale()) {
            if (comparisonPredicate instanceof EqualTo) {
                long originValue = right.getMicroSecond();
                right = right.roundFloor(toScale);
                if (right.getMicroSecond() != originValue) {
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
                right = right.roundFloor(toScale);
                if (right.getMicroSecond() != originValue) {
                    return BooleanLiteral.of(false);
                }
            } else if (comparisonPredicate instanceof GreaterThan
                    || comparisonPredicate instanceof LessThanEqual) {
                right = right.roundFloor(toScale);
            } else if (comparisonPredicate instanceof LessThan
                    || comparisonPredicate instanceof GreaterThanEqual) {
                try {
                    right = right.roundCeiling(toScale);
                } catch (AnalysisException e) {
                    // '9999-12-31 23:59:59.9'.roundCeiling(0) overflow
                    DateTimeLiteral newRight = right.roundFloor(toScale);
                    if (leftType instanceof DateTimeType) {
                        newRight = migrateToDateTime((DateTimeV2Literal) newRight);
                    }
                    if (comparisonPredicate instanceof LessThan) {
                        return new LessThanEqual(left, newRight);
                    } else {
                        return new GreaterThan(left, newRight);
                    }
                }
            } else {
                return comparisonPredicate;
            }
            Expression newRight = leftType instanceof DateTimeType ? migrateToDateTime(right) : right;
            return comparisonPredicate.withChildren(left, newRight);
        } else if (toScale > rightType.getScale()) {
            // when toScale > right's scale, then left must be datetimev2, not datetimev1
            Preconditions.checkArgument(leftType instanceof DateTimeV2Type, leftType);

            // for expression cast(left as datetime(2)) = '2020-12-20 01:02:03.45'
            // then left scale is 5, right = '2020-12-20 01:02:03.45",  right scale is 2,
            // then left >= '2020-12-20 01:02:03.45000' && left <= '2020-12-20 01:02:03.45999'
            // for low bound, it add (5-2) '0' to the origin right's tail
            // for up bound, it add (5-2) '9' to the origin right's tail
            // when roundFloor to high scale, its microsecond shouldn't change, only change its data type.
            DateTimeV2Literal lowBound = right.roundFloor(toScale);
            long upMicroSecond = 0;
            for (int i = 0; i < toScale - rightType.getScale(); i++) {
                upMicroSecond = 10 * upMicroSecond + 9;
            }
            upMicroSecond *= (int) Math.pow(10, 6 - toScale);
            upMicroSecond += lowBound.getMicroSecond();
            // left must be a datetimev2
            DateTimeV2Literal upBound = new DateTimeV2Literal((DateTimeV2Type) leftType,
                    right.getYear(), right.getMonth(), right.getDay(),
                    right.getHour(), right.getMinute(), right.getSecond(), upMicroSecond);

            if (comparisonPredicate instanceof GreaterThanEqual || comparisonPredicate instanceof LessThan) {
                return comparisonPredicate.withChildren(left, lowBound);
            }

            if (comparisonPredicate instanceof GreaterThan || comparisonPredicate instanceof LessThanEqual) {
                return comparisonPredicate.withChildren(left, upBound);
            }

            if (comparisonPredicate instanceof EqualTo || comparisonPredicate instanceof NullSafeEqual) {
                List<Expression> conjunctions;
                if (left.nullable() && comparisonPredicate instanceof NullSafeEqual) {
                    conjunctions = ImmutableList.of(
                            new GreaterThanEqual(new NonNullable(left), lowBound),
                            new LessThanEqual(new NonNullable(left), upBound),
                            new Not(new IsNull(left))
                    );
                } else {
                    conjunctions = ImmutableList.of(
                            new GreaterThanEqual(left, lowBound),
                            new LessThanEqual(left, upBound)
                    );
                }
                return new And(conjunctions);
            }
        }

        if (leftType instanceof DateTimeType) {
            return comparisonPredicate.withChildren(left, migrateToDateTime(right));
        } else {
            return comparisonPredicate;
        }
    }

    // process cast(datetime as date) cmp date
    private static Expression processDateTimeLikeComparisonPredicateDateLiteral(
            ComparisonPredicate comparisonPredicate, Expression left, DateLiteral right) {
        DataType leftType = left.getDataType();
        if (!(leftType instanceof DateTimeType) && !(leftType instanceof DateTimeV2Type)) {
            return comparisonPredicate;
        }
        if (right instanceof DateTimeLiteral) {
            return comparisonPredicate;
        }

        DateTimeLiteral lowBound = null;
        DateTimeLiteral upBound = null;
        if (leftType instanceof DateTimeType) {
            lowBound = new DateTimeLiteral(right.getYear(), right.getMonth(), right.getDay(), 0, 0, 0);
            upBound = new DateTimeLiteral(right.getYear(), right.getMonth(), right.getDay(), 23, 59, 59);
        } else {
            long upMicroSecond = 0;
            for (int i = 0; i < ((DateTimeV2Type) leftType).getScale(); i++) {
                upMicroSecond = 10 * upMicroSecond + 9;
            }
            upMicroSecond *= (int) Math.pow(10, 6 - ((DateTimeV2Type) leftType).getScale());
            lowBound = new DateTimeV2Literal((DateTimeV2Type) leftType,
                    right.getYear(), right.getMonth(), right.getDay(), 0, 0, 0, 0);
            upBound = new DateTimeV2Literal((DateTimeV2Type) leftType,
                    right.getYear(), right.getMonth(), right.getDay(), 23, 59, 59, upMicroSecond);
        }

        if (comparisonPredicate instanceof GreaterThanEqual || comparisonPredicate instanceof LessThan) {
            return comparisonPredicate.withChildren(left, lowBound);
        }
        if (comparisonPredicate instanceof GreaterThan || comparisonPredicate instanceof LessThanEqual) {
            return comparisonPredicate.withChildren(left, upBound);
        }

        if (comparisonPredicate instanceof EqualTo || comparisonPredicate instanceof NullSafeEqual) {
            List<Expression> conjunctions;
            if (left.nullable() && comparisonPredicate instanceof NullSafeEqual) {
                conjunctions = ImmutableList.of(
                        new GreaterThanEqual(new NonNullable(left), lowBound),
                        new LessThanEqual(new NonNullable(left), upBound),
                        new Not(new IsNull(left))
                );
            } else {
                conjunctions = ImmutableList.of(
                        new GreaterThanEqual(left, lowBound),
                        new LessThanEqual(left, upBound)
                );
            }
            return new And(conjunctions);
        }

        return comparisonPredicate;
    }

    // process cast(date as datetime/date) cmp datetime/date
    private static Expression processDateLikeComparisonPredicateDateLiteral(
            ComparisonPredicate comparisonPredicate, Expression left, DateLiteral right) {
        if (!(left.getDataType() instanceof DateType) && !(left.getDataType() instanceof DateV2Type)) {
            return comparisonPredicate;
        }
        if (right instanceof DateTimeLiteral) {
            DateTimeLiteral dateTimeLiteral = (DateTimeLiteral) right;
            right = migrateToDateV2(dateTimeLiteral);
            if (dateTimeLiteral.getHour() != 0 || dateTimeLiteral.getMinute() != 0
                    || dateTimeLiteral.getSecond() != 0 || dateTimeLiteral.getMicroSecond() != 0) {
                if (comparisonPredicate instanceof EqualTo) {
                    return ExpressionUtils.falseOrNull(left);
                } else if (comparisonPredicate instanceof NullSafeEqual) {
                    return BooleanLiteral.FALSE;
                } else if (comparisonPredicate instanceof GreaterThanEqual
                        || comparisonPredicate instanceof LessThan) {
                    // '9999-12-31' + 1 will overflow
                    if (DateLiteral.isDateOutOfRange(right.toJavaDateType().plusDays(1))) {
                        right = convertDateLiteralToDateType(right, left.getDataType());
                        if (comparisonPredicate instanceof GreaterThanEqual) {
                            return new GreaterThan(left, right);
                        } else {
                            return new LessThanEqual(left, right);
                        }
                    }
                    right = (DateLiteral) right.plusDays(1);
                }
            }
        }
        right = convertDateLiteralToDateType(right, left.getDataType());
        if (right != comparisonPredicate.right()) {
            return comparisonPredicate.withChildren(left, right);
        }
        return comparisonPredicate;
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
                Optional<Expression> toSmallerDecimalDataTypeExpr = convertDecimalToSmallerDecimalV3Type(
                        comparisonPredicate, cast, literal);
                if (toSmallerDecimalDataTypeExpr.isPresent()) {
                    return toSmallerDecimalDataTypeExpr.get();
                }

                DecimalV3Type leftType = (DecimalV3Type) left.getDataType();
                DecimalV3Type literalType = (DecimalV3Type) literal.getDataType();
                if (cast.getDataType().isDecimalV3Type()
                        && ((DecimalV3Type) cast.getDataType()).getScale() >= leftType.getScale()
                        && leftType.getScale() < literalType.getScale()) {
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

    private static Optional<Expression> convertDecimalToSmallerDecimalV3Type(ComparisonPredicate comparisonPredicate,
            Cast castLeft, DecimalV3Literal right) {
        Expression left = castLeft.child();
        if (!castLeft.getDataType().isDecimalV3Type() || !left.getDataType().isDecimalV3Type()
                || ((DecimalV3Type) castLeft.getDataType()).getScale()
                        < ((DecimalV3Type) left.getDataType()).getScale()) {
            return Optional.empty();
        }
        DecimalV3Type leftType = (DecimalV3Type) left.getDataType();
        try {
            BigDecimal trailingZerosValue = right.getValue().stripTrailingZeros();
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
                        DecimalV3Type.createDecimalV3TypeLooseCheck(leftType.getPrecision(), leftType.getScale()),
                        trailingZerosValue);
                return Optional.of(comparisonPredicate.withChildren(left, newLiteral));
            }
        } catch (ArithmeticException e) {
            // stripTrailingZeros and setScale may cause exception if overflow
        }

        return Optional.empty();
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

    private static DateLiteral convertDateLiteralToDateType(DateLiteral l, DataType dateType) {
        if (dateType instanceof DateType) {
            if (l instanceof DateV2Literal) {
                return migrateToDate((DateV2Literal) l);
            }
        }
        if (dateType instanceof DateV2Type) {
            if (!(l instanceof DateV2Literal)) {
                return migrateToDateV2(l);
            }
        }
        return l;
    }

    private static DateTimeLiteral migrateToDateTime(DateTimeV2Literal l) {
        return new DateTimeLiteral(l.getYear(), l.getMonth(), l.getDay(), l.getHour(), l.getMinute(), l.getSecond());
    }

    private static DateV2Literal migrateToDateV2(DateLiteral l) {
        return new DateV2Literal(l.getYear(), l.getMonth(), l.getDay());
    }

    private static DateLiteral migrateToDate(DateV2Literal l) {
        return new DateLiteral(l.getYear(), l.getMonth(), l.getDay());
    }
}
