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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
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
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.coercion.DateLikeType;

import com.google.common.base.Preconditions;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * simplify comparison
 * such as: cast(c1 as DateV2) >= DateV2Literal --> c1 >= DateLiteral
 *          cast(c1 AS double) > 2.0 --> c1 >= 2 (c1 is integer like type)
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

        // float like type: float, double
        if (left.getDataType().isFloatLikeType() && right.getDataType().isFloatLikeType()) {
            return processFloatLikeTypeCoercion(cp, left, right);
        }

        // decimalv3 type
        if (left.getDataType() instanceof DecimalV3Type
                && right.getDataType() instanceof DecimalV3Type) {
            return processDecimalV3TypeCoercion(cp, left, right);
        }

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
                    if (left.nullable()) {
                        // TODO: the ideal way is to return an If expr like:
                        // return new If(new IsNull(left), new NullLiteral(BooleanType.INSTANCE),
                        // BooleanLiteral.of(false));
                        // but current fold constant rule can't handle such complex expr with null literal
                        // before supporting complex conjuncts with null literal folding rules,
                        // we use a trick way like this:
                        return new And(new IsNull(left), new NullLiteral(BooleanType.INSTANCE));
                    } else {
                        return BooleanLiteral.of(false);
                    }
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
            if (cast.child().getDataType() instanceof DateTimeV2Type) {
                if (right instanceof DateTimeV2Literal) {
                    left = cast.child();
                    return processComparisonPredicateDateTimeV2Literal(cp, left,
                            (DateTimeV2Literal) right);
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

    private Expression processFloatLikeTypeCoercion(ComparisonPredicate comparisonPredicate,
            Expression left, Expression right) {
        if (left instanceof Literal) {
            comparisonPredicate = comparisonPredicate.commute();
            Expression temp = left;
            left = right;
            right = temp;
        }

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

    private Expression processDecimalV3TypeCoercion(ComparisonPredicate comparisonPredicate,
            Expression left, Expression right) {
        if (left instanceof DecimalV3Literal) {
            comparisonPredicate = comparisonPredicate.commute();
            Expression temp = left;
            left = right;
            right = temp;
        }

        if (left instanceof Cast && right instanceof DecimalV3Literal) {
            Cast cast = (Cast) left;
            left = cast.child();
            DecimalV3Literal literal = (DecimalV3Literal) right;
            if (left.getDataType().isDecimalV3Type()) {
                if (((DecimalV3Type) left.getDataType())
                        .getScale() < ((DecimalV3Type) literal.getDataType()).getScale()) {
                    int toScale = ((DecimalV3Type) left.getDataType()).getScale();
                    if (comparisonPredicate instanceof EqualTo) {
                        try {
                            return comparisonPredicate.withChildren(left,
                                    new DecimalV3Literal((DecimalV3Type) left.getDataType(),
                                            literal.getValue().setScale(toScale)));
                        } catch (ArithmeticException e) {
                            if (left.nullable()) {
                                // TODO: the ideal way is to return an If expr like:
                                // return new If(new IsNull(left), new NullLiteral(BooleanType.INSTANCE),
                                // BooleanLiteral.of(false));
                                // but current fold constant rule can't handle such complex expr with null literal
                                // before supporting complex conjuncts with null literal folding rules,
                                // we use a trick way like this:
                                return new And(new IsNull(left),
                                        new NullLiteral(BooleanType.INSTANCE));
                            } else {
                                return BooleanLiteral.of(false);
                            }
                        }
                    } else if (comparisonPredicate instanceof NullSafeEqual) {
                        try {
                            return comparisonPredicate.withChildren(left,
                                    new DecimalV3Literal((DecimalV3Type) left.getDataType(),
                                            literal.getValue().setScale(toScale)));
                        } catch (ArithmeticException e) {
                            return BooleanLiteral.of(false);
                        }
                    } else if (comparisonPredicate instanceof GreaterThan
                            || comparisonPredicate instanceof LessThanEqual) {
                        return comparisonPredicate.withChildren(left, literal.roundFloor(toScale));
                    } else if (comparisonPredicate instanceof LessThan
                            || comparisonPredicate instanceof GreaterThanEqual) {
                        return comparisonPredicate.withChildren(left,
                                literal.roundCeiling(toScale));
                    }
                }
            } else if (left.getDataType().isIntegerLikeType()) {
                return processIntegerDecimalLiteralComparison(comparisonPredicate, left,
                        literal.getValue());
            }
        }

        return comparisonPredicate;
    }

    private Expression processIntegerDecimalLiteralComparison(
            ComparisonPredicate comparisonPredicate, Expression left, BigDecimal literal) {
        // we only process isIntegerLikeType, which are tinyint, smallint, int, bigint
        if (literal.compareTo(new BigDecimal(Long.MAX_VALUE)) <= 0) {
            if (literal.scale() > 0) {
                if (comparisonPredicate instanceof EqualTo) {
                    if (left.nullable()) {
                        // TODO: the ideal way is to return an If expr like:
                        // return new If(new IsNull(left), new NullLiteral(BooleanType.INSTANCE),
                        // BooleanLiteral.of(false));
                        // but current fold constant rule can't handle such complex expr with null literal
                        // before supporting complex conjuncts with null literal folding rules,
                        // we use a trick way like this:
                        return new And(new IsNull(left), new NullLiteral(BooleanType.INSTANCE));
                    } else {
                        return BooleanLiteral.of(false);
                    }
                } else if (comparisonPredicate instanceof NullSafeEqual) {
                    return BooleanLiteral.of(false);
                } else if (comparisonPredicate instanceof GreaterThan
                        || comparisonPredicate instanceof LessThanEqual) {
                    return comparisonPredicate.withChildren(left,
                            convertDecimalToIntegerLikeLiteral(
                                    literal.setScale(0, RoundingMode.FLOOR)));
                } else if (comparisonPredicate instanceof LessThan
                        || comparisonPredicate instanceof GreaterThanEqual) {
                    return comparisonPredicate.withChildren(left,
                            convertDecimalToIntegerLikeLiteral(
                                    literal.setScale(0, RoundingMode.CEILING)));
                }
            } else {
                return comparisonPredicate.withChildren(left,
                        convertDecimalToIntegerLikeLiteral(literal));
            }
        }
        return comparisonPredicate;
    }

    private IntegerLikeLiteral convertDecimalToIntegerLikeLiteral(BigDecimal decimal) {
        Preconditions.checkArgument(
                decimal.scale() == 0 && decimal.compareTo(new BigDecimal(Long.MAX_VALUE)) <= 0,
                "decimal literal must have 0 scale and smaller than Long.MAX_VALUE");
        long val = decimal.longValue();
        if (val <= Byte.MAX_VALUE) {
            return new TinyIntLiteral((byte) val);
        } else if (val <= Short.MAX_VALUE) {
            return new SmallIntLiteral((short) val);
        } else if (val <= Integer.MAX_VALUE) {
            return new IntegerLiteral((int) val);
        } else {
            return new BigIntLiteral(val);
        }
    }

    private Expression migrateCastToDateTime(Cast cast) {
        //cast( cast(v as date) as datetime) if v is datetime, set left = v
        if (cast.child() instanceof Cast
                && cast.child().child(0).getDataType() instanceof DateTimeType) {
            return cast.child().child(0);
        } else {
            return new Cast(cast.child(), DateTimeType.INSTANCE);
        }
    }

    /*
    derive tree:
    DateLiteral
      |
      +--->DateTimeLiteral
      |        |
      |        +----->DateTimeV2Literal
      +--->DateV2Literal
    */
    private Expression migrateLiteralToDateTime(DateLiteral l) {
        if (l instanceof DateV2Literal) {
            return new DateTimeLiteral(l.getYear(), l.getMonth(), l.getDay(), 0, 0, 0);
        } else if (l instanceof DateTimeV2Literal) {
            DateTimeV2Literal dtv2 = (DateTimeV2Literal) l;
            return new DateTimeLiteral(dtv2.getYear(), dtv2.getMonth(), dtv2.getDay(),
                    dtv2.getHour(), dtv2.getMinute(), dtv2.getSecond());
        } else if (l instanceof DateTimeLiteral) {
            return l;
        } else if (l instanceof DateLiteral) {
            return new DateTimeLiteral(l.getYear(), l.getMonth(), l.getDay(), 0, 0, 0);
        }
        throw new AnalysisException("cannot convert" + l.toSql() + " to DateTime");
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
            d = ((DateV2Literal) d.plusDays(1));
        }
        return d;
    }

    private Expression migrateToDate(DateV2Literal l) {
        return new DateLiteral(l.getYear(), l.getMonth(), l.getDay());
    }
}
