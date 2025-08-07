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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.functions.Monotonic;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * cast function.
 */
public class Cast extends Expression implements UnaryExpression, Monotonic {

    // CAST can be from SQL Query or Type Coercion.
    private final boolean isExplicitType;

    private final DataType targetType;

    public Cast(Expression child, DataType targetType, boolean isExplicitType) {
        super(ImmutableList.of(child));
        this.targetType = Objects.requireNonNull(targetType, "targetType can not be null");
        this.isExplicitType = isExplicitType;
    }

    public Cast(Expression child, DataType targetType) {
        super(ImmutableList.of(child));
        this.targetType = Objects.requireNonNull(targetType, "targetType can not be null");
        this.isExplicitType = false;
    }

    private Cast(List<Expression> child, DataType targetType, boolean isExplicitType) {
        super(child);
        this.targetType = Objects.requireNonNull(targetType, "targetType can not be null");
        this.isExplicitType = isExplicitType;
    }

    public boolean isExplicitType() {
        return isExplicitType;
    }

    @Override
    public DataType getDataType() {
        return targetType;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCast(this, context);
    }

    @Override
    public boolean nullable() {
        if (SessionVariable.enableStrictCast()) {
            DataType childDataType = child().getDataType();
            if (childDataType.isJsonType() && !targetType.isJsonType()) {
                return true;
            }
            return child().nullable();
        } else {
            return unStrictCastNullable();
        }
    }

    protected boolean unStrictCastNullable() {
        if (child().nullable()) {
            return true;
        }
        // Not allowed cast is forbidden in CheckCast, and all the Propagation Nullable cases are handled above
        // and the default return false below.
        // The if branches below only handle 2 cases: always nullable and nullable that may overflow.
        DataType childDataType = child().getDataType();
        // StringLike to other type is always nullable.
        if (childDataType.isStringLikeType() && !targetType.isStringLikeType()) {
            return true;
        } else if ((childDataType.isDateTimeType() || childDataType.isDateTimeV2Type())
                && (targetType.isDateTimeType() || targetType.isDateTimeV2Type())) {
            // datetime to datetime is always nullable
            return true;
        } else if (childDataType.isTimeType()) {
            // time to tinyint, smallint, int and time is always nullable.
            return targetType.isTinyIntType() || targetType.isSmallIntType() || targetType.isIntegerType()
                    || targetType.isTimeType();
        } else if (childDataType.isIntegralType()) {
            // integral to integral
            if (targetType.isIntegralType()) {
                if (childDataType.isLargeIntType() && !targetType.isLargeIntType()) {
                    return true;
                }
                if (childDataType.isBigIntType() && !targetType.isLargeIntType() && !targetType.isBigIntType()) {
                    return true;
                }
                if (childDataType.isIntegerType() && (targetType.isSmallIntType() || targetType.isTinyIntType())) {
                    return true;
                }
                return childDataType.isSmallIntType() && targetType.isTinyIntType();
            } else if (targetType.isDecimalLikeType()) {
                // Integral to decimal
                int range = targetType.isDecimalV2Type() ? ((DecimalV2Type) targetType).getRange()
                        : ((DecimalV3Type) targetType).getRange();
                if (childDataType.isTinyIntType() && range < TinyIntType.RANGE) {
                    return true;
                } else if (childDataType.isSmallIntType() && range < SmallIntType.RANGE) {
                    return true;
                } else if (childDataType.isIntegerType() && range < IntegerType.RANGE) {
                    return true;
                } else if (childDataType.isBigIntType() && range < BigIntType.RANGE) {
                    return true;
                } else {
                    return childDataType.isLargeIntType() && range < LargeIntType.RANGE;
                }
            } else if (targetType.isDateLikeType() || targetType.isTimeType()) {
                // integral to date like and time is always nullable.
                return true;
            }
        } else if (childDataType.isFloatType() || childDataType.isDoubleType()) {
            // Double/Float to integral, decimal, date like and time are always nullable.
            return targetType.isIntegralType() || targetType.isDecimalLikeType()
                    || targetType.isDateLikeType() || targetType.isTimeType();
        } else if (childDataType.isDecimalLikeType()) {
            // Decimal to integral
            if (targetType.isIntegralType()) {
                int range = 0;
                if (childDataType.isDecimalV2Type()) {
                    range = ((DecimalV2Type) childDataType).getRange();
                } else {
                    range = ((DecimalV3Type) childDataType).getRange();
                }
                if (range >= LargeIntType.RANGE) {
                    return true;
                }
                if (targetType.isTinyIntType() && range >= TinyIntType.RANGE) {
                    return true;
                }
                if (targetType.isSmallIntType() && range >= SmallIntType.RANGE) {
                    return true;
                }
                if (targetType.isIntegerType() && range >= IntegerType.RANGE) {
                    return true;
                }
                return targetType.isBigIntType() && range >= BigIntType.RANGE;
            } else if (targetType.isDecimalLikeType()) {
                // Decimal to decimal
                int targetRange = targetType.isDecimalV2Type() ? ((DecimalV2Type) targetType).getRange()
                        : ((DecimalV3Type) targetType).getRange();
                int sourceRange = childDataType.isDecimalV2Type() ? ((DecimalV2Type) childDataType).getRange()
                        : ((DecimalV3Type) childDataType).getRange();
                if (sourceRange > targetRange) {
                    return true;
                }
                if (sourceRange < targetRange) {
                    return false;
                }
                // When source range == target range, if source precision is larger than target precision,
                // it is possible to be null when fraction part overflow.
                // e.g. decimal(3, 2) to decimal(2, 1), 9.99 to decimal(2, 1) overflow, result is null.
                int targetPrecision = targetType.isDecimalV2Type() ? ((DecimalV2Type) targetType).getPrecision()
                        : ((DecimalV3Type) targetType).getPrecision();
                int sourcePrecision = childDataType.isDecimalV2Type() ? ((DecimalV2Type) childDataType).getPrecision()
                        : ((DecimalV3Type) childDataType).getPrecision();
                return sourcePrecision > targetPrecision;
            } else if (targetType.isTimeType() || targetType.isDateLikeType()) {
                //Decimal to date like and time are always nullable.
                return true;
            }
        } else if (childDataType.isBooleanType() && targetType.isDecimalLikeType()) {
            // Boolean to decimal
            return (targetType.isDecimalV2Type() ? ((DecimalV2Type) targetType).getRange()
                    : ((DecimalV3Type) targetType).getRange()) < 1;
        } else if (childDataType.isJsonType() && !targetType.isJsonType()) {
            // Json to other type is always nullable
            return true;
        }
        return false;
    }

    @Override
    public Cast withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Cast(children, targetType, isExplicitType);
    }

    @Override
    public String computeToSql() throws UnboundException {
        return "cast(" + child().toSql() + " as " + targetType.toSql() + ")";
    }

    @Override
    public String toString() {
        return "cast(" + child() + " as " + targetType + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        Cast cast = (Cast) o;
        return Objects.equals(targetType, cast.targetType);
    }

    @Override
    public int computeHashCode() {
        return Objects.hash(super.computeHashCode(), targetType);
    }

    @Override
    public boolean isPositive() {
        return true;
    }

    @Override
    public int getMonotonicFunctionChildIndex() {
        return 0;
    }

    @Override
    public Expression withConstantArgs(Expression literal) {
        return new Cast(literal, targetType, isExplicitType);
    }

    @Override
    public boolean isMonotonic(Literal lower, Literal upper) {
        // Both upward and downward casting of date types satisfy monotonicity.
        if (child().getDataType() instanceof DateLikeType && targetType instanceof DateLikeType) {
            return true;
        }
        return false;
    }
}
