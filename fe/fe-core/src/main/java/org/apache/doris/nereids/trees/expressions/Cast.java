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

import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.functions.Monotonic;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.nereids.util.DateUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;

/**
 * cast function.
 */
public class Cast extends Expression implements UnaryExpression, Monotonic {

    // CAST can be from SQL Query or Type Coercion. true for explicitly cast from SQL query.
    protected final boolean isExplicitType; //FIXME: now not useful

    // Some system-inserted casts are part of correctness-sensitive normalization and must fail
    // instead of producing NULL, independently of the session's enable_strict_cast setting.
    protected final boolean isStrict;

    protected final DataType targetType;

    public Cast(Expression child, DataType targetType) {
        this(child, targetType, false, false);
    }

    public Cast(Expression child, DataType targetType, boolean isExplicitType) {
        this(child, targetType, isExplicitType, false);
    }

    public Cast(Expression child, DataType targetType, boolean isExplicitType, boolean isStrict) {
        this(ImmutableList.of(child), targetType, isExplicitType, isStrict);
    }

    protected Cast(List<Expression> child, DataType targetType, boolean isExplicitType) {
        this(child, targetType, isExplicitType, false);
    }

    protected Cast(List<Expression> child, DataType targetType, boolean isExplicitType, boolean isStrict) {
        super(child);
        this.targetType = Objects.requireNonNull(targetType, "targetType can not be null");
        this.isExplicitType = isExplicitType;
        this.isStrict = isStrict;
    }

    public boolean isExplicitType() {
        return isExplicitType;
    }

    public boolean isStrict() {
        return isStrict;
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
        return castNullable(child().nullable(), child().getDataType(), targetType);
    }

    /**
     * process cast nullable.
     * @param srcNullable src expr is nullable if true
     * @param srcType src expr's type
     * @param targetType target type
     * @return true if result should be nullable
     */
    public static boolean castNullable(boolean srcNullable, DataType srcType, DataType targetType) {
        if (srcNullable) {
            return true;
        }
        // Identity casts are inserted while merging unchanged complex-type siblings. They cannot create NULL,
        // so test exact type equality before the conservative datetime/timestamptz conversion rules below.
        if (srcType.equals(targetType)) {
            return false;
        }
        // Not allowed cast is forbidden in CheckCast, and all the Propagation Nullable cases are handled above
        // and the default return false below.
        // The if branches below only handle 2 cases: always nullable and nullable that may overflow.
        DataType childDataType = srcType;
        // StringLike to other type is always nullable.
        if (childDataType.isStringLikeType() && !targetType.isStringLikeType()) {
            return true;
        } else if ((childDataType.isDateLikeType() || childDataType.isTimeType())
                && targetType instanceof TimeStampNsType) {
            // Temporal inputs can fail because TIMESTAMP_NS has a narrower signed epoch-nanos range.
            return true;
        } else if ((childDataType.isDateTimeType() || childDataType.isDateTimeV2Type()
                || childDataType.isTimeStampTzType())
                && (targetType.isDateTimeType() || targetType.isDateTimeV2Type())) {
            // datetime to datetime is always nullable
            return true;
        } else if ((childDataType.isDateTimeV2Type() || childDataType.isTimeStampNsType())
                && targetType.isTimeStampTzType()) {
            // Datetime and timestamp_ns to timestamptz are always nullable
            return true;
        } else if (childDataType.isTimeStampTzType() && targetType.isTimeStampTzType()) {
            // timestamptz to timestamptz is always nullable
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
        } else if (childDataType.isVariantType() && !targetType.isVariantType()) {
            // Variant values can have a shape that is incompatible with the target type.
            return true;
        }
        return false;
    }

    @Override
    public Cast withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Cast(children, targetType, isExplicitType, isStrict);
    }

    /** Return this cast with a different immutable target type. */
    public Cast withTargetType(DataType targetType) {
        return this.targetType.equals(targetType)
                ? this : new Cast(children, targetType, isExplicitType, isStrict);
    }

    @Override
    public String computeToSql() throws UnboundException {
        return "cast(" + child().toSql() + " as " + targetType.toSql() + ")";
    }

    @Override
    public String shapeInfo() {
        return "cast(" + child().shapeInfo() + " as " + targetType.toSql() + ")";
    }

    @Override
    public String toString() {
        return "cast(" + child() + " as " + targetType + ")";
    }

    @Override
    public String toDigest() {
        StringBuilder sb = new StringBuilder();
        sb.append("cast(")
                .append(child().toDigest())
                .append(" as ")
                .append(targetType)
                .append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        Cast cast = (Cast) o;
        return isStrict == cast.isStrict && Objects.equals(targetType, cast.targetType);
    }

    @Override
    public int computeHashCode() {
        return Objects.hash(super.computeHashCode(), targetType, isStrict);
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
        return new Cast(literal, targetType, isExplicitType, isStrict);
    }

    @Override
    public boolean isMonotonic(Literal lower, Literal upper) {
        DataType childType = child().getDataType();
        if (!(childType instanceof DateLikeType && targetType instanceof DateLikeType)) {
            return false;
        }

        if (targetType instanceof TimeStampNsType && !isRangeWithinTimeStampNs(lower, upper)) {
            return false;
        }

        if (childType instanceof TimeStampTzType
                && (targetType instanceof DateTimeV2Type || targetType instanceof TimeStampNsType)) {
            int destinationScale = targetType instanceof DateTimeV2Type
                    ? ((DateTimeV2Type) targetType).getScale() : TimeStampNsType.SCALE;
            return isTimeStampTzToLocalDateTimeMonotonic(
                    (TimeStampTzType) childType, destinationScale, lower, upper);
        }
        if (childType instanceof TimeStampNsType && targetType instanceof TimeStampTzType) {
            return isTimeStampNsToTimeStampTzMonotonic(
                    (TimeStampTzType) targetType, lower, upper);
        }
        return true;
    }

    private boolean isRangeWithinTimeStampNs(Literal lower, Literal upper) {
        if (lower == null || upper == null) {
            return false;
        }
        try {
            return !(lower.checkedCastTo(targetType) instanceof NullLiteral)
                    && !(upper.checkedCastTo(targetType) instanceof NullLiteral);
        } catch (AnalysisException e) {
            return false;
        }
    }

    private boolean isTimeStampTzToLocalDateTimeMonotonic(
            TimeStampTzType sourceType, int destinationScale, Literal lower, Literal upper) {
        ZoneId timeZone;
        try {
            timeZone = TimeUtils.getDorisZoneId();
        } catch (DateTimeException e) {
            return false;
        }
        if (timeZone.getRules().isFixedOffset()) {
            return true;
        }
        // Scale reduction rounds the UTC value before applying the session timezone. That rounding
        // can move values across a fall-back transition just outside the original partition range.
        if (destinationScale < sourceType.getScale()) {
            return false;
        }
        if (!(lower instanceof TimestampTzLiteral) || !(upper instanceof TimestampTzLiteral)) {
            return false;
        }

        // TimestampTzLiteral stores UTC civil fields. The cast renders those instants in the
        // session timezone, which moves backward at a fall-back transition.
        Instant lowerInstant = ((TimestampTzLiteral) lower).toJavaDateType().toInstant(ZoneOffset.UTC);
        Instant upperInstant = ((TimestampTzLiteral) upper).toJavaDateType().toInstant(ZoneOffset.UTC);
        if (upperInstant.isBefore(lowerInstant)) {
            return false;
        }
        return !DateUtils.hasFallbackTransitionInInstantRange(timeZone, lowerInstant, upperInstant);
    }

    private boolean isTimeStampNsToTimeStampTzMonotonic(
            TimeStampTzType destinationType, Literal lower, Literal upper) {
        ZoneId timeZone;
        try {
            timeZone = TimeUtils.getDorisZoneId();
        } catch (DateTimeException e) {
            return false;
        }
        if (timeZone.getRules().isFixedOffset()) {
            return true;
        }
        if (!(lower instanceof TimeStampNsLiteral) || !(upper instanceof TimeStampNsLiteral)) {
            return false;
        }
        LocalDateTime lowerDateTime = roundTimeStampNs(
                (TimeStampNsLiteral) lower, destinationType.getScale());
        LocalDateTime upperDateTime = roundTimeStampNs(
                (TimeStampNsLiteral) upper, destinationType.getScale());
        if (upperDateTime.isBefore(lowerDateTime)) {
            return false;
        }
        return !DateUtils.hasGapTransitionInLocalDateTimeRange(
                timeZone, lowerDateTime, upperDateTime);
    }

    private LocalDateTime roundTimeStampNs(TimeStampNsLiteral literal, int scale) {
        long factor = (long) Math.pow(10, DateUtils.NANOSECOND_SCALE - scale);
        LocalDateTime dateTime = literal.toJavaDateType().plusNanos(factor / 2);
        return dateTime.withNano((int) (dateTime.getNano() / factor * factor));
    }
}
