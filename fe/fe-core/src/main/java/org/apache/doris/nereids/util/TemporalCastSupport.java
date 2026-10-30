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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.TimeStampNsLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeStampTzType;

import java.util.List;
import java.util.Optional;

/** Properties of temporal conversions shared by coercion and CAST rewrites. */
public final class TemporalCastSupport {
    private TemporalCastSupport() {
    }

    /**
     * Common-type promotion to NS narrows the calendar range, including nested values.
     * Fail instead of introducing NULL into a comparison or a value-producing expression.
     * Explicit CAST and single-family function arguments keep their existing cast mode.
     */
    public static boolean requiresStrictTemporalCast(DataType sourceType, DataType targetType) {
        if (sourceType instanceof ArrayType && targetType instanceof ArrayType) {
            return requiresStrictTemporalCast(((ArrayType) sourceType).getItemType(),
                    ((ArrayType) targetType).getItemType());
        } else if (sourceType instanceof MapType && targetType instanceof MapType) {
            MapType sourceMap = (MapType) sourceType;
            MapType targetMap = (MapType) targetType;
            return requiresStrictTemporalCast(sourceMap.getKeyType(), targetMap.getKeyType())
                    || requiresStrictTemporalCast(sourceMap.getValueType(), targetMap.getValueType());
        } else if (sourceType instanceof StructType && targetType instanceof StructType) {
            List<StructField> sourceFields = ((StructType) sourceType).getFields();
            List<StructField> targetFields = ((StructType) targetType).getFields();
            // CheckCast rejects different field counts; such structs have no aligned conversion to inspect.
            if (sourceFields.size() != targetFields.size()) {
                return false;
            }
            for (int i = 0; i < sourceFields.size(); i++) {
                if (requiresStrictTemporalCast(sourceFields.get(i).getDataType(),
                        targetFields.get(i).getDataType())) {
                    return true;
                }
            }
            return false;
        }
        return sourceType.isDateLikeType() && !sourceType.isTimeStampNsType() && targetType.isTimeStampNsType();
    }

    /**
     * Prove the value-aware exception for narrowing an NS literal to another temporal type.
     * A non-literal has no such proof, even when an explicit CAST would be legal.
     */
    public static boolean canExactlyCastTemporalLiteralTo(Expression expression, DataType targetType) {
        Optional<TimeStampNsLiteral> literal = getTimeStampNsLiteral(expression);
        if (!literal.isPresent()) {
            return false;
        }
        return canExactlyCastTimeStampNsLiteralTo(literal.get(), targetType);
    }

    private static boolean canExactlyCastTimeStampNsLiteralTo(
            TimeStampNsLiteral literal, DataType targetType) {
        if (targetType.isDateType() || targetType.isDateV2Type()) {
            return literal.isMidnight();
        }
        if (targetType.isDateTimeType()) {
            return literal.getNanoSecond() == 0;
        }
        if (targetType instanceof DateTimeV2Type) {
            int scale = ((DateTimeV2Type) targetType).getScale();
            if (scale < 0) {
                scale = DateTimeV2Type.MAX_SCALE;
            }
            return isFractionExactlyRepresentable(literal.getNanoSecond(), scale);
        }
        if (targetType instanceof TimeStampTzType) {
            int scale = ((TimeStampTzType) targetType).getScale();
            if (scale < 0) {
                scale = TimeStampTzType.MAX_SCALE;
            }
            return isFractionExactlyRepresentable(literal.getNanoSecond(), scale);
        }
        return false;
    }

    private static Optional<TimeStampNsLiteral> getTimeStampNsLiteral(Expression expression) {
        Optional<Literal> literal = getLiteralAfterExplicitCast(expression);
        return literal.filter(TimeStampNsLiteral.class::isInstance)
                .map(TimeStampNsLiteral.class::cast);
    }

    private static Optional<Literal> getLiteralAfterExplicitCast(Expression expression) {
        Optional<Literal> literal = ExpressionUtils.getLiteralAfterUnwrapNullable(expression);
        if (!literal.isPresent() && expression instanceof Cast && expression.child(0) instanceof Literal) {
            try {
                Expression value = ((Literal) expression.child(0)).checkedCastTo(expression.getDataType());
                return value instanceof Literal ? Optional.of((Literal) value) : Optional.empty();
            } catch (AnalysisException e) {
                return Optional.empty();
            }
        }
        return literal;
    }

    /** Exact fractional representability; unlike CAST, this must not round the value. */
    public static boolean isFractionExactlyRepresentable(long nanos, int scale) {
        long factor = (long) Math.pow(10, TimeStampNsType.SCALE - scale);
        return nanos % factor == 0;
    }

    /** Nullable output layout required by temporal CAST kernels, independent of strict mode. */
    public static boolean hasNullableResult(DataType source, DataType target) {
        if (source.equals(target)) {
            return false;
        }
        if ((source.isDateLikeType() || source.isTimeType()) && target.isTimeStampNsType()) {
            // Nanosecond precision comes with a narrower signed epoch-nanosecond range.
            return true;
        }
        // Scale reduction may overflow at the calendar boundary; zoned conversions can fail
        // while resolving a local datetime. Preserve the kernels' nullable layout in either mode.
        return (source.isDateTimeV2Type() && target.isDateTimeV2Type())
                || (source.isTimeStampTzType() && target.isDateTimeV2Type())
                || ((source.isDateTimeV2Type() || source.isTimeStampNsType() || source.isTimeStampTzType())
                    && target.isTimeStampTzType());
    }

    /** Removing this conversion could suppress the range error of a strict implicit CAST. */
    public static boolean mustPreserveTemporalRangeCheck(DataType target) {
        return target.isTimeStampNsType();
    }

    /** Temporal conversions whose value representation is only implemented by the BE evaluator. */
    public static boolean requiresBackendEvaluation(DataType source, DataType target) {
        return source.isTimeStampNsType() && target.isVariantType();
    }
}
