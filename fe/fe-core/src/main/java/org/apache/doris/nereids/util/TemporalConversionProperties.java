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

/**
 * Properties of a temporal conversion with a given source and target: calendar range,
 * literal exactness and nullable output layout. Shared by implicit coercion and explicit CAST.
 *
 * <p>This class does not choose target types, admit function overloads or select a cast mode.
 * {@link TypeCoercionUtils} chooses common types, applies implicit conversion and constructs casts.
 * Rewrite rules also consume these properties.
 * Explicit CAST legality remains in CheckCast, and
 * timezone value conversion is implemented by {@link DateUtils}.
 */
public final class TemporalConversionProperties {
    private TemporalConversionProperties() {
    }

    /**
     * Whether a date-like source is converted to the narrower TIMESTAMP_NS calendar range,
     * including aligned ARRAY, MAP and STRUCT components. This describes the type ranges,
     * not whether a particular value overflows or which cast mode the caller should use.
     */
    public static boolean narrowsCalendarRangeToTimeStampNs(DataType sourceType, DataType targetType) {
        if (sourceType instanceof ArrayType && targetType instanceof ArrayType) {
            return narrowsCalendarRangeToTimeStampNs(((ArrayType) sourceType).getItemType(),
                    ((ArrayType) targetType).getItemType());
        } else if (sourceType instanceof MapType && targetType instanceof MapType) {
            MapType sourceMap = (MapType) sourceType;
            MapType targetMap = (MapType) targetType;
            return narrowsCalendarRangeToTimeStampNs(sourceMap.getKeyType(), targetMap.getKeyType())
                    || narrowsCalendarRangeToTimeStampNs(sourceMap.getValueType(), targetMap.getValueType());
        } else if (sourceType instanceof StructType && targetType instanceof StructType) {
            List<StructField> sourceFields = ((StructType) sourceType).getFields();
            List<StructField> targetFields = ((StructType) targetType).getFields();
            // CheckCast rejects different field counts; such structs have no aligned conversion to inspect.
            if (sourceFields.size() != targetFields.size()) {
                return false;
            }
            for (int i = 0; i < sourceFields.size(); i++) {
                if (narrowsCalendarRangeToTimeStampNs(sourceFields.get(i).getDataType(),
                        targetFields.get(i).getDataType())) {
                    return true;
                }
            }
            return false;
        }
        return sourceType.isDateLikeType() && !sourceType.isTimeStampNsType() && targetType.isTimeStampNsType();
    }

    /**
     * Whether an already evaluated NS value is exactly representable by another temporal type.
     * Callers must evaluate any enclosing CAST before checking the result: a loss of precision
     * explicitly requested by that CAST is distinct from the subsequent implicit conversion.
     * This property neither evaluates expressions nor determines explicit CAST legality.
     */
    public static boolean isTimeStampNsLiteralExactlyRepresentable(
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

    /** Exact fractional representability; unlike CAST, this must not round the value. */
    private static boolean isFractionExactlyRepresentable(long nanos, int scale) {
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
}
