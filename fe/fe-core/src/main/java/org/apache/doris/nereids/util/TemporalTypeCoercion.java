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

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.format.DateTimeChecker;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.TimeStampNsType;
import org.apache.doris.nereids.types.TimeV2Type;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.NumericType;

import java.util.List;
import java.util.Optional;

/** Temporal promotion and overload policies. Explicit CAST legality remains in CheckCast. */
public final class TemporalTypeCoercion {
    private TemporalTypeCoercion() {
    }

    /** Whether a type tree contains a temporal peer with a calendar range different from NS. */
    public static boolean hasOtherDateLikeType(DataType dataType) {
        return TypeCoercionUtils.hasSpecifiedType(dataType, TemporalTypeCoercion::isOtherDateLikeType);
    }

    /** Whether function arguments require the mixed-temporal overflow policy, including nested types. */
    public static boolean containsMixedTimeStampNsDateLikeTypes(List<DataType> dataTypes) {
        return dataTypes.stream().anyMatch(TypeCoercionUtils::hasTimeStampNsType)
                && dataTypes.stream().anyMatch(TemporalTypeCoercion::hasOtherDateLikeType);
    }

    /** Choose the common type when one operand is TIMESTAMP_NS. */
    public static Optional<DataType> getCommonDataTypeWithTimeStampNsType(DataType otherType) {
        if (otherType instanceof TimeStampNsType) {
            return Optional.of(TimeStampNsType.INSTANCE);
        }
        if (isOtherDateLikeType(otherType)) {
            return Optional.of(TimeStampNsType.INSTANCE);
        }
        if (otherType.isNumericType() || otherType instanceof TimeV2Type
                || otherType.isStringLikeType()) {
            return Optional.of(TimeStampNsType.INSTANCE);
        }
        return Optional.empty();
    }

    /** Whether a comparison must preserve NS precision and check the temporal peer for overflow. */
    public static boolean isTimeStampNsAndDateLikePair(DataType left, DataType right) {
        return (left instanceof TimeStampNsType && right.isDateLikeType()
                && !(right instanceof TimeStampNsType))
                || (right instanceof TimeStampNsType && left.isDateLikeType()
                && !(left instanceof TimeStampNsType));
    }

    /** A date-like type other than the fixed-range nanosecond representation. */
    public static boolean isOtherDateLikeType(DataType dataType) {
        return dataType.isDateLikeType() && !dataType.isTimeStampNsType();
    }

    /** Recognize an all-temporal NS group before order-sensitive legacy common-type reduction. */
    public static boolean containsTimeStampNsAndOnlyCompatibleTemporalTypes(List<DataType> dataTypes) {
        boolean containsTimeStampNs = false;
        for (DataType dataType : dataTypes) {
            if (dataType instanceof TimeStampNsType) {
                containsTimeStampNs = true;
            } else if (!isOtherDateLikeType(dataType)
                    && !dataType.isNullType() && !(dataType instanceof TimeV2Type)) {
                return false;
            }
        }
        return containsTimeStampNs;
    }

    /** Called only for a conversion involving TIMESTAMP_NS; rejection is final. */
    public static Optional<DataType> implicitCastPrimitive(DataType input, DataType expected) {
        if (input.equals(expected) || expected instanceof AnyDataType) {
            return Optional.of(input);
        } else if (input instanceof NullType) {
            return Optional.of(expected.defaultConcreteType());
        } else if (expected instanceof TimeStampNsType
                && (input instanceof NumericType || input.isDateLikeType()
                        || input instanceof TimeV2Type || input instanceof CharacterType)) {
            return Optional.of(expected);
        } else if (input instanceof TimeStampNsType
                && (expected instanceof FloatType || expected instanceof DoubleType)) {
            return Optional.of(expected);
        } else if (input instanceof TimeStampNsType && expected instanceof CharacterType) {
            return Optional.of(expected.defaultConcreteType());
        }
        return Optional.empty();
    }

    /** Apply the same value-aware exception used when admitting a temporal function overload. */
    public static Optional<DataType> implicitCastArgument(Expression input, DataType expectedType) {
        Optional<DataType> castType = TypeCoercionUtils.implicitCast(input.getDataType(), expectedType);
        if (!castType.isPresent() && input.getDataType().isTimeStampNsType() && expectedType.isDateLikeType()
                && TemporalCastSupport.canExactlyCastTemporalLiteralTo(input, expectedType)) {
            return Optional.of(expectedType);
        }
        return castType;
    }

    /** Preserve the diagnostic of mixed temporal indexed-ANY arguments. */
    public static void checkIndexedAnyCommonType(List<DataType> types) {
        if (types.stream().anyMatch(DataType::isTimeStampNsType)
                && types.stream().anyMatch(TemporalTypeCoercion::isOtherDateLikeType)) {
            throw new AnalysisException("Cannot find an exact common type for indexed ANY arguments");
        }
    }

    /** Per-search policy. Candidate ordering and matching rounds remain in SearchSignature. */
    public static final class SignaturePolicy {
        private final boolean hasTimeStampNsArgument;
        private final boolean hasOtherDateLikeArgument;
        private final boolean hasHomogeneousDateLikeSignature;

        /** Capture only per-search type facts; no value or session-dependent results are cached. */
        public SignaturePolicy(List<FunctionSignature> signatures, List<Expression> arguments) {
            this.hasTimeStampNsArgument = arguments.stream()
                    .anyMatch(argument -> argument.getDataType().isTimeStampNsType());
            this.hasOtherDateLikeArgument = arguments.stream()
                    .anyMatch(argument -> argument.getDataType().isDateLikeType()
                            && !argument.getDataType().isTimeStampNsType());
            this.hasHomogeneousDateLikeSignature = signatures.stream()
                    .anyMatch(SignaturePolicy::hasHomogeneousDateLikeArguments);
        }

        /** Apply temporal restrictions before the generic signature matching predicate. */
        public boolean accepts(Expression argument, DataType sigArgType) {
            DataType realType = argument.getDataType();
            if (hasTimeStampNsArgument && hasOtherDateLikeArgument && hasHomogeneousDateLikeSignature
                    && realType.isDateLikeType() && !sigArgType.isDateLikeType()) {
                // Do not bypass temporal exactness checks through a generic string overload.
                return false;
            }
            if (hasTimeStampNsArgument && hasOtherDateLikeArgument && hasHomogeneousDateLikeSignature
                    && realType.isDateLikeType()
                    && sigArgType.isDateLikeType() && !sigArgType.isTimeStampNsType()) {
                // Mixed temporal calls use TIMESTAMP_NS without truncation or nullable overflow.
                return false;
            }
            if (sigArgType.isTimeStampNsType() && !hasTimeStampNsArgument) {
                // TIMESTAMP_NS overloads preserve a typed nanosecond argument. They must not
                // change the historical binding of character or other temporal input.
                return false;
            }
            if (realType.isTimeStampNsType() && sigArgType.isDateLikeType()
                    && !sigArgType.isTimeStampNsType()
                    && !TemporalCastSupport.canExactlyCastTemporalLiteralTo(argument, sigArgType)) {
                return false;
            }
            return true;
        }

        /** Prefer a zoned overload for a string with an explicit timezone; retain historical scores. */
        public int literalPreference(DataType targetType, String value) {
            if (!targetType.isTimeStampTzType()) {
                return 0;
            }
            return DateTimeChecker.hasTimeZone(value) ? 1 : -1;
        }

        private static boolean hasHomogeneousDateLikeArguments(FunctionSignature signature) {
            Optional<DataType> temporalType = signature.argumentsTypes.stream()
                    .filter(DataType::isDateLikeType).findFirst();
            if (!temporalType.isPresent()) {
                return signature.getVarArgType().filter(DataType::isDateLikeType).isPresent();
            }
            // Temporal overloads share one argument type. STRUCT fields and MAP keys/values
            // can instead have independent types, e.g. struct(DATEV2, TIMESTAMP_NS).
            return signature.argumentsTypes.stream().filter(DataType::isDateLikeType)
                    .allMatch(temporalType.get()::equals)
                    && signature.getVarArgType().filter(DataType::isDateLikeType)
                            .map(temporalType.get()::equals).orElse(true);
        }
    }
}
