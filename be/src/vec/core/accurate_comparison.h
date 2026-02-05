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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/AccurateComparison.h
// and modified by Doris

#pragma once

#include "common/compare.h"
#include "runtime/primitive_type.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
namespace doris::vectorized {

template <PrimitiveType PT>
struct CompareType {
    using NativeType = typename PrimitiveTypeTraits<PT>::CppType;
};

template <>
struct CompareType<TYPE_DECIMAL32> {
    using NativeType = typename PrimitiveTypeTraits<TYPE_DECIMAL32>::CppType::NativeType;
};
template <>
struct CompareType<TYPE_DECIMAL64> {
    using NativeType = typename PrimitiveTypeTraits<TYPE_DECIMAL64>::CppType::NativeType;
};
template <>
struct CompareType<TYPE_DECIMAL128I> {
    using NativeType = typename PrimitiveTypeTraits<TYPE_DECIMAL128I>::CppType::NativeType;
};
template <>
struct CompareType<TYPE_DECIMALV2> {
    using NativeType = typename PrimitiveTypeTraits<TYPE_DECIMALV2>::CppType::NativeType;
};
template <>
struct CompareType<TYPE_DECIMAL256> {
    using NativeType = typename PrimitiveTypeTraits<TYPE_DECIMAL256>::CppType::NativeType;
};

template <PrimitiveType PT>
struct EqualsOp {
    /// An operation that gives the same result, if arguments are passed in reverse order.
    using SymmetricOp = EqualsOp<PT>;
    using NativeType = typename CompareType<PT>::NativeType;

    static UInt8 apply(NativeType a, NativeType b) { return Compare::equal(a, b); }
};

template <>
struct EqualsOp<TYPE_DECIMALV2> {
    static UInt8 apply(const Int128& a, const Int128& b) { return a == b; }
};

template <>
struct EqualsOp<TYPE_DATE> {
    using SymmetricOp = EqualsOp<TYPE_DATE>;
    using NativeType = typename CompareType<TYPE_DATE>::NativeType;
    static UInt8 apply(const VecDateTimeValue& a, const VecDateTimeValue& b) { return a == b; }
};

template <>
struct EqualsOp<TYPE_DATETIME> {
    using SymmetricOp = EqualsOp<TYPE_DATETIME>;
    using NativeType = typename CompareType<TYPE_DATETIME>::NativeType;
    static UInt8 apply(const VecDateTimeValue& a, const VecDateTimeValue& b) { return a == b; }
};

template <>
struct EqualsOp<TYPE_DATEV2> {
    using SymmetricOp = EqualsOp<TYPE_DATEV2>;
    using NativeType = typename CompareType<TYPE_DATEV2>::NativeType;
    static UInt8 apply(const DateV2Value<DateV2ValueType>& a,
                       const DateV2Value<DateV2ValueType>& b) {
        return a == b;
    }
};

template <>
struct EqualsOp<TYPE_DATETIMEV2> {
    using SymmetricOp = EqualsOp<TYPE_DATETIMEV2>;
    using NativeType = typename CompareType<TYPE_DATETIMEV2>::NativeType;
    static UInt8 apply(const DateV2Value<DateTimeV2ValueType>& a,
                       const DateV2Value<DateTimeV2ValueType>& b) {
        return a == b;
    }
};

template <>
struct EqualsOp<TYPE_TIMESTAMPTZ> {
    using SymmetricOp = EqualsOp<TYPE_TIMESTAMPTZ>;
    using NativeType = typename CompareType<TYPE_TIMESTAMPTZ>::NativeType;
    static UInt8 apply(const TimestampTzValue& a, const TimestampTzValue& b) { return a == b; }
};

template <>
struct EqualsOp<TYPE_STRING> {
    static UInt8 apply(const StringRef& a, const StringRef& b) { return a == b; }
};

template <PrimitiveType PT>
struct NotEqualsOp {
    using SymmetricOp = NotEqualsOp<PT>;
    using NativeType = typename CompareType<PT>::NativeType;
    static UInt8 apply(NativeType a, NativeType b) { return Compare::not_equal(a, b); }
};

template <>
struct NotEqualsOp<TYPE_DECIMALV2> {
    static UInt8 apply(const DecimalV2Value& a, const DecimalV2Value& b) { return a != b; }
};

template <>
struct NotEqualsOp<TYPE_DATE> {
    using SymmetricOp = NotEqualsOp<TYPE_DATE>;
    using NativeType = typename CompareType<TYPE_DATE>::NativeType;
    static UInt8 apply(const VecDateTimeValue& a, const VecDateTimeValue& b) { return a != b; }
};

template <>
struct NotEqualsOp<TYPE_DATETIME> {
    using SymmetricOp = NotEqualsOp<TYPE_DATETIME>;
    using NativeType = typename CompareType<TYPE_DATETIME>::NativeType;
    static UInt8 apply(const VecDateTimeValue& a, const VecDateTimeValue& b) { return a != b; }
};

template <>
struct NotEqualsOp<TYPE_DATEV2> {
    using SymmetricOp = NotEqualsOp<TYPE_DATEV2>;
    using NativeType = typename CompareType<TYPE_DATEV2>::NativeType;
    static UInt8 apply(const DateV2Value<DateV2ValueType>& a,
                       const DateV2Value<DateV2ValueType>& b) {
        return a != b;
    }
};

template <>
struct NotEqualsOp<TYPE_DATETIMEV2> {
    using SymmetricOp = NotEqualsOp<TYPE_DATETIMEV2>;
    using NativeType = typename CompareType<TYPE_DATETIMEV2>::NativeType;
    static UInt8 apply(const DateV2Value<DateTimeV2ValueType>& a,
                       const DateV2Value<DateTimeV2ValueType>& b) {
        return a != b;
    }
};

template <>
struct NotEqualsOp<TYPE_TIMESTAMPTZ> {
    using SymmetricOp = NotEqualsOp<TYPE_TIMESTAMPTZ>;
    using NativeType = typename CompareType<TYPE_TIMESTAMPTZ>::NativeType;
    static UInt8 apply(const TimestampTzValue& a, const TimestampTzValue& b) { return a != b; }
};

template <PrimitiveType PT>
struct GreaterOp;

template <PrimitiveType PT>
struct LessOp {
    using SymmetricOp = GreaterOp<PT>;
    using NativeType = typename CompareType<PT>::NativeType;
    static UInt8 apply(NativeType a, NativeType b) { return Compare::less(a, b); }
};

template <>
struct LessOp<TYPE_DECIMALV2> {
    static UInt8 apply(Int128 a, Int128 b) { return a < b; }
};

template <>
struct LessOp<TYPE_DATE> {
    using SymmetricOp = GreaterOp<TYPE_DATE>;
    using NativeType = typename CompareType<TYPE_DATE>::NativeType;
    static UInt8 apply(const VecDateTimeValue& a, const VecDateTimeValue& b) { return a < b; }
};

template <>
struct LessOp<TYPE_DATETIME> {
    using SymmetricOp = GreaterOp<TYPE_DATETIME>;
    using NativeType = typename CompareType<TYPE_DATETIME>::NativeType;
    static UInt8 apply(const VecDateTimeValue& a, const VecDateTimeValue& b) { return a < b; }
};

template <>
struct LessOp<TYPE_DATEV2> {
    using SymmetricOp = GreaterOp<TYPE_DATEV2>;
    using NativeType = typename CompareType<TYPE_DATEV2>::NativeType;
    static UInt8 apply(const DateV2Value<DateV2ValueType>& a,
                       const DateV2Value<DateV2ValueType>& b) {
        return a < b;
    }
};

template <>
struct LessOp<TYPE_DATETIMEV2> {
    using SymmetricOp = GreaterOp<TYPE_DATETIMEV2>;
    using NativeType = typename CompareType<TYPE_DATETIMEV2>::NativeType;
    static UInt8 apply(const DateV2Value<DateTimeV2ValueType>& a,
                       const DateV2Value<DateTimeV2ValueType>& b) {
        return a < b;
    }
};

template <>
struct LessOp<TYPE_TIMESTAMPTZ> {
    using SymmetricOp = GreaterOp<TYPE_TIMESTAMPTZ>;
    using NativeType = typename CompareType<TYPE_TIMESTAMPTZ>::NativeType;
    static UInt8 apply(const TimestampTzValue& a, const TimestampTzValue& b) { return a < b; }
};

template <>
struct LessOp<TYPE_STRING> {
    static UInt8 apply(StringRef a, StringRef b) { return a < b; }
};

template <PrimitiveType PT>
struct GreaterOp {
    using SymmetricOp = LessOp<PT>;
    using NativeType = typename CompareType<PT>::NativeType;
    static UInt8 apply(NativeType a, NativeType b) { return Compare::greater(a, b); }
};

template <>
struct GreaterOp<TYPE_DECIMALV2> {
    static UInt8 apply(Int128 a, Int128 b) { return a > b; }
};

template <>
struct GreaterOp<TYPE_DATE> {
    using SymmetricOp = LessOp<TYPE_DATE>;
    using NativeType = typename CompareType<TYPE_DATE>::NativeType;
    static UInt8 apply(const VecDateTimeValue& a, const VecDateTimeValue& b) { return a > b; }
};

template <>
struct GreaterOp<TYPE_DATETIME> {
    using SymmetricOp = LessOp<TYPE_DATETIME>;
    using NativeType = typename CompareType<TYPE_DATETIME>::NativeType;
    static UInt8 apply(const VecDateTimeValue& a, const VecDateTimeValue& b) { return a > b; }
};

template <>
struct GreaterOp<TYPE_DATEV2> {
    using SymmetricOp = LessOp<TYPE_DATEV2>;
    using NativeType = typename CompareType<TYPE_DATEV2>::NativeType;
    static UInt8 apply(const DateV2Value<DateV2ValueType>& a,
                       const DateV2Value<DateV2ValueType>& b) {
        return a > b;
    }
};

template <>
struct GreaterOp<TYPE_DATETIMEV2> {
    using SymmetricOp = LessOp<TYPE_DATETIMEV2>;
    using NativeType = typename CompareType<TYPE_DATETIMEV2>::NativeType;
    static UInt8 apply(const DateV2Value<DateTimeV2ValueType>& a,
                       const DateV2Value<DateTimeV2ValueType>& b) {
        return a > b;
    }
};

template <>
struct GreaterOp<TYPE_TIMESTAMPTZ> {
    using SymmetricOp = LessOp<TYPE_TIMESTAMPTZ>;
    using NativeType = typename CompareType<TYPE_TIMESTAMPTZ>::NativeType;
    static UInt8 apply(const TimestampTzValue& a, const TimestampTzValue& b) { return a > b; }
};

template <>
struct GreaterOp<TYPE_STRING> {
    static UInt8 apply(StringRef a, StringRef b) { return a > b; }
};

template <PrimitiveType PT>
struct GreaterOrEqualsOp;

template <PrimitiveType PT>
struct LessOrEqualsOp {
    using SymmetricOp = GreaterOrEqualsOp<PT>;
    using NativeType = typename CompareType<PT>::NativeType;
    static UInt8 apply(NativeType a, NativeType b) { return Compare::less_equal(a, b); }
};

template <>
struct LessOrEqualsOp<TYPE_DECIMALV2> {
    static UInt8 apply(DecimalV2Value a, DecimalV2Value b) { return a <= b; }
};

template <>
struct LessOrEqualsOp<TYPE_DATE> {
    using SymmetricOp = GreaterOrEqualsOp<TYPE_DATE>;
    using NativeType = typename CompareType<TYPE_DATE>::NativeType;
    static UInt8 apply(const VecDateTimeValue& a, const VecDateTimeValue& b) { return a <= b; }
};

template <>
struct LessOrEqualsOp<TYPE_DATETIME> {
    using SymmetricOp = GreaterOrEqualsOp<TYPE_DATETIME>;
    using NativeType = typename CompareType<TYPE_DATETIME>::NativeType;
    static UInt8 apply(const VecDateTimeValue& a, const VecDateTimeValue& b) { return a <= b; }
};

template <>
struct LessOrEqualsOp<TYPE_DATEV2> {
    using SymmetricOp = GreaterOrEqualsOp<TYPE_DATEV2>;
    using NativeType = typename CompareType<TYPE_DATEV2>::NativeType;
    static UInt8 apply(const DateV2Value<DateV2ValueType>& a,
                       const DateV2Value<DateV2ValueType>& b) {
        return a <= b;
    }
};

template <>
struct LessOrEqualsOp<TYPE_DATETIMEV2> {
    using SymmetricOp = GreaterOrEqualsOp<TYPE_DATETIMEV2>;
    using NativeType = typename CompareType<TYPE_DATETIMEV2>::NativeType;
    static UInt8 apply(const DateV2Value<DateTimeV2ValueType>& a,
                       const DateV2Value<DateTimeV2ValueType>& b) {
        return a <= b;
    }
};

template <>
struct LessOrEqualsOp<TYPE_TIMESTAMPTZ> {
    using SymmetricOp = GreaterOrEqualsOp<TYPE_TIMESTAMPTZ>;
    using NativeType = typename CompareType<TYPE_TIMESTAMPTZ>::NativeType;
    static UInt8 apply(const TimestampTzValue& a, const TimestampTzValue& b) { return a <= b; }
};

template <PrimitiveType PT>
struct GreaterOrEqualsOp {
    using SymmetricOp = LessOrEqualsOp<PT>;
    using NativeType = typename CompareType<PT>::NativeType;
    static UInt8 apply(NativeType a, NativeType b) { return Compare::greater_equal(a, b); }
};

template <>
struct GreaterOrEqualsOp<TYPE_DECIMALV2> {
    static UInt8 apply(DecimalV2Value a, DecimalV2Value b) { return a >= b; }
};

template <>
struct GreaterOrEqualsOp<TYPE_DATE> {
    using SymmetricOp = LessOrEqualsOp<TYPE_DATE>;
    using NativeType = typename CompareType<TYPE_DATE>::NativeType;
    static UInt8 apply(const VecDateTimeValue& a, const VecDateTimeValue& b) { return a >= b; }
};

template <>
struct GreaterOrEqualsOp<TYPE_DATETIME> {
    using SymmetricOp = LessOrEqualsOp<TYPE_DATETIME>;
    using NativeType = typename CompareType<TYPE_DATETIME>::NativeType;
    static UInt8 apply(const VecDateTimeValue& a, const VecDateTimeValue& b) { return a >= b; }
};

template <>
struct GreaterOrEqualsOp<TYPE_DATEV2> {
    using SymmetricOp = LessOrEqualsOp<TYPE_DATEV2>;
    using NativeType = typename CompareType<TYPE_DATEV2>::NativeType;
    static UInt8 apply(const DateV2Value<DateV2ValueType>& a,
                       const DateV2Value<DateV2ValueType>& b) {
        return a >= b;
    }
};

template <>
struct GreaterOrEqualsOp<TYPE_DATETIMEV2> {
    using SymmetricOp = LessOrEqualsOp<TYPE_DATETIMEV2>;
    using NativeType = typename CompareType<TYPE_DATETIMEV2>::NativeType;
    static UInt8 apply(const DateV2Value<DateTimeV2ValueType>& a,
                       const DateV2Value<DateTimeV2ValueType>& b) {
        return a >= b;
    }
};

template <>
struct GreaterOrEqualsOp<TYPE_TIMESTAMPTZ> {
    using SymmetricOp = LessOrEqualsOp<TYPE_TIMESTAMPTZ>;
    using NativeType = typename CompareType<TYPE_TIMESTAMPTZ>::NativeType;
    static UInt8 apply(const TimestampTzValue& a, const TimestampTzValue& b) { return a >= b; }
};

} // namespace doris::vectorized
