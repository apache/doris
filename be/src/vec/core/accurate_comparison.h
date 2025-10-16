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
using CompareType =
        std::conditional_t<PT == TYPE_BOOLEAN, typename PrimitiveTypeTraits<PT>::ColumnItemType,
                           typename PrimitiveTypeTraits<PT>::CppNativeType>;

template <PrimitiveType PT>
struct EqualsOp {
    /// An operation that gives the same result, if arguments are passed in reverse order.
    using SymmetricOp = EqualsOp<PT>;
    using NativeType = CompareType<PT>;

    static UInt8 apply(NativeType a, NativeType b) { return Compare::equal(a, b); }
};

template <>
struct EqualsOp<TYPE_DECIMALV2> {
    static UInt8 apply(const Int128& a, const Int128& b) { return a == b; }
};

template <>
struct EqualsOp<TYPE_STRING> {
    static UInt8 apply(const StringRef& a, const StringRef& b) { return a == b; }
};

template <PrimitiveType PT>
struct NotEqualsOp {
    using SymmetricOp = NotEqualsOp<PT>;
    using NativeType = CompareType<PT>;
    static UInt8 apply(NativeType a, NativeType b) { return Compare::not_equal(a, b); }
};

template <>
struct NotEqualsOp<TYPE_DECIMALV2> {
    static UInt8 apply(const DecimalV2Value& a, const DecimalV2Value& b) { return a != b; }
};

template <PrimitiveType PT>
struct GreaterOp;

template <PrimitiveType PT>
struct LessOp {
    using SymmetricOp = GreaterOp<PT>;
    using NativeType = CompareType<PT>;
    static UInt8 apply(NativeType a, NativeType b) { return Compare::less(a, b); }
};

template <>
struct LessOp<TYPE_DECIMALV2> {
    static UInt8 apply(Int128 a, Int128 b) { return a < b; }
};

template <>
struct LessOp<TYPE_STRING> {
    static UInt8 apply(StringRef a, StringRef b) { return a < b; }
};

template <PrimitiveType PT>
struct GreaterOp {
    using SymmetricOp = LessOp<PT>;
    using NativeType = CompareType<PT>;
    static UInt8 apply(NativeType a, NativeType b) { return Compare::greater(a, b); }
};

template <>
struct GreaterOp<TYPE_DECIMALV2> {
    static UInt8 apply(Int128 a, Int128 b) { return a > b; }
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
    using NativeType = CompareType<PT>;
    static UInt8 apply(NativeType a, NativeType b) { return Compare::less_equal(a, b); }
};

template <>
struct LessOrEqualsOp<TYPE_DECIMALV2> {
    static UInt8 apply(DecimalV2Value a, DecimalV2Value b) { return a <= b; }
};

template <PrimitiveType PT>
struct GreaterOrEqualsOp {
    using SymmetricOp = LessOrEqualsOp<PT>;
    using NativeType = CompareType<PT>;
    static UInt8 apply(NativeType a, NativeType b) { return Compare::greater_equal(a, b); }
};

template <>
struct GreaterOrEqualsOp<TYPE_DECIMALV2> {
    static UInt8 apply(DecimalV2Value a, DecimalV2Value b) { return a >= b; }
};

} // namespace doris::vectorized
