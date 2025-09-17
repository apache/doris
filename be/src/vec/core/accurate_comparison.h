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

#include <cmath>
#include <limits>

#include "common/compare.h"
#include "runtime/primitive_type.h"
#include "util/binary_cast.hpp"
#include "vec/common/nan_utils.h"
#include "vec/common/string_ref.h"
#include "vec/common/uint128.h"
#include "vec/core/decomposed_float.h"
#include "vec/core/extended_types.h"
#include "vec/core/types.h"
#include "vec/runtime/vdatetime_value.h"
/** Perceptually-correct number comparisons.
  * Example: Int8(-1) != UInt8(255)
*/

namespace accurate {

/** Cases:
    1) Safe conversion (in case of default C++ operators)
        a) int vs any int
        b) uint vs any uint
        c) float vs any float
    2) int vs uint
        a) sizeof(int) <= sizeof(uint). Accurate comparison with MAX_INT thresholds
        b) sizeof(int)  > sizeof(uint). Casting to int
    3) integral_type vs floating_type
        a) sizeof(integral_type) <= 4. Comparison via casting arguments to Float64
        b) sizeof(integral_type) == 8. Accurate comparison. Consider 3 sets of intervals:
            1) interval between adjacent floats less or equal 1
            2) interval between adjacent floats greater then 2
            3) float is outside [MIN_INT64; MAX_INT64]
*/

// Case 1. Is pair of floats or pair of ints or pair of uints
template <typename A, typename B>
constexpr bool is_safe_conversion =
        (std::is_floating_point_v<A> && std::is_floating_point_v<B>) ||
        (IsIntegralV<A> && IsIntegralV<B> && !(IsSignedV<A> ^ IsSignedV<B>)) ||
        (std::is_same_v<A, doris::vectorized::Int128> &&
         std::is_same_v<B, doris::vectorized::Int128>) ||
        (IsIntegralV<A> && std::is_same_v<B, doris::vectorized::Int128>) ||
        (std::is_same_v<A, doris::vectorized::Int128> && IsIntegralV<B>);
template <typename A, typename B>
using bool_if_safe_conversion = std::enable_if_t<is_safe_conversion<A, B>, bool>;
template <typename A, typename B>
using bool_if_not_safe_conversion = std::enable_if_t<!is_safe_conversion<A, B>, bool>;

/* Final realizations */

template <typename A, typename B>
bool lessOp(A a, B b) {
    if constexpr (std::is_same_v<A, B>) {
        return a < b;
    }

    /// float vs float
    if constexpr (std::is_floating_point_v<A> && std::is_floating_point_v<B>) {
        return a < b;
    }

    /// anything vs NaN
    if (is_nan(a) || is_nan(b)) {
        return false;
    }

    /// int vs int
    if constexpr (IsIntegralV<A> && IsIntegralV<B>) {
        /// same signedness
        if constexpr (IsSignedV<A> == IsSignedV<B>) {
            return a < b;
        }

        /// different signedness

        if constexpr (IsSignedV<A> && !IsSignedV<B>) {
            return a < 0 || static_cast<std::make_unsigned_t<A>>(a) < b;
        }

        if constexpr (!IsSignedV<A> && IsSignedV<B>) {
            return b >= 0 && a < static_cast<std::make_unsigned_t<B>>(b);
        }
    }

    /// int vs float
    if constexpr (IsIntegralV<A> && std::is_floating_point_v<B>) {
        if constexpr (sizeof(A) <= 4) {
            return static_cast<double>(a) < static_cast<double>(b);
        }

        return DecomposedFloat<B>(b).greater(a);
    }

    if constexpr (std::is_floating_point_v<A> && IsIntegralV<B>) {
        if constexpr (sizeof(B) <= 4) {
            return static_cast<double>(a) < static_cast<double>(b);
        }

        return DecomposedFloat<A>(a).less(b);
    }

    static_assert(IsIntegralV<A> || std::is_floating_point_v<A>);
    static_assert(IsIntegralV<B> || std::is_floating_point_v<B>);
    __builtin_unreachable();
}

template <typename A, typename B>
bool greaterOp(A a, B b) {
    return lessOp(b, a);
}

template <typename A, typename B>
inline bool_if_not_safe_conversion<A, B> greaterOrEqualsOp(A a, B b) {
    if (is_nan(a) || is_nan(b)) {
        return false;
    }

    return !lessOp(a, b);
}

template <typename A, typename B>
inline bool_if_safe_conversion<A, B> greaterOrEqualsOp(A a, B b) {
    return a >= b;
}

template <typename A, typename B>
inline bool_if_not_safe_conversion<A, B> lessOrEqualsOp(A a, B b) {
    if (is_nan(a) || is_nan(b)) {
        return false;
    }

    return !lessOp(b, a);
}

template <typename A, typename B>
inline bool_if_safe_conversion<A, B> lessOrEqualsOp(A a, B b) {
    return a <= b;
}

template <typename A, typename B>
bool equalsOp(A a, B b) {
    if constexpr (std::is_same_v<A, B>) {
        return a == b;
    }

    /// float vs float
    if constexpr (std::is_floating_point_v<A> && std::is_floating_point_v<B>) {
        return a == b;
    }

    /// anything vs NaN
    if (is_nan(a) || is_nan(b)) {
        return false;
    }

    /// int vs int
    if constexpr (IsIntegralV<A> && IsIntegralV<B>) {
        /// same signedness
        if constexpr (IsSignedV<A> == IsSignedV<B>) {
            return a == b;
        }

        /// different signedness

        if constexpr (IsSignedV<A> && !IsSignedV<B>) {
            return a >= 0 && static_cast<std::make_unsigned_t<A>>(a) == b;
        }

        if constexpr (!IsSignedV<A> && IsSignedV<B>) {
            return b >= 0 && a == static_cast<std::make_unsigned_t<B>>(b);
        }
    }

    /// int vs float
    if constexpr (IsIntegralV<A> && std::is_floating_point_v<B>) {
        if constexpr (sizeof(A) <= 4) {
            return static_cast<double>(a) == static_cast<double>(b);
        }

        return DecomposedFloat<B>(b).equals(a);
    }

    if constexpr (std::is_floating_point_v<A> && IsIntegralV<B>) {
        if constexpr (sizeof(B) <= 4) {
            return static_cast<double>(a) == static_cast<double>(b);
        }

        return DecomposedFloat<A>(a).equals(b);
    }

    /// e.g comparing UUID with integer.
    return false;
}

template <typename A, typename B>
bool notEqualsOp(A a, B b) {
    return !equalsOp(a, b);
}
} // namespace accurate

namespace doris::vectorized {

template <PrimitiveType A, PrimitiveType B>
struct EqualsOp {
    /// An operation that gives the same result, if arguments are passed in reverse order.
    using SymmetricOp = EqualsOp<B, A>;
    using NativeTypeA =
            std::conditional_t<A == TYPE_BOOLEAN, typename PrimitiveTypeTraits<A>::ColumnItemType,
                               typename PrimitiveTypeTraits<A>::CppNativeType>;
    using NativeTypeB =
            std::conditional_t<B == TYPE_BOOLEAN, typename PrimitiveTypeTraits<B>::ColumnItemType,
                               typename PrimitiveTypeTraits<B>::CppNativeType>;
    static UInt8 apply(NativeTypeA a, NativeTypeB b) { return Compare::equal(a, b); }
};

template <>
struct EqualsOp<TYPE_DECIMALV2, TYPE_DECIMALV2> {
    static UInt8 apply(const Int128& a, const Int128& b) { return a == b; }
};

template <>
struct EqualsOp<TYPE_STRING, TYPE_STRING> {
    static UInt8 apply(const StringRef& a, const StringRef& b) { return a == b; }
};

template <PrimitiveType A, PrimitiveType B>
struct NotEqualsOp {
    using SymmetricOp = NotEqualsOp<B, A>;
    using NativeTypeA =
            std::conditional_t<A == TYPE_BOOLEAN, typename PrimitiveTypeTraits<A>::ColumnItemType,
                               typename PrimitiveTypeTraits<A>::CppNativeType>;
    using NativeTypeB =
            std::conditional_t<B == TYPE_BOOLEAN, typename PrimitiveTypeTraits<B>::ColumnItemType,
                               typename PrimitiveTypeTraits<B>::CppNativeType>;
    static UInt8 apply(NativeTypeA a, NativeTypeB b) { return Compare::not_equal(a, b); }
};

template <>
struct NotEqualsOp<TYPE_DECIMALV2, TYPE_DECIMALV2> {
    static UInt8 apply(const DecimalV2Value& a, const DecimalV2Value& b) { return a != b; }
};

template <PrimitiveType A, PrimitiveType B>
struct GreaterOp;

template <PrimitiveType A, PrimitiveType B>
struct LessOp {
    using SymmetricOp = GreaterOp<B, A>;
    using NativeTypeA =
            std::conditional_t<A == TYPE_BOOLEAN, typename PrimitiveTypeTraits<A>::ColumnItemType,
                               typename PrimitiveTypeTraits<A>::CppNativeType>;
    using NativeTypeB =
            std::conditional_t<B == TYPE_BOOLEAN, typename PrimitiveTypeTraits<B>::ColumnItemType,
                               typename PrimitiveTypeTraits<B>::CppNativeType>;
    static UInt8 apply(NativeTypeA a, NativeTypeB b) { return Compare::less(a, b); }
};

template <>
struct LessOp<TYPE_DECIMALV2, TYPE_DECIMALV2> {
    static UInt8 apply(Int128 a, Int128 b) { return a < b; }
};

template <>
struct LessOp<TYPE_STRING, TYPE_STRING> {
    static UInt8 apply(StringRef a, StringRef b) { return a < b; }
};

template <PrimitiveType A, PrimitiveType B>
struct GreaterOp {
    using SymmetricOp = LessOp<B, A>;
    using NativeTypeA =
            std::conditional_t<A == TYPE_BOOLEAN, typename PrimitiveTypeTraits<A>::ColumnItemType,
                               typename PrimitiveTypeTraits<A>::CppNativeType>;
    using NativeTypeB =
            std::conditional_t<B == TYPE_BOOLEAN, typename PrimitiveTypeTraits<B>::ColumnItemType,
                               typename PrimitiveTypeTraits<B>::CppNativeType>;
    static UInt8 apply(NativeTypeA a, NativeTypeB b) { return Compare::greater(a, b); }
};

template <>
struct GreaterOp<TYPE_DECIMALV2, TYPE_DECIMALV2> {
    static UInt8 apply(Int128 a, Int128 b) { return a > b; }
};

template <>
struct GreaterOp<TYPE_STRING, TYPE_STRING> {
    static UInt8 apply(StringRef a, StringRef b) { return a > b; }
};

template <PrimitiveType A, PrimitiveType B>
struct GreaterOrEqualsOp;

template <PrimitiveType A, PrimitiveType B>
struct LessOrEqualsOp {
    using SymmetricOp = GreaterOrEqualsOp<B, A>;
    using NativeTypeA =
            std::conditional_t<A == TYPE_BOOLEAN, typename PrimitiveTypeTraits<A>::ColumnItemType,
                               typename PrimitiveTypeTraits<A>::CppNativeType>;
    using NativeTypeB =
            std::conditional_t<B == TYPE_BOOLEAN, typename PrimitiveTypeTraits<B>::ColumnItemType,
                               typename PrimitiveTypeTraits<B>::CppNativeType>;
    static UInt8 apply(NativeTypeA a, NativeTypeB b) { return Compare::less_equal(a, b); }
};

template <>
struct LessOrEqualsOp<TYPE_DECIMALV2, TYPE_DECIMALV2> {
    static UInt8 apply(DecimalV2Value a, DecimalV2Value b) { return a <= b; }
};

template <PrimitiveType A, PrimitiveType B>
struct GreaterOrEqualsOp {
    using SymmetricOp = LessOrEqualsOp<B, A>;
    using NativeTypeA =
            std::conditional_t<A == TYPE_BOOLEAN, typename PrimitiveTypeTraits<A>::ColumnItemType,
                               typename PrimitiveTypeTraits<A>::CppNativeType>;
    using NativeTypeB =
            std::conditional_t<B == TYPE_BOOLEAN, typename PrimitiveTypeTraits<B>::ColumnItemType,
                               typename PrimitiveTypeTraits<B>::CppNativeType>;
    static UInt8 apply(NativeTypeA a, NativeTypeB b) { return Compare::greater_equal(a, b); }
};

template <>
struct GreaterOrEqualsOp<TYPE_DECIMALV2, TYPE_DECIMALV2> {
    static UInt8 apply(DecimalV2Value a, DecimalV2Value b) { return a >= b; }
};

} // namespace doris::vectorized
