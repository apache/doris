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
        (std::is_integral_v<A> && std::is_integral_v<B> &&
         !(std::is_signed_v<A> ^ std::is_signed_v<B>)) ||
        (std::is_same_v<A, doris::vectorized::Int128> &&
         std::is_same_v<B, doris::vectorized::Int128>) ||
        (std::is_integral_v<A> && std::is_same_v<B, doris::vectorized::Int128>) ||
        (std::is_same_v<A, doris::vectorized::Int128> && std::is_integral_v<B>);
template <typename A, typename B>
using bool_if_safe_conversion = std::enable_if_t<is_safe_conversion<A, B>, bool>;
template <typename A, typename B>
using bool_if_not_safe_conversion = std::enable_if_t<!is_safe_conversion<A, B>, bool>;

/// Case 2. Are params IntXX and UIntYY ?
template <typename TInt, typename TUInt>
constexpr bool is_any_int_vs_uint = std::is_integral_v<TInt> && std::is_integral_v<TUInt> &&
                                    std::is_signed_v<TInt> && std::is_unsigned_v<TUInt>;

// Case 2a. Are params IntXX and UIntYY and sizeof(IntXX) >= sizeof(UIntYY) (in such case will use accurate compare)
template <typename TInt, typename TUInt>
constexpr bool is_le_int_vs_uint =
        is_any_int_vs_uint<TInt, TUInt> && (sizeof(TInt) <= sizeof(TUInt));

template <typename TInt, typename TUInt>
using bool_if_le_int_vs_uint_t = std::enable_if_t<is_le_int_vs_uint<TInt, TUInt>, bool>;

template <typename TInt, typename TUInt>
inline bool_if_le_int_vs_uint_t<TInt, TUInt> greaterOpTmpl(TInt a, TUInt b) {
    return static_cast<TUInt>(a) > b && a >= 0 &&
           b <= static_cast<TUInt>(std::numeric_limits<TInt>::max());
}

template <typename TUInt, typename TInt>
inline bool_if_le_int_vs_uint_t<TInt, TUInt> greaterOpTmpl(TUInt a, TInt b) {
    return a > static_cast<TUInt>(b) || b < 0 ||
           a > static_cast<TUInt>(std::numeric_limits<TInt>::max());
}

template <typename TInt, typename TUInt>
inline bool_if_le_int_vs_uint_t<TInt, TUInt> equalsOpTmpl(TInt a, TUInt b) {
    return static_cast<TUInt>(a) == b && a >= 0 &&
           b <= static_cast<TUInt>(std::numeric_limits<TInt>::max());
}

template <typename TUInt, typename TInt>
inline bool_if_le_int_vs_uint_t<TInt, TUInt> equalsOpTmpl(TUInt a, TInt b) {
    return a == static_cast<TUInt>(b) && b >= 0 &&
           a <= static_cast<TUInt>(std::numeric_limits<TInt>::max());
}

// Case 2b. Are params IntXX and UIntYY and sizeof(IntXX) > sizeof(UIntYY) (in such case will cast UIntYY to IntXX and compare)
template <typename TInt, typename TUInt>
constexpr bool is_gt_int_vs_uint =
        is_any_int_vs_uint<TInt, TUInt> && (sizeof(TInt) > sizeof(TUInt));

template <typename TInt, typename TUInt>
using bool_if_gt_int_vs_uint = std::enable_if_t<is_gt_int_vs_uint<TInt, TUInt>, bool>;

template <typename TInt, typename TUInt>
inline bool_if_gt_int_vs_uint<TInt, TUInt> greaterOpTmpl(TInt a, TUInt b) {
    return static_cast<TInt>(a) > static_cast<TInt>(b);
}

template <typename TInt, typename TUInt>
inline bool_if_gt_int_vs_uint<TInt, TUInt> greaterOpTmpl(TUInt a, TInt b) {
    return static_cast<TInt>(a) > static_cast<TInt>(b);
}

template <typename TInt, typename TUInt>
inline bool_if_gt_int_vs_uint<TInt, TUInt> equalsOpTmpl(TInt a, TUInt b) {
    return static_cast<TInt>(a) == static_cast<TInt>(b);
}

template <typename TInt, typename TUInt>
inline bool_if_gt_int_vs_uint<TInt, TUInt> equalsOpTmpl(TUInt a, TInt b) {
    return static_cast<TInt>(a) == static_cast<TInt>(b);
}

// Case 3a. Comparison via conversion to double.
template <typename TAInt, typename TAFloat>
using bool_if_double_can_be_used =
        std::enable_if_t<std::is_integral_v<TAInt> && (sizeof(TAInt) <= 4) &&
                                 std::is_floating_point_v<TAFloat>,
                         bool>;

template <typename TAInt, typename TAFloat>
inline bool_if_double_can_be_used<TAInt, TAFloat> greaterOpTmpl(TAInt a, TAFloat b) {
    return static_cast<double>(a) > static_cast<double>(b);
}

template <typename TAInt, typename TAFloat>
inline bool_if_double_can_be_used<TAInt, TAFloat> greaterOpTmpl(TAFloat a, TAInt b) {
    return static_cast<double>(a) > static_cast<double>(b);
}

template <typename TAInt, typename TAFloat>
inline bool_if_double_can_be_used<TAInt, TAFloat> equalsOpTmpl(TAInt a, TAFloat b) {
    return static_cast<double>(a) == static_cast<double>(b);
}

template <typename TAInt, typename TAFloat>
inline bool_if_double_can_be_used<TAInt, TAFloat> equalsOpTmpl(TAFloat a, TAInt b) {
    return static_cast<double>(a) == static_cast<double>(b);
}

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
    if constexpr (wide::is_integer<A> && wide::is_integer<B>) {
        /// same signedness
        if constexpr (std::is_signed_v<A> == std::is_signed_v<B>) {
            return a < b;
        }

        /// different signedness

        if constexpr (std::is_signed_v<A> && !std::is_signed_v<B>) {
            return a < 0 || static_cast<std::make_unsigned_t<A>>(a) < b;
        }

        if constexpr (!std::is_signed_v<A> && std::is_signed_v<B>) {
            return b >= 0 && a < static_cast<std::make_unsigned_t<B>>(b);
        }
    }

    /// int vs float
    if constexpr (wide::is_integer<A> && std::is_floating_point_v<B>) {
        if constexpr (sizeof(A) <= 4) {
            return static_cast<double>(a) < static_cast<double>(b);
        }

        return DecomposedFloat<B>(b).greater(a);
    }

    if constexpr (std::is_floating_point_v<A> && wide::is_integer<B>) {
        if constexpr (sizeof(B) <= 4) {
            return static_cast<double>(a) < static_cast<double>(b);
        }

        return DecomposedFloat<A>(a).less(b);
    }

    static_assert(wide::is_integer<A> || std::is_floating_point_v<A>);
    static_assert(wide::is_integer<B> || std::is_floating_point_v<B>);
    __builtin_unreachable();
}

template <typename A, typename B>
bool greaterOp(A a, B b) {
    return lessOp(b, a);
}

template <typename A, typename B>
bool greaterOrEqualsOp(A a, B b) {
    if (is_nan(a) || is_nan(b)) {
        return false;
    }

    return !lessOp(a, b);
}

template <typename A, typename B>
bool lessOrEqualsOp(A a, B b) {
    if (is_nan(a) || is_nan(b)) {
        return false;
    }

    return !lessOp(b, a);
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
    if constexpr (wide::is_integer<A> && wide::is_integer<B>) {
        /// same signedness
        if constexpr (std::is_signed_v<A> == std::is_signed_v<B>) {
            return a == b;
        }

        /// different signedness

        if constexpr (std::is_signed_v<A> && !std::is_signed_v<B>) {
            return a >= 0 && static_cast<std::make_unsigned_t<A>>(a) == b;
        }

        if constexpr (!std::is_signed_v<A> && std::is_signed_v<B>) {
            return b >= 0 && a == static_cast<std::make_unsigned_t<B>>(b);
        }
    }

    /// int vs float
    if constexpr (wide::is_integer<A> && std::is_floating_point_v<B>) {
        if constexpr (sizeof(A) <= 4) {
            return static_cast<double>(a) == static_cast<double>(b);
        }

        return DecomposedFloat<B>(b).equals(a);
    }

    if constexpr (std::is_floating_point_v<A> && wide::is_integer<B>) {
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

/// Converts numeric to an equal numeric of other type.
/// When `strict` is `true` check that result exactly same as input, otherwise just check overflow
template <typename From, typename To, bool strict = true>
inline bool convertNumeric(From value, To& result) {
    /// If the type is actually the same it's not necessary to do any checks.
    if constexpr (std::is_same_v<From, To>) {
        result = value;
        return true;
    }
    if constexpr (std::is_floating_point_v<From> && std::is_floating_point_v<To>) {
        /// Note that NaNs doesn't compare equal to anything, but they are still in range of any Float type.
        if (is_nan(value)) {
            result = value;
            return true;
        }
        if (value == std::numeric_limits<From>::infinity()) {
            result = std::numeric_limits<To>::infinity();
            return true;
        }
        if (value == -std::numeric_limits<From>::infinity()) {
            result = -std::numeric_limits<To>::infinity();
            return true;
        }
    }
    if (greaterOp(value, std::numeric_limits<To>::max()) ||
        lessOp(value, std::numeric_limits<To>::lowest())) {
        return false;
    }
    result = static_cast<To>(value);
    if constexpr (strict) {
        return equalsOp(value, result);
    }
    return true;
}

} // namespace accurate

namespace doris::vectorized {

template <typename A, typename B>
struct EqualsOp {
    /// An operation that gives the same result, if arguments are passed in reverse order.
    using SymmetricOp = EqualsOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::equalsOp(a, b); }
};

template <>
struct EqualsOp<DecimalV2Value, DecimalV2Value> {
    static UInt8 apply(const Int128& a, const Int128& b) { return a == b; }
};

template <>
struct EqualsOp<StringRef, StringRef> {
    static UInt8 apply(const StringRef& a, const StringRef& b) { return a == b; }
};

template <typename A, typename B>
struct NotEqualsOp {
    using SymmetricOp = NotEqualsOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::notEqualsOp(a, b); }
};

template <>
struct NotEqualsOp<DecimalV2Value, DecimalV2Value> {
    static UInt8 apply(const Int128& a, const Int128& b) { return a != b; }
};

template <typename A, typename B>
struct GreaterOp;

template <typename A, typename B>
struct LessOp {
    using SymmetricOp = GreaterOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::lessOp(a, b); }
};

template <>
struct LessOp<DecimalV2Value, DecimalV2Value> {
    static UInt8 apply(Int128 a, Int128 b) {
        return binary_cast<Int128, DecimalV2Value>(a) < binary_cast<Int128, DecimalV2Value>(b);
    }
};

template <>
struct LessOp<StringRef, StringRef> {
    static UInt8 apply(StringRef a, StringRef b) { return a < b; }
};

template <typename A, typename B>
struct GreaterOp {
    using SymmetricOp = LessOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::greaterOp(a, b); }
};

template <>
struct GreaterOp<DecimalV2Value, DecimalV2Value> {
    static UInt8 apply(Int128 a, Int128 b) {
        return binary_cast<Int128, DecimalV2Value>(a) > binary_cast<Int128, DecimalV2Value>(b);
    }
};

template <>
struct GreaterOp<StringRef, StringRef> {
    static UInt8 apply(StringRef a, StringRef b) { return a > b; }
};

template <typename A, typename B>
struct GreaterOrEqualsOp;

template <typename A, typename B>
struct LessOrEqualsOp {
    using SymmetricOp = GreaterOrEqualsOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::lessOrEqualsOp(a, b); }
};

template <>
struct LessOrEqualsOp<DecimalV2Value, DecimalV2Value> {
    static UInt8 apply(Int128 a, Int128 b) {
        return binary_cast<Int128, DecimalV2Value>(a) <= binary_cast<Int128, DecimalV2Value>(b);
    }
};

template <typename A, typename B>
struct GreaterOrEqualsOp {
    using SymmetricOp = LessOrEqualsOp<B, A>;
    static UInt8 apply(A a, B b) { return accurate::greaterOrEqualsOp(a, b); }
};

template <>
struct GreaterOrEqualsOp<DecimalV2Value, DecimalV2Value> {
    static UInt8 apply(Int128 a, Int128 b) {
        return binary_cast<Int128, DecimalV2Value>(a) >= binary_cast<Int128, DecimalV2Value>(b);
    }
};

} // namespace doris::vectorized
