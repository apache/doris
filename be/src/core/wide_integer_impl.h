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
/// Original is here https://github.com/cerevra/int
/// Distributed under the Boost Software License, Version 1.0.
/// (See at http://www.boost.org/LICENSE_1_0.txt)

// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/base/base/wide_integer_impl.h
// and modified by Doris
#pragma once

#include <cassert>
#include <cfloat>
#include <cmath>
#include <compare>
#include <limits>
#include <tuple>
#include <type_traits>

#include "common/exception.h"
// NOLINTBEGIN(*)

/// Use same extended double for all platforms
#if (LDBL_MANT_DIG == 64)
#include <boost/math/special_functions/fpclassify.hpp>

#define CONSTEXPR_FROM_DOUBLE constexpr
#define DORIS_WIDE_FROM_DOUBLE_INLINE 1
using FromDoubleIntermediateType = long double;
#elif defined(DORIS_WIDE_INTEGER_FROM_DOUBLE_IMPL_TU)
#include <boost/math/special_functions/fpclassify.hpp>
#include <boost/multiprecision/cpp_bin_float.hpp>
/// `wide_integer_from_builtin` can't be constexpr with non-literal `cpp_bin_float_double_extended`
#define CONSTEXPR_FROM_DOUBLE
#define DORIS_WIDE_FROM_DOUBLE_INLINE 1
using FromDoubleIntermediateType = boost::multiprecision::cpp_bin_float_double_extended;
#else
/// Platforms without an 80-bit long double emulate the intermediate type with
/// boost::multiprecision, which drags ~4.3MB of preprocessed closure into every
/// TU. Ordinary TUs therefore see only declarations of the two from-double
/// members below; the bodies are compiled once in
/// core/wide_integer_from_double.cpp (which defines the *_IMPL_TU macro above
/// and emits explicit instantiations). wide_integer_from_builtin(double) was
/// never constexpr on these platforms, so no constant evaluation is lost.
#define CONSTEXPR_FROM_DOUBLE
#define DORIS_WIDE_FROM_DOUBLE_INLINE 0
#endif

namespace wide {

template <typename T>
struct IsWideInteger {
    static const constexpr bool value = false;
};

template <size_t Bits, typename Signed>
struct IsWideInteger<wide::integer<Bits, Signed>> {
    static const constexpr bool value = true;
};

template <typename T>
static constexpr bool ArithmeticConcept() noexcept {
    return std::is_arithmetic_v<T> || IsWideInteger<T>::value;
}

template <typename T>
static constexpr bool IntegralConcept() noexcept {
    return std::is_integral_v<T> || IsWideInteger<T>::value;
}

template <typename T>
class IsTupleLike {
    template <typename U>
    static auto check(U* p) -> decltype(std::tuple_size<U>::value, int());
    template <typename>
    static void check(...);

public:
    static constexpr const bool value = !std::is_void<decltype(check<T>(nullptr))>::value;
};

} // namespace wide

namespace std {

// numeric limits
template <size_t Bits, typename Signed>
class numeric_limits<wide::integer<Bits, Signed>> {
public:
    static constexpr bool is_specialized = true;
    static constexpr bool is_signed = is_same<Signed, signed>::value;
    static constexpr bool is_integer = true;
    static constexpr bool is_exact = true;
    static constexpr bool has_infinity = false;
    static constexpr bool has_quiet_NaN = false;
    static constexpr bool has_signaling_NaN = true;
    static constexpr std::float_denorm_style has_denorm = std::denorm_absent;
    static constexpr bool has_denorm_loss = false;
    static constexpr std::float_round_style round_style = std::round_toward_zero;
    static constexpr bool is_iec559 = false;
    static constexpr bool is_bounded = true;
    static constexpr bool is_modulo = true;
    static constexpr int digits = Bits - (is_same<Signed, signed>::value ? 1 : 0);
    static constexpr int digits10 = digits * 0.30103 /*std::log10(2)*/;
    static constexpr int max_digits10 = 0;
    static constexpr int radix = 2;
    static constexpr int min_exponent = 0;
    static constexpr int min_exponent10 = 0;
    static constexpr int max_exponent = 0;
    static constexpr int max_exponent10 = 0;
    static constexpr bool traps = true;
    static constexpr bool tinyness_before = false;

    static constexpr wide::integer<Bits, Signed> min() noexcept {
        if (is_same<Signed, signed>::value) {
            using T = wide::integer<Bits, signed>;
            T res {};
            res.items[T::_impl::big(0)] = std::numeric_limits<
                    typename wide::integer<Bits, Signed>::signed_base_type>::min();
            return res;
        }
        return wide::integer<Bits, Signed>(0);
    }

    static constexpr wide::integer<Bits, Signed> max() noexcept {
        using T = wide::integer<Bits, Signed>;
        T res {};
        res.items[T::_impl::big(0)] =
                is_same<Signed, signed>::value
                        ? std::numeric_limits<
                                  typename wide::integer<Bits, Signed>::signed_base_type>::max()
                        : std::numeric_limits<
                                  typename wide::integer<Bits, Signed>::base_type>::max();
        for (unsigned i = 1; i < wide::integer<Bits, Signed>::_impl::item_count; ++i) {
            res.items[T::_impl::big(i)] =
                    std::numeric_limits<typename wide::integer<Bits, Signed>::base_type>::max();
        }
        return res;
    }

    static constexpr wide::integer<Bits, Signed> lowest() noexcept { return min(); }
    static constexpr wide::integer<Bits, Signed> epsilon() noexcept { return 0; }
    static constexpr wide::integer<Bits, Signed> round_error() noexcept { return 0; }
    static constexpr wide::integer<Bits, Signed> infinity() noexcept { return 0; }
    static constexpr wide::integer<Bits, Signed> quiet_NaN() noexcept { return 0; }
    static constexpr wide::integer<Bits, Signed> signaling_NaN() noexcept { return 0; }
    static constexpr wide::integer<Bits, Signed> denorm_min() noexcept { return 0; }
};

// type traits
template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
struct common_type<wide::integer<Bits, Signed>, wide::integer<Bits2, Signed2>> {
    using type = std::conditional_t < Bits == Bits2,
          wide::integer<Bits, std::conditional_t<(std::is_same_v<Signed, Signed2> &&
                                                  std::is_same_v<Signed2, signed>),
                                                 signed, unsigned>>,
          std::conditional_t<
                  Bits2<Bits, wide::integer<Bits, Signed>, wide::integer<Bits2, Signed2>>>;
};

template <size_t Bits, typename Signed, typename Arithmetic>
struct common_type<wide::integer<Bits, Signed>, Arithmetic> {
    static_assert(wide::ArithmeticConcept<Arithmetic>());

    using type = std::conditional_t < std::is_floating_point_v<Arithmetic>, Arithmetic,
          std::conditional_t<sizeof(Arithmetic) * 8 < Bits, wide::integer<Bits, Signed>,
                             std::conditional_t<Bits<
                                     sizeof(Arithmetic) * 8, Arithmetic,
                                     std::conditional_t<Bits == sizeof(Arithmetic) * 8 &&
                                                                (std::is_same_v<Signed, signed> ||
                                                                 std::is_signed_v<Arithmetic>),
                                                        Arithmetic, wide::integer<Bits, Signed>>>>>;
};

template <typename Arithmetic, size_t Bits, typename Signed>
struct common_type<Arithmetic, wide::integer<Bits, Signed>>
        : common_type<wide::integer<Bits, Signed>, Arithmetic> {};

} // namespace std

namespace wide {

template <size_t Bits, typename Signed>
struct integer<Bits, Signed>::_impl {
    static constexpr size_t _bits = Bits;
    static constexpr const unsigned byte_count = Bits / 8;
    static constexpr const unsigned item_count = byte_count / sizeof(base_type);
    static constexpr const unsigned base_bits = sizeof(base_type) * 8;

    static_assert(Bits % base_bits == 0);

    /// Simple iteration in both directions
    static constexpr unsigned little(unsigned idx) {
        if constexpr (std::endian::native == std::endian::little) {
            return idx;
        } else {
            return item_count - 1 - idx;
        }
    }
    static constexpr unsigned big(unsigned idx) {
        if constexpr (std::endian::native == std::endian::little) {
            return item_count - 1 - idx;
        } else {
            return idx;
        }
    }
    static constexpr unsigned any(unsigned idx) { return idx; }

    template <class T>
    constexpr static bool is_negative(const T& n) noexcept {
        if constexpr (std::is_signed_v<T>) {
            return n < 0;
        } else {
            return false;
        }
    }

    template <size_t B, class T>
    constexpr static bool is_negative(const integer<B, T>& n) noexcept {
        if constexpr (std::is_same_v<T, signed>) {
            return static_cast<signed_base_type>(n.items[integer<B, T>::_impl::big(0)]) < 0;
        } else {
            return false;
        }
    }

    template <typename T>
    constexpr static auto make_positive(const T& n) noexcept {
        if constexpr (std::is_signed_v<T>) {
            return n < 0 ? -n : n;
        } else {
            return n;
        }
    }

    template <size_t B, class S>
    constexpr static integer<B, S> make_positive(const integer<B, S>& n) noexcept {
        return is_negative(n) ? integer<B, S>(operator_unary_minus(n)) : n;
    }

    template <typename T>
    __attribute__((no_sanitize("undefined"))) constexpr static auto to_Integral(T f) noexcept {
        /// NOTE: this can be called with DB::Decimal, and in this case, result
        /// will be wrong
        if constexpr (std::is_signed_v<T>) {
            return static_cast<int64_t>(f);
        } else {
            return static_cast<uint64_t>(f);
        }
    }

    template <typename Integral>
    constexpr static void wide_integer_from_builtin(integer<Bits, Signed>& self,
                                                    Integral rhs) noexcept {
        if constexpr (std::is_same_v<Integral, __int128> ||
                      std::is_same_v<Integral, unsigned __int128>) {
            self.items[little(0)] = static_cast<base_type>(rhs);
            self.items[little(1)] = rhs >> 64;
            if (rhs < 0) {
                for (unsigned i = 2; i < item_count; ++i) {
                    self.items[little(i)] = -1;
                }
                return;
            } else {
                for (unsigned i = 2; i < item_count; ++i) {
                    self.items[little(i)] = 0;
                }
            }
        } else {
            static_assert(sizeof(Integral) <= sizeof(base_type));

            self.items[little(0)] = _impl::to_Integral(rhs);

            if constexpr (std::is_signed_v<Integral>) {
                if (rhs < 0) {
                    for (unsigned i = 1; i < item_count; ++i) {
                        self.items[little(i)] = -1;
                    }
                    return;
                }
            }

            for (unsigned i = 1; i < item_count; ++i) {
                self.items[little(i)] = 0;
            }
        }
    }

    template <typename TupleLike, size_t i = 0>
    constexpr static void wide_integer_from_tuple_like(integer<Bits, Signed>& self,
                                                       const TupleLike& tuple) noexcept {
        if constexpr (i < item_count) {
            if constexpr (i < std::tuple_size_v<TupleLike>) {
                self.items[i] = std::get<i>(tuple);
            } else {
                self.items[i] = 0;
            }
            wide_integer_from_tuple_like<TupleLike, i + 1>(self, tuple);
        }
    }

    /**
     * N.B. t is constructed from double, so max(t) = max(double) ~ 2^310
     * the recursive call happens when t / 2^64 > 2^64, so there won't be more than 5 of them.
     *
     * t = a1 * max_int + b1,   a1 > max_int, b1 < max_int
     * a1 = a2 * max_int + b2,  a2 > max_int, b2 < max_int
     * a_(n - 1) = a_n * max_int + b2, a_n <= max_int <- base case.
     */
    template <class T>
    CONSTEXPR_FROM_DOUBLE static void set_multiplier(integer<Bits, Signed>& self, T t) noexcept
#if !DORIS_WIDE_FROM_DOUBLE_INLINE
            ;
#else
    {
        constexpr uint64_t max_int = std::numeric_limits<uint64_t>::max();
        static_assert(std::is_same_v<T, double> || std::is_same_v<T, FromDoubleIntermediateType>);
        /// Implementation specific behaviour on overflow (if we don't check here, stack overflow will triggered in bigint_cast).
        if constexpr (std::is_same_v<T, double>) {
            if (!std::isfinite(t)) {
                self = 0;
                return;
            }
        } else {
            if (!boost::math::isfinite(t)) {
                self = 0;
                return;
            }
        }

        const T alpha = t / static_cast<T>(max_int);

        /** Here we have to use strict comparison.
          * The max_int is 2^64 - 1.
          * When casted to floating point type, it will be rounded to the closest representable number,
          * which is 2^64.
          * But 2^64 is not representable in uint64_t,
          * so the maximum representable number will be strictly less.
          */
        if (alpha < static_cast<T>(max_int)) {
            self = static_cast<uint64_t>(alpha);
        } else { // max(double) / 2^64 will surely contain less than 52 precision bits, so speed up computations.
            set_multiplier<double>(self, static_cast<double>(alpha));
        }

        self *= max_int;
        self += static_cast<uint64_t>(t - floor(static_cast<double>(alpha)) *
                                                  static_cast<T>(max_int)); // += b_i
    }
#endif

    CONSTEXPR_FROM_DOUBLE static void wide_integer_from_builtin(integer<Bits, Signed>& self,
                                                                double rhs) noexcept
#if !DORIS_WIDE_FROM_DOUBLE_INLINE
            ;
#else
    {
        constexpr int64_t max_int = std::numeric_limits<int64_t>::max();
        constexpr int64_t min_int = std::numeric_limits<int64_t>::lowest();

        /// There are values in int64 that have more than 53 significant bits (in terms of double
        /// representation). Such values, being promoted to double, are rounded up or down. If they are rounded up,
        /// the result may not fit in 64 bits.
        /// The example of such a number is 9.22337e+18.
        /// As to_Integral does a static_cast to int64_t, it may result in UB.
        /// The necessary check here is that FromDoubleIntermediateType has enough significant (mantissa) bits to store the
        /// int64_t max value precisely.

        if (rhs > static_cast<FromDoubleIntermediateType>(min_int) &&
            rhs < static_cast<FromDoubleIntermediateType>(max_int)) {
            self = static_cast<int64_t>(rhs);
            return;
        }

        const FromDoubleIntermediateType rhs_long_double =
                (static_cast<FromDoubleIntermediateType>(rhs) < 0)
                        ? -static_cast<FromDoubleIntermediateType>(rhs)
                        : rhs;

        set_multiplier(self, rhs_long_double);

        if (rhs < 0) {
            self = -self;
        }
    }
#endif

    template <size_t Bits2, typename Signed2>
    constexpr static void wide_integer_from_wide_integer(
            integer<Bits, Signed>& self, const integer<Bits2, Signed2>& rhs) noexcept {
        constexpr const unsigned min_bits = (Bits < Bits2) ? Bits : Bits2;
        constexpr const unsigned to_copy = min_bits / base_bits;

        for (unsigned i = 0; i < to_copy; ++i) {
            self.items[little(i)] = rhs.items[integer<Bits2, Signed2>::_impl::little(i)];
        }

        if constexpr (Bits > Bits2) {
            if constexpr (std::is_signed_v<Signed2>) {
                if (rhs < 0) {
                    for (unsigned i = to_copy; i < item_count; ++i) {
                        self.items[little(i)] = -1;
                    }
                    return;
                }
            }

            for (unsigned i = to_copy; i < item_count; ++i) {
                self.items[little(i)] = 0;
            }
        }
    }

    template <typename T>
    constexpr static bool should_keep_size() {
        return sizeof(T) <= byte_count;
    }

    constexpr static integer<Bits, Signed> shift_left(const integer<Bits, Signed>& rhs,
                                                      unsigned n) noexcept {
        integer<Bits, Signed> lhs;
        unsigned items_shift = n / base_bits;

        if (unsigned bit_shift = n % base_bits) {
            unsigned overflow_shift = base_bits - bit_shift;

            lhs.items[big(0)] = rhs.items[big(items_shift)] << bit_shift;
            for (unsigned i = 1; i < item_count - items_shift; ++i) {
                lhs.items[big(i - 1)] |= rhs.items[big(items_shift + i)] >> overflow_shift;
                lhs.items[big(i)] = rhs.items[big(items_shift + i)] << bit_shift;
            }
        } else {
            for (unsigned i = 0; i < item_count - items_shift; ++i) {
                lhs.items[big(i)] = rhs.items[big(items_shift + i)];
            }
        }

        for (unsigned i = 0; i < items_shift; ++i) {
            lhs.items[little(i)] = 0;
        }
        return lhs;
    }

    constexpr static integer<Bits, Signed> shift_right(const integer<Bits, Signed>& rhs,
                                                       unsigned n) noexcept {
        integer<Bits, Signed> lhs;
        unsigned items_shift = n / base_bits;
        unsigned bit_shift = n % base_bits;

        if (bit_shift) {
            unsigned overflow_shift = base_bits - bit_shift;

            lhs.items[little(0)] = rhs.items[little(items_shift)] >> bit_shift;
            for (unsigned i = 1; i < item_count - items_shift; ++i) {
                lhs.items[little(i - 1)] |= rhs.items[little(items_shift + i)] << overflow_shift;
                lhs.items[little(i)] = rhs.items[little(items_shift + i)] >> bit_shift;
            }
        } else {
            for (unsigned i = 0; i < item_count - items_shift; ++i) {
                lhs.items[little(i)] = rhs.items[little(items_shift + i)];
            }
        }

        if (is_negative(rhs)) {
            if (bit_shift) {
                lhs.items[big(items_shift)] |= std::numeric_limits<base_type>::max()
                                               << (base_bits - bit_shift);
            }

            for (unsigned i = 0; i < items_shift; ++i) {
                lhs.items[big(i)] = std::numeric_limits<base_type>::max();
            }
        } else {
            for (unsigned i = 0; i < items_shift; ++i) {
                lhs.items[big(i)] = 0;
            }
        }

        return lhs;
    }

private:
    template <typename T>
    constexpr static base_type get_item(const T& x, unsigned idx) {
        if constexpr (IsWideInteger<T>::value) {
            if (idx < T::_impl::item_count) {
                return x.items[idx];
            }
            return 0;
        } else {
            if constexpr (sizeof(T) <= sizeof(base_type)) {
                if (little(0) == idx) {
                    return static_cast<base_type>(x);
                }
            } else if (idx * sizeof(base_type) < sizeof(T)) {
                return x >> (idx * base_bits); // & std::numeric_limits<base_type>::max()
            }
            return 0;
        }
    }

    template <typename T>
    constexpr static integer<Bits, Signed> minus(const integer<Bits, Signed>& lhs, T rhs) {
        constexpr const unsigned rhs_items =
                (sizeof(T) > sizeof(base_type)) ? (sizeof(T) / sizeof(base_type)) : 1;
        constexpr const unsigned op_items = (item_count < rhs_items) ? item_count : rhs_items;

        integer<Bits, Signed> res(lhs);
        bool underflows[item_count] = {};

        for (unsigned i = 0; i < op_items; ++i) {
            base_type rhs_item = get_item(rhs, little(i));
            base_type& res_item = res.items[little(i)];

            underflows[i] = res_item < rhs_item;
            res_item -= rhs_item;
        }

        for (unsigned i = 1; i < item_count; ++i) {
            if (underflows[i - 1]) {
                base_type& res_item = res.items[little(i)];
                if (res_item == 0) {
                    underflows[i] = true;
                }
                --res_item;
            }
        }

        return res;
    }

    template <typename T>
    constexpr static integer<Bits, Signed> plus(const integer<Bits, Signed>& lhs, T rhs) {
        constexpr const unsigned rhs_items =
                (sizeof(T) > sizeof(base_type)) ? (sizeof(T) / sizeof(base_type)) : 1;
        constexpr const unsigned op_items = (item_count < rhs_items) ? item_count : rhs_items;

        integer<Bits, Signed> res(lhs);
        bool overflows[item_count] = {};

        for (unsigned i = 0; i < op_items; ++i) {
            base_type rhs_item = get_item(rhs, little(i));
            base_type& res_item = res.items[little(i)];

            res_item += rhs_item;
            overflows[i] = res_item < rhs_item;
        }

        for (unsigned i = 1; i < item_count; ++i) {
            if (overflows[i - 1]) {
                base_type& res_item = res.items[little(i)];
                ++res_item;
                if (res_item == 0) {
                    overflows[i] = true;
                }
            }
        }

        return res;
    }

    template <typename T>
    constexpr static integer<Bits, Signed> multiply(const integer<Bits, Signed>& lhs,
                                                    const T& rhs) {
        if constexpr (Bits == 256 && sizeof(base_type) == 8) {
            /// @sa https://github.com/abseil/abseil-cpp/blob/master/absl/numeric/int128.h
            using HalfType = unsigned __int128;

            HalfType a01 = (HalfType(lhs.items[little(1)]) << 64) + lhs.items[little(0)];
            HalfType a23 = (HalfType(lhs.items[little(3)]) << 64) + lhs.items[little(2)];
            HalfType a0 = lhs.items[little(0)];
            HalfType a1 = lhs.items[little(1)];

            HalfType b01 = rhs;
            uint64_t b0 = static_cast<uint64_t>(b01);
            uint64_t b1 = 0;
            HalfType b23 = 0;
            if constexpr (sizeof(T) > 8) {
                b1 = b01 >> 64;
            }
            if constexpr (sizeof(T) > 16) {
                b23 = (HalfType(rhs.items[little(3)]) << 64) + rhs.items[little(2)];
            }

            HalfType r23 = a23 * b01 + a01 * b23 + a1 * b1;
            HalfType r01 = a0 * b0;
            HalfType r12 = (r01 >> 64) + (r23 << 64);
            HalfType r12_x = a1 * b0;

            integer<Bits, Signed> res;
            res.items[little(0)] = static_cast<base_type>(r01);
            res.items[little(3)] = r23 >> 64;

            if constexpr (sizeof(T) > 8) {
                HalfType r12_y = a0 * b1;
                r12_x += r12_y;
                if (r12_x < r12_y) {
                    ++res.items[little(3)];
                }
            }

            r12 += r12_x;
            if (r12 < r12_x) {
                ++res.items[little(3)];
            }

            res.items[little(1)] = static_cast<base_type>(r12);
            res.items[little(2)] = r12 >> 64;
            return res;
        } else if constexpr (Bits == 128 && sizeof(base_type) == 8) {
            using CompilerUInt128 = unsigned __int128;
            CompilerUInt128 a =
                    (CompilerUInt128(lhs.items[little(1)]) << 64) +
                    lhs.items[little(
                            0)]; // NOLINT(clang-analyzer-core.UndefinedBinaryOperatorResult)
            CompilerUInt128 b =
                    (CompilerUInt128(rhs.items[little(1)]) << 64) +
                    rhs.items[little(
                            0)]; // NOLINT(clang-analyzer-core.UndefinedBinaryOperatorResult)
            CompilerUInt128 c = a * b;
            integer<Bits, Signed> res;
            res.items[little(0)] = static_cast<base_type>(c);
            res.items[little(1)] = c >> 64;
            return res;
        } else {
            integer<Bits, Signed> res {};
#if 1
            integer<Bits, Signed> lhs2 = plus(lhs, shift_left(lhs, 1));
            integer<Bits, Signed> lhs3 = plus(lhs2, shift_left(lhs, 2));
#endif
            for (unsigned i = 0; i < item_count; ++i) {
                base_type rhs_item = get_item(rhs, little(i));
                unsigned pos = i * base_bits;

                while (rhs_item) {
#if 1 /// optimization
                    if ((rhs_item & 0x7) == 0x7) {
                        res = plus(res, shift_left(lhs3, pos));
                        rhs_item >>= 3;
                        pos += 3;
                        continue;
                    }

                    if ((rhs_item & 0x3) == 0x3) {
                        res = plus(res, shift_left(lhs2, pos));
                        rhs_item >>= 2;
                        pos += 2;
                        continue;
                    }
#endif
                    if (rhs_item & 1) {
                        res = plus(res, shift_left(lhs, pos));
                    }

                    rhs_item >>= 1;
                    ++pos;
                }
            }

            return res;
        }
    }

public:
    constexpr static integer<Bits, Signed> operator_unary_tilda(
            const integer<Bits, Signed>& lhs) noexcept {
        integer<Bits, Signed> res;

        for (unsigned i = 0; i < item_count; ++i) {
            res.items[any(i)] = ~lhs.items[any(i)];
        }
        return res;
    }

    constexpr static integer<Bits, Signed> operator_unary_minus(
            const integer<Bits, Signed>& lhs) noexcept(std::is_same_v<Signed, unsigned>) {
        return plus(operator_unary_tilda(lhs), 1);
    }

    template <typename T>
    constexpr static auto operator_plus(const integer<Bits, Signed>& lhs,
                                        const T& rhs) noexcept(std::is_same_v<Signed, unsigned>) {
        if constexpr (should_keep_size<T>()) {
            if (is_negative(rhs)) {
                return minus(lhs, -rhs);
            } else {
                return plus(lhs, rhs);
            }
        } else {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>, integer<T::_impl::_bits, Signed>>::
                    _impl::operator_plus(integer<T::_impl::_bits, Signed>(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static auto operator_minus(const integer<Bits, Signed>& lhs,
                                         const T& rhs) noexcept(std::is_same_v<Signed, unsigned>) {
        if constexpr (should_keep_size<T>()) {
            if (is_negative(rhs)) {
                return plus(lhs, -rhs);
            } else {
                return minus(lhs, rhs);
            }
        } else {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>, integer<T::_impl::_bits, Signed>>::
                    _impl::operator_minus(integer<T::_impl::_bits, Signed>(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static auto operator_star(const integer<Bits, Signed>& lhs, const T& rhs) {
        if constexpr (should_keep_size<T>()) {
            integer<Bits, Signed> res;

            if constexpr (std::is_signed_v<Signed>) {
                res = multiply((is_negative(lhs) ? make_positive(lhs) : lhs),
                               (is_negative(rhs) ? make_positive(rhs) : rhs));
            } else {
                res = multiply(lhs, (is_negative(rhs) ? make_positive(rhs) : rhs));
            }

            if (std::is_same_v<Signed, signed> && is_negative(lhs) != is_negative(rhs)) {
                res = operator_unary_minus(res);
            }

            return res;
        } else {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>, T>::_impl::operator_star(T(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static bool operator_greater(const integer<Bits, Signed>& lhs,
                                           const T& rhs) noexcept {
        if constexpr (should_keep_size<T>()) {
            if (std::numeric_limits<T>::is_signed && (is_negative(lhs) != is_negative(rhs))) {
                return is_negative(rhs);
            }

            integer<Bits, Signed> t = rhs;
            for (unsigned i = 0; i < item_count; ++i) {
                base_type rhs_item = get_item(t, big(i));

                if (lhs.items[big(i)] != rhs_item) {
                    return lhs.items[big(i)] > rhs_item;
                }
            }

            return false;
        } else {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>, T>::_impl::operator_greater(T(lhs),
                                                                                         rhs);
        }
    }

    template <typename T>
    constexpr static bool operator_less(const integer<Bits, Signed>& lhs, const T& rhs) noexcept {
        if constexpr (should_keep_size<T>()) {
            if (std::numeric_limits<T>::is_signed && (is_negative(lhs) != is_negative(rhs))) {
                return is_negative(lhs);
            }

            integer<Bits, Signed> t = rhs;
            for (unsigned i = 0; i < item_count; ++i) {
                base_type rhs_item = get_item(t, big(i));

                if (lhs.items[big(i)] != rhs_item) {
                    return lhs.items[big(i)] < rhs_item;
                }
            }

            return false;
        } else {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>, T>::_impl::operator_less(T(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static bool operator_eq(const integer<Bits, Signed>& lhs, const T& rhs) noexcept {
        if constexpr (should_keep_size<T>()) {
            integer<Bits, Signed> t = rhs;
            for (unsigned i = 0; i < item_count; ++i) {
                base_type rhs_item = get_item(t, any(i));

                if (lhs.items[any(i)] != rhs_item) {
                    return false;
                }
            }

            return true;
        } else {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>, T>::_impl::operator_eq(T(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static auto operator_pipe(const integer<Bits, Signed>& lhs, const T& rhs) noexcept {
        if constexpr (should_keep_size<T>()) {
            integer<Bits, Signed> res;

            for (unsigned i = 0; i < item_count; ++i) {
                res.items[little(i)] = lhs.items[little(i)] | get_item(rhs, little(i));
            }
            return res;
        } else {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>, T>::_impl::operator_pipe(T(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static auto operator_amp(const integer<Bits, Signed>& lhs, const T& rhs) noexcept {
        if constexpr (should_keep_size<T>()) {
            integer<Bits, Signed> res;

            for (unsigned i = 0; i < item_count; ++i) {
                res.items[little(i)] = lhs.items[little(i)] & get_item(rhs, little(i));
            }
            return res;
        } else {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>, T>::_impl::operator_amp(T(lhs), rhs);
        }
    }

    template <typename T>
    constexpr static bool is_zero(const T& x) {
        bool is_zero = true;
        for (auto item : x.items) {
            if (item != 0) {
                is_zero = false;
                break;
            }
        }
        return is_zero;
    }

    /// Multi-word unsigned division via Knuth's Algorithm D, a faithful port of
    /// Hacker's Delight `divmnu` (Fig. 9-2, Henry S. Warren) run in base 2^32 over
    /// 32-bit half-limbs. Working in base 2^32 keeps every intermediate product
    /// (qhat * vn[i], with both factors < 2^32) within a uint64_t, so no 128-bit
    /// arithmetic is needed and the estimate-correction stays overflow-safe.
    /// Preconditions: denominator is non-zero and occupies at least two 32-bit
    /// half-limbs (i.e. >= 2^32). Returns quotient; leaves remainder in numerator,
    /// matching the divide() contract.
    template <size_t Bits2>
    constexpr static integer<Bits2, unsigned> divide_knuth(
            integer<Bits2, unsigned>& numerator, const integer<Bits2, unsigned>& denominator) {
        constexpr unsigned half_count = Bits2 / 32; // 32-bit half-limbs (8 for 256-bit)
        constexpr uint64_t base = uint64_t(1) << 32;

        // Unpack the 64-bit limbs into little-endian 32-bit half-limbs.
        uint32_t u[half_count];
        uint32_t v[half_count];
        for (unsigned i = 0; i < item_count; ++i) {
            const uint64_t nu = numerator.items[little(i)];
            const uint64_t de = denominator.items[little(i)];
            u[2 * i] = static_cast<uint32_t>(nu);
            u[2 * i + 1] = static_cast<uint32_t>(nu >> 32);
            v[2 * i] = static_cast<uint32_t>(de);
            v[2 * i + 1] = static_cast<uint32_t>(de >> 32);
        }

        int n = half_count;
        while (n > 0 && v[n - 1] == 0) {
            --n;
        }
        int m = half_count;
        while (m > 0 && u[m - 1] == 0) {
            --m;
        }

        integer<Bits2, unsigned> quotient = 0;
        // Dividend shorter than divisor: quotient is 0 and remainder is the dividend
        // (already sitting in numerator), so nothing else to do.
        if (m < n) {
            return quotient;
        }

        // Normalize: left-shift so the divisor's top half-limb has its high bit set.
        int s = 0;
        for (uint32_t hi = v[n - 1]; (hi & 0x80000000u) == 0u; hi <<= 1) {
            ++s;
        }

        uint32_t vn[half_count];
        for (int i = n - 1; i > 0; --i) {
            vn[i] = (v[i] << s) | (s == 0 ? 0u : (v[i - 1] >> (32 - s)));
        }
        vn[0] = v[0] << s;

        uint32_t un[half_count + 1];
        un[m] = (s == 0) ? 0u : (u[m - 1] >> (32 - s));
        for (int i = m - 1; i > 0; --i) {
            un[i] = (u[i] << s) | (s == 0 ? 0u : (u[i - 1] >> (32 - s)));
        }
        un[0] = u[0] << s;

        uint32_t q[half_count];
        for (unsigned i = 0; i < half_count; ++i) {
            q[i] = 0;
        }

        for (int j = m - n; j >= 0; --j) {
            // Estimate quotient digit qhat and its remainder rhat, then correct a
            // possible overestimate. The short-circuit keeps qhat * vn[n-2] from
            // being evaluated (and overflowing) while qhat >= base.
            const uint64_t num_top = static_cast<uint64_t>(un[j + n]) * base + un[j + n - 1];
            uint64_t qhat = num_top / vn[n - 1];
            uint64_t rhat = num_top - qhat * vn[n - 1];
            while (qhat >= base || qhat * vn[n - 2] > base * rhat + un[j + n - 2]) {
                --qhat;
                rhat += vn[n - 1];
                if (rhat >= base) {
                    break;
                }
            }

            // Multiply the divisor by qhat and subtract from the running dividend,
            // propagating the borrow through k (signed).
            int64_t k = 0;
            int64_t t = 0;
            for (int i = 0; i < n; ++i) {
                const uint64_t p = qhat * vn[i];
                t = static_cast<int64_t>(un[i + j]) - k - static_cast<int64_t>(p & 0xFFFFFFFFu);
                un[i + j] = static_cast<uint32_t>(t);
                k = static_cast<int64_t>(p >> 32) - (t >> 32);
            }
            t = static_cast<int64_t>(un[j + n]) - k;
            un[j + n] = static_cast<uint32_t>(t);

            q[j] = static_cast<uint32_t>(qhat);
            if (t < 0) {
                // qhat was one too large: decrement and add the divisor back.
                --q[j];
                uint64_t carry = 0;
                for (int i = 0; i < n; ++i) {
                    const uint64_t sum = static_cast<uint64_t>(un[i + j]) + vn[i] + carry;
                    un[i + j] = static_cast<uint32_t>(sum);
                    carry = sum >> 32;
                }
                un[j + n] = static_cast<uint32_t>(static_cast<uint64_t>(un[j + n]) + carry);
            }
        }

        // Repack quotient half-limbs into 64-bit limbs.
        for (unsigned i = 0; i < item_count; ++i) {
            quotient.items[little(i)] =
                    static_cast<uint64_t>(q[2 * i]) | (static_cast<uint64_t>(q[2 * i + 1]) << 32);
        }

        // De-normalize the remainder (right-shift by s) back into numerator.
        uint32_t rem[half_count];
        for (unsigned i = 0; i < half_count; ++i) {
            rem[i] = 0;
        }
        for (int i = 0; i < n; ++i) {
            rem[i] = (un[i] >> s) | (s == 0 ? 0u : (un[i + 1] << (32 - s)));
        }
        for (unsigned i = 0; i < item_count; ++i) {
            numerator.items[little(i)] = static_cast<uint64_t>(rem[2 * i]) |
                                         (static_cast<uint64_t>(rem[2 * i + 1]) << 32);
        }

        return quotient;
    }

    /// returns quotient as result and remainder in numerator.
    template <size_t Bits2>
    constexpr static integer<Bits2, unsigned> divide(integer<Bits2, unsigned>& numerator,
                                                     integer<Bits2, unsigned> denominator) {
        static_assert(std::is_unsigned_v<Signed>);

        if constexpr (Bits == 128 && sizeof(base_type) == 8) {
            using CompilerUInt128 = unsigned __int128;

            CompilerUInt128 a =
                    (CompilerUInt128(numerator.items[little(1)]) << 64) +
                    numerator.items[little(
                            0)]; // NOLINT(clang-analyzer-core.UndefinedBinaryOperatorResult)
            CompilerUInt128 b =
                    (CompilerUInt128(denominator.items[little(1)]) << 64) +
                    denominator.items[little(
                            0)];       // NOLINT(clang-analyzer-core.UndefinedBinaryOperatorResult)
            CompilerUInt128 c = a / b; // NOLINT

            integer<Bits, Signed> res;
            res.items[little(0)] = static_cast<base_type>(c);
            res.items[little(1)] = c >> 64;

            CompilerUInt128 remainder = a - b * c;
            numerator.items[little(0)] = static_cast<base_type>(remainder);
            numerator.items[little(1)] = remainder >> 64;

            return res;
        }

        // Fast path for wider integers (Decimal256) whose operands BOTH fit in 128
        // bits: money/count magnitudes almost always do. A single hardware __int128
        // divide suffices. This layer sits ahead of the divisor-width fast paths
        // below on purpose: when the divisor is 65..128 bits those paths route to
        // Knuth Algorithm D (a multi-word estimate/correct loop), whereas here -- if
        // the dividend is also <= 128 bits -- one native divide wins outright.
        // Bit-exact with the slow path, and the remainder written back to numerator
        // keeps operator_percent correct. On a miss the extra cost is only the limb
        // scan, then control falls through to the divisor-width fast paths and the
        // generic loop below (no regression).
        if constexpr (Bits > 128 && sizeof(base_type) == 8) {
            bool operands_fit_128 = true;
            for (unsigned i = 2; i < item_count; ++i) {
                if (numerator.items[little(i)] != 0 || denominator.items[little(i)] != 0) {
                    operands_fit_128 = false;
                    break;
                }
            }
            if (operands_fit_128) {
                using CompilerUInt128 = unsigned __int128;
                CompilerUInt128 b = (CompilerUInt128(denominator.items[little(1)]) << 64) +
                                    denominator.items[little(0)];
                // A zero denominator falls through to the throw below; never divide by it.
                if (b != 0) {
                    CompilerUInt128 a = (CompilerUInt128(numerator.items[little(1)]) << 64) +
                                        numerator.items[little(0)];
                    CompilerUInt128 c = a / b;

                    integer<Bits2, unsigned> res;
                    res.items[little(0)] = static_cast<base_type>(c);
                    res.items[little(1)] = static_cast<base_type>(c >> 64);
                    for (unsigned i = 2; i < item_count; ++i) {
                        res.items[little(i)] = 0;
                    }

                    CompilerUInt128 remainder = a - b * c;
                    numerator.items[little(0)] = static_cast<base_type>(remainder);
                    numerator.items[little(1)] = static_cast<base_type>(remainder >> 64);
                    for (unsigned i = 2; i < item_count; ++i) {
                        numerator.items[little(i)] = 0;
                    }

                    return res;
                }
            }
        }

        if (is_zero(denominator)) {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT, "Division by zero");
        }

        /// Fast path for a divisor that fits in a single 64-bit limb: schoolbook long
        /// division word-by-word from the most- to least-significant limb. Each step
        /// divides {remainder : current_word} (128 bits) by the 64-bit divisor, which
        /// lowers to a single hardware divide (e.g. `divq` on x86-64). O(item_count)
        /// divides vs ~Bits iterations of the generic binary long division below.
        {
            bool divisor_is_single_limb = true;
            for (unsigned i = 1; i < item_count; ++i) {
                if (denominator.items[little(i)] != 0) {
                    divisor_is_single_limb = false;
                    break;
                }
            }

            if (divisor_is_single_limb) {
                using CompilerUInt128 = unsigned __int128;
                // Non-zero divisor guaranteed: denominator != 0 (checked) and high limbs are 0.
                const CompilerUInt128 d = denominator.items[little(0)];

                integer<Bits2, unsigned> quotient = 0;
                CompilerUInt128 remainder = 0;
                for (int i = static_cast<int>(item_count) - 1; i >= 0; --i) {
                    // remainder < d < 2^64, so cur < 2^128 and cur / d < 2^64 (fits one limb).
                    const CompilerUInt128 cur =
                            (remainder << base_bits) |
                            static_cast<CompilerUInt128>(numerator.items[little(i)]);
                    quotient.items[little(i)] = static_cast<base_type>(cur / d);
                    remainder = cur % d;
                }

                // Contract: return quotient, leave remainder (single limb) in numerator.
                for (unsigned i = 0; i < item_count; ++i) {
                    numerator.items[little(i)] = 0;
                }
                numerator.items[little(0)] = static_cast<base_type>(remainder);
                return quotient;
            }
        }

        /// Fast path for a divisor that fits in two 64-bit limbs (128-bit): route it
        /// through Knuth's Algorithm D (base 2^32). Reaching here means the divisor
        /// did not fit a single limb, so it has >= 3 significant 32-bit half-limbs.
        /// Divisors wider than 128 bits fall through to the generic binary long
        /// division below.
        if constexpr (item_count > 2) {
            bool divisor_fits_two_limbs = true;
            for (unsigned i = 2; i < item_count; ++i) {
                if (denominator.items[little(i)] != 0) {
                    divisor_fits_two_limbs = false;
                    break;
                }
            }
            if (divisor_fits_two_limbs) {
                return divide_knuth(numerator, denominator);
            }
        }

        integer<Bits2, unsigned> x = 1;
        integer<Bits2, unsigned> quotient = 0;

        while (!operator_greater(denominator, numerator) &&
               is_zero(operator_amp(shift_right(denominator, Bits2 - 1), 1))) {
            x = shift_left(x, 1);
            denominator = shift_left(denominator, 1);
        }

        while (!is_zero(x)) {
            if (!operator_greater(denominator, numerator)) {
                numerator = operator_minus(numerator, denominator);
                quotient = operator_pipe(quotient, x);
            }

            x = shift_right(x, 1);
            denominator = shift_right(denominator, 1);
        }

        return quotient;
    }

    template <typename T>
    constexpr static auto operator_slash(const integer<Bits, Signed>& lhs, const T& rhs) {
        if constexpr (should_keep_size<T>()) {
            integer<Bits, unsigned> numerator = make_positive(lhs);
            integer<Bits, unsigned> denominator = make_positive(integer<Bits, Signed>(rhs));
            integer<Bits, unsigned> quotient =
                    integer<Bits, unsigned>::_impl::divide(numerator, std::move(denominator));

            if (std::is_same_v<Signed, signed> && is_negative(rhs) != is_negative(lhs))
                quotient = operator_unary_minus(quotient);
            return quotient;
        } else {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>,
                                      integer<T::_impl::_bits, Signed>>::operator_slash(T(lhs),
                                                                                        rhs);
        }
    }

    template <typename T>
    constexpr static auto operator_percent(const integer<Bits, Signed>& lhs, const T& rhs) {
        if constexpr (should_keep_size<T>()) {
            integer<Bits, unsigned> remainder = make_positive(lhs);
            integer<Bits, unsigned> denominator = make_positive(integer<Bits, Signed>(rhs));
            integer<Bits, unsigned>::_impl::divide(remainder, std::move(denominator));

            if (std::is_same_v<Signed, signed> && is_negative(lhs)) {
                remainder = operator_unary_minus(remainder);
            }
            return remainder;
        } else {
            static_assert(IsWideInteger<T>::value);
            return std::common_type_t<integer<Bits, Signed>,
                                      integer<T::_impl::_bits, Signed>>::operator_percent(T(lhs),
                                                                                          rhs);
        }
    }

    // ^
    template <typename T>
    constexpr static auto operator_circumflex(const integer<Bits, Signed>& lhs,
                                              const T& rhs) noexcept {
        if constexpr (should_keep_size<T>()) {
            integer<Bits, Signed> t(rhs);
            integer<Bits, Signed> res = lhs;

            for (unsigned i = 0; i < item_count; ++i) {
                res.items[any(i)] ^= t.items[any(i)];
            }
            return res;
        } else {
            static_assert(IsWideInteger<T>::value);
            return T::operator_circumflex(T(lhs), rhs);
        }
    }

    constexpr static integer<Bits, Signed> from_str(const char* c) {
        integer<Bits, Signed> res = 0;

        bool is_neg = std::is_same_v<Signed, signed> && *c == '-';
        if (is_neg) {
            ++c;
        }

        if (*c == '0' && (*(c + 1) == 'x' || *(c + 1) == 'X')) { // hex
            ++c;
            ++c;
            while (*c) {
                if (*c >= '0' && *c <= '9') {
                    res = multiply(res, 16U);
                    res = plus(res, *c - '0');
                    ++c;
                } else if (*c >= 'a' && *c <= 'f') {
                    res = multiply(res, 16U);
                    res = plus(res, *c - 'a' + 10U);
                    ++c;
                } else if (*c >= 'A' &&
                           *c <= 'F') { // tolower must be used, but it is not constexpr
                    res = multiply(res, 16U);
                    res = plus(res, *c - 'A' + 10U);
                    ++c;
                } else {
                    throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT, "Invalid char from");
                }
            }
        } else { // dec
            while (*c) {
                if (*c < '0' || *c > '9') {
                    throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT, "Invalid char from");
                }

                res = multiply(res, 10U);
                res = plus(res, *c - '0');
                ++c;
            }
        }

        if (is_neg) {
            res = operator_unary_minus(res);
        }

        return res;
    }
};

// Members

template <size_t Bits, typename Signed>
template <size_t Bits2, typename Signed2>
constexpr integer<Bits, Signed>::integer(const integer<Bits2, Signed2> rhs) noexcept : items {} {
    _impl::wide_integer_from_wide_integer(*this, rhs);
}

template <size_t Bits, typename Signed>
template <typename T>
    requires(std::is_arithmetic_v<T> || std::is_same_v<T, __int128> ||
             std::is_same_v<T, unsigned __int128>)
constexpr integer<Bits, Signed>::integer(T rhs) noexcept : items {} {
    if constexpr (IsTupleLike<T>::value) {
        _impl::wide_integer_from_tuple_like(*this, rhs);
    } else {
        _impl::wide_integer_from_builtin(*this, rhs);
    }
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed>::integer(std::initializer_list<T> il) noexcept : items {} {
    if (il.size() == 1) {
        if constexpr (IsWideInteger<T>::value) {
            _impl::wide_integer_from_wide_integer(*this, *il.begin());
        } else if constexpr (IsTupleLike<T>::value) {
            _impl::wide_integer_from_tuple_like(*this, *il.begin());
        } else {
            _impl::wide_integer_from_builtin(*this, *il.begin());
        }
    } else if (il.size() == 0) {
        _impl::wide_integer_from_builtin(*this, 0);
    } else {
        auto it = il.begin();
        for (unsigned i = 0; i < _impl::item_count; ++i) {
            if (it < il.end()) {
                items[_impl::little(i)] = static_cast<base_type>(*it);
                ++it;
            } else {
                items[_impl::little(i)] = 0;
            }
        }
    }
}

template <size_t Bits, typename Signed>
template <size_t Bits2, typename Signed2>
constexpr integer<Bits, Signed>& integer<Bits, Signed>::operator=(
        const integer<Bits2, Signed2>& rhs) noexcept {
    _impl::wide_integer_from_wide_integer(*this, rhs);
    return *this;
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed>& integer<Bits, Signed>::operator=(T rhs) noexcept {
    if constexpr (IsTupleLike<T>::value) {
        _impl::wide_integer_from_tuple_like(*this, rhs);
    } else {
        _impl::wide_integer_from_builtin(*this, rhs);
    }
    return *this;
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed>& integer<Bits, Signed>::operator*=(const T& rhs) {
    *this = *this * rhs;
    return *this;
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed>& integer<Bits, Signed>::operator/=(const T& rhs) {
    *this = *this / rhs;
    return *this;
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed>& integer<Bits, Signed>::operator+=(const T& rhs) noexcept(
        std::is_same_v<Signed, unsigned>) {
    *this = *this + rhs;
    return *this;
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed>& integer<Bits, Signed>::operator-=(const T& rhs) noexcept(
        std::is_same_v<Signed, unsigned>) {
    *this = *this - rhs;
    return *this;
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed>& integer<Bits, Signed>::operator%=(const T& rhs) {
    *this = *this % rhs;
    return *this;
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed>& integer<Bits, Signed>::operator&=(const T& rhs) noexcept {
    *this = *this & rhs;
    return *this;
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed>& integer<Bits, Signed>::operator|=(const T& rhs) noexcept {
    *this = *this | rhs;
    return *this;
}

template <size_t Bits, typename Signed>
template <typename T>
constexpr integer<Bits, Signed>& integer<Bits, Signed>::operator^=(const T& rhs) noexcept {
    *this = *this ^ rhs;
    return *this;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed>& integer<Bits, Signed>::operator<<=(int n) noexcept {
    if (static_cast<size_t>(n) >= Bits) {
        *this = 0;
    } else if (n > 0) {
        *this = _impl::shift_left(*this, n);
    }
    return *this;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed>& integer<Bits, Signed>::operator>>=(int n) noexcept {
    if (static_cast<size_t>(n) >= Bits) {
        if (_impl::is_negative(*this)) {
            *this = -1;
        } else {
            *this = 0;
        }
    } else if (n > 0) {
        *this = _impl::shift_right(*this, n);
    }
    return *this;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed>& integer<Bits, Signed>::operator++() noexcept(
        std::is_same_v<Signed, unsigned>) {
    *this = _impl::operator_plus(*this, 1);
    return *this;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> integer<Bits, Signed>::operator++(int) noexcept(
        std::is_same_v<Signed, unsigned>) {
    auto tmp = *this;
    *this = _impl::operator_plus(*this, 1);
    return tmp;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed>& integer<Bits, Signed>::operator--() noexcept(
        std::is_same_v<Signed, unsigned>) {
    *this = _impl::operator_minus(*this, 1);
    return *this;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> integer<Bits, Signed>::operator--(int) noexcept(
        std::is_same_v<Signed, unsigned>) {
    auto tmp = *this;
    *this = _impl::operator_minus(*this, 1);
    return tmp;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed>::operator bool() const noexcept {
    return !_impl::operator_eq(*this, 0);
}

template <size_t Bits, typename Signed>
template <class T, class>
constexpr integer<Bits, Signed>::operator T() const noexcept {
    static_assert(std::numeric_limits<T>::is_integer);

    /// NOTE: memcpy will suffice, but unfortunately, this function is constexpr.

    using UnsignedT = std::make_unsigned_t<T>;

    UnsignedT res {};
    for (unsigned i = 0;
         i < _impl::item_count && i < (sizeof(T) + sizeof(base_type) - 1) / sizeof(base_type);
         ++i) {
        res += UnsignedT(items[_impl::little(i)])
               << (sizeof(base_type) * 8 *
                   i); // NOLINT(clang-analyzer-core.UndefinedBinaryOperatorResult)
    }

    return res;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed>::operator long double() const noexcept {
    if (_impl::operator_eq(*this, 0)) {
        return 0;
    }

    integer<Bits, Signed> tmp = *this;
    if (_impl::is_negative(*this)) {
        tmp = -tmp;
    }

    long double res = 0;
    for (unsigned i = 0; i < _impl::item_count; ++i) {
        long double t = res;
        res *= static_cast<long double>(std::numeric_limits<base_type>::max());
        res += t;
        res += static_cast<long double>(tmp.items[_impl::big(i)]);
    }

    if (_impl::is_negative(*this)) {
        res = -res;
    }

    return res;
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed>::operator double() const noexcept {
    return static_cast<double>(static_cast<long double>(*this));
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed>::operator float() const noexcept {
    return static_cast<float>(static_cast<long double>(*this));
}

// Unary operators
template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> operator~(const integer<Bits, Signed>& lhs) noexcept {
    return integer<Bits, Signed>::_impl::operator_unary_tilda(lhs);
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> operator-(const integer<Bits, Signed>& lhs) noexcept(
        std::is_same_v<Signed, unsigned>) {
    return integer<Bits, Signed>::_impl::operator_unary_minus(lhs);
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> operator+(const integer<Bits, Signed>& lhs) noexcept(
        std::is_same_v<Signed, unsigned>) {
    return lhs;
}

#define CT(x)                                                                      \
    std::common_type_t<std::decay_t<decltype(rhs)>, std::decay_t<decltype(lhs)>> { \
        x                                                                          \
    }

// Binary operators
template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr operator*(
        const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs) {
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_star(
            lhs, rhs);
}

template <typename Arithmetic, typename Arithmetic2, class>
std::common_type_t<Arithmetic, Arithmetic2> constexpr operator*(const Arithmetic& lhs,
                                                                const Arithmetic2& rhs) {
    return CT(lhs) * CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr operator/(
        const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs) {
    return std::common_type_t<integer<Bits, Signed>,
                              integer<Bits2, Signed2>>::_impl::operator_slash(lhs, rhs);
}
template <typename Arithmetic, typename Arithmetic2, class>
std::common_type_t<Arithmetic, Arithmetic2> constexpr operator/(const Arithmetic& lhs,
                                                                const Arithmetic2& rhs) {
    return CT(lhs) / CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr operator+(
        const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs) {
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_plus(
            lhs, rhs);
}
template <typename Arithmetic, typename Arithmetic2, class>
std::common_type_t<Arithmetic, Arithmetic2> constexpr operator+(const Arithmetic& lhs,
                                                                const Arithmetic2& rhs) {
    return CT(lhs) + CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr operator-(
        const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs) {
    return std::common_type_t<integer<Bits, Signed>,
                              integer<Bits2, Signed2>>::_impl::operator_minus(lhs, rhs);
}
template <typename Arithmetic, typename Arithmetic2, class>
std::common_type_t<Arithmetic, Arithmetic2> constexpr operator-(const Arithmetic& lhs,
                                                                const Arithmetic2& rhs) {
    return CT(lhs) - CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr operator%(
        const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs) {
    return std::common_type_t<integer<Bits, Signed>,
                              integer<Bits2, Signed2>>::_impl::operator_percent(lhs, rhs);
}
template <typename Integral, typename Integral2, class>
std::common_type_t<Integral, Integral2> constexpr operator%(const Integral& lhs,
                                                            const Integral2& rhs) {
    return CT(lhs) % CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr operator&(
        const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs) {
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_amp(
            lhs, rhs);
}
template <typename Integral, typename Integral2, class>
std::common_type_t<Integral, Integral2> constexpr operator&(const Integral& lhs,
                                                            const Integral2& rhs) {
    return CT(lhs) & CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr operator|(
        const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs) {
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_pipe(
            lhs, rhs);
}
template <typename Integral, typename Integral2, class>
std::common_type_t<Integral, Integral2> constexpr operator|(const Integral& lhs,
                                                            const Integral2& rhs) {
    return CT(lhs) | CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>> constexpr operator^(
        const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs) {
    return std::common_type_t<integer<Bits, Signed>,
                              integer<Bits2, Signed2>>::_impl::operator_circumflex(lhs, rhs);
}
template <typename Integral, typename Integral2, class>
std::common_type_t<Integral, Integral2> constexpr operator^(const Integral& lhs,
                                                            const Integral2& rhs) {
    return CT(lhs) ^ CT(rhs);
}

template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> operator<<(const integer<Bits, Signed>& lhs, int n) noexcept {
    if (static_cast<size_t>(n) >= Bits) {
        return integer<Bits, Signed>(0);
    }
    if (n <= 0) {
        return lhs;
    }
    return integer<Bits, Signed>::_impl::shift_left(lhs, n);
}
template <size_t Bits, typename Signed>
constexpr integer<Bits, Signed> operator>>(const integer<Bits, Signed>& lhs, int n) noexcept {
    if (static_cast<size_t>(n) >= Bits) {
        return integer<Bits, Signed>(0);
    }
    if (n <= 0) {
        return lhs;
    }
    return integer<Bits, Signed>::_impl::shift_right(lhs, n);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator<(const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs) {
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_less(
            lhs, rhs);
}
template <typename Arithmetic, typename Arithmetic2, class>
constexpr bool operator<(const Arithmetic& lhs, const Arithmetic2& rhs) {
    return CT(lhs) < CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator>(const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs) {
    return std::common_type_t<integer<Bits, Signed>,
                              integer<Bits2, Signed2>>::_impl::operator_greater(lhs, rhs);
}
template <typename Arithmetic, typename Arithmetic2, class>
constexpr bool operator>(const Arithmetic& lhs, const Arithmetic2& rhs) {
    return CT(lhs) > CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator<=(const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs) {
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_less(
                   lhs, rhs) ||
           std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_eq(
                   lhs, rhs);
}
template <typename Arithmetic, typename Arithmetic2, class>
constexpr bool operator<=(const Arithmetic& lhs, const Arithmetic2& rhs) {
    return CT(lhs) <= CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator>=(const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs) {
    return std::common_type_t<integer<Bits, Signed>,
                              integer<Bits2, Signed2>>::_impl::operator_greater(lhs, rhs) ||
           std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_eq(
                   lhs, rhs);
}
template <typename Arithmetic, typename Arithmetic2, class>
constexpr bool operator>=(const Arithmetic& lhs, const Arithmetic2& rhs) {
    return CT(lhs) >= CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator==(const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs) {
    return std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_eq(
            lhs, rhs);
}
template <typename Arithmetic, typename Arithmetic2, class>
constexpr bool operator==(const Arithmetic& lhs, const Arithmetic2& rhs) {
    return CT(lhs) == CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr bool operator!=(const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs) {
    return !std::common_type_t<integer<Bits, Signed>, integer<Bits2, Signed2>>::_impl::operator_eq(
            lhs, rhs);
}
template <typename Arithmetic, typename Arithmetic2, class>
constexpr bool operator!=(const Arithmetic& lhs, const Arithmetic2& rhs) {
    return CT(lhs) != CT(rhs);
}

template <size_t Bits, typename Signed, size_t Bits2, typename Signed2>
constexpr auto operator<=>(const integer<Bits, Signed>& lhs, const integer<Bits2, Signed2>& rhs) {
    return (lhs == rhs ? std::strong_ordering::equal
                       : (lhs > rhs ? std::strong_ordering::greater : std::strong_ordering::less));
}

template <typename Arithmetic, typename Arithmetic2, class>
constexpr auto operator<=>(const Arithmetic& rhs, const Arithmetic2& lhs) {
    return (lhs == rhs ? std::strong_ordering::equal
                       : (lhs > rhs ? std::strong_ordering::greater : std::strong_ordering::less));
}

#undef CT

} // namespace wide

namespace std {

template <size_t Bits, typename Signed>
struct hash<wide::integer<Bits, Signed>> {
    std::size_t operator()(const wide::integer<Bits, Signed>& lhs) const {
        static_assert(Bits % (sizeof(size_t) * 8) == 0);

        const auto* ptr = reinterpret_cast<const size_t*>(lhs.items);
        unsigned count = Bits / (sizeof(size_t) * 8);

        size_t res = 0;
        for (unsigned i = 0; i < count; ++i) {
            res ^= ptr[i];
        }
        return res;
    }
};

} // namespace std

// NOLINTEND(*)
