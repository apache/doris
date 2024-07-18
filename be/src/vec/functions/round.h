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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionRound.h
// and modified by Doris

#pragma once

#include <cstddef>
#include <memory>

#include "common/exception.h"
#include "common/status.h"
#include "vec/columns/column_const.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exec/format/format_common.h"
#include "vec/functions/function.h"
#if defined(__SSE4_1__) || defined(__aarch64__)
#include "util/sse_util.hpp"
#else
#include <fenv.h>
#endif
#include <algorithm>

#include "vec/columns/column.h"
#include "vec/columns/column_decimal.h"
#include "vec/core/call_on_type_index.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

enum class ScaleMode {
    Positive, // round to a number with N decimal places after the decimal point
    Negative, // round to an integer with N zero characters
    Zero,     // round to an integer
};

enum class RoundingMode {
#if defined(__SSE4_1__) || defined(__aarch64__)
    Round = _MM_FROUND_TO_NEAREST_INT | _MM_FROUND_NO_EXC,
    Floor = _MM_FROUND_TO_NEG_INF | _MM_FROUND_NO_EXC,
    Ceil = _MM_FROUND_TO_POS_INF | _MM_FROUND_NO_EXC,
    Trunc = _MM_FROUND_TO_ZERO | _MM_FROUND_NO_EXC,
#else
    Round = 8, /// Values are correspond to above just in case.
    Floor = 9,
    Ceil = 10,
    Trunc = 11,
#endif
};

enum class TieBreakingMode {
    Auto,    // use round up
    Bankers, // use banker's rounding
};

template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode,
          TieBreakingMode tie_breaking_mode>
struct IntegerRoundingComputation {
    static const size_t data_count = 1;

    static size_t prepare(size_t scale) { return scale; }

    /// Integer overflow is Ok.
    static ALWAYS_INLINE T compute_impl(T x, T scale, T target_scale) {
        switch (rounding_mode) {
        case RoundingMode::Trunc: {
            return target_scale > 1 ? x / scale * target_scale : x / scale;
        }
        case RoundingMode::Floor: {
            if (x < 0) {
                x -= scale - 1;
            }
            return target_scale > 1 ? x / scale * target_scale : x / scale;
        }
        case RoundingMode::Ceil: {
            if (x >= 0) {
                x += scale - 1;
            }
            return target_scale > 1 ? x / scale * target_scale : x / scale;
        }
        case RoundingMode::Round: {
            if (x < 0) {
                x -= scale;
            }
            switch (tie_breaking_mode) {
            case TieBreakingMode::Auto: {
                x = (x + scale / 2) / scale;
                break;
            }
            case TieBreakingMode::Bankers: {
                T quotient = (x + scale / 2) / scale;
                if (quotient * scale == x + scale / 2) {
                    // round half to even
                    x = (quotient + (x < 0)) & ~1;
                } else {
                    // round the others as usual
                    x = quotient;
                }
                break;
            }
            }
            return target_scale > 1 ? x * target_scale : x;
        }
        }
        LOG(FATAL) << "__builtin_unreachable";
        __builtin_unreachable();
    }

    static ALWAYS_INLINE T compute(T x, T scale, T target_scale) {
        switch (scale_mode) {
        case ScaleMode::Zero:
        case ScaleMode::Positive:
            return x;
        case ScaleMode::Negative:
            return compute_impl(x, scale, target_scale);
        }
        LOG(FATAL) << "__builtin_unreachable";
        __builtin_unreachable();
    }

    static ALWAYS_INLINE void compute(const T* __restrict in, size_t scale, T* __restrict out,
                                      size_t target_scale) {
        if constexpr (sizeof(T) <= sizeof(scale) && scale_mode == ScaleMode::Negative) {
            if (scale > size_t(std::numeric_limits<T>::max())) {
                *out = 0;
                return;
            }
        }
        *out = compute(*in, scale, target_scale);
    }
};

template <typename T, RoundingMode rounding_mode, TieBreakingMode tie_breaking_mode>
class DecimalRoundingImpl {
private:
    using NativeType = typename T::NativeType;
    using Op = IntegerRoundingComputation<NativeType, rounding_mode, ScaleMode::Negative,
                                          tie_breaking_mode>;
    using Container = typename ColumnDecimal<T>::Container;

public:
    static NO_INLINE void apply(const Container& in, UInt32 in_scale, Container& out,
                                Int16 out_scale) {
        Int16 scale_arg = in_scale - out_scale;
        if (scale_arg > 0) {
            auto scale = DecimalScaleParams::get_scale_factor<T>(scale_arg);

            const NativeType* __restrict p_in = reinterpret_cast<const NativeType*>(in.data());
            const NativeType* end_in = reinterpret_cast<const NativeType*>(in.data()) + in.size();
            NativeType* __restrict p_out = reinterpret_cast<NativeType*>(out.data());

            if (out_scale < 0) {
                auto negative_scale = DecimalScaleParams::get_scale_factor<T>(-out_scale);
                while (p_in < end_in) {
                    *p_out = Op::compute(*p_in, scale, negative_scale);
                    ++p_in;
                    ++p_out;
                }
            } else {
                while (p_in < end_in) {
                    *p_out = Op::compute(*p_in, scale, 1);
                    ++p_in;
                    ++p_out;
                }
            }
        } else {
            memcpy(out.data(), in.data(), in.size() * sizeof(T));
        }
    }

    static NO_INLINE void apply(const NativeType& in, UInt32 in_scale, NativeType& out,
                                Int16 out_scale) {
        Int16 scale_arg = in_scale - out_scale;
        if (scale_arg > 0) {
            auto scale = DecimalScaleParams::get_scale_factor<T>(scale_arg);
            if (out_scale < 0) {
                auto negative_scale = DecimalScaleParams::get_scale_factor<T>(-out_scale);
                out = Op::compute(in, scale, negative_scale);
            } else {
                out = Op::compute(in, scale, 1);
            }
        } else {
            memcpy(&out, &in, sizeof(NativeType));
        }
    }
};

template <TieBreakingMode tie_breaking_mode>
inline float roundWithMode(float x, RoundingMode mode) {
    switch (mode) {
    case RoundingMode::Round: {
        if constexpr (tie_breaking_mode == TieBreakingMode::Bankers) {
            return nearbyintf(x);
        } else {
            return roundf(x);
        }
    }
    case RoundingMode::Floor:
        return floorf(x);
    case RoundingMode::Ceil:
        return ceilf(x);
    case RoundingMode::Trunc:
        return truncf(x);
    }

    LOG(FATAL) << "__builtin_unreachable";
    __builtin_unreachable();
}

template <TieBreakingMode tie_breaking_mode>
inline double roundWithMode(double x, RoundingMode mode) {
    switch (mode) {
    case RoundingMode::Round: {
        if constexpr (tie_breaking_mode == TieBreakingMode::Bankers) {
            return nearbyint(x);
        } else {
            return round(x);
        }
    }
    case RoundingMode::Floor:
        return floor(x);
    case RoundingMode::Ceil:
        return ceil(x);
    case RoundingMode::Trunc:
        return trunc(x);
    }

    LOG(FATAL) << "__builtin_unreachable";
    __builtin_unreachable();
}

template <typename T, TieBreakingMode tie_breaking_mode>
class BaseFloatRoundingComputation {
public:
    using ScalarType = T;
    using VectorType = T;
    static const size_t data_count = 1;

    static VectorType load(const ScalarType* in) { return *in; }
    static VectorType load1(const ScalarType in) { return in; }
    static VectorType store(ScalarType* out, ScalarType val) { return *out = val; }
    static VectorType multiply(VectorType val, VectorType scale) { return val * scale; }
    static VectorType divide(VectorType val, VectorType scale) { return val / scale; }
    template <RoundingMode mode>
    static VectorType apply(VectorType val) {
        return roundWithMode<tie_breaking_mode>(val, mode);
    }

    static VectorType prepare(size_t scale) { return load1(scale); }
};

/** Implementation of low-level round-off functions for floating-point values.
  */
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode,
          TieBreakingMode tie_breaking_mode>
class FloatRoundingComputation : public BaseFloatRoundingComputation<T, tie_breaking_mode> {
    using Base = BaseFloatRoundingComputation<T, tie_breaking_mode>;

public:
    static inline void compute(const T* __restrict in, const typename Base::VectorType& scale,
                               T* __restrict out) {
        auto val = Base::load(in);

        if (scale_mode == ScaleMode::Positive) {
            val = Base::multiply(val, scale);
        } else if (scale_mode == ScaleMode::Negative) {
            val = Base::divide(val, scale);
        }

        val = Base::template apply<rounding_mode>(val);

        if (scale_mode == ScaleMode::Positive) {
            val = Base::divide(val, scale);
        } else if (scale_mode == ScaleMode::Negative) {
            val = Base::multiply(val, scale);
        }

        Base::store(out, val);
    }
};

/** Implementing high-level rounding functions.
  */
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode,
          TieBreakingMode tie_breaking_mode>
struct FloatRoundingImpl {
private:
    static_assert(!IsDecimalNumber<T>);

    using Op = FloatRoundingComputation<T, rounding_mode, scale_mode, tie_breaking_mode>;
    using Data = std::array<T, Op::data_count>;
    using ColumnType = ColumnVector<T>;
    using Container = typename ColumnType::Container;

public:
    static NO_INLINE void apply(const Container& in, size_t scale, Container& out) {
        auto mm_scale = Op::prepare(scale);

        const size_t data_count = std::tuple_size<Data>();

        const T* end_in = in.data() + in.size();
        const T* limit = in.data() + in.size() / data_count * data_count;

        const T* __restrict p_in = in.data();
        T* __restrict p_out = out.data();

        while (p_in < limit) {
            Op::compute(p_in, mm_scale, p_out);
            p_in += data_count;
            p_out += data_count;
        }

        if (p_in < end_in) {
            Data tmp_src {{}};
            Data tmp_dst;

            size_t tail_size_bytes = (end_in - p_in) * sizeof(*p_in);

            memcpy(&tmp_src, p_in, tail_size_bytes);
            Op::compute(reinterpret_cast<T*>(&tmp_src), mm_scale, reinterpret_cast<T*>(&tmp_dst));
            memcpy(p_out, &tmp_dst, tail_size_bytes);
        }
    }

    static NO_INLINE void apply(const T& in, size_t scale, T& out) {
        auto mm_scale = Op::prepare(scale);
        Op::compute(&in, mm_scale, &out);
    }
};

template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode,
          TieBreakingMode tie_breaking_mode>
struct IntegerRoundingImpl {
private:
    using Op = IntegerRoundingComputation<T, rounding_mode, scale_mode, tie_breaking_mode>;
    using Container = typename ColumnVector<T>::Container;

public:
    template <size_t scale>
    static NO_INLINE void applyImpl(const Container& in, Container& out) {
        const T* end_in = in.data() + in.size();

        const T* __restrict p_in = in.data();
        T* __restrict p_out = out.data();

        while (p_in < end_in) {
            Op::compute(p_in, scale, p_out, 1);
            ++p_in;
            ++p_out;
        }
    }

    static NO_INLINE void apply(const Container& in, size_t scale, Container& out) {
        /// Manual function cloning for compiler to generate integer division by constant.
        switch (scale) {
        case 1ULL:
            return applyImpl<1ULL>(in, out);
        case 10ULL:
            return applyImpl<10ULL>(in, out);
        case 100ULL:
            return applyImpl<100ULL>(in, out);
        case 1000ULL:
            return applyImpl<1000ULL>(in, out);
        case 10000ULL:
            return applyImpl<10000ULL>(in, out);
        case 100000ULL:
            return applyImpl<100000ULL>(in, out);
        case 1000000ULL:
            return applyImpl<1000000ULL>(in, out);
        case 10000000ULL:
            return applyImpl<10000000ULL>(in, out);
        case 100000000ULL:
            return applyImpl<100000000ULL>(in, out);
        case 1000000000ULL:
            return applyImpl<1000000000ULL>(in, out);
        case 10000000000ULL:
            return applyImpl<10000000000ULL>(in, out);
        case 100000000000ULL:
            return applyImpl<100000000000ULL>(in, out);
        case 1000000000000ULL:
            return applyImpl<1000000000000ULL>(in, out);
        case 10000000000000ULL:
            return applyImpl<10000000000000ULL>(in, out);
        case 100000000000000ULL:
            return applyImpl<100000000000000ULL>(in, out);
        case 1000000000000000ULL:
            return applyImpl<1000000000000000ULL>(in, out);
        case 10000000000000000ULL:
            return applyImpl<10000000000000000ULL>(in, out);
        case 100000000000000000ULL:
            return applyImpl<100000000000000000ULL>(in, out);
        case 1000000000000000000ULL:
            return applyImpl<1000000000000000000ULL>(in, out);
        case 10000000000000000000ULL:
            return applyImpl<10000000000000000000ULL>(in, out);
        default:
            LOG(FATAL) << "__builtin_unreachable";
            __builtin_unreachable();
        }
    }

    static NO_INLINE void apply(const T& in, size_t scale, T& out) {
        Op::compute(&in, scale, &out, 1);
    }
};

/** Select the appropriate processing algorithm depending on the scale.
  */
template <typename T, RoundingMode rounding_mode, TieBreakingMode tie_breaking_mode>
struct Dispatcher {
    template <ScaleMode scale_mode>
    using FunctionRoundingImpl = std::conditional_t<
            IsDecimalNumber<T>, DecimalRoundingImpl<T, rounding_mode, tie_breaking_mode>,
            std::conditional_t<
                    std::is_floating_point_v<T>,
                    FloatRoundingImpl<T, rounding_mode, scale_mode, tie_breaking_mode>,
                    IntegerRoundingImpl<T, rounding_mode, scale_mode, tie_breaking_mode>>>;

    // scale_arg: scale for function computation
    // result_scale: scale for result decimal, this scale is got from planner
    static ColumnPtr apply_vec_const(const IColumn* col_general, const Int16 scale_arg,
                                     [[maybe_unused]] Int16 result_scale) {
        if constexpr (IsNumber<T>) {
            const auto* const col = check_and_get_column<ColumnVector<T>>(col_general);
            auto col_res = ColumnVector<T>::create();

            typename ColumnVector<T>::Container& vec_res = col_res->get_data();
            vec_res.resize(col->get_data().size());

            if (!vec_res.empty()) {
                if (scale_arg == 0) {
                    size_t scale = 1;
                    FunctionRoundingImpl<ScaleMode::Zero>::apply(col->get_data(), scale, vec_res);
                } else if (scale_arg > 0) {
                    size_t scale = int_exp10(scale_arg);
                    FunctionRoundingImpl<ScaleMode::Positive>::apply(col->get_data(), scale,
                                                                     vec_res);
                } else {
                    size_t scale = int_exp10(-scale_arg);
                    FunctionRoundingImpl<ScaleMode::Negative>::apply(col->get_data(), scale,
                                                                     vec_res);
                }
            }

            return col_res;
        } else if constexpr (IsDecimalNumber<T>) {
            const auto* const decimal_col = check_and_get_column<ColumnDecimal<T>>(col_general);
            const auto& vec_src = decimal_col->get_data();
            const size_t input_rows_count = vec_src.size();
            auto col_res = ColumnDecimal<T>::create(vec_src.size(), result_scale);
            auto& vec_res = col_res->get_data();

            if (!vec_res.empty()) {
                FunctionRoundingImpl<ScaleMode::Negative>::apply(
                        decimal_col->get_data(), decimal_col->get_scale(), vec_res, scale_arg);
            }
            // We need to always make sure result decimal's scale is as expected as its in plan
            // So we need to append enough zero to result.

            // Case 0: scale_arg <= -(integer part digits count)
            //      do nothing, because result is 0
            // Case 1: scale_arg <= 0 && scale_arg > -(integer part digits count)
            //      decimal parts has been erased, so add them back by multiply 10^(result_scale)
            // Case 2: scale_arg > 0 && scale_arg < result_scale
            //      decimal part now has scale_arg digits, so multiply 10^(result_scale - scal_arg)
            // Case 3: scale_arg >= input_scale
            //      do nothing

            if (scale_arg <= 0) {
                for (size_t i = 0; i < input_rows_count; ++i) {
                    vec_res[i].value *= int_exp10(result_scale);
                }
            } else if (scale_arg > 0 && scale_arg < result_scale) {
                for (size_t i = 0; i < input_rows_count; ++i) {
                    vec_res[i].value *= int_exp10(result_scale - scale_arg);
                }
            }

            return col_res;
        } else {
            LOG(FATAL) << "__builtin_unreachable";
            __builtin_unreachable();
            return nullptr;
        }
    }

    // result_scale: scale for result decimal, this scale is got from planner
    static ColumnPtr apply_vec_vec(const IColumn* col_general, const IColumn* col_scale,
                                   [[maybe_unused]] Int16 result_scale) {
        const auto& col_scale_i32 = assert_cast<const ColumnInt32&>(*col_scale);
        const size_t input_row_count = col_scale_i32.size();
        for (size_t i = 0; i < input_row_count; ++i) {
            const Int32 scale_arg = col_scale_i32.get_data()[i];
            if (scale_arg > std::numeric_limits<Int16>::max() ||
                scale_arg < std::numeric_limits<Int16>::min()) {
                throw doris::Exception(ErrorCode::OUT_OF_BOUND,
                                       "Scale argument for function is out of bound: {}",
                                       scale_arg);
            }
        }

        if constexpr (IsNumber<T>) {
            const auto* col = assert_cast<const ColumnVector<T>*>(col_general);
            auto col_res = ColumnVector<T>::create();
            typename ColumnVector<T>::Container& vec_res = col_res->get_data();
            vec_res.resize(input_row_count);

            for (size_t i = 0; i < input_row_count; ++i) {
                const Int32 scale_arg = col_scale_i32.get_data()[i];
                if (scale_arg == 0) {
                    size_t scale = 1;
                    FunctionRoundingImpl<ScaleMode::Zero>::apply(col->get_data()[i], scale,
                                                                 vec_res[i]);
                } else if (scale_arg > 0) {
                    size_t scale = int_exp10(scale_arg);
                    FunctionRoundingImpl<ScaleMode::Positive>::apply(col->get_data()[i], scale,
                                                                     vec_res[i]);
                } else {
                    size_t scale = int_exp10(-scale_arg);
                    FunctionRoundingImpl<ScaleMode::Negative>::apply(col->get_data()[i], scale,
                                                                     vec_res[i]);
                }
            }
            return col_res;
        } else if constexpr (IsDecimalNumber<T>) {
            const auto* decimal_col = assert_cast<const ColumnDecimal<T>*>(col_general);
            const Int32 input_scale = decimal_col->get_scale();
            auto col_res = ColumnDecimal<T>::create(input_row_count, result_scale);

            for (size_t i = 0; i < input_row_count; ++i) {
                DecimalRoundingImpl<T, rounding_mode, tie_breaking_mode>::apply(
                        decimal_col->get_element(i).value, input_scale,
                        col_res->get_element(i).value, col_scale_i32.get_data()[i]);
            }

            for (size_t i = 0; i < input_row_count; ++i) {
                // For func(ColumnDecimal, ColumnInt32), we should always have same scale with source Decimal column
                // So we need this check to make sure the result have correct digits count
                //
                // Case 0: scale_arg <= -(integer part digits count)
                //      do nothing, because result is 0
                // Case 1: scale_arg <= 0 && scale_arg > -(integer part digits count)
                //      decimal parts has been erased, so add them back by multiply 10^(scale_arg)
                // Case 2: scale_arg > 0 && scale_arg < result_scale
                //      decimal part now has scale_arg digits, so multiply 10^(result_scale - scal_arg)
                // Case 3: scale_arg >= input_scale
                //      do nothing
                const Int32 scale_arg = col_scale_i32.get_data()[i];
                if (scale_arg <= 0) {
                    col_res->get_element(i).value *= int_exp10(result_scale);
                } else if (scale_arg > 0 && scale_arg < result_scale) {
                    col_res->get_element(i).value *= int_exp10(result_scale - scale_arg);
                }
            }

            return col_res;
        } else {
            LOG(FATAL) << "__builtin_unreachable";
            __builtin_unreachable();
            return nullptr;
        }
    }

    // result_scale: scale for result decimal, this scale is got from planner
    static ColumnPtr apply_const_vec(const ColumnConst* const_col_general, const IColumn* col_scale,
                                     [[maybe_unused]] Int16 result_scale) {
        const auto& col_scale_i32 = assert_cast<const ColumnInt32&>(*col_scale);
        const size_t input_rows_count = col_scale->size();

        for (size_t i = 0; i < input_rows_count; ++i) {
            const Int32 scale_arg = col_scale_i32.get_data()[i];

            if (scale_arg > std::numeric_limits<Int16>::max() ||
                scale_arg < std::numeric_limits<Int16>::min()) {
                throw doris::Exception(ErrorCode::OUT_OF_BOUND,
                                       "Scale argument for function is out of bound: {}",
                                       scale_arg);
            }
        }

        if constexpr (IsDecimalNumber<T>) {
            const ColumnDecimal<T>& data_col_general =
                    assert_cast<const ColumnDecimal<T>&>(const_col_general->get_data_column());
            const T& general_val = data_col_general.get_data()[0];
            Int32 input_scale = data_col_general.get_scale();
            auto col_res = ColumnDecimal<T>::create(input_rows_count, result_scale);

            for (size_t i = 0; i < input_rows_count; ++i) {
                DecimalRoundingImpl<T, rounding_mode, tie_breaking_mode>::apply(
                        general_val, input_scale, col_res->get_element(i).value,
                        col_scale_i32.get_data()[i]);
            }

            for (size_t i = 0; i < input_rows_count; ++i) {
                // For func(ColumnDecimal, ColumnInt32), we should always have same scale with source Decimal column
                // So we need this check to make sure the result have correct digits count
                //
                // Case 0: scale_arg <= -(integer part digits count)
                //      do nothing, because result is 0
                // Case 1: scale_arg <= 0 && scale_arg > -(integer part digits count)
                //      decimal parts has been erased, so add them back by multiply 10^(scale_arg)
                // Case 2: scale_arg > 0 && scale_arg < result_scale
                //      decimal part now has scale_arg digits, so multiply 10^(result_scale - scal_arg)
                // Case 3: scale_arg >= input_scale
                //      do nothing
                const Int32 scale_arg = col_scale_i32.get_data()[i];
                if (scale_arg <= 0) {
                    col_res->get_element(i).value *= int_exp10(result_scale);
                } else if (scale_arg > 0 && scale_arg < result_scale) {
                    col_res->get_element(i).value *= int_exp10(result_scale - scale_arg);
                }
            }

            return col_res;
        } else if constexpr (IsNumber<T>) {
            const ColumnVector<T>& data_col_general =
                    assert_cast<const ColumnVector<T>&>(const_col_general->get_data_column());
            const T& general_val = data_col_general.get_data()[0];
            auto col_res = ColumnVector<T>::create(input_rows_count);
            typename ColumnVector<T>::Container& vec_res = col_res->get_data();

            for (size_t i = 0; i < input_rows_count; ++i) {
                const Int16 scale_arg = col_scale_i32.get_data()[i];
                if (scale_arg == 0) {
                    size_t scale = 1;
                    FunctionRoundingImpl<ScaleMode::Zero>::apply(general_val, scale, vec_res[i]);
                } else if (scale_arg > 0) {
                    size_t scale = int_exp10(col_scale_i32.get_data()[i]);
                    FunctionRoundingImpl<ScaleMode::Positive>::apply(general_val, scale,
                                                                     vec_res[i]);
                } else {
                    size_t scale = int_exp10(-col_scale_i32.get_data()[i]);
                    FunctionRoundingImpl<ScaleMode::Negative>::apply(general_val, scale,
                                                                     vec_res[i]);
                }
            }

            return col_res;
        } else {
            LOG(FATAL) << "__builtin_unreachable";
            __builtin_unreachable();
            return nullptr;
        }
    }
};

template <typename Impl, RoundingMode rounding_mode, TieBreakingMode tie_breaking_mode>
class FunctionRounding : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionRounding>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }

    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if ((arguments.empty()) || (arguments.size() > 2)) {
            LOG(FATAL) << "Number of arguments for function " + get_name() +
                                  " doesn't match: should be 1 or 2. ";
        }

        return arguments[0];
    }

    static Status get_scale_arg(const ColumnWithTypeAndName& arguments, Int16* scale) {
        const IColumn& scale_column = *arguments.column;

        Int32 scale_arg = assert_cast<const ColumnInt32&>(
                                  assert_cast<const ColumnConst*>(&scale_column)->get_data_column())
                                  .get_element(0);

        if (scale_arg > std::numeric_limits<Int16>::max() ||
            scale_arg < std::numeric_limits<Int16>::min()) {
            return Status::InvalidArgument("Scale argument for function {} is out of bound: {}",
                                           name, scale_arg);
        }

        *scale = scale_arg;
        return Status::OK();
    }

    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const ColumnWithTypeAndName& column_general = block.get_by_position(arguments[0]);
        ColumnWithTypeAndName& column_result = block.get_by_position(result);
        const DataTypePtr result_type = block.get_by_position(result).type;
        const bool is_col_general_const = is_column_const(*column_general.column);
        const auto* col_general = is_col_general_const
                                          ? assert_cast<const ColumnConst&>(*column_general.column)
                                                    .get_data_column_ptr()
                                          : column_general.column.get();
        ColumnPtr res;

        /// potential argument types:
        /// if the SECOND argument is MISSING(would be considered as ZERO const) or CONST, then we have the following type:
        ///    1. func(Column), func(Column, ColumnConst)
        /// otherwise, the SECOND arugment is COLUMN, we have another type:
        ///    2. func(Column, Column), func(ColumnConst, Column)

        auto call = [&](const auto& types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using DataType = typename Types::LeftType;

            // For decimal, we will always make sure result Decimal has exactly same precision and scale with
            // arguments from query plan.
            Int16 result_scale = 0;
            if constexpr (IsDataTypeDecimal<DataType>) {
                if (column_result.type->get_type_id() == TypeIndex::Nullable) {
                    if (auto nullable_type = std::dynamic_pointer_cast<const DataTypeNullable>(
                                column_result.type)) {
                        result_scale = nullable_type->get_nested_type()->get_scale();
                    } else {
                        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                               "Illegal nullable column");
                    }
                } else {
                    result_scale = column_result.type->get_scale();
                }
            }

            if constexpr (IsDataTypeNumber<DataType> || IsDataTypeDecimal<DataType>) {
                using FieldType = typename DataType::FieldType;
                if (arguments.size() == 1 ||
                    is_column_const(*block.get_by_position(arguments[1]).column)) {
                    // the SECOND argument is MISSING or CONST
                    Int16 scale_arg = 0;
                    if (arguments.size() == 2) {
                        RETURN_IF_ERROR(
                                get_scale_arg(block.get_by_position(arguments[1]), &scale_arg));
                    }

                    res = Dispatcher<FieldType, rounding_mode, tie_breaking_mode>::apply_vec_const(
                            col_general, scale_arg, result_scale);
                } else {
                    // the SECOND arugment is COLUMN
                    if (is_col_general_const) {
                        res = Dispatcher<FieldType, rounding_mode, tie_breaking_mode>::
                                apply_const_vec(
                                        &assert_cast<const ColumnConst&>(*column_general.column),
                                        block.get_by_position(arguments[1]).column.get(),
                                        result_scale);
                    } else {
                        res = Dispatcher<FieldType, rounding_mode, tie_breaking_mode>::
                                apply_vec_vec(col_general,
                                              block.get_by_position(arguments[1]).column.get(),
                                              result_scale);
                    }
                }
                return true;
            }

            return false;
        };

#if !defined(__SSE4_1__) && !defined(__aarch64__)
        /// In case of "nearbyint" function is used, we should ensure the expected rounding mode for the Banker's rounding.
        /// Actually it is by default. But we will set it just in case.
        if constexpr (rounding_mode == RoundingMode::Round) {
            if (0 != fesetround(FE_TONEAREST)) {
                return Status::InvalidArgument("Cannot set floating point rounding mode");
            }
        }
#endif

        if (!call_on_index_and_data_type<void>(column_general.type->get_type_id(), call)) {
            return Status::InvalidArgument("Invalid argument type {} for function {}",
                                           column_general.type->get_name(), name);
        }

        column_result.column = std::move(res);
        return Status::OK();
    }
};

struct TruncateName {
    static constexpr auto name = "truncate";
};

struct FloorName {
    static constexpr auto name = "floor";
};

struct CeilName {
    static constexpr auto name = "ceil";
};

struct RoundName {
    static constexpr auto name = "round";
};

struct RoundBankersName {
    static constexpr auto name = "round_bankers";
};

/// round(double,int32)-->double
/// key_str:roundFloat64Int32
template <typename Name>
struct DoubleRoundTwoImpl {
    static constexpr auto name = Name::name;

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<vectorized::DataTypeFloat64>(),
                std::make_shared<vectorized::DataTypeInt32>()};
    }
};

template <typename Name>
struct DoubleRoundOneImpl {
    static constexpr auto name = Name::name;

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<vectorized::DataTypeFloat64>()};
    }
};

template <typename Name>
struct DecimalRoundTwoImpl {
    static constexpr auto name = Name::name;

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<vectorized::DataTypeDecimal<Decimal32>>(9, 0),
                std::make_shared<vectorized::DataTypeInt32>()};
    }
};

template <typename Name>
struct DecimalRoundOneImpl {
    static constexpr auto name = Name::name;

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<vectorized::DataTypeDecimal<Decimal32>>(9, 0)};
    }
};

} // namespace doris::vectorized
