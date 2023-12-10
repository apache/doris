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

#include "vec/columns/column_const.h"
#include "vec/columns/columns_number.h"
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
    Auto,    // use banker's rounding for floating point numbers, round up otherwise
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

    static ALWAYS_INLINE T compute(T x, T scale, size_t target_scale) {
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
            size_t scale = int_exp10(scale_arg);

            const NativeType* __restrict p_in = reinterpret_cast<const NativeType*>(in.data());
            const NativeType* end_in = reinterpret_cast<const NativeType*>(in.data()) + in.size();
            NativeType* __restrict p_out = reinterpret_cast<NativeType*>(out.data());

            if (out_scale < 0) {
                while (p_in < end_in) {
                    Op::compute(p_in, scale, p_out, int_exp10(-out_scale));
                    ++p_in;
                    ++p_out;
                }
            } else {
                while (p_in < end_in) {
                    Op::compute(p_in, scale, p_out, 1);
                    ++p_in;
                    ++p_out;
                }
            }
        } else {
            memcpy(out.data(), in.data(), in.size() * sizeof(T));
        }
    }
};

#if defined(__SSE4_1__) || defined(__aarch64__)

template <typename T>
class BaseFloatRoundingComputation;

template <>
class BaseFloatRoundingComputation<Float32> {
public:
    using ScalarType = Float32;
    using VectorType = __m128;
    static const size_t data_count = 4;

    static VectorType load(const ScalarType* in) { return _mm_loadu_ps(in); }
    static VectorType load1(const ScalarType in) { return _mm_load1_ps(&in); }
    static void store(ScalarType* out, VectorType val) { _mm_storeu_ps(out, val); }
    static VectorType multiply(VectorType val, VectorType scale) { return _mm_mul_ps(val, scale); }
    static VectorType divide(VectorType val, VectorType scale) { return _mm_div_ps(val, scale); }
    template <RoundingMode mode>
    static VectorType apply(VectorType val) {
        return _mm_round_ps(val, int(mode));
    }

    static VectorType prepare(size_t scale) { return load1(scale); }
};

template <>
class BaseFloatRoundingComputation<Float64> {
public:
    using ScalarType = Float64;
    using VectorType = __m128d;
    static const size_t data_count = 2;

    static VectorType load(const ScalarType* in) { return _mm_loadu_pd(in); }
    static VectorType load1(const ScalarType in) { return _mm_load1_pd(&in); }
    static void store(ScalarType* out, VectorType val) { _mm_storeu_pd(out, val); }
    static VectorType multiply(VectorType val, VectorType scale) { return _mm_mul_pd(val, scale); }
    static VectorType divide(VectorType val, VectorType scale) { return _mm_div_pd(val, scale); }
    template <RoundingMode mode>
    static VectorType apply(VectorType val) {
        return _mm_round_pd(val, int(mode));
    }

    static VectorType prepare(size_t scale) { return load1(scale); }
};

#else

/// Implementation for ARM. Not vectorized.

inline float roundWithMode(float x, RoundingMode mode) {
    switch (mode) {
    case RoundingMode::Round:
        return nearbyintf(x);
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

inline double roundWithMode(double x, RoundingMode mode) {
    switch (mode) {
    case RoundingMode::Round:
        return nearbyint(x);
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

template <typename T>
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
        return roundWithMode(val, mode);
    }

    static VectorType prepare(size_t scale) { return load1(scale); }
};

#endif

/** Implementation of low-level round-off functions for floating-point values.
  */
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
class FloatRoundingComputation : public BaseFloatRoundingComputation<T> {
    using Base = BaseFloatRoundingComputation<T>;

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
template <typename T, RoundingMode rounding_mode, ScaleMode scale_mode>
struct FloatRoundingImpl {
private:
    static_assert(!IsDecimalNumber<T>);

    using Op = FloatRoundingComputation<T, rounding_mode, scale_mode>;
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
};

/** Select the appropriate processing algorithm depending on the scale.
  */
template <typename T, RoundingMode rounding_mode, TieBreakingMode tie_breaking_mode>
struct Dispatcher {
    template <ScaleMode scale_mode>
    using FunctionRoundingImpl = std::conditional_t<
            IsDecimalNumber<T>, DecimalRoundingImpl<T, rounding_mode, tie_breaking_mode>,
            std::conditional_t<
                    std::is_floating_point_v<T>, FloatRoundingImpl<T, rounding_mode, scale_mode>,
                    IntegerRoundingImpl<T, rounding_mode, scale_mode, tie_breaking_mode>>>;

    static ColumnPtr apply(const IColumn* col_general, Int16 scale_arg) {
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

            UInt32 result_scale =
                    std::min(static_cast<UInt32>(std::max(scale_arg, static_cast<Int16>(0))),
                             decimal_col->get_scale());
            auto col_res = ColumnDecimal<T>::create(vec_src.size(), result_scale);
            auto& vec_res = col_res->get_data();

            if (!vec_res.empty()) {
                FunctionRoundingImpl<ScaleMode::Negative>::apply(
                        decimal_col->get_data(), decimal_col->get_scale(), vec_res, scale_arg);
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

        Int32 scale64 = static_cast<const ColumnInt32&>(
                                static_cast<const ColumnConst*>(&scale_column)->get_data_column())
                                .get_element(0);

        if (scale64 > std::numeric_limits<Int16>::max() ||
            scale64 < std::numeric_limits<Int16>::min()) {
            return Status::InvalidArgument("Scale argument for function {} is out of bound: {}",
                                           name, scale64);
        }

        *scale = scale64;
        return Status::OK();
    }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t /*input_rows_count*/) const override {
        const ColumnWithTypeAndName& column = block.get_by_position(arguments[0]);
        Int16 scale_arg = 0;
        if (arguments.size() == 2) {
            RETURN_IF_ERROR(get_scale_arg(block.get_by_position(arguments[1]), &scale_arg));
        }

        ColumnPtr res;
        auto call = [&](const auto& types) -> bool {
            using Types = std::decay_t<decltype(types)>;
            using DataType = typename Types::LeftType;

            if constexpr (IsDataTypeNumber<DataType> || IsDataTypeDecimal<DataType>) {
                using FieldType = typename DataType::FieldType;
                res = Dispatcher<FieldType, rounding_mode, tie_breaking_mode>::apply(
                        column.column.get(), scale_arg);
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

        if (!call_on_index_and_data_type<void>(column.type->get_type_id(), call)) {
            return Status::InvalidArgument("Invalid argument type {} for function {}",
                                           column.type->get_name(), name);
        }

        block.replace_by_position(result, std::move(res));
        return Status::OK();
    }
};

} // namespace doris::vectorized
