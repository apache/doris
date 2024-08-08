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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionBinaryArithmetic.h
// and modified by Doris

#pragma once

#include <type_traits>

#include "common/exception.h"
#include "common/status.h"
#include "runtime/decimalv2_value.h"
#include "udf/udf.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/arithmetic_overflow.h"
#include "vec/core/types.h"
#include "vec/core/wide_integer.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/cast_type_to_either.h"
#include "vec/functions/function.h"
#include "vec/utils/template_helpers.hpp"

namespace doris::vectorized {

// Arithmetic operations: +, -, *, |, &, ^, ~
// need implement apply(a, b)

// Arithmetic operations (to null type): /, %, intDiv (integer division), log
// need implement apply(a, b, is_null), apply(array_a, b, null_map)
// apply(array_a, b, null_map) is only used on vector_constant

// TODO: vector_constant optimization not work on decimal type now

template <typename, typename>
struct PlusImpl;
template <typename, typename>
struct MinusImpl;
template <typename, typename>
struct MultiplyImpl;
template <typename, typename>
struct DivideFloatingImpl;
template <typename, typename>
struct DivideIntegralImpl;
template <typename, typename>
struct ModuloImpl;

template <template <typename, typename> typename Operation, typename OpA = UInt8,
          typename OpB = UInt8>
struct OperationTraits {
    using T = UInt8;
    using Op = Operation<T, T>;
    static constexpr bool is_plus_minus =
            std::is_same_v<Op, PlusImpl<T, T>> || std::is_same_v<Op, MinusImpl<T, T>>;
    static constexpr bool is_multiply = std::is_same_v<Op, MultiplyImpl<T, T>>;
    static constexpr bool is_division = std::is_same_v<Op, DivideFloatingImpl<T, T>> ||
                                        std::is_same_v<Op, DivideIntegralImpl<T, T>>;
    static constexpr bool is_mod = std::is_same_v<Op, ModuloImpl<T, T>>;
    static constexpr bool allow_decimal =
            std::is_same_v<Op, PlusImpl<T, T>> || std::is_same_v<Op, MinusImpl<T, T>> ||
            std::is_same_v<Op, MultiplyImpl<T, T>> || std::is_same_v<Op, ModuloImpl<T, T>> ||
            std::is_same_v<Op, DivideFloatingImpl<T, T>> ||
            std::is_same_v<Op, DivideIntegralImpl<T, T>>;
    static constexpr bool can_overflow =
            (is_plus_minus || is_multiply) &&
            (IsDecimalV2<OpA> || IsDecimalV2<OpB> || IsDecimal128V3<OpA> || IsDecimal128V3<OpB> ||
             IsDecimal256<OpA> || IsDecimal256<OpB>);
    static constexpr bool has_variadic_argument =
            !std::is_void_v<decltype(has_variadic_argument_types(std::declval<Op>()))>;
};

template <typename A, typename B, typename Op, typename ResultType = typename Op::ResultType>
struct BinaryOperationImplBase {
    using Traits = NumberTraits::BinaryOperatorTraits<A, B>;
    using ColumnVectorResult =
            std::conditional_t<IsDecimalNumber<ResultType>, ColumnDecimal<ResultType>,
                               ColumnVector<ResultType>>;

    static void vector_vector(const PaddedPODArray<A>& a, const PaddedPODArray<B>& b,
                              PaddedPODArray<ResultType>& c) {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::template apply<ResultType>(a[i], b[i]);
        }
    }

    static void vector_vector(const PaddedPODArray<A>& a, const PaddedPODArray<B>& b,
                              PaddedPODArray<ResultType>& c, PaddedPODArray<UInt8>& null_map) {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::template apply<ResultType>(a[i], b[i], null_map[i]);
        }
    }

    static void vector_constant(const PaddedPODArray<A>& a, B b, PaddedPODArray<ResultType>& c) {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::template apply<ResultType>(a[i], b);
        }
    }

    static void vector_constant(const PaddedPODArray<A>& a, B b, PaddedPODArray<ResultType>& c,
                                PaddedPODArray<UInt8>& null_map) {
        Op::template apply<ResultType>(a, b, c, null_map);
    }

    static void constant_vector(A a, const PaddedPODArray<B>& b, PaddedPODArray<ResultType>& c) {
        size_t size = b.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::template apply<ResultType>(a, b[i]);
        }
    }

    static void constant_vector(A a, const PaddedPODArray<B>& b, PaddedPODArray<ResultType>& c,
                                PaddedPODArray<UInt8>& null_map) {
        size_t size = b.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::template apply<ResultType>(a, b[i], null_map[i]);
        }
    }

    static ResultType constant_constant(A a, B b) { return Op::template apply<ResultType>(a, b); }

    static ResultType constant_constant(A a, B b, UInt8& is_null) {
        return Op::template apply<ResultType>(a, b, is_null);
    }
};

template <typename A, typename B, typename Op, bool is_to_null_type,
          typename ResultType = typename Op::ResultType>
struct BinaryOperationImpl {
    using Base = BinaryOperationImplBase<A, B, Op, ResultType>;

    static ColumnPtr adapt_normal_constant_constant(A a, B b) {
        auto column_result = Base::ColumnVectorResult::create(1);

        if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(1, 0);
            column_result->get_element(0) = Base::constant_constant(a, b, null_map->get_element(0));
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            column_result->get_element(0) = Base::constant_constant(a, b);
            return column_result;
        }
    }

    static ColumnPtr adapt_normal_vector_constant(ColumnPtr column_left, B b) {
        auto column_left_ptr =
                check_and_get_column<typename Base::Traits::ColumnVectorA>(column_left);
        auto column_result = Base::ColumnVectorResult::create(column_left->size());
        DCHECK(column_left_ptr != nullptr);

        if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(column_left->size(), 0);
            Base::vector_constant(column_left_ptr->get_data(), b, column_result->get_data(),
                                  null_map->get_data());
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            Base::vector_constant(column_left_ptr->get_data(), b, column_result->get_data());
            return column_result;
        }
    }

    static ColumnPtr adapt_normal_constant_vector(A a, ColumnPtr column_right) {
        auto column_right_ptr =
                check_and_get_column<typename Base::Traits::ColumnVectorB>(column_right);
        auto column_result = Base::ColumnVectorResult::create(column_right->size());
        DCHECK(column_right_ptr != nullptr);

        if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(column_right->size(), 0);
            Base::constant_vector(a, column_right_ptr->get_data(), column_result->get_data(),
                                  null_map->get_data());
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            Base::constant_vector(a, column_right_ptr->get_data(), column_result->get_data());
            return column_result;
        }
    }

    static ColumnPtr adapt_normal_vector_vector(ColumnPtr column_left, ColumnPtr column_right) {
        auto column_left_ptr =
                check_and_get_column<typename Base::Traits::ColumnVectorA>(column_left);
        auto column_right_ptr =
                check_and_get_column<typename Base::Traits::ColumnVectorB>(column_right);

        auto column_result = Base::ColumnVectorResult::create(column_left->size());
        DCHECK(column_left_ptr != nullptr && column_right_ptr != nullptr);

        if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(column_result->size(), 0);
            Base::vector_vector(column_left_ptr->get_data(), column_right_ptr->get_data(),
                                column_result->get_data(), null_map->get_data());
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            Base::vector_vector(column_left_ptr->get_data(), column_right_ptr->get_data(),
                                column_result->get_data());
            return column_result;
        }
    }
};

#define THROW_DECIMAL_BINARY_OP_OVERFLOW_EXCEPTION(left_value, op_name, right_value, result_value, \
                                                   result_type_name)                               \
    throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,                                         \
                    "Arithmetic overflow: {} {} {} = {}, result type: {}", left_value, op_name,    \
                    right_value, result_value, result_type_name)
/// Binary operations for Decimals need scale args
/// +|- scale one of args (which scale factor is not 1). ScaleR = oneof(Scale1, Scale2);
/// *   no agrs scale. ScaleR = Scale1 + Scale2;
/// /   first arg scale. ScaleR = Scale1 (scale_a = DecimalType<B>::get_scale()).
template <typename LeftDataType, typename RightDataType, typename ResultDataType,
          template <typename, typename> typename Operation, typename Name, typename ResultType,
          bool is_to_null_type, bool check_overflow>
struct DecimalBinaryOperation {
    using A = typename LeftDataType::FieldType;
    using B = typename RightDataType::FieldType;
    using OpTraits = OperationTraits<Operation, A, B>;

    using NativeResultType = typename NativeType<ResultType>::Type;
    using Op = Operation<NativeResultType, NativeResultType>;

    using Traits = NumberTraits::BinaryOperatorTraits<A, B>;
    using ArrayC = typename ColumnDecimal<ResultType>::Container;

private:
    template <typename T>
    static int8_t sgn(const T& x) {
        return (x > 0) ? 1 : ((x < 0) ? -1 : 0);
    }

    static void vector_vector(const typename Traits::ArrayA::value_type* __restrict a,
                              const typename Traits::ArrayB::value_type* __restrict b,
                              typename ArrayC::value_type* c, const LeftDataType& type_left,
                              const RightDataType& type_right, const ResultDataType& type_result,
                              size_t size, const ResultType& max_result_number,
                              const ResultType& scale_diff_multiplier) {
        static_assert(OpTraits::is_plus_minus || OpTraits::is_multiply);
        if constexpr (OpTraits::is_multiply && IsDecimalV2<A> && IsDecimalV2<B> &&
                      IsDecimalV2<ResultType>) {
            Op::template vector_vector<check_overflow>(a, b, c, size);
        } else {
            bool need_adjust_scale = scale_diff_multiplier.value > 1;
            std::visit(
                    [&](auto need_adjust_scale) {
                        for (size_t i = 0; i < size; i++) {
                            c[i] = typename ArrayC::value_type(apply<need_adjust_scale>(
                                    a[i], b[i], type_left, type_right, type_result,
                                    max_result_number, scale_diff_multiplier));
                        }
                    },
                    make_bool_variant(need_adjust_scale && check_overflow));

            if (OpTraits::is_multiply && need_adjust_scale && !check_overflow) {
                auto sig_uptr = std::unique_ptr<int8_t[]>(new int8_t[size]);
                int8_t* sig = sig_uptr.get();
                for (size_t i = 0; i < size; i++) {
                    sig[i] = sgn(c[i].value);
                }
                for (size_t i = 0; i < size; i++) {
                    c[i].value = (c[i].value - sig[i]) / scale_diff_multiplier.value + sig[i];
                }
            }
        }
    }

    /// null_map for divide and mod
    static void vector_vector(const typename Traits::ArrayA::value_type* __restrict a,
                              const typename Traits::ArrayB::value_type* __restrict b,
                              typename ArrayC::value_type* c, NullMap& null_map, size_t size,
                              const ResultType& max_result_number) {
        static_assert(OpTraits::is_division || OpTraits::is_mod);
        if constexpr (IsDecimalV2<B> || IsDecimalV2<A>) {
            for (size_t i = 0; i < size; ++i) {
                c[i] = typename ArrayC::value_type(
                        apply(a[i], b[i], null_map[i], max_result_number));
            }
        } else if constexpr (OpTraits::is_division && (IsDecimalNumber<B> || IsDecimalNumber<A>)) {
            for (size_t i = 0; i < size; ++i) {
                if constexpr (IsDecimalNumber<B> && IsDecimalNumber<A>) {
                    c[i] = typename ArrayC::value_type(
                            apply(a[i].value, b[i].value, null_map[i], max_result_number));
                } else if constexpr (IsDecimalNumber<A>) {
                    c[i] = typename ArrayC::value_type(
                            apply(a[i].value, b[i], null_map[i], max_result_number));
                } else {
                    c[i] = typename ArrayC::value_type(
                            apply(a[i], b[i].value, null_map[i], max_result_number));
                }
            }
        } else {
            for (size_t i = 0; i < size; ++i) {
                c[i] = typename ArrayC::value_type(
                        apply(a[i], b[i], null_map[i], max_result_number));
            }
        }
    }

    static void vector_constant(const typename Traits::ArrayA::value_type* __restrict a, B b,
                                typename ArrayC::value_type* c, const LeftDataType& type_left,
                                const RightDataType& type_right, const ResultDataType& type_result,
                                size_t size, const ResultType& max_result_number,
                                const ResultType& scale_diff_multiplier) {
        static_assert(!OpTraits::is_division);

        bool need_adjust_scale = scale_diff_multiplier.value > 1;
        std::visit(
                [&](auto need_adjust_scale) {
                    for (size_t i = 0; i < size; ++i) {
                        c[i] = typename ArrayC::value_type(apply<need_adjust_scale>(
                                a[i], b, type_left, type_right, type_result, max_result_number,
                                scale_diff_multiplier));
                    }
                },
                make_bool_variant(need_adjust_scale));
    }

    static void vector_constant(const typename Traits::ArrayA::value_type* __restrict a, B b,
                                typename ArrayC::value_type* c, NullMap& null_map, size_t size,
                                const ResultType& max_result_number) {
        static_assert(OpTraits::is_division || OpTraits::is_mod);
        if constexpr (OpTraits::is_division && IsDecimalNumber<B>) {
            for (size_t i = 0; i < size; ++i) {
                c[i] = typename ArrayC::value_type(
                        apply(a[i], b.value, null_map[i], max_result_number));
            }
        } else {
            for (size_t i = 0; i < size; ++i) {
                c[i] = typename ArrayC::value_type(apply(a[i], b, null_map[i], max_result_number));
            }
        }
    }

    static void constant_vector(A a, const typename Traits::ArrayB::value_type* __restrict b,
                                typename ArrayC::value_type* c, const LeftDataType& type_left,
                                const RightDataType& type_right, const ResultDataType& type_result,
                                size_t size, const ResultType& max_result_number,
                                const ResultType& scale_diff_multiplier) {
        bool need_adjust_scale = scale_diff_multiplier.value > 1;
        std::visit(
                [&](auto need_adjust_scale) {
                    for (size_t i = 0; i < size; ++i) {
                        c[i] = typename ArrayC::value_type(apply<need_adjust_scale>(
                                a, b[i], type_left, type_right, type_result, max_result_number,
                                scale_diff_multiplier));
                    }
                },
                make_bool_variant(need_adjust_scale));
    }

    static void constant_vector(A a, const typename Traits::ArrayB::value_type* __restrict b,
                                typename ArrayC::value_type* c, NullMap& null_map, size_t size,
                                const ResultType& max_result_number) {
        static_assert(OpTraits::is_division || OpTraits::is_mod);
        if constexpr (OpTraits::is_division && IsDecimalNumber<B>) {
            for (size_t i = 0; i < size; ++i) {
                c[i] = typename ArrayC::value_type(
                        apply(a, b[i].value, null_map[i], max_result_number));
            }
        } else {
            for (size_t i = 0; i < size; ++i) {
                c[i] = typename ArrayC::value_type(apply(a, b[i], null_map[i], max_result_number));
            }
        }
    }

    static ResultType constant_constant(A a, B b, const LeftDataType& type_left,
                                        const RightDataType& type_right,
                                        const ResultDataType& type_result,
                                        const ResultType& max_result_number,
                                        const ResultType& scale_diff_multiplier) {
        return ResultType(apply<true>(a, b, type_left, type_right, type_result, max_result_number,
                                      scale_diff_multiplier));
    }

    static ResultType constant_constant(A a, B b, UInt8& is_null,
                                        const ResultType& max_result_number) {
        static_assert(OpTraits::is_division || OpTraits::is_mod);
        if constexpr (OpTraits::is_division && IsDecimalNumber<B>) {
            if constexpr (IsDecimalNumber<A>) {
                return ResultType(apply(a.value, b.value, is_null, max_result_number));
            } else {
                return ResultType(apply(a, b.value, is_null, max_result_number));
            }
        } else {
            return ResultType(apply(a, b, is_null, max_result_number));
        }
    }

public:
    static ColumnPtr adapt_decimal_constant_constant(A a, B b, const LeftDataType& type_left,
                                                     const RightDataType& type_right,
                                                     const ResultType& max_result_number,
                                                     const ResultType& scale_diff_multiplier,
                                                     DataTypePtr res_data_type) {
        auto type_result =
                assert_cast<const DataTypeDecimal<ResultType>&, TypeCheckOnRelease::DISABLE>(
                        *res_data_type);
        auto column_result = ColumnDecimal<ResultType>::create(
                1, assert_cast<const DataTypeDecimal<ResultType>&, TypeCheckOnRelease::DISABLE>(
                           *res_data_type)
                           .get_scale());

        if constexpr (check_overflow && !is_to_null_type &&
                      ((!OpTraits::is_multiply && !OpTraits::is_plus_minus))) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "adapt_decimal_constant_constant Invalid function type!");
            return column_result;
        } else if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(1, 0);
            column_result->get_element(0) =
                    constant_constant(a, b, null_map->get_element(0), max_result_number);
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            column_result->get_element(0) =
                    constant_constant(a, b, type_left, type_right, type_result, max_result_number,
                                      scale_diff_multiplier);
            return column_result;
        }
    }

    static ColumnPtr adapt_decimal_vector_constant(ColumnPtr column_left, B b,
                                                   const LeftDataType& type_left,
                                                   const RightDataType& type_right,
                                                   const ResultType& max_result_number,
                                                   const ResultType& scale_diff_multiplier,
                                                   DataTypePtr res_data_type) {
        auto type_result =
                assert_cast<const DataTypeDecimal<ResultType>&, TypeCheckOnRelease::DISABLE>(
                        *res_data_type);
        auto column_left_ptr = check_and_get_column<typename Traits::ColumnVectorA>(column_left);
        auto column_result = ColumnDecimal<ResultType>::create(
                column_left->size(),
                assert_cast<const DataTypeDecimal<ResultType>&, TypeCheckOnRelease::DISABLE>(
                        *res_data_type)
                        .get_scale());
        DCHECK(column_left_ptr != nullptr);

        if constexpr (check_overflow && !is_to_null_type &&
                      ((!OpTraits::is_multiply && !OpTraits::is_plus_minus))) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "adapt_decimal_vector_constant Invalid function type!");
            return column_result;
        } else if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(column_left->size(), 0);
            vector_constant(column_left_ptr->get_data().data(), b, column_result->get_data().data(),
                            null_map->get_data(), column_left->size(), max_result_number);
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            vector_constant(column_left_ptr->get_data().data(), b, column_result->get_data().data(),
                            type_left, type_right, type_result, column_left->size(),
                            max_result_number, scale_diff_multiplier);
            return column_result;
        }
    }

    static ColumnPtr adapt_decimal_constant_vector(A a, ColumnPtr column_right,
                                                   const LeftDataType& type_left,
                                                   const RightDataType& type_right,
                                                   const ResultType& max_result_number,
                                                   const ResultType& scale_diff_multiplier,
                                                   DataTypePtr res_data_type) {
        auto type_result =
                assert_cast<const DataTypeDecimal<ResultType>&, TypeCheckOnRelease::DISABLE>(
                        *res_data_type);
        auto column_right_ptr = check_and_get_column<typename Traits::ColumnVectorB>(column_right);
        auto column_result = ColumnDecimal<ResultType>::create(
                column_right->size(),
                assert_cast<const DataTypeDecimal<ResultType>&, TypeCheckOnRelease::DISABLE>(
                        *res_data_type)
                        .get_scale());
        DCHECK(column_right_ptr != nullptr);

        if constexpr (check_overflow && !is_to_null_type &&
                      ((!OpTraits::is_multiply && !OpTraits::is_plus_minus))) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "adapt_decimal_constant_vector Invalid function type!");
            return column_result;
        } else if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(column_right->size(), 0);
            constant_vector(a, column_right_ptr->get_data().data(),
                            column_result->get_data().data(), null_map->get_data(),
                            column_right->size(), max_result_number);
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            constant_vector(a, column_right_ptr->get_data().data(),
                            column_result->get_data().data(), type_left, type_right, type_result,
                            column_right->size(), max_result_number, scale_diff_multiplier);
            return column_result;
        }
    }

    static ColumnPtr adapt_decimal_vector_vector(ColumnPtr column_left, ColumnPtr column_right,
                                                 const LeftDataType& type_left,
                                                 const RightDataType& type_right,
                                                 const ResultType& max_result_number,
                                                 const ResultType& scale_diff_multiplier,
                                                 DataTypePtr res_data_type) {
        auto column_left_ptr = check_and_get_column<typename Traits::ColumnVectorA>(column_left);
        auto column_right_ptr = check_and_get_column<typename Traits::ColumnVectorB>(column_right);

        const auto& type_result = assert_cast<const DataTypeDecimal<ResultType>&>(*res_data_type);
        auto column_result =
                ColumnDecimal<ResultType>::create(column_left->size(), type_result.get_scale());
        DCHECK(column_left_ptr != nullptr && column_right_ptr != nullptr);

        if constexpr (check_overflow && !is_to_null_type &&
                      ((!OpTraits::is_multiply && !OpTraits::is_plus_minus))) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "adapt_decimal_vector_vector Invalid function type!");
            return column_result;
        } else if constexpr (is_to_null_type) {
            // function divide, modulo and pmod
            auto null_map = ColumnUInt8::create(column_result->size(), 0);
            vector_vector(column_left_ptr->get_data().data(), column_right_ptr->get_data().data(),
                          column_result->get_data().data(), null_map->get_data(),
                          column_left->size(), max_result_number);
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            vector_vector(column_left_ptr->get_data().data(), column_right_ptr->get_data().data(),
                          column_result->get_data().data(), type_left, type_right, type_result,
                          column_left->size(), max_result_number, scale_diff_multiplier);
            return column_result;
        }
    }

private:
    /// there's implicit type conversion here
    template <bool need_adjust_scale>
    static ALWAYS_INLINE NativeResultType apply(NativeResultType a, NativeResultType b,
                                                const LeftDataType& type_left,
                                                const RightDataType& type_right,
                                                const ResultDataType& type_result,
                                                const ResultType& max_result_number,
                                                const ResultType& scale_diff_multiplier) {
        static_assert(OpTraits::is_plus_minus || OpTraits::is_multiply);
        if constexpr (IsDecimalV2<B> || IsDecimalV2<A>) {
            // Now, Doris only support decimal +-*/ decimal.
            if constexpr (check_overflow) {
                auto res = Op::template apply(DecimalV2Value(a), DecimalV2Value(b)).value();
                if (res > max_result_number.value || res < -max_result_number.value) {
                    THROW_DECIMAL_BINARY_OP_OVERFLOW_EXCEPTION(
                            DecimalV2Value(a).to_string(), Name::name,
                            DecimalV2Value(b).to_string(), DecimalV2Value(res).to_string(),
                            ResultDataType {}.get_name());
                }
                return res;
            } else {
                return Op::template apply(DecimalV2Value(a), DecimalV2Value(b)).value();
            }
        } else {
            NativeResultType res;
            if constexpr (OpTraits::can_overflow && check_overflow) {
                // TODO handle overflow gracefully
                if (UNLIKELY(Op::template apply<NativeResultType>(a, b, res))) {
                    if constexpr (OpTraits::is_plus_minus) {
                        auto result_str =
                                DataTypeDecimal<Decimal256> {BeConsts::MAX_DECIMAL256_PRECISION,
                                                             type_result.get_scale()}
                                        .to_string(Decimal256(res));
                        THROW_DECIMAL_BINARY_OP_OVERFLOW_EXCEPTION(
                                type_left.to_string(A(a)), Name::name, type_right.to_string(B(b)),
                                result_str, type_result.get_name());
                    }
                    // multiply
                    if constexpr (std::is_same_v<NativeResultType, __int128>) {
                        wide::Int256 res256 = Op::template apply<wide::Int256>(a, b);
                        if constexpr (OpTraits::is_multiply && need_adjust_scale) {
                            if (res256 > 0) {
                                res256 = (res256 + scale_diff_multiplier.value / 2) /
                                         scale_diff_multiplier.value;

                            } else {
                                res256 = (res256 - scale_diff_multiplier.value / 2) /
                                         scale_diff_multiplier.value;
                            }
                        }
                        // check if final result is overflow
                        if (res256 > wide::Int256(max_result_number.value) ||
                            res256 < wide::Int256(-max_result_number.value)) {
                            auto result_str =
                                    DataTypeDecimal<Decimal256> {BeConsts::MAX_DECIMAL256_PRECISION,
                                                                 type_result.get_scale()}
                                            .to_string(Decimal256(res256));
                            THROW_DECIMAL_BINARY_OP_OVERFLOW_EXCEPTION(
                                    type_left.to_string(A(a)), Name::name,
                                    type_right.to_string(B(b)), result_str, type_result.get_name());
                        } else {
                            res = res256;
                        }
                    } else {
                        auto result_str =
                                DataTypeDecimal<Decimal256> {BeConsts::MAX_DECIMAL256_PRECISION,
                                                             type_result.get_scale()}
                                        .to_string(Decimal256(res));
                        THROW_DECIMAL_BINARY_OP_OVERFLOW_EXCEPTION(
                                type_left.to_string(A(a)), Name::name, type_right.to_string(B(b)),
                                result_str, type_result.get_name());
                    }
                } else {
                    // round to final result precision
                    if constexpr (OpTraits::is_multiply && need_adjust_scale) {
                        if (res >= 0) {
                            res = (res + scale_diff_multiplier.value / 2) /
                                  scale_diff_multiplier.value;
                        } else {
                            res = (res - scale_diff_multiplier.value / 2) /
                                  scale_diff_multiplier.value;
                        }
                    }
                    if (res > max_result_number.value || res < -max_result_number.value) {
                        auto result_str =
                                DataTypeDecimal<Decimal256> {BeConsts::MAX_DECIMAL256_PRECISION,
                                                             type_result.get_scale()}
                                        .to_string(Decimal256(res));
                        THROW_DECIMAL_BINARY_OP_OVERFLOW_EXCEPTION(
                                type_left.to_string(A(a)), Name::name, type_right.to_string(B(b)),
                                result_str, type_result.get_name());
                    }
                }
                return res;
            } else {
                res = Op::template apply<NativeResultType>(a, b);
                if constexpr (OpTraits::is_multiply && need_adjust_scale) {
                    if (res >= 0) {
                        res = (res + scale_diff_multiplier.value / 2) / scale_diff_multiplier.value;
                    } else {
                        res = (res - scale_diff_multiplier.value / 2) / scale_diff_multiplier.value;
                    }
                }
                return res;
            }
        }
    }

    /// null_map for divide and mod
    static ALWAYS_INLINE NativeResultType apply(NativeResultType a, NativeResultType b,
                                                UInt8& is_null,
                                                const ResultType& max_result_number) {
        static_assert(OpTraits::is_division || OpTraits::is_mod);
        if constexpr (IsDecimalV2<B> || IsDecimalV2<A>) {
            DecimalV2Value l(a);
            DecimalV2Value r(b);
            auto ans = Op::template apply(l, r, is_null);
            using ANS_TYPE = std::decay_t<decltype(ans)>;
            if constexpr (check_overflow && OpTraits::is_division) {
                if constexpr (std::is_same_v<ANS_TYPE, DecimalV2Value>) {
                    if (ans.value() > max_result_number.value ||
                        ans.value() < -max_result_number.value) {
                        THROW_DECIMAL_BINARY_OP_OVERFLOW_EXCEPTION(
                                DecimalV2Value(a).to_string(), Name::name,
                                DecimalV2Value(b).to_string(), DecimalV2Value(ans).to_string(),
                                ResultDataType {}.get_name());
                    }
                } else if constexpr (IsDecimalNumber<ANS_TYPE>) {
                    if (ans.value > max_result_number.value ||
                        ans.value < -max_result_number.value) {
                        THROW_DECIMAL_BINARY_OP_OVERFLOW_EXCEPTION(
                                DecimalV2Value(a).to_string(), Name::name,
                                DecimalV2Value(b).to_string(), DecimalV2Value(ans).to_string(),
                                ResultDataType {}.get_name());
                    }
                } else {
                    if (ans > max_result_number.value || ans < -max_result_number.value) {
                        THROW_DECIMAL_BINARY_OP_OVERFLOW_EXCEPTION(
                                DecimalV2Value(a).to_string(), Name::name,
                                DecimalV2Value(b).to_string(), DecimalV2Value(ans).to_string(),
                                ResultDataType {}.get_name());
                    }
                }
            }
            NativeResultType result {};
            memcpy(&result, &ans, std::min(sizeof(result), sizeof(ans)));
            return result;
        } else {
            return Op::template apply<NativeResultType>(a, b, is_null);
        }
    }
};

/// Used to indicate undefined operation
struct InvalidType;

template <bool V, typename T>
struct Case : std::bool_constant<V> {
    using type = T;
};

/// Switch<Case<C0, T0>, ...> -- select the first Ti for which Ci is true; InvalidType if none.
template <typename... Ts>
using Switch = typename std::disjunction<Ts..., Case<true, InvalidType>>::type;

template <typename DataType>
constexpr bool IsIntegral = false;
template <>
inline constexpr bool IsIntegral<DataTypeUInt8> = true;
template <>
inline constexpr bool IsIntegral<DataTypeInt8> = true;
template <>
inline constexpr bool IsIntegral<DataTypeInt16> = true;
template <>
inline constexpr bool IsIntegral<DataTypeInt32> = true;
template <>
inline constexpr bool IsIntegral<DataTypeInt64> = true;
template <>
inline constexpr bool IsIntegral<DataTypeInt128> = true;

template <typename A, typename B>
constexpr bool UseLeftDecimal = false;
template <>
inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal256>, DataTypeDecimal<Decimal32>> =
        true;
template <>
inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal256>, DataTypeDecimal<Decimal64>> =
        true;
template <>
inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal256>, DataTypeDecimal<Decimal128V3>> =
        true;
template <>
inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal128V3>, DataTypeDecimal<Decimal32>> =
        true;
template <>
inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal128V3>, DataTypeDecimal<Decimal64>> =
        true;
template <>
inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal64>, DataTypeDecimal<Decimal32>> = true;

template <typename T>
using DataTypeFromFieldType =
        std::conditional_t<std::is_same_v<T, NumberTraits::Error>, InvalidType, DataTypeNumber<T>>;

template <template <typename, typename> class Operation, typename LeftDataType,
          typename RightDataType>
struct BinaryOperationTraits {
    using Op = Operation<typename LeftDataType::FieldType, typename RightDataType::FieldType>;
    using OpTraits = OperationTraits<Operation>;
    /// Appropriate result type for binary operator on numeric types. "Date" can also mean
    /// DateTime, but if both operands are Dates, their type must be the same (e.g. Date - DateTime is invalid).
    using ResultDataType = Switch<
            /// Decimal cases
            Case<!OpTraits::allow_decimal &&
                         (IsDataTypeDecimal<LeftDataType> || IsDataTypeDecimal<RightDataType>),
                 InvalidType>,
            Case<(IsDataTypeDecimalV2<LeftDataType> && IsDataTypeDecimal<RightDataType> &&
                  !IsDataTypeDecimalV2<RightDataType>) ||
                         (IsDataTypeDecimalV2<RightDataType> && IsDataTypeDecimal<LeftDataType> &&
                          !IsDataTypeDecimalV2<LeftDataType>),
                 InvalidType>,
            Case<IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType> &&
                         UseLeftDecimal<LeftDataType, RightDataType>,
                 LeftDataType>,
            Case<IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType>,
                 RightDataType>,
            Case<IsDataTypeDecimal<LeftDataType> && !IsDataTypeDecimal<RightDataType> &&
                         IsIntegral<RightDataType>,
                 LeftDataType>,
            Case<!IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType> &&
                         IsIntegral<LeftDataType>,
                 RightDataType>,
            /// Decimal <op> Real is not supported (traditional DBs convert Decimal <op> Real to Real)
            Case<IsDataTypeDecimal<LeftDataType> && !IsDataTypeDecimal<RightDataType> &&
                         !IsIntegral<RightDataType>,
                 InvalidType>,
            Case<!IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType> &&
                         !IsIntegral<LeftDataType>,
                 InvalidType>,
            /// number <op> number -> see corresponding impl
            Case<!IsDataTypeDecimal<LeftDataType> && !IsDataTypeDecimal<RightDataType>,
                 DataTypeFromFieldType<typename Op::ResultType>>>;
};

template <typename LeftDataType, typename RightDataType, typename ExpectedResultDataType,
          template <typename, typename> class Operation, typename Name, bool is_to_null_type,
          bool check_overflow_for_decimal>
struct ConstOrVectorAdapter {
    static constexpr bool result_is_decimal =
            IsDataTypeDecimal<LeftDataType> || IsDataTypeDecimal<RightDataType>;
    using ResultDataType = ExpectedResultDataType;
    using ResultType = typename ResultDataType::FieldType;
    using A = typename LeftDataType::FieldType;
    using B = typename RightDataType::FieldType;
    using OpTraits = OperationTraits<Operation, A, B>;

    using OperationImpl = std::conditional_t<
            IsDataTypeDecimal<ResultDataType>,
            DecimalBinaryOperation<LeftDataType, RightDataType, ResultDataType, Operation, Name,
                                   ResultType, is_to_null_type, check_overflow_for_decimal>,
            BinaryOperationImpl<A, B, Operation<A, B>, is_to_null_type, ResultType>>;

    static ColumnPtr execute(ColumnPtr column_left, ColumnPtr column_right,
                             const LeftDataType& type_left, const RightDataType& type_right,
                             DataTypePtr res_data_type) {
        bool is_const_left = is_column_const(*column_left);
        bool is_const_right = is_column_const(*column_right);

        if (is_const_left && is_const_right) {
            return constant_constant(column_left, column_right, type_left, type_right,
                                     res_data_type);
        } else if (is_const_left) {
            return constant_vector(column_left, column_right, type_left, type_right, res_data_type);
        } else if (is_const_right) {
            return vector_constant(column_left, column_right, type_left, type_right, res_data_type);
        } else {
            return vector_vector(column_left, column_right, type_left, type_right, res_data_type);
        }
    }

private:
    // for multiply, e1: {p1, s1}, e2: {p2, s2}, the original result precision
    // is {p1 + p2, s1 + s2}, but if the precision or scale is overflow, FE will adjust
    // the result precsion and scale to the values specified in type_result, so
    // we need to adjust the multiply result accordingly.
    static std::pair<ResultType, ResultType> get_max_and_multiplier(
            const LeftDataType& type_left, const RightDataType& type_right,
            const DataTypeDecimal<ResultType>& type_result) {
        auto max_result_number =
                DataTypeDecimal<ResultType>::get_max_digits_number(type_result.get_precision());

        auto orig_result_scale = type_left.get_scale() + type_right.get_scale();
        auto result_scale = type_result.get_scale();
        DCHECK(orig_result_scale >= result_scale);
        auto scale_diff_multiplier =
                DataTypeDecimal<ResultType>::get_scale_multiplier(orig_result_scale - result_scale)
                        .value;
        return {ResultType(max_result_number), ResultType(scale_diff_multiplier)};
    }

    static ColumnPtr constant_constant(ColumnPtr column_left, ColumnPtr column_right,
                                       const LeftDataType& type_left,
                                       const RightDataType& type_right, DataTypePtr res_data_type) {
        auto column_left_ptr = check_and_get_column<ColumnConst>(column_left);
        auto column_right_ptr = check_and_get_column<ColumnConst>(column_right);
        DCHECK(column_left_ptr != nullptr && column_right_ptr != nullptr);

        ColumnPtr column_result = nullptr;

        if constexpr (result_is_decimal) {
            const auto& type_result =
                    assert_cast<const DataTypeDecimal<ResultType>&>(*res_data_type);
            auto max_and_multiplier = get_max_and_multiplier(type_left, type_right, type_result);

            column_result = OperationImpl::adapt_decimal_constant_constant(
                    column_left_ptr->template get_value<A>(),
                    column_right_ptr->template get_value<B>(), type_left, type_right,
                    max_and_multiplier.first, max_and_multiplier.second, res_data_type);

        } else {
            column_result = OperationImpl::adapt_normal_constant_constant(
                    column_left_ptr->template get_value<A>(),
                    column_right_ptr->template get_value<B>());
        }

        return ColumnConst::create(std::move(column_result), column_left->size());
    }

    static ColumnPtr vector_constant(ColumnPtr column_left, ColumnPtr column_right,
                                     const LeftDataType& type_left, const RightDataType& type_right,
                                     DataTypePtr res_data_type) {
        auto column_right_ptr = check_and_get_column<ColumnConst>(column_right);
        DCHECK(column_right_ptr != nullptr);

        if constexpr (result_is_decimal) {
            const auto& type_result =
                    assert_cast<const DataTypeDecimal<ResultType>&>(*res_data_type);
            auto max_and_multiplier = get_max_and_multiplier(type_left, type_right, type_result);
            return OperationImpl::adapt_decimal_vector_constant(
                    column_left->get_ptr(), column_right_ptr->template get_value<B>(), type_left,
                    type_right, max_and_multiplier.first, max_and_multiplier.second, res_data_type);
        } else {
            return OperationImpl::adapt_normal_vector_constant(
                    column_left->get_ptr(), column_right_ptr->template get_value<B>());
        }
    }

    static ColumnPtr constant_vector(ColumnPtr column_left, ColumnPtr column_right,
                                     const LeftDataType& type_left, const RightDataType& type_right,
                                     DataTypePtr res_data_type) {
        auto column_left_ptr = check_and_get_column<ColumnConst>(column_left);
        DCHECK(column_left_ptr != nullptr);

        if constexpr (result_is_decimal) {
            const auto& type_result =
                    assert_cast<const DataTypeDecimal<ResultType>&>(*res_data_type);
            auto max_and_multiplier = get_max_and_multiplier(type_left, type_right, type_result);
            return OperationImpl::adapt_decimal_constant_vector(
                    column_left_ptr->template get_value<A>(), column_right->get_ptr(), type_left,
                    type_right, max_and_multiplier.first, max_and_multiplier.second, res_data_type);
        } else {
            return OperationImpl::adapt_normal_constant_vector(
                    column_left_ptr->template get_value<A>(), column_right->get_ptr());
        }
    }

    static ColumnPtr vector_vector(ColumnPtr column_left, ColumnPtr column_right,
                                   const LeftDataType& type_left, const RightDataType& type_right,
                                   DataTypePtr res_data_type) {
        if constexpr (result_is_decimal) {
            const auto& type_result =
                    assert_cast<const DataTypeDecimal<ResultType>&>(*res_data_type);
            auto max_and_multiplier = get_max_and_multiplier(type_left, type_right, type_result);
            return OperationImpl::adapt_decimal_vector_vector(
                    column_left->get_ptr(), column_right->get_ptr(), type_left, type_right,
                    max_and_multiplier.first, max_and_multiplier.second, res_data_type);
        } else {
            return OperationImpl::adapt_normal_vector_vector(column_left->get_ptr(),
                                                             column_right->get_ptr());
        }
    }
};

template <template <typename, typename> class Operation, typename Name, bool is_to_null_type>
class FunctionBinaryArithmetic : public IFunction {
    using OpTraits = OperationTraits<Operation>;

    mutable bool need_replace_null_data_to_default_ = false;

    template <typename F>
    static bool cast_type(const IDataType* type, F&& f) {
        return cast_type_to_either<DataTypeUInt8, DataTypeInt8, DataTypeInt16, DataTypeInt32,
                                   DataTypeInt64, DataTypeInt128, DataTypeFloat32, DataTypeFloat64,
                                   DataTypeDecimal<Decimal32>, DataTypeDecimal<Decimal64>,
                                   DataTypeDecimal<Decimal128V2>, DataTypeDecimal<Decimal128V3>,
                                   DataTypeDecimal<Decimal256>>(type, std::forward<F>(f));
    }

    template <typename F>
    static bool cast_both_types(const IDataType* left, const IDataType* right, F&& f) {
        return cast_type(left, [&](const auto& left_) {
            return cast_type(right, [&](const auto& right_) { return f(left_, right_); });
        });
    }

    template <typename F>
    static bool cast_both_types(const IDataType* left, const IDataType* right, const IDataType* res,
                                F&& f) {
        return cast_type(left, [&](const auto& left_) {
            return cast_type(right, [&](const auto& right_) {
                return cast_type(res, [&](const auto& res_) { return f(left_, right_, res_); });
            });
        });
    }

public:
    static constexpr auto name = Name::name;

    static FunctionPtr create() { return std::make_shared<FunctionBinaryArithmetic>(); }

    FunctionBinaryArithmetic() = default;

    String get_name() const override { return name; }

    bool need_replace_null_data_to_default() const override {
        return need_replace_null_data_to_default_;
    }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (OpTraits::has_variadic_argument) {
            return OpTraits::Op::get_variadic_argument_types();
        }
        return {};
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DataTypePtr type_res;
        bool valid = cast_both_types(
                arguments[0].get(), arguments[1].get(), [&](const auto& left, const auto& right) {
                    using LeftDataType = std::decay_t<decltype(left)>;
                    using RightDataType = std::decay_t<decltype(right)>;
                    using ResultDataType =
                            typename BinaryOperationTraits<Operation, LeftDataType,
                                                           RightDataType>::ResultDataType;
                    if constexpr (!std::is_same_v<ResultDataType, InvalidType>) {
                        need_replace_null_data_to_default_ =
                                IsDataTypeDecimal<ResultDataType> ||
                                (get_name() == "pow" &&
                                 std::is_floating_point_v<typename ResultDataType::FieldType>);
                        if constexpr (IsDataTypeDecimal<LeftDataType> &&
                                      IsDataTypeDecimal<RightDataType>) {
                            type_res = decimal_result_type(left, right, OpTraits::is_multiply,
                                                           OpTraits::is_division,
                                                           OpTraits::is_plus_minus);
                        } else if constexpr (IsDataTypeDecimal<LeftDataType>) {
                            type_res = std::make_shared<LeftDataType>(left.get_precision(),
                                                                      left.get_scale());
                        } else if constexpr (IsDataTypeDecimal<RightDataType>) {
                            type_res = std::make_shared<RightDataType>(right.get_precision(),
                                                                       right.get_scale());
                        } else if constexpr (IsDataTypeDecimal<ResultDataType>) {
                            type_res = std::make_shared<ResultDataType>(27, 9);
                        } else {
                            type_res = std::make_shared<ResultDataType>();
                        }
                        return true;
                    }
                    return false;
                });
        if (!valid) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Illegal types {} and {} of arguments of function {}",
                            arguments[0]->get_name(), arguments[1]->get_name(), get_name());
        }

        if constexpr (is_to_null_type) {
            return make_nullable(type_res);
        }

        return type_res;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto* left_generic = block.get_by_position(arguments[0]).type.get();
        auto* right_generic = block.get_by_position(arguments[1]).type.get();
        auto* result_generic = block.get_by_position(result).type.get();
        if (left_generic->is_nullable()) {
            left_generic =
                    static_cast<const DataTypeNullable*>(left_generic)->get_nested_type().get();
        }
        if (right_generic->is_nullable()) {
            right_generic =
                    static_cast<const DataTypeNullable*>(right_generic)->get_nested_type().get();
        }
        if (result_generic->is_nullable()) {
            result_generic =
                    static_cast<const DataTypeNullable*>(result_generic)->get_nested_type().get();
        }

        bool check_overflow_for_decimal = context->check_overflow_for_decimal();
        Status status;
        bool valid = cast_both_types(
                left_generic, right_generic, result_generic,
                [&](const auto& left, const auto& right, const auto& res) {
                    using LeftDataType = std::decay_t<decltype(left)>;
                    using RightDataType = std::decay_t<decltype(right)>;
                    using ExpectedResultDataType = std::decay_t<decltype(res)>;
                    using ResultDataType =
                            typename BinaryOperationTraits<Operation, LeftDataType,
                                                           RightDataType>::ResultDataType;
                    if constexpr (
                            !std::is_same_v<ResultDataType, InvalidType> &&
                            (IsDataTypeDecimal<ExpectedResultDataType> ==
                             IsDataTypeDecimal<
                                     ResultDataType>)&&(IsDataTypeDecimal<ExpectedResultDataType> ==
                                                        (IsDataTypeDecimal<LeftDataType> ||
                                                         IsDataTypeDecimal<RightDataType>))) {
                        if (check_overflow_for_decimal) {
                            // !is_to_null_type: plus, minus, multiply,
                            //                   pow, bitxor, bitor, bitand
                            // if check_overflow and params are decimal types:
                            //   for functions pow, bitxor, bitor, bitand, return error
                            if constexpr (IsDataTypeDecimal<ResultDataType> && !is_to_null_type &&
                                          !OpTraits::is_multiply && !OpTraits::is_plus_minus) {
                                status = Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                                        "cannot check overflow with decimal for function {}", name);
                                return false;
                            }
                            auto column_result = ConstOrVectorAdapter<
                                    LeftDataType, RightDataType,
                                    std::conditional_t<IsDataTypeDecimal<ExpectedResultDataType>,
                                                       ExpectedResultDataType, ResultDataType>,
                                    Operation, Name, is_to_null_type,
                                    true>::execute(block.get_by_position(arguments[0]).column,
                                                   block.get_by_position(arguments[1]).column, left,
                                                   right,
                                                   remove_nullable(
                                                           block.get_by_position(result).type));
                            block.replace_by_position(result, std::move(column_result));
                        } else {
                            auto column_result = ConstOrVectorAdapter<
                                    LeftDataType, RightDataType,
                                    std::conditional_t<IsDataTypeDecimal<ExpectedResultDataType>,
                                                       ExpectedResultDataType, ResultDataType>,
                                    Operation, Name, is_to_null_type,
                                    false>::execute(block.get_by_position(arguments[0]).column,
                                                    block.get_by_position(arguments[1]).column,
                                                    left, right,
                                                    remove_nullable(
                                                            block.get_by_position(result).type));
                            block.replace_by_position(result, std::move(column_result));
                        }
                        return true;
                    }
                    return false;
                });
        if (!valid) {
            if (status.ok()) {
                return Status::RuntimeError("{}'s arguments do not match the expected data types",
                                            get_name());
            }
            return status;
        }

        return Status::OK();
    }
};

} // namespace doris::vectorized
