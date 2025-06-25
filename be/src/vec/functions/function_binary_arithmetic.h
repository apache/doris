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

#include <functional>
#include <memory>
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
#include "vec/data_types/data_type_factory.hpp"
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

template <template <PrimitiveType, PrimitiveType> typename Operation,
          PrimitiveType OpA = TYPE_BOOLEAN, PrimitiveType OpB = TYPE_BOOLEAN>
struct OperationTraits {
    static constexpr PrimitiveType T = TYPE_BOOLEAN;
    using Op = Operation<T, T>;
    static constexpr bool has_variadic_argument =
            !std::is_void_v<decltype(has_variadic_argument_types(std::declval<Op>()))>;
};

template <PrimitiveType A, PrimitiveType B, typename Op, PrimitiveType ResultType = Op::ResultType>
struct BinaryOperationImplBase {
    using Traits = NumberTraits::BinaryOperatorTraits<A, B>;
    using ColumnVectorResult = typename PrimitiveTypeTraits<ResultType>::ColumnType;
    using DataItemA = typename PrimitiveTypeTraits<A>::ColumnItemType;
    using DataItemB = typename PrimitiveTypeTraits<B>::ColumnItemType;
    using ResultDataItem = typename PrimitiveTypeTraits<ResultType>::ColumnItemType;

    static void vector_vector(const PaddedPODArray<DataItemA>& a,
                              const PaddedPODArray<DataItemB>& b,
                              PaddedPODArray<ResultDataItem>& c) {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::template apply<ResultType>(a[i], b[i]);
        }
    }

    static void vector_vector(const PaddedPODArray<DataItemA>& a,
                              const PaddedPODArray<DataItemB>& b, PaddedPODArray<ResultDataItem>& c,
                              PaddedPODArray<UInt8>& null_map) {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::template apply<ResultType>(a[i], b[i], null_map[i]);
        }
    }

    static void vector_constant(const PaddedPODArray<DataItemA>& a, DataItemB b,
                                PaddedPODArray<ResultDataItem>& c) {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::template apply<ResultType>(a[i], b);
        }
    }

    static void vector_constant(const PaddedPODArray<DataItemA>& a, DataItemB b,
                                PaddedPODArray<ResultDataItem>& c,
                                PaddedPODArray<UInt8>& null_map) {
        Op::template apply<ResultType>(a, b, c, null_map);
    }

    static void constant_vector(DataItemA a, const PaddedPODArray<DataItemB>& b,
                                PaddedPODArray<ResultDataItem>& c) {
        size_t size = b.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::template apply<ResultType>(a, b[i]);
        }
    }

    static void constant_vector(DataItemA a, const PaddedPODArray<DataItemB>& b,
                                PaddedPODArray<ResultDataItem>& c,
                                PaddedPODArray<UInt8>& null_map) {
        size_t size = b.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Op::template apply<ResultType>(a, b[i], null_map[i]);
        }
    }

    static typename PrimitiveTypeTraits<ResultType>::ColumnItemType constant_constant(DataItemA a,
                                                                                      DataItemB b) {
        return Op::template apply<ResultType>(a, b);
    }

    static typename PrimitiveTypeTraits<ResultType>::ColumnItemType constant_constant(
            DataItemA a, DataItemB b, UInt8& is_null) {
        return Op::template apply<ResultType>(a, b, is_null);
    }
};

template <PrimitiveType A, PrimitiveType B, typename Op, bool is_to_null_type,
          PrimitiveType ResultType = Op::ResultType>
struct BinaryOperationImpl {
    using Base = BinaryOperationImplBase<A, B, Op, ResultType>;

    static ColumnPtr adapt_normal_constant_constant(typename Base::DataItemA a,
                                                    typename Base::DataItemB b) {
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

    static ColumnPtr adapt_normal_vector_constant(ColumnPtr column_left,
                                                  typename Base::DataItemB b) {
        auto column_left_ptr =
                check_and_get_column<typename Base::Traits::ColumnVectorA>(column_left.get());
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

    static ColumnPtr adapt_normal_constant_vector(typename Base::DataItemA a,
                                                  ColumnPtr column_right) {
        auto column_right_ptr =
                check_and_get_column<typename Base::Traits::ColumnVectorB>(column_right.get());
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
                check_and_get_column<typename Base::Traits::ColumnVectorA>(column_left.get());
        auto column_right_ptr =
                check_and_get_column<typename Base::Traits::ColumnVectorB>(column_right.get());

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
template <PrimitiveType LeftDataPType, PrimitiveType RightDataPType, typename ResultDataType,
          template <PrimitiveType, PrimitiveType> typename Operation, typename Name,
          PrimitiveType ResultPType, bool is_to_null_type, bool check_overflow>
struct DecimalBinaryOperation {
    using LeftDataType = typename PrimitiveTypeTraits<LeftDataPType>::DataType;
    using RightDataType = typename PrimitiveTypeTraits<RightDataPType>::DataType;
    using A = typename LeftDataType::FieldType;
    using B = typename RightDataType::FieldType;
    using OpTraits = OperationTraits<Operation, LeftDataPType, RightDataPType>;

    using Op = Operation<ResultPType, ResultPType>;

    using ResultType = typename PrimitiveTypeTraits<ResultPType>::ColumnItemType;
    using Traits = NumberTraits::BinaryOperatorTraits<LeftDataPType, RightDataPType>;
    using ArrayC = typename ColumnDecimal<ResultPType>::Container;
    using NativeResultType = typename PrimitiveTypeTraits<ResultPType>::CppNativeType;

private:
    template <typename T>
    static int8_t sgn(const T& x) {
        return (x > 0) ? 1 : ((x < 0) ? -1 : 0);
    }

    static void vector_constant(const typename Traits::ArrayA::value_type* __restrict a, B b,
                                typename ArrayC::value_type* c, const LeftDataType& type_left,
                                const RightDataType& type_right, const ResultDataType& type_result,
                                size_t size, const ResultType& max_result_number,
                                const ResultType& scale_diff_multiplier) {
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

    static ResultType constant_constant(A a, B b, const LeftDataType& type_left,
                                        const RightDataType& type_right,
                                        const ResultDataType& type_result,
                                        const ResultType& max_result_number,
                                        const ResultType& scale_diff_multiplier) {
        return ResultType(apply<true>(a, b, type_left, type_right, type_result, max_result_number,
                                      scale_diff_multiplier));
    }

public:
    static ColumnPtr adapt_decimal_constant_constant(A a, B b, const LeftDataType& type_left,
                                                     const RightDataType& type_right,
                                                     const ResultType& max_result_number,
                                                     const ResultType& scale_diff_multiplier,
                                                     DataTypePtr res_data_type) {
        auto type_result =
                assert_cast<const DataTypeDecimal<ResultType::PType>&, TypeCheckOnRelease::DISABLE>(
                        *res_data_type);
        auto column_result = ColumnDecimal<ResultType::PType>::create(
                1,
                assert_cast<const DataTypeDecimal<ResultType::PType>&, TypeCheckOnRelease::DISABLE>(
                        *res_data_type)
                        .get_scale());

        if constexpr (check_overflow && !is_to_null_type) {
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
                assert_cast<const DataTypeDecimal<ResultType::PType>&, TypeCheckOnRelease::DISABLE>(
                        *res_data_type);
        auto column_left_ptr =
                check_and_get_column<typename Traits::ColumnVectorA>(column_left.get());
        auto column_result = ColumnDecimal<ResultType::PType>::create(
                column_left->size(),
                assert_cast<const DataTypeDecimal<ResultType::PType>&, TypeCheckOnRelease::DISABLE>(
                        *res_data_type)
                        .get_scale());
        DCHECK(column_left_ptr != nullptr);

        if constexpr (check_overflow && !is_to_null_type) {
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
                assert_cast<const DataTypeDecimal<ResultType::PType>&, TypeCheckOnRelease::DISABLE>(
                        *res_data_type);
        auto column_right_ptr =
                check_and_get_column<typename Traits::ColumnVectorB>(column_right.get());
        auto column_result = ColumnDecimal<ResultType::PType>::create(
                column_right->size(),
                assert_cast<const DataTypeDecimal<ResultType::PType>&, TypeCheckOnRelease::DISABLE>(
                        *res_data_type)
                        .get_scale());
        DCHECK(column_right_ptr != nullptr);

        if constexpr (check_overflow && !is_to_null_type) {
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
        auto column_left_ptr =
                check_and_get_column<typename Traits::ColumnVectorA>(column_left.get());
        auto column_right_ptr =
                check_and_get_column<typename Traits::ColumnVectorB>(column_right.get());

        const auto& type_result =
                assert_cast<const DataTypeDecimal<ResultType::PType>&>(*res_data_type);
        auto column_result = ColumnDecimal<ResultType::PType>::create(column_left->size(),
                                                                      type_result.get_scale());
        DCHECK(column_left_ptr != nullptr && column_right_ptr != nullptr);

        if constexpr (check_overflow && !is_to_null_type) {
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
};

/// Used to indicate undefined operation
struct InvalidType {
    static constexpr PrimitiveType PType = INVALID_TYPE;
};

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

template <PrimitiveType A, PrimitiveType B>
constexpr bool UseLeftDecimal = false;
template <>
inline constexpr bool UseLeftDecimal<TYPE_DECIMAL256, TYPE_DECIMAL32> = true;
template <>
inline constexpr bool UseLeftDecimal<TYPE_DECIMAL256, TYPE_DECIMAL64> = true;
template <>
inline constexpr bool UseLeftDecimal<TYPE_DECIMAL256, TYPE_DECIMAL128I> = true;
template <>
inline constexpr bool UseLeftDecimal<TYPE_DECIMAL128I, TYPE_DECIMAL32> = true;
template <>
inline constexpr bool UseLeftDecimal<TYPE_DECIMAL128I, TYPE_DECIMAL64> = true;
template <>
inline constexpr bool UseLeftDecimal<TYPE_DECIMAL64, TYPE_DECIMAL32> = true;

template <template <PrimitiveType, PrimitiveType> class Operation, typename LeftDataType,
          typename RightDataType>
struct BinaryOperationTraits {
    using Op = Operation<LeftDataType::PType, RightDataType::PType>;
    using OpTraits = OperationTraits<Operation>;
    /// Appropriate result type for binary operator on numeric types. "Date" can also mean
    /// DateTime, but if both operands are Dates, their type must be the same (e.g. Date - DateTime is invalid).
    using ResultDataType = Switch<
            /// Decimal cases
            Case<(IsDataTypeDecimal<LeftDataType> || IsDataTypeDecimal<RightDataType>),
                 InvalidType>,
            Case<(IsDataTypeDecimalV2<LeftDataType> && IsDataTypeDecimal<RightDataType> &&
                  !IsDataTypeDecimalV2<RightDataType>) ||
                         (IsDataTypeDecimalV2<RightDataType> && IsDataTypeDecimal<LeftDataType> &&
                          !IsDataTypeDecimalV2<LeftDataType>),
                 InvalidType>,
            Case<IsDataTypeDecimal<LeftDataType> && IsDataTypeDecimal<RightDataType> &&
                         UseLeftDecimal<LeftDataType::PType, RightDataType::PType>,
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
                 typename PrimitiveTypeTraits<Op::ResultType>::DataType>>;
};

template <PrimitiveType LeftPType, PrimitiveType RightPType, PrimitiveType FEResultDataPType,
          template <PrimitiveType, PrimitiveType> class Operation, typename Name,
          bool is_to_null_type, bool check_overflow_for_decimal>
struct ConstOrVectorAdapter {
    using LeftDataType = typename PrimitiveTypeTraits<LeftPType>::DataType;
    using RightDataType = typename PrimitiveTypeTraits<RightPType>::DataType;
    static constexpr bool result_is_decimal =
            IsDataTypeDecimal<LeftDataType> || IsDataTypeDecimal<RightDataType>;
    using ResultDataType = typename PrimitiveTypeTraits<FEResultDataPType>::DataType;
    using ResultType = typename ResultDataType::FieldType;
    using A = typename LeftDataType::FieldType;
    using B = typename RightDataType::FieldType;
    using OpTraits = OperationTraits<Operation, LeftPType, RightPType>;

    using OperationImpl = std::conditional_t<
            IsDataTypeDecimal<ResultDataType>,
            DecimalBinaryOperation<LeftPType, RightPType, ResultDataType, Operation, Name,
                                   FEResultDataPType, is_to_null_type, check_overflow_for_decimal>,
            BinaryOperationImpl<LeftPType, RightPType, Operation<LeftPType, RightPType>,
                                is_to_null_type, FEResultDataPType>>;

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
    template <PrimitiveType PT>
    static std::pair<ResultType, ResultType> get_max_and_multiplier(
            const LeftDataType& type_left, const RightDataType& type_right,
            const DataTypeDecimal<PT>& type_result) {
        auto max_result_number =
                DataTypeDecimal<PT>::get_max_digits_number(type_result.get_precision());

        auto orig_result_scale = type_left.get_scale() + type_right.get_scale();
        auto result_scale = type_result.get_scale();
        DCHECK(orig_result_scale >= result_scale);
        auto scale_diff_multiplier =
                DataTypeDecimal<PT>::get_scale_multiplier(orig_result_scale - result_scale).value;
        return {ResultType(max_result_number), ResultType(scale_diff_multiplier)};
    }

    static ColumnPtr constant_constant(ColumnPtr column_left, ColumnPtr column_right,
                                       const LeftDataType& type_left,
                                       const RightDataType& type_right, DataTypePtr res_data_type) {
        const auto* column_left_ptr = check_and_get_column<ColumnConst>(column_left.get());
        const auto* column_right_ptr = check_and_get_column<ColumnConst>(column_right.get());
        DCHECK(column_left_ptr != nullptr && column_right_ptr != nullptr);

        ColumnPtr column_result = nullptr;

        if constexpr (result_is_decimal) {
            const auto& type_result =
                    assert_cast<const DataTypeDecimal<ResultType::PType>&>(*res_data_type);
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
        const auto* column_right_ptr = check_and_get_column<ColumnConst>(column_right.get());
        DCHECK(column_right_ptr != nullptr);

        if constexpr (result_is_decimal) {
            const auto& type_result =
                    assert_cast<const DataTypeDecimal<ResultType::PType>&>(*res_data_type);
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
        const auto* column_left_ptr = check_and_get_column<ColumnConst>(column_left.get());
        DCHECK(column_left_ptr != nullptr);

        if constexpr (result_is_decimal) {
            const auto& type_result =
                    assert_cast<const DataTypeDecimal<ResultType::PType>&>(*res_data_type);
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
                    assert_cast<const DataTypeDecimal<ResultType::PType>&>(*res_data_type);
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

struct BinaryArithmeticState {
    std::function<Status(FunctionContext*, Block&, const ColumnNumbers&, uint32_t, size_t)> impl;
    DataTypePtr left_type;
    DataTypePtr right_type;
    DataTypePtr result_type;
};

template <template <PrimitiveType, PrimitiveType> class Operation, typename Name,
          bool is_to_null_type>
class FunctionBinaryArithmetic : public IFunction {
    using OpTraits = OperationTraits<Operation>;

    mutable bool need_replace_null_data_to_default_ = false;

    template <typename F>
    static bool cast_type(const IDataType* type, F&& f) {
        return cast_type_to_either<DataTypeUInt8, DataTypeInt8, DataTypeInt16, DataTypeInt32,
                                   DataTypeInt64, DataTypeInt128, DataTypeFloat32, DataTypeFloat64,
                                   DataTypeDecimal32, DataTypeDecimal64, DataTypeDecimalV2,
                                   DataTypeDecimal128, DataTypeDecimal256>(type,
                                                                           std::forward<F>(f));
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
                    constexpr PrimitiveType ResultDataType =
                            BinaryOperationTraits<Operation, LeftDataType,
                                                  RightDataType>::ResultDataType::PType;
                    if constexpr (ResultDataType != INVALID_TYPE) {
                        need_replace_null_data_to_default_ =
                                is_decimal(ResultDataType) ||
                                (get_name() == "pow" &&
                                 std::is_floating_point_v<typename PrimitiveTypeTraits<
                                         ResultDataType>::DataType::FieldType>);
                        if constexpr (IsDataTypeDecimal<LeftDataType> &&
                                      IsDataTypeDecimal<RightDataType>) {
                            type_res = decimal_result_type(left, right, false, false, false);
                        } else if constexpr (IsDataTypeDecimal<LeftDataType>) {
                            type_res = std::make_shared<LeftDataType>(left.get_precision(),
                                                                      left.get_scale());
                        } else if constexpr (IsDataTypeDecimal<RightDataType>) {
                            type_res = std::make_shared<RightDataType>(right.get_precision(),
                                                                       right.get_scale());
                        } else if constexpr (is_decimal(ResultDataType)) {
                            type_res = std::make_shared<
                                    typename PrimitiveTypeTraits<ResultDataType>::DataType>(27, 9);
                        } else {
                            type_res = std::make_shared<
                                    typename PrimitiveTypeTraits<ResultDataType>::DataType>();
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

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }
        std::shared_ptr<BinaryArithmeticState> state = std::make_shared<BinaryArithmeticState>();
        context->set_function_state(scope, state);

        state->left_type = remove_nullable(context->get_arg_type(0));
        state->right_type = remove_nullable(context->get_arg_type(1));
        state->result_type = remove_nullable(context->get_return_type());
        const auto* left_generic = state->left_type.get();
        const auto* right_generic = state->right_type.get();
        const auto* result_generic = state->result_type.get();

        const bool check_overflow_for_decimal = context->check_overflow_for_decimal();
        bool valid = cast_both_types(
                left_generic, right_generic, result_generic,
                [&](const auto& left, const auto& right, const auto& res) {
                    using LeftDataType = std::decay_t<decltype(left)>;
                    using RightDataType = std::decay_t<decltype(right)>;
                    using FEResultDataType = std::decay_t<decltype(res)>;
                    using BEResultDataType =
                            typename BinaryOperationTraits<Operation, LeftDataType,
                                                           RightDataType>::ResultDataType;
                    if constexpr (
                            (!std::is_same_v<BEResultDataType,
                                             InvalidType> /* Cannot be InvalidType */) &&
                            (IsDataTypeDecimal<FEResultDataType> ==
                             IsDataTypeDecimal<
                                     BEResultDataType> /* The type planned by FE and the type planned by BE must both be Decimal or not */) &&
                            (IsDataTypeDecimal<FEResultDataType> ==
                             (IsDataTypeDecimal<LeftDataType> ||
                              IsDataTypeDecimal<
                                      RightDataType>)/* Only when at least one of left or right is Decimal, the return value can be Decimal */)) {
                        if (check_overflow_for_decimal) {
                            // !is_to_null_type: plus, minus, multiply,
                            //                   pow, bitxor, bitor, bitand
                            // if check_overflow and params are decimal types:
                            //   for functions pow, bitxor, bitor, bitand, return error
                            static_assert(
                                    !(IsDataTypeDecimal<BEResultDataType> && !is_to_null_type),
                                    "cannot check overflow with decimal for function");

                            state->impl =
                                    execute_with_type<LeftDataType::PType, RightDataType::PType,
                                                      FEResultDataType::PType, true>;
                        } else {
                            state->impl =
                                    execute_with_type<LeftDataType::PType, RightDataType::PType,
                                                      FEResultDataType::PType, false>;
                        }

                        return true;
                    }
                    return false;
                });
        if (!valid) {
            return Status::RuntimeError("{}'s arguments do not match the expected data types",
                                        get_name());
        }

        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto* state = reinterpret_cast<BinaryArithmeticState*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (!state || !state->impl) {
            return Status::RuntimeError("function context for function '{}' must have state;",
                                        get_name());
        }
        return state->impl(context, block, arguments, result, input_rows_count);
    }

    template <PrimitiveType LeftDataType, PrimitiveType RightDataType,
              PrimitiveType FEResultDataType, bool check_overflow_for_decimal>
    static Status execute_with_type(FunctionContext* context, Block& block,
                                    const ColumnNumbers& arguments, uint32_t result,
                                    size_t input_rows_count) {
        const auto& left_type =
                assert_cast<const typename PrimitiveTypeTraits<LeftDataType>::DataType&>(
                        *block.get_by_position(arguments[0]).type);
        const auto& right_type =
                assert_cast<const typename PrimitiveTypeTraits<RightDataType>::DataType&>(
                        *block.get_by_position(arguments[1]).type);

        auto column_result = ConstOrVectorAdapter < LeftDataType, RightDataType,
             is_decimal(FEResultDataType)
                     ? FEResultDataType
                     : BinaryOperationTraits<
                               Operation, typename PrimitiveTypeTraits<LeftDataType>::DataType,
                               typename PrimitiveTypeTraits<RightDataType>::DataType>::
                               ResultDataType::PType,
             Operation, Name, is_to_null_type,
             check_overflow_for_decimal >
                     ::execute(block.get_by_position(arguments[0]).column,
                               block.get_by_position(arguments[1]).column, left_type, right_type,
                               remove_nullable(block.get_by_position(result).type));
        block.replace_by_position(result, std::move(column_result));

        return Status::OK();
    }
};

} // namespace doris::vectorized
