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

#include "runtime/decimalv2_value.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/arithmetic_overflow.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/cast_type_to_either.h"
#include "vec/functions/function.h"

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

template <template <typename, typename> typename Operation>
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
    static constexpr bool can_overflow = is_plus_minus || is_multiply;
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

/// Binary operations for Decimals need scale args
/// +|- scale one of args (which scale factor is not 1). ScaleR = oneof(Scale1, Scale2);
/// *   no agrs scale. ScaleR = Scale1 + Scale2;
/// /   first arg scale. ScaleR = Scale1 (scale_a = DecimalType<B>::get_scale()).
template <typename A, typename B, template <typename, typename> typename Operation,
          typename ResultType, bool is_to_null_type, bool check_overflow = true>
struct DecimalBinaryOperation {
    using OpTraits = OperationTraits<Operation>;

    using NativeResultType = typename NativeType<ResultType>::Type;
    using Op = Operation<NativeResultType, NativeResultType>;

    using Traits = NumberTraits::BinaryOperatorTraits<A, B>;
    using ArrayC = typename ColumnDecimal<ResultType>::Container;

    static void vector_vector(const typename Traits::ArrayA& a, const typename Traits::ArrayB& b,
                              ArrayC& c) {
        size_t size = a.size();

        if constexpr (OpTraits::is_multiply && IsDecimalV2<A> && IsDecimalV2<B> &&
                      IsDecimalV2<ResultType>) {
            Op::vector_vector(a, b, c);
        } else {
            for (size_t i = 0; i < size; i++) {
                c[i] = apply(a[i], b[i]);
            }
        }
    }

    /// null_map for divide and mod
    static void vector_vector(const typename Traits::ArrayA& a, const typename Traits::ArrayB& b,
                              ArrayC& c, NullMap& null_map) {
        size_t size = a.size();
        if constexpr (IsDecimalV2<B> || IsDecimalV2<A>) {
            /// default: use it if no return before
            for (size_t i = 0; i < size; ++i) {
                c[i] = apply(a[i], b[i], null_map[i]);
            }
        } else {
            if constexpr (OpTraits::is_division && IsDecimalNumber<B>) {
                for (size_t i = 0; i < size; ++i) {
                    c[i] = apply_scaled_div(a[i], b[i], null_map[i]);
                }
                return;
            }
        }
    }

    static void vector_constant(const typename Traits::ArrayA& a, B b, ArrayC& c) {
        size_t size = a.size();
        if constexpr (OpTraits::is_division && IsDecimalNumber<B>) {
            for (size_t i = 0; i < size; ++i) {
                c[i] = apply_scaled_div(a[i], b);
            }
            return;
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i) {
            c[i] = apply(a[i], b);
        }
    }

    static void vector_constant(const typename Traits::ArrayA& a, B b, ArrayC& c,
                                NullMap& null_map) {
        size_t size = a.size();
        if constexpr (OpTraits::is_division && IsDecimalNumber<B>) {
            for (size_t i = 0; i < size; ++i) {
                c[i] = apply_scaled_div(a[i], b, null_map[i]);
            }
            return;
        }

        for (size_t i = 0; i < size; ++i) {
            c[i] = apply(a[i], b, null_map[i]);
        }
    }

    static void constant_vector(A a, const typename Traits::ArrayB& b, ArrayC& c) {
        size_t size = b.size();
        if constexpr (OpTraits::is_division && IsDecimalNumber<B>) {
            for (size_t i = 0; i < size; ++i) {
                c[i] = apply_scaled_div(a, b[i]);
            }
        } else if constexpr (IsDecimalV2<A> || IsDecimalV2<B>) {
            DecimalV2Value da(a);
            for (size_t i = 0; i < size; ++i) {
                c[i] = Op::template apply(da, DecimalV2Value(b[i])).value();
            }
        } else {
            for (size_t i = 0; i < size; ++i) {
                c[i] = apply(a, b[i]);
            }
        }
    }

    static void constant_vector(A a, const typename Traits::ArrayB& b, ArrayC& c,
                                NullMap& null_map) {
        size_t size = b.size();
        if constexpr (OpTraits::is_division && IsDecimalNumber<B>) {
            for (size_t i = 0; i < size; ++i) {
                c[i] = apply_scaled_div(a, b[i], null_map[i]);
            }
            return;
        }

        for (size_t i = 0; i < size; ++i) {
            c[i] = apply(a, b[i], null_map[i]);
        }
    }

    static ResultType constant_constant(A a, B b) {
        if constexpr (OpTraits::is_division && IsDecimalNumber<B>) {
            return apply_scaled_div(a, b);
        }
        return apply(a, b);
    }

    static ResultType constant_constant(A a, B b, UInt8& is_null) {
        if constexpr (OpTraits::is_division && IsDecimalNumber<B>) {
            return apply_scaled_div(a, b, is_null);
        }
        return apply(a, b, is_null);
    }

    static ColumnPtr adapt_decimal_constant_constant(A a, B b, DataTypePtr res_data_type) {
        auto column_result = ColumnDecimal<ResultType>::create(
                1, assert_cast<const DataTypeDecimal<ResultType>&>(*res_data_type).get_scale());

        if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(1, 0);
            column_result->get_element(0) = constant_constant(a, b, null_map->get_element(0));
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            column_result->get_element(0) = constant_constant(a, b);
            return column_result;
        }
    }

    static ColumnPtr adapt_decimal_vector_constant(ColumnPtr column_left, B b,
                                                   DataTypePtr res_data_type) {
        auto column_left_ptr = check_and_get_column<typename Traits::ColumnVectorA>(column_left);
        auto column_result = ColumnDecimal<ResultType>::create(
                column_left->size(),
                assert_cast<const DataTypeDecimal<ResultType>&>(*res_data_type).get_scale());
        DCHECK(column_left_ptr != nullptr);

        if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(column_left->size(), 0);
            vector_constant(column_left_ptr->get_data(), b, column_result->get_data(),
                            null_map->get_data());
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            vector_constant(column_left_ptr->get_data(), b, column_result->get_data());
            return column_result;
        }
    }

    static ColumnPtr adapt_decimal_constant_vector(A a, ColumnPtr column_right,
                                                   DataTypePtr res_data_type) {
        auto column_right_ptr = check_and_get_column<typename Traits::ColumnVectorB>(column_right);
        auto column_result = ColumnDecimal<ResultType>::create(
                column_right->size(),
                assert_cast<const DataTypeDecimal<ResultType>&>(*res_data_type).get_scale());
        DCHECK(column_right_ptr != nullptr);

        if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(column_right->size(), 0);
            constant_vector(a, column_right_ptr->get_data(), column_result->get_data(),
                            null_map->get_data());
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            constant_vector(a, column_right_ptr->get_data(), column_result->get_data());
            return column_result;
        }
    }

    static ColumnPtr adapt_decimal_vector_vector(ColumnPtr column_left, ColumnPtr column_right,
                                                 DataTypePtr res_data_type) {
        auto column_left_ptr = check_and_get_column<typename Traits::ColumnVectorA>(column_left);
        auto column_right_ptr = check_and_get_column<typename Traits::ColumnVectorB>(column_right);

        auto column_result = ColumnDecimal<ResultType>::create(
                column_left->size(),
                assert_cast<const DataTypeDecimal<ResultType>&>(*res_data_type).get_scale());
        DCHECK(column_left_ptr != nullptr && column_right_ptr != nullptr);

        if constexpr (is_to_null_type) {
            auto null_map = ColumnUInt8::create(column_result->size(), 0);
            vector_vector(column_left_ptr->get_data(), column_right_ptr->get_data(),
                          column_result->get_data(), null_map->get_data());
            return ColumnNullable::create(std::move(column_result), std::move(null_map));
        } else {
            vector_vector(column_left_ptr->get_data(), column_right_ptr->get_data(),
                          column_result->get_data());
            return column_result;
        }
    }

private:
    /// there's implicit type conversion here
    static NativeResultType apply(NativeResultType a, NativeResultType b) {
        if constexpr (IsDecimalV2<B> || IsDecimalV2<A>) {
            // Now, Doris only support decimal +-*/ decimal.
            // overflow in consider in operator
            return Op::template apply(DecimalV2Value(a), DecimalV2Value(b)).value();
        } else {
            if constexpr (OpTraits::can_overflow && check_overflow) {
                NativeResultType res;
                // TODO handle overflow gracefully
                if (Op::template apply<NativeResultType>(a, b, res)) {
                    LOG(WARNING) << "Decimal math overflow";
                    res = max_decimal_value<ResultType>();
                }
                return res;
            } else {
                return Op::template apply<NativeResultType>(a, b);
            }
        }
    }

    /// null_map for divide and mod
    static NativeResultType apply(NativeResultType a, NativeResultType b, UInt8& is_null) {
        if constexpr (IsDecimalV2<B> || IsDecimalV2<A>) {
            DecimalV2Value l(a);
            DecimalV2Value r(b);
            auto ans = Op::template apply(l, r, is_null);
            NativeResultType result;
            memcpy(&result, &ans, std::min(sizeof(result), sizeof(ans)));
            return result;
        } else {
            return Op::template apply<NativeResultType>(a, b, is_null);
        }
    }

    static NativeResultType apply_scaled(NativeResultType a, NativeResultType b) {
        if constexpr (OpTraits::is_plus_minus) {
            NativeResultType res;

            if constexpr (check_overflow) {
                bool overflow = false;

                if constexpr (OpTraits::can_overflow) {
                    overflow |= Op::template apply<NativeResultType>(a, b, res);
                } else {
                    res = Op::template apply<NativeResultType>(a, b);
                }

                // TODO handle overflow gracefully
                if (overflow) {
                    LOG(WARNING) << "Decimal math overflow";
                    res = max_decimal_value<ResultType>();
                }
            } else {
                res = apply(a, b);
            }

            return res;
        }
    }

    static NativeResultType apply_scaled_div(NativeResultType a, NativeResultType b,
                                             UInt8& is_null) {
        if constexpr (OpTraits::is_division) {
            return apply(a, b, is_null);
        }
    }

    static NativeResultType apply_scaled_mod(NativeResultType a, NativeResultType b,
                                             UInt8& is_null) {
        return apply(a, b, is_null);
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
inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal128I>, DataTypeDecimal<Decimal32>> =
        true;
template <>
inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal128I>, DataTypeDecimal<Decimal64>> =
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
          template <typename, typename> class Operation, bool is_to_null_type>
struct ConstOrVectorAdapter {
    static constexpr bool result_is_decimal =
            IsDataTypeDecimal<LeftDataType> || IsDataTypeDecimal<RightDataType>;

    using ResultDataType = ExpectedResultDataType;
    using ResultType = typename ResultDataType::FieldType;
    using A = typename LeftDataType::FieldType;
    using B = typename RightDataType::FieldType;

    using OperationImpl = std::conditional_t<
            IsDataTypeDecimal<ResultDataType>,
            DecimalBinaryOperation<A, B, Operation, ResultType, is_to_null_type>,
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
    static ColumnPtr constant_constant(ColumnPtr column_left, ColumnPtr column_right,
                                       const LeftDataType& type_left,
                                       const RightDataType& type_right, DataTypePtr res_data_type) {
        auto column_left_ptr = check_and_get_column<ColumnConst>(column_left);
        auto column_right_ptr = check_and_get_column<ColumnConst>(column_right);
        DCHECK(column_left_ptr != nullptr && column_right_ptr != nullptr);

        ColumnPtr column_result = nullptr;

        if constexpr (result_is_decimal) {
            column_result = OperationImpl::adapt_decimal_constant_constant(
                    column_left_ptr->template get_value<A>(),
                    column_right_ptr->template get_value<B>(), res_data_type);

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
            return OperationImpl::adapt_decimal_vector_constant(
                    column_left->get_ptr(), column_right_ptr->template get_value<B>(),
                    res_data_type);
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
            return OperationImpl::adapt_decimal_constant_vector(
                    column_left_ptr->template get_value<A>(), column_right->get_ptr(),
                    res_data_type);
        } else {
            return OperationImpl::adapt_normal_constant_vector(
                    column_left_ptr->template get_value<A>(), column_right->get_ptr());
        }
    }

    static ColumnPtr vector_vector(ColumnPtr column_left, ColumnPtr column_right,
                                   const LeftDataType& type_left, const RightDataType& type_right,
                                   DataTypePtr res_data_type) {
        if constexpr (result_is_decimal) {
            return OperationImpl::adapt_decimal_vector_vector(
                    column_left->get_ptr(), column_right->get_ptr(), res_data_type);
        } else {
            return OperationImpl::adapt_normal_vector_vector(column_left->get_ptr(),
                                                             column_right->get_ptr());
        }
    }
};

template <template <typename, typename> class Operation, typename Name, bool is_to_null_type>
class FunctionBinaryArithmetic : public IFunction {
    using OpTraits = OperationTraits<Operation>;

    template <typename F>
    static bool cast_type(const IDataType* type, F&& f) {
        return cast_type_to_either<DataTypeUInt8, DataTypeInt8, DataTypeInt16, DataTypeInt32,
                                   DataTypeInt64, DataTypeInt128, DataTypeFloat32, DataTypeFloat64,
                                   DataTypeDecimal<Decimal32>, DataTypeDecimal<Decimal64>,
                                   DataTypeDecimal<Decimal128>, DataTypeDecimal<Decimal128I>>(
                type, std::forward<F>(f));
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
                        if constexpr (IsDataTypeDecimal<LeftDataType> &&
                                      IsDataTypeDecimal<RightDataType>) {
                            ResultDataType result_type = decimal_result_type(
                                    left, right, OpTraits::is_multiply, OpTraits::is_division);
                            type_res = std::make_shared<ResultDataType>(result_type.get_precision(),
                                                                        result_type.get_scale());
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
            LOG(FATAL) << fmt::format("Illegal types {} and {} of arguments of function {}",
                                      arguments[0]->get_name(), arguments[1]->get_name(),
                                      get_name());
        }

        if constexpr (is_to_null_type) {
            return make_nullable(type_res);
        }

        return type_res;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
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
                        auto column_result = ConstOrVectorAdapter<
                                LeftDataType, RightDataType,
                                std::conditional_t<IsDataTypeDecimal<ExpectedResultDataType>,
                                                   ExpectedResultDataType, ResultDataType>,
                                Operation, is_to_null_type>::
                                execute(block.get_by_position(arguments[0]).column,
                                        block.get_by_position(arguments[1]).column, left, right,
                                        remove_nullable(block.get_by_position(result).type));
                        block.replace_by_position(result, std::move(column_result));
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
};

} // namespace doris::vectorized
