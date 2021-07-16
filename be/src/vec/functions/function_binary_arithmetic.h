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

#pragma once

#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/cast_type_to_either.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/int_div.h"

namespace doris::vectorized {

/** Arithmetic operations: +, -, *, /, %,
  * intDiv (integer division)
  * Bitwise operations: |, &, ^, ~.
  * Etc.
  */

template <typename A, typename B, typename Op, typename ResultType_ = typename Op::ResultType>
struct BinaryOperationImplBase {
    using ResultType = ResultType_;

    static void NO_INLINE vector_vector(const PaddedPODArray<A>& a, const PaddedPODArray<B>& b,
                                        PaddedPODArray<ResultType>& c) {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) c[i] = Op::template apply<ResultType>(a[i], b[i]);
    }

    static void NO_INLINE vector_constant(const PaddedPODArray<A>& a, B b,
                                          PaddedPODArray<ResultType>& c) {
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) c[i] = Op::template apply<ResultType>(a[i], b);
    }

    static void NO_INLINE constant_vector(A a, const PaddedPODArray<B>& b,
                                          PaddedPODArray<ResultType>& c) {
        size_t size = b.size();
        for (size_t i = 0; i < size; ++i) c[i] = Op::template apply<ResultType>(a, b[i]);
    }

    static ResultType constant_constant(A a, B b) { return Op::template apply<ResultType>(a, b); }
};

template <typename A, typename B, typename Op, typename ResultType = typename Op::ResultType>
struct BinaryOperationImpl : BinaryOperationImplBase<A, B, Op, ResultType> {};

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
struct DivideIntegralOrZeroImpl;
template <typename, typename>
struct LeastBaseImpl;
template <typename, typename>
struct GreatestBaseImpl;
template <typename, typename>
struct ModuloImpl;

/// Binary operations for Decimals need scale args
/// +|- scale one of args (which scale factor is not 1). ScaleR = oneof(Scale1, Scale2);
/// *   no agrs scale. ScaleR = Scale1 + Scale2;
/// /   first arg scale. ScaleR = Scale1 (scale_a = DecimalType<B>::get_scale()).
template <typename A, typename B, template <typename, typename> typename Operation,
          typename ResultType_, bool _check_overflow = true>
struct DecimalBinaryOperation {
    static constexpr bool is_plus_minus =
            std::is_same_v<Operation<Int32, Int32>, PlusImpl<Int32, Int32>> ||
            std::is_same_v<Operation<Int32, Int32>, MinusImpl<Int32, Int32>>;
    static constexpr bool is_multiply =
            std::is_same_v<Operation<Int32, Int32>, MultiplyImpl<Int32, Int32>>;
    static constexpr bool is_float_division =
            std::is_same_v<Operation<Int32, Int32>, DivideFloatingImpl<Int32, Int32>>;
    static constexpr bool is_int_division =
            std::is_same_v<Operation<Int32, Int32>, DivideIntegralImpl<Int32, Int32>> ||
            std::is_same_v<Operation<Int32, Int32>, DivideIntegralOrZeroImpl<Int32, Int32>>;
    static constexpr bool is_division = is_float_division || is_int_division;
    static constexpr bool is_compare =
            std::is_same_v<Operation<Int32, Int32>, LeastBaseImpl<Int32, Int32>> ||
            std::is_same_v<Operation<Int32, Int32>, GreatestBaseImpl<Int32, Int32>>;
    static constexpr bool is_plus_minus_compare = is_plus_minus || is_compare;
    static constexpr bool can_overflow = is_plus_minus || is_multiply;

    using ResultType = ResultType_;
    using NativeResultType = typename NativeType<ResultType>::Type;
    using Op = std::conditional_t<
            is_float_division,
            DivideIntegralImpl<
                    NativeResultType,
                    NativeResultType>, /// substitute divide by intDiv (throw on division by zero)
            Operation<NativeResultType, NativeResultType>>;
    using ColVecA = std::conditional_t<IsDecimalNumber<A>, ColumnDecimal<A>, ColumnVector<A>>;
    using ColVecB = std::conditional_t<IsDecimalNumber<B>, ColumnDecimal<B>, ColumnVector<B>>;
    using ArrayA = typename ColVecA::Container;
    using ArrayB = typename ColVecB::Container;
    using ArrayC = typename ColumnDecimal<ResultType>::Container;
    using SelfNoOverflow = DecimalBinaryOperation<A, B, Operation, ResultType_, false>;

    static void vector_vector(const ArrayA& a, const ArrayB& b, ArrayC& c, ResultType scale_a,
                              ResultType scale_b, bool check_overflow) {
        if (check_overflow)
            vector_vector(a, b, c, scale_a, scale_b);
        else
            SelfNoOverflow::vector_vector(a, b, c, scale_a, scale_b);
    }

    static void vector_constant(const ArrayA& a, B b, ArrayC& c, ResultType scale_a,
                                ResultType scale_b, bool check_overflow) {
        if (check_overflow)
            vector_constant(a, b, c, scale_a, scale_b);
        else
            SelfNoOverflow::vector_constant(a, b, c, scale_a, scale_b);
    }

    static void constant_vector(A a, const ArrayB& b, ArrayC& c, ResultType scale_a,
                                ResultType scale_b, bool check_overflow) {
        if (check_overflow)
            constant_vector(a, b, c, scale_a, scale_b);
        else
            SelfNoOverflow::constant_vector(a, b, c, scale_a, scale_b);
    }

    static ResultType constant_constant(A a, B b, ResultType scale_a, ResultType scale_b,
                                        bool check_overflow) {
        if (check_overflow)
            return constant_constant(a, b, scale_a, scale_b);
        else
            return SelfNoOverflow::constant_constant(a, b, scale_a, scale_b);
    }

    static void NO_INLINE vector_vector(const ArrayA& a, const ArrayB& b, ArrayC& c,
                                        ResultType scale_a [[maybe_unused]],
                                        ResultType scale_b [[maybe_unused]]) {
        size_t size = a.size();
        if constexpr (is_plus_minus_compare) {
            if (scale_a != 1) {
                for (size_t i = 0; i < size; ++i) c[i] = apply_scaled<true>(a[i], b[i], scale_a);
                return;
            } else if (scale_b != 1) {
                for (size_t i = 0; i < size; ++i) c[i] = apply_scaled<false>(a[i], b[i], scale_b);
                return;
            }
        } else if constexpr (is_division && IsDecimalNumber<B>) {
            for (size_t i = 0; i < size; ++i) c[i] = apply_scaled_div(a[i], b[i], scale_a);
            return;
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i) c[i] = apply(a[i], b[i]);
    }

    static void NO_INLINE vector_constant(const ArrayA& a, B b, ArrayC& c,
                                          ResultType scale_a [[maybe_unused]],
                                          ResultType scale_b [[maybe_unused]]) {
        size_t size = a.size();
        if constexpr (is_plus_minus_compare) {
            if (scale_a != 1) {
                for (size_t i = 0; i < size; ++i) c[i] = apply_scaled<true>(a[i], b, scale_a);
                return;
            } else if (scale_b != 1) {
                for (size_t i = 0; i < size; ++i) c[i] = apply_scaled<false>(a[i], b, scale_b);
                return;
            }
        } else if constexpr (is_division && IsDecimalNumber<B>) {
            for (size_t i = 0; i < size; ++i) c[i] = apply_scaled_div(a[i], b, scale_a);
            return;
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i) c[i] = apply(a[i], b);
    }

    static void NO_INLINE constant_vector(A a, const ArrayB& b, ArrayC& c,
                                          ResultType scale_a [[maybe_unused]],
                                          ResultType scale_b [[maybe_unused]]) {
        size_t size = b.size();
        if constexpr (is_plus_minus_compare) {
            if (scale_a != 1) {
                for (size_t i = 0; i < size; ++i) c[i] = apply_scaled<true>(a, b[i], scale_a);
                return;
            } else if (scale_b != 1) {
                for (size_t i = 0; i < size; ++i) c[i] = apply_scaled<false>(a, b[i], scale_b);
                return;
            }
        } else if constexpr (is_division && IsDecimalNumber<B>) {
            for (size_t i = 0; i < size; ++i) c[i] = apply_scaled_div(a, b[i], scale_a);
            return;
        }

        /// default: use it if no return before
        for (size_t i = 0; i < size; ++i) c[i] = apply(a, b[i]);
    }

    static ResultType constant_constant(A a, B b, ResultType scale_a [[maybe_unused]],
                                        ResultType scale_b [[maybe_unused]]) {
        if constexpr (is_plus_minus_compare) {
            if (scale_a != 1)
                return apply_scaled<true>(a, b, scale_a);
            else if (scale_b != 1)
                return apply_scaled<false>(a, b, scale_b);
        } else if constexpr (is_division && IsDecimalNumber<B>)
            return apply_scaled_div(a, b, scale_a);
        return apply(a, b);
    }

private:
    /// there's implicit type convertion here
    static NativeResultType apply(NativeResultType a, NativeResultType b) {
        if constexpr (can_overflow && _check_overflow) {
            NativeResultType res;
            if (Op::template apply<NativeResultType>(a, b, res)) {
                LOG(FATAL) << "Decimal math overflow";
            }
            return res;
        } else
            return Op::template apply<NativeResultType>(a, b);
    }

    template <bool scale_left>
    static NativeResultType apply_scaled(NativeResultType a, NativeResultType b,
                                         NativeResultType scale) {
        if constexpr (is_plus_minus_compare) {
            NativeResultType res;

            if constexpr (_check_overflow) {
                bool overflow = false;
                if constexpr (scale_left)
                    overflow |= common::mul_overflow(a, scale, a);
                else
                    overflow |= common::mul_overflow(b, scale, b);

                if constexpr (can_overflow)
                    overflow |= Op::template apply<NativeResultType>(a, b, res);
                else
                    res = Op::template apply<NativeResultType>(a, b);

                if (overflow) {
                    LOG(FATAL) << "Decimal math overflow";
                }
            } else {
                if constexpr (scale_left)
                    a *= scale;
                else
                    b *= scale;
                res = Op::template apply<NativeResultType>(a, b);
            }

            return res;
        }
    }

    static NativeResultType apply_scaled_div(NativeResultType a, NativeResultType b,
                                             NativeResultType scale) {
        if constexpr (is_division) {
            if constexpr (_check_overflow) {
                bool overflow = false;
                if constexpr (!IsDecimalNumber<A>)
                    overflow |= common::mul_overflow(scale, scale, scale);
                overflow |= common::mul_overflow(a, scale, a);
                if (overflow) {
                    LOG(FATAL) << "Decimal math overflow";
                }
            } else {
                if constexpr (!IsDecimalNumber<A>) scale *= scale;
                a *= scale;
            }

            return Op::template apply<NativeResultType>(a, b);
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
inline constexpr bool IsIntegral<DataTypeUInt16> = true;
template <>
inline constexpr bool IsIntegral<DataTypeUInt32> = true;
template <>
inline constexpr bool IsIntegral<DataTypeUInt64> = true;
template <>
inline constexpr bool IsIntegral<DataTypeInt8> = true;
template <>
inline constexpr bool IsIntegral<DataTypeInt16> = true;
template <>
inline constexpr bool IsIntegral<DataTypeInt32> = true;
template <>
inline constexpr bool IsIntegral<DataTypeInt64> = true;

template <typename DataType>
constexpr bool IsFloatingPoint = false;
template <>
inline constexpr bool IsFloatingPoint<DataTypeFloat32> = true;
template <>
inline constexpr bool IsFloatingPoint<DataTypeFloat64> = true;

template <typename T0, typename T1>
constexpr bool UseLeftDecimal = false;
template <>
inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal128>, DataTypeDecimal<Decimal32>> =
        true;
template <>
inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal128>, DataTypeDecimal<Decimal64>> =
        true;
template <>
inline constexpr bool UseLeftDecimal<DataTypeDecimal<Decimal64>, DataTypeDecimal<Decimal32>> = true;

template <typename T>
using DataTypeFromFieldType =
        std::conditional_t<std::is_same_v<T, NumberTraits::Error>, InvalidType, DataTypeNumber<T>>;

template <template <typename, typename> class Operation, typename LeftDataType,
          typename RightDataType>
struct BinaryOperationTraits {
    using T0 = typename LeftDataType::FieldType;
    using T1 = typename RightDataType::FieldType;

private: /// it's not correct for Decimal
    using Op = Operation<T0, T1>;

public:
    static constexpr bool allow_decimal =
            std::is_same_v<Operation<T0, T0>, PlusImpl<T0, T0>> ||
            std::is_same_v<Operation<T0, T0>, MinusImpl<T0, T0>> ||
            std::is_same_v<Operation<T0, T0>, MultiplyImpl<T0, T0>> ||
            std::is_same_v<Operation<T0, T0>, DivideFloatingImpl<T0, T0>> ||
            std::is_same_v<Operation<T0, T0>, DivideIntegralImpl<T0, T0>> ||
            std::is_same_v<Operation<T0, T0>, DivideIntegralOrZeroImpl<T0, T0>> ||
            std::is_same_v<Operation<T0, T0>, LeastBaseImpl<T0, T0>> ||
            std::is_same_v<Operation<T0, T0>, GreatestBaseImpl<T0, T0>>;

    /// Appropriate result type for binary operator on numeric types. "Date" can also mean
    /// DateTime, but if both operands are Dates, their type must be the same (e.g. Date - DateTime is invalid).
    using ResultDataType = Switch<
            /// Decimal cases
            Case<!allow_decimal &&
                         (IsDataTypeDecimal<LeftDataType> || IsDataTypeDecimal<RightDataType>),
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

template <template <typename, typename> class Op, typename Name,
          bool CanBeExecutedOnDefaultArguments = true>
class FunctionBinaryArithmetic : public IFunction {
    //    const Context & context;
    bool check_decimal_overflow = true;

    template <typename F>
    static bool cast_type(const IDataType* type, F&& f) {
        return cast_type_to_either<DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64,
                                   DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
                                   DataTypeFloat32, DataTypeFloat64,
                                   //            DataTypeDate,
                                   //            DataTypeDateTime,
                                   DataTypeDecimal<Decimal32>, DataTypeDecimal<Decimal64>,
                                   DataTypeDecimal<Decimal128>>(type, std::forward<F>(f));
    }

    template <typename F>
    static bool cast_both_types(const IDataType* left, const IDataType* right, F&& f) {
        return cast_type(left, [&](const auto& left_) {
            return cast_type(right, [&](const auto& right_) { return f(left_, right_); });
        });
    }

    bool is_aggregate_multiply(const DataTypePtr& type0, const DataTypePtr& type1) const {
        if constexpr (!std::is_same_v<Op<UInt8, UInt8>, MultiplyImpl<UInt8, UInt8>>) return false;

        WhichDataType which0(type0);
        WhichDataType which1(type1);

        return (which0.is_aggregate_function() && which1.is_native_uint()) ||
               (which0.is_native_uint() && which1.is_aggregate_function());
    }

    bool is_aggregate_addition(const DataTypePtr& type0, const DataTypePtr& type1) const {
        if constexpr (!std::is_same_v<Op<UInt8, UInt8>, PlusImpl<UInt8, UInt8>>) return false;

        WhichDataType which0(type0);
        WhichDataType which1(type1);

        return which0.is_aggregate_function() && which1.is_aggregate_function();
    }

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionBinaryArithmetic>(); }

    FunctionBinaryArithmetic() {}
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DataTypePtr type_res;
        bool valid = cast_both_types(
                arguments[0].get(), arguments[1].get(), [&](const auto& left, const auto& right) {
                    using LeftDataType = std::decay_t<decltype(left)>;
                    using RightDataType = std::decay_t<decltype(right)>;
                    using ResultDataType =
                            typename BinaryOperationTraits<Op, LeftDataType,
                                                           RightDataType>::ResultDataType;
                    if constexpr (!std::is_same_v<ResultDataType, InvalidType>) {
                        if constexpr (IsDataTypeDecimal<LeftDataType> &&
                                      IsDataTypeDecimal<RightDataType>) {
                            constexpr bool is_multiply =
                                    std::is_same_v<Op<UInt8, UInt8>, MultiplyImpl<UInt8, UInt8>>;
                            constexpr bool is_division =
                                    std::is_same_v<Op<UInt8, UInt8>,
                                                   DivideFloatingImpl<UInt8, UInt8>> ||
                                    std::is_same_v<Op<UInt8, UInt8>,
                                                   DivideIntegralImpl<UInt8, UInt8>> ||
                                    std::is_same_v<Op<UInt8, UInt8>,
                                                   DivideIntegralOrZeroImpl<UInt8, UInt8>>;

                            ResultDataType result_type =
                                    decimal_result_type(left, right, is_multiply, is_division);
                            type_res = std::make_shared<ResultDataType>(result_type.get_precision(),
                                                                        result_type.get_scale());
                        } else if constexpr (IsDataTypeDecimal<LeftDataType>)
                            type_res = std::make_shared<LeftDataType>(left.get_precision(),
                                                                      left.get_scale());
                        else if constexpr (IsDataTypeDecimal<RightDataType>)
                            type_res = std::make_shared<RightDataType>(right.get_precision(),
                                                                       right.get_scale());
                        else
                            type_res = std::make_shared<ResultDataType>();
                        return true;
                    }
                    return false;
                });
        if (!valid) {
            LOG(FATAL) << fmt::format("Illegal types {} and {} of arguments of function {}",
                                      arguments[0]->get_name(), arguments[1]->get_name(),
                                      get_name());
        }

        return type_res;
    }

    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t input_rows_count) override {
        auto* left_generic = block.get_by_position(arguments[0]).type.get();
        auto* right_generic = block.get_by_position(arguments[1]).type.get();
        bool valid = cast_both_types(
                left_generic, right_generic, [&](const auto& left, const auto& right) {
                    using LeftDataType = std::decay_t<decltype(left)>;
                    using RightDataType = std::decay_t<decltype(right)>;
                    using ResultDataType =
                            typename BinaryOperationTraits<Op, LeftDataType,
                                                           RightDataType>::ResultDataType;
                    if constexpr (!std::is_same_v<ResultDataType, InvalidType>) {
                        constexpr bool result_is_decimal =
                                IsDataTypeDecimal<LeftDataType> || IsDataTypeDecimal<RightDataType>;
                        constexpr bool is_multiply =
                                std::is_same_v<Op<UInt8, UInt8>, MultiplyImpl<UInt8, UInt8>>;
                        constexpr bool is_division =
                                std::is_same_v<Op<UInt8, UInt8>,
                                               DivideFloatingImpl<UInt8, UInt8>> ||
                                std::is_same_v<Op<UInt8, UInt8>,
                                               DivideIntegralImpl<UInt8, UInt8>> ||
                                std::is_same_v<Op<UInt8, UInt8>,
                                               DivideIntegralOrZeroImpl<UInt8, UInt8>>;

                        using T0 = typename LeftDataType::FieldType;
                        using T1 = typename RightDataType::FieldType;
                        using ResultType = typename ResultDataType::FieldType;
                        using ColVecT0 = std::conditional_t<IsDecimalNumber<T0>, ColumnDecimal<T0>,
                                                            ColumnVector<T0>>;
                        using ColVecT1 = std::conditional_t<IsDecimalNumber<T1>, ColumnDecimal<T1>,
                                                            ColumnVector<T1>>;
                        using ColVecResult = std::conditional_t<IsDecimalNumber<ResultType>,
                                                                ColumnDecimal<ResultType>,
                                                                ColumnVector<ResultType>>;

                        /// Decimal operations need scale. Operations are on result type.
                        using OpImpl = std::conditional_t<
                                IsDataTypeDecimal<ResultDataType>,
                                DecimalBinaryOperation<T0, T1, Op, ResultType>,
                                BinaryOperationImpl<T0, T1, Op<T0, T1>, ResultType>>;

                        auto col_left_raw = block.get_by_position(arguments[0]).column.get();
                        auto col_right_raw = block.get_by_position(arguments[1]).column.get();
                        if (auto col_left = check_and_get_column_const<ColVecT0>(col_left_raw)) {
                            if (auto col_right =
                                        check_and_get_column_const<ColVecT1>(col_right_raw)) {
                                /// the only case with a non-vector result
                                if constexpr (result_is_decimal) {
                                    ResultDataType type = decimal_result_type(
                                            left, right, is_multiply, is_division);
                                    typename ResultDataType::FieldType scale_a =
                                            type.scale_factor_for(left, is_multiply);
                                    typename ResultDataType::FieldType scale_b =
                                            type.scale_factor_for(right,
                                                                  is_multiply || is_division);
                                    if constexpr (IsDataTypeDecimal<RightDataType> && is_division)
                                        scale_a = right.get_scale_multiplier();

                                    auto res = OpImpl::constant_constant(
                                            col_left->template get_value<T0>(),
                                            col_right->template get_value<T1>(), scale_a, scale_b,
                                            check_decimal_overflow);
                                    block.get_by_position(result).column =
                                            ResultDataType(type.get_precision(), type.get_scale())
                                                    .create_column_const(
                                                            col_left->size(),
                                                            to_field(res, type.get_scale()));

                                } else {
                                    auto res = OpImpl::constant_constant(
                                            col_left->template get_value<T0>(),
                                            col_right->template get_value<T1>());
                                    block.get_by_position(result).column =
                                            ResultDataType().create_column_const(col_left->size(),
                                                                                 to_field(res));
                                }
                                return true;
                            }
                        }

                        typename ColVecResult::MutablePtr col_res = nullptr;
                        if constexpr (result_is_decimal) {
                            ResultDataType type =
                                    decimal_result_type(left, right, is_multiply, is_division);
                            col_res = ColVecResult::create(0, type.get_scale());
                        } else
                            col_res = ColVecResult::create();

                        auto& vec_res = col_res->get_data();
                        vec_res.resize(block.rows());

                        if (auto col_left_const =
                                    check_and_get_column_const<ColVecT0>(col_left_raw)) {
                            if (auto col_right = check_and_get_column<ColVecT1>(col_right_raw)) {
                                if constexpr (result_is_decimal) {
                                    ResultDataType type = decimal_result_type(
                                            left, right, is_multiply, is_division);

                                    typename ResultDataType::FieldType scale_a =
                                            type.scale_factor_for(left, is_multiply);
                                    typename ResultDataType::FieldType scale_b =
                                            type.scale_factor_for(right,
                                                                  is_multiply || is_division);
                                    if constexpr (IsDataTypeDecimal<RightDataType> && is_division)
                                        scale_a = right.get_scale_multiplier();

                                    OpImpl::constant_vector(
                                            col_left_const->template get_value<T0>(),
                                            col_right->get_data(), vec_res, scale_a, scale_b,
                                            check_decimal_overflow);
                                } else
                                    OpImpl::constant_vector(
                                            col_left_const->template get_value<T0>(),
                                            col_right->get_data(), vec_res);
                            } else
                                return false;
                        } else if (auto col_left = check_and_get_column<ColVecT0>(col_left_raw)) {
                            if constexpr (result_is_decimal) {
                                ResultDataType type =
                                        decimal_result_type(left, right, is_multiply, is_division);

                                typename ResultDataType::FieldType scale_a =
                                        type.scale_factor_for(left, is_multiply);
                                typename ResultDataType::FieldType scale_b =
                                        type.scale_factor_for(right, is_multiply || is_division);
                                if constexpr (IsDataTypeDecimal<RightDataType> && is_division)
                                    scale_a = right.get_scale_multiplier();
                                if (auto col_right =
                                            check_and_get_column<ColVecT1>(col_right_raw)) {
                                    OpImpl::vector_vector(col_left->get_data(),
                                                          col_right->get_data(), vec_res, scale_a,
                                                          scale_b, check_decimal_overflow);
                                } else if (auto col_right_const =
                                                   check_and_get_column_const<ColVecT1>(
                                                           col_right_raw)) {
                                    OpImpl::vector_constant(
                                            col_left->get_data(),
                                            col_right_const->template get_value<T1>(), vec_res,
                                            scale_a, scale_b, check_decimal_overflow);
                                } else
                                    return false;
                            } else {
                                if (auto col_right = check_and_get_column<ColVecT1>(col_right_raw))
                                    OpImpl::vector_vector(col_left->get_data(),
                                                          col_right->get_data(), vec_res);
                                else if (auto col_right_const =
                                                 check_and_get_column_const<ColVecT1>(
                                                         col_right_raw))
                                    OpImpl::vector_constant(
                                            col_left->get_data(),
                                            col_right_const->template get_value<T1>(), vec_res);
                                else
                                    return false;
                            }
                        } else
                            return false;

                        block.replace_by_position(result, std::move(col_res));
                        return true;
                    }
                    return false;
                });
        if (!valid) {
            return Status::RuntimeError(
                    fmt::format("{}'s arguments do not match the expected data types", get_name()));
        }

        return Status::OK();
    }

    bool can_be_executed_on_default_arguments() const override {
        return CanBeExecutedOnDefaultArguments;
    }
};

} // namespace doris::vectorized
