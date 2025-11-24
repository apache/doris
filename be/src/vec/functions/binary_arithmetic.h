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

#include "runtime/decimalv2_value.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_vector.h"
#include "vec/common/arithmetic_overflow.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/cast_type_to_either.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
#include "common/compile_check_avoid_begin.h"
template <typename Impl>
struct PlusMinusIntegralImpl {
    static constexpr PrimitiveType Type = Impl::PType;
    static constexpr bool result_is_decimal = false;
    static constexpr auto name = Impl::name;
    using Arg = typename Impl::Arg;
    using ColumnType = typename PrimitiveTypeTraits<Type>::ColumnType;
    using ArgA = Arg;
    using ArgB = Arg;
    using DataTypeA = typename PrimitiveTypeTraits<Type>::DataType;
    using DataTypeB = typename PrimitiveTypeTraits<Type>::DataType;
    static constexpr PrimitiveType ResultType = Type;

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<typename PrimitiveTypeTraits<Type>::DataType>(),
                std::make_shared<typename PrimitiveTypeTraits<Type>::DataType>()};
    }

    static ColumnPtr constant_constant(Arg a, Arg b) {
        auto column_result = ColumnType ::create(1);
        column_result->get_element(0) = Impl::apply(a, b);
        return column_result;
    }

    static ColumnPtr vector_constant(ColumnPtr column_left, Arg b) {
        const auto* column_left_ptr = assert_cast<const ColumnType*>(column_left.get());
        auto column_result = ColumnType::create(column_left->size());

        auto& a = column_left_ptr->get_data();
        auto& c = column_result->get_data();
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Impl::apply(a[i], b);
        }
        return column_result;
    }

    static ColumnPtr constant_vector(Arg a, ColumnPtr column_right) {
        const auto* column_right_ptr = assert_cast<const ColumnType*>(column_right.get());
        auto column_result = ColumnType::create(column_right->size());
        DCHECK(column_right_ptr != nullptr);

        auto& b = column_right_ptr->get_data();
        auto& c = column_result->get_data();
        size_t size = b.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Impl::apply(a, b[i]);
        }
        return column_result;
    }

    static ColumnPtr vector_vector(ColumnPtr column_left, ColumnPtr column_right) {
        const auto* column_left_ptr = assert_cast<const ColumnType*>(column_left.get());
        const auto* column_right_ptr = assert_cast<const ColumnType*>(column_right.get());

        auto column_result = ColumnType::create(column_left->size());

        auto& a = column_left_ptr->get_data();
        auto& b = column_right_ptr->get_data();
        auto& c = column_result->get_data();
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = Impl::apply(a[i], b[i]);
        }
        return column_result;
    }
};

template <typename Impl>
struct PlusMinusDecimalImpl {
    static constexpr bool result_is_decimal = true;
    static constexpr auto name = Impl::name;
    static constexpr PrimitiveType TypeA = Impl::PTypeA;
    static constexpr PrimitiveType TypeB = Impl::PTypeB;
    using ArgA = typename PrimitiveTypeTraits<TypeA>::ColumnItemType;
    using ArgB = typename PrimitiveTypeTraits<TypeB>::ColumnItemType;
    using ArgNativeTypeA = typename Impl::ArgNativeTypeA;
    using ArgNativeTypeB = typename Impl::ArgNativeTypeA;
    using DataTypeA = typename PrimitiveTypeTraits<TypeA>::DataType;
    using DataTypeB = typename PrimitiveTypeTraits<TypeB>::DataType;
    using ColumnTypeA = typename PrimitiveTypeTraits<TypeA>::ColumnType;
    using ColumnTypeB = typename PrimitiveTypeTraits<TypeB>::ColumnType;

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<typename PrimitiveTypeTraits<TypeA>::DataType>(),
                std::make_shared<typename PrimitiveTypeTraits<TypeB>::DataType>()};
    }

    template <PrimitiveType ResultType>
        requires(is_decimal(ResultType))
    static ColumnPtr constant_constant(
            ArgA a, ArgB b, const DataTypeA* type_left, const DataTypeB* type_right,
            const typename PrimitiveTypeTraits<ResultType>::ColumnItemType& max_result_number,
            const typename PrimitiveTypeTraits<ResultType>::ColumnItemType& scale_diff_multiplier,
            const DataTypeDecimal<ResultType>& res_data_type, bool check_overflow_for_decimal) {
        auto column_result = ColumnDecimal<ResultType>::create(1, res_data_type.get_scale());

        if (check_overflow_for_decimal) {
            column_result->get_element(0) =
                    typename PrimitiveTypeTraits<ResultType>::ColumnItemType(
                            apply<true>(a, b, *type_left, *type_right, res_data_type,
                                        max_result_number, scale_diff_multiplier));
        } else {
            column_result->get_element(0) =
                    typename PrimitiveTypeTraits<ResultType>::ColumnItemType(
                            apply<false>(a, b, *type_left, *type_right, res_data_type,
                                         max_result_number, scale_diff_multiplier));
        }
        return column_result;
    }

    template <PrimitiveType ResultType>
        requires(is_decimal(ResultType))
    static ColumnPtr vector_constant(
            ColumnPtr column_left, ArgB b, const DataTypeA* type_left, const DataTypeB* type_right,
            const typename PrimitiveTypeTraits<ResultType>::ColumnItemType& max_result_number,
            const typename PrimitiveTypeTraits<ResultType>::ColumnItemType& scale_diff_multiplier,
            const DataTypeDecimal<ResultType>& res_data_type, bool check_overflow_for_decimal) {
        const auto* column_left_ptr = assert_cast<const ColumnTypeA*>(column_left.get());
        auto column_result =
                ColumnDecimal<ResultType>::create(column_left->size(), res_data_type.get_scale());
        DCHECK(column_left_ptr != nullptr);

        const auto& a = column_left_ptr->get_data();
        auto& c = column_result->get_data();
        std::visit(
                [&](auto check_overflow_for_decimal) {
                    for (size_t i = 0; i < column_left->size(); ++i) {
                        c[i] = typename DataTypeDecimal<ResultType>::FieldType(
                                apply<check_overflow_for_decimal>(a[i], b, *type_left, *type_right,
                                                                  res_data_type, max_result_number,
                                                                  scale_diff_multiplier));
                    }
                },
                make_bool_variant(check_overflow_for_decimal));

        return column_result;
    }

    template <PrimitiveType ResultType>
        requires(is_decimal(ResultType))
    static ColumnPtr constant_vector(
            ArgA a, ColumnPtr column_right, const DataTypeA* type_left, const DataTypeB* type_right,
            const typename PrimitiveTypeTraits<ResultType>::ColumnItemType& max_result_number,
            const typename PrimitiveTypeTraits<ResultType>::ColumnItemType& scale_diff_multiplier,
            const DataTypeDecimal<ResultType>& res_data_type, bool check_overflow_for_decimal) {
        const auto* column_right_ptr = assert_cast<const ColumnTypeB*>(column_right.get());
        auto column_result =
                ColumnDecimal<ResultType>::create(column_right->size(), res_data_type.get_scale());

        auto& b = column_right_ptr->get_data();
        auto& c = column_result->get_data();
        std::visit(
                [&](auto check_overflow_for_decimal) {
                    for (size_t i = 0; i < column_right->size(); ++i) {
                        c[i] = typename DataTypeDecimal<ResultType>::FieldType(
                                apply<check_overflow_for_decimal>(a, b[i], *type_left, *type_right,
                                                                  res_data_type, max_result_number,
                                                                  scale_diff_multiplier));
                    }
                },
                make_bool_variant(check_overflow_for_decimal));
        return column_result;
    }

    /*
    select 999999999999999999999999999 * 999999999999999999999999999;
    999999999999999999999999998000000000.000000000000000001 54 digits
    */
    template <bool check_overflow>
    static void vector_vector(const ColumnDecimal128V2::Container::value_type* __restrict a,
                              const ColumnDecimal128V2::Container::value_type* __restrict b,
                              ColumnDecimal128V2::Container::value_type* c, size_t size) {
        auto sng_uptr = std::unique_ptr<int8_t[]>(new int8_t[size]);
        int8_t* sgn = sng_uptr.get();
        auto max = DecimalV2Value::get_max_decimal();
        auto min = DecimalV2Value::get_min_decimal();

        for (int i = 0; i < size; i++) {
            sgn[i] = ((DecimalV2Value(a[i]).value() > 0) && (DecimalV2Value(b[i]).value() > 0)) ||
                                     ((DecimalV2Value(a[i]).value() < 0) &&
                                      (DecimalV2Value(b[i]).value() < 0))
                             ? 1
                     : ((DecimalV2Value(a[i]).value() == 0) || (DecimalV2Value(b[i]).value() == 0))
                             ? 0
                             : -1;
        }

        for (int i = 0; i < size; i++) {
            if constexpr (check_overflow) {
                int128_t i128_mul_result;
                if (common::mul_overflow(DecimalV2Value(a[i]).value(), DecimalV2Value(b[i]).value(),
                                         i128_mul_result)) {
                    throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                    "Arithmetic overflow: {} {} {} = {}, result type: {}",
                                    DecimalV2Value(a[i]).to_string(), "add",
                                    DecimalV2Value(b[i]).to_string(),
                                    DecimalV2Value(i128_mul_result).to_string(), "decimalv2");
                }
                c[i] = (i128_mul_result - sgn[i]) / DecimalV2Value::ONE_BILLION + sgn[i];
                if (c[i].value > max.value() || c[i].value < min.value()) {
                    throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                    "Arithmetic overflow: {} {} {} = {}, result type: {}",
                                    DecimalV2Value(a[i]).to_string(), "add",
                                    DecimalV2Value(b[i]).to_string(),
                                    DecimalV2Value(i128_mul_result).to_string(), "decimalv2");
                }
            } else {
                c[i] = (DecimalV2Value(a[i]).value() * DecimalV2Value(b[i]).value() - sgn[i]) /
                               DecimalV2Value::ONE_BILLION +
                       sgn[i];
            }
        }
    }

    template <typename T>
    static int8_t sgn(const T& x) {
        return (x > 0) ? 1 : ((x < 0) ? -1 : 0);
    }

    template <PrimitiveType ResultType>
        requires(is_decimal(ResultType))
    static ColumnPtr vector_vector(
            ColumnPtr column_left, ColumnPtr column_right, const DataTypeA* type_left,
            const DataTypeB* type_right,
            const typename PrimitiveTypeTraits<ResultType>::ColumnItemType& max_result_number,
            const typename PrimitiveTypeTraits<ResultType>::ColumnItemType& scale_diff_multiplier,
            const DataTypeDecimal<ResultType>& res_data_type, bool check_overflow_for_decimal) {
        const auto* column_left_ptr = assert_cast<const ColumnTypeA*>(column_left.get());
        const auto* column_right_ptr = assert_cast<const ColumnTypeB*>(column_right.get());

        auto column_result =
                ColumnDecimal<ResultType>::create(column_left->size(), res_data_type.get_scale());
        auto sz = column_left->size();
        const auto& a = column_left_ptr->get_data().data();
        const auto& b = column_right_ptr->get_data().data();
        const auto& c = column_result->get_data().data();
        std::visit(
                [&](auto check_overflow_for_decimal) {
                    for (size_t i = 0; i < sz; i++) {
                        c[i] = typename ColumnDecimal<ResultType>::value_type(
                                apply<check_overflow_for_decimal>(
                                        a[i], b[i], *type_left, *type_right, res_data_type,
                                        max_result_number, scale_diff_multiplier));
                    }
                },
                make_bool_variant(check_overflow_for_decimal));
        return column_result;
    }

    template <bool check_overflow, PrimitiveType ResultType>
        requires(is_decimal(ResultType))
    static ALWAYS_INLINE typename PrimitiveTypeTraits<ResultType>::CppNativeType apply(
            ArgNativeTypeA a, ArgNativeTypeB b, const DataTypeA& type_left,
            const DataTypeB& type_right, const DataTypeDecimal<ResultType>& type_result,
            const typename PrimitiveTypeTraits<ResultType>::ColumnItemType& max_result_number,
            const typename PrimitiveTypeTraits<ResultType>::ColumnItemType& scale_diff_multiplier) {
        if constexpr (ResultType == TYPE_DECIMALV2) {
            // Now, Doris only support decimal +-*/ decimal.
            if constexpr (check_overflow) {
                auto res = Impl::apply(DecimalV2Value(a), DecimalV2Value(b)).value();
                if (res > max_result_number.value || res < -max_result_number.value) {
                    throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                    "Arithmetic overflow: {} {} {} = {}, result type: {}",
                                    DecimalV2Value(a).to_string(), "add",
                                    DecimalV2Value(b).to_string(), DecimalV2Value(res).to_string(),
                                    type_to_string(ResultType));
                }
                return res;
            } else {
                return Impl::apply(DecimalV2Value(a), DecimalV2Value(b)).value();
            }
        } else {
            typename PrimitiveTypeTraits<ResultType>::CppNativeType res;
            if constexpr (check_overflow) {
                // TODO handle overflow gracefully
                if (UNLIKELY(Impl::template apply<ResultType>(a, b, res))) {
                    throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                    "Arithmetic overflow: {} {} {} = {}, result type: {}",
                                    type_left.to_string(ArgA(a)), "add",
                                    type_right.to_string(ArgB(b)), type_to_string(ResultType),
                                    type_result.get_name());
                } else {
                    if (res > max_result_number.value || res < -max_result_number.value) {
                        throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                        "Arithmetic overflow: {} {} {} = {}, result type: {}",
                                        type_left.to_string(ArgA(a)), "add",
                                        type_right.to_string(ArgB(b)), type_to_string(ResultType),
                                        type_result.get_name());
                    }
                }
                return res;
            } else {
                res = Impl::template apply<ResultType>(a, b);
                return res;
            }
        }
    }

    template <PrimitiveType PT>
    static std::pair<typename PrimitiveTypeTraits<PT>::ColumnItemType,
                     typename PrimitiveTypeTraits<PT>::ColumnItemType>
    get_max_and_multiplier(const DataTypeA* type_left, const DataTypeB* type_right,
                           const DataTypeDecimal<PT>& type_result) {
        auto max_result_number =
                DataTypeDecimal<PT>::get_max_digits_number(type_result.get_precision());

        auto orig_result_scale = type_left->get_scale() + type_right->get_scale();
        auto result_scale = type_result.get_scale();
        DCHECK(orig_result_scale >= result_scale);
        auto scale_diff_multiplier =
                DataTypeDecimal<PT>::get_scale_multiplier(orig_result_scale - result_scale);
        return {typename PrimitiveTypeTraits<PT>::ColumnItemType(max_result_number),
                typename PrimitiveTypeTraits<PT>::ColumnItemType(scale_diff_multiplier)};
    }
};

template <typename Impl>
class FunctionPlusMinus : public IFunction {
    static constexpr bool result_is_decimal = Impl::result_is_decimal;
    mutable bool need_replace_null_data_to_default_ = false;

public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionPlusMinus>(); }

    FunctionPlusMinus() = default;

    String get_name() const override { return name; }

    bool need_replace_null_data_to_default() const override {
        return need_replace_null_data_to_default_;
    }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        need_replace_null_data_to_default_ = is_decimal(arguments[0]->get_primitive_type());
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto& column_left = block.get_by_position(arguments[0]).column;
        auto& column_right = block.get_by_position(arguments[1]).column;
        const auto* type_left = assert_cast<const typename Impl::DataTypeA*>(
                block.get_by_position(arguments[0]).type.get());
        const auto* type_right = assert_cast<const typename Impl::DataTypeB*>(
                block.get_by_position(arguments[1]).type.get());
        const auto& res_data_type = remove_nullable(block.get_by_position(result).type);
        bool is_const_left = is_column_const(*column_left);
        bool is_const_right = is_column_const(*column_right);

        ColumnPtr column_result = nullptr;
        if (is_const_left && is_const_right) {
            column_result = constant_constant(column_left, column_right, type_left, type_right,
                                              res_data_type, context->check_overflow_for_decimal());
        } else if (is_const_left) {
            column_result = constant_vector(column_left, column_right, type_left, type_right,
                                            res_data_type, context->check_overflow_for_decimal());
        } else if (is_const_right) {
            column_result = vector_constant(column_left, column_right, type_left, type_right,
                                            res_data_type, context->check_overflow_for_decimal());
        } else {
            column_result = vector_vector(column_left, column_right, type_left, type_right,
                                          res_data_type, context->check_overflow_for_decimal());
        }
        block.replace_by_position(result, std::move(column_result));
        return Status::OK();
    }

private:
    ColumnPtr constant_constant(ColumnPtr column_left, ColumnPtr column_right,
                                const typename Impl::DataTypeA* type_left,
                                const typename Impl::DataTypeB* type_right,
                                DataTypePtr res_data_type, bool check_overflow_for_decimal) const {
        const auto* column_left_ptr = assert_cast<const ColumnConst*>(column_left.get());
        const auto* column_right_ptr = assert_cast<const ColumnConst*>(column_right.get());
        DCHECK(column_left_ptr != nullptr && column_right_ptr != nullptr);

        ColumnPtr column_result = nullptr;

        if constexpr (result_is_decimal) {
            if constexpr (Impl::DataTypeA::PType == TYPE_DECIMALV2) {
                if (!cast_type_to_either<DataTypeDecimalV2>(
                            remove_nullable(res_data_type).get(), [&](const auto& type_result) {
                                auto max_and_multiplier = Impl::get_max_and_multiplier(
                                        type_left, type_right, type_result);

                                column_result = Impl::constant_constant(
                                        column_left_ptr->template get_value<typename Impl::ArgA>(),
                                        column_right_ptr->template get_value<typename Impl::ArgB>(),
                                        type_left, type_right, max_and_multiplier.first,
                                        max_and_multiplier.second, type_result,
                                        check_overflow_for_decimal);
                                return true;
                            })) {
                    throw Exception(ErrorCode::INTERNAL_ERROR,
                                    "Wrong type. Expected: Decimal, Actually: {}",
                                    type_to_string(res_data_type->get_primitive_type()));
                }
            } else {
                if (!cast_type_to_either<DataTypeDecimal32, DataTypeDecimal64, DataTypeDecimal128,
                                         DataTypeDecimal256>(
                            remove_nullable(res_data_type).get(), [&](const auto& type_result) {
                                auto max_and_multiplier = Impl::get_max_and_multiplier(
                                        type_left, type_right, type_result);

                                column_result = Impl::constant_constant(
                                        column_left_ptr->template get_value<typename Impl::ArgA>(),
                                        column_right_ptr->template get_value<typename Impl::ArgB>(),
                                        type_left, type_right, max_and_multiplier.first,
                                        max_and_multiplier.second, type_result,
                                        check_overflow_for_decimal);
                                return true;
                            })) {
                    throw Exception(ErrorCode::INTERNAL_ERROR,
                                    "Wrong type. Expected: Decimal, Actually: {}",
                                    type_to_string(res_data_type->get_primitive_type()));
                }
            }
        } else {
            column_result = Impl::constant_constant(
                    column_left_ptr->template get_value<typename Impl::ArgA>(),
                    column_right_ptr->template get_value<typename Impl::ArgB>());
        }

        return ColumnConst::create(std::move(column_result), column_left->size());
    }

    ColumnPtr vector_constant(ColumnPtr column_left, ColumnPtr column_right,
                              const typename Impl::DataTypeA* type_left,
                              const typename Impl::DataTypeB* type_right, DataTypePtr res_data_type,
                              bool check_overflow_for_decimal) const {
        const auto* column_right_ptr = assert_cast<const ColumnConst*>(column_right.get());
        DCHECK(column_right_ptr != nullptr);

        ColumnPtr res = nullptr;
        if constexpr (result_is_decimal) {
            if constexpr (Impl::DataTypeA::PType == TYPE_DECIMALV2) {
                if (!cast_type_to_either<DataTypeDecimalV2>(
                            remove_nullable(res_data_type).get(), [&](const auto& type_result) {
                                auto max_and_multiplier = Impl::get_max_and_multiplier(
                                        type_left, type_right, type_result);
                                res = Impl::vector_constant(
                                        column_left->get_ptr(),
                                        column_right_ptr->template get_value<typename Impl::ArgB>(),
                                        type_left, type_right, max_and_multiplier.first,
                                        max_and_multiplier.second, type_result,
                                        check_overflow_for_decimal);
                                return true;
                            })) {
                    throw Exception(ErrorCode::INTERNAL_ERROR,
                                    "Wrong type. Expected: Decimal, Actually: {}",
                                    type_to_string(res_data_type->get_primitive_type()));
                }
            } else {
                if (!cast_type_to_either<DataTypeDecimal32, DataTypeDecimal64, DataTypeDecimal128,
                                         DataTypeDecimal256>(
                            remove_nullable(res_data_type).get(), [&](const auto& type_result) {
                                auto max_and_multiplier = Impl::get_max_and_multiplier(
                                        type_left, type_right, type_result);
                                res = Impl::vector_constant(
                                        column_left->get_ptr(),
                                        column_right_ptr->template get_value<typename Impl::ArgB>(),
                                        type_left, type_right, max_and_multiplier.first,
                                        max_and_multiplier.second, type_result,
                                        check_overflow_for_decimal);
                                return true;
                            })) {
                    throw Exception(ErrorCode::INTERNAL_ERROR,
                                    "Wrong type. Expected: Decimal, Actually: {}",
                                    type_to_string(res_data_type->get_primitive_type()));
                }
            }
        } else {
            res = Impl::vector_constant(
                    column_left->get_ptr(),
                    column_right_ptr->template get_value<typename Impl::ArgB>());
        }
        return res;
    }

    ColumnPtr constant_vector(ColumnPtr column_left, ColumnPtr column_right,
                              const typename Impl::DataTypeA* type_left,
                              const typename Impl::DataTypeB* type_right, DataTypePtr res_data_type,
                              bool check_overflow_for_decimal) const {
        const auto* column_left_ptr = assert_cast<const ColumnConst*>(column_left.get());
        DCHECK(column_left_ptr != nullptr);

        ColumnPtr res = nullptr;
        if constexpr (result_is_decimal) {
            if constexpr (Impl::DataTypeA::PType == TYPE_DECIMALV2) {
                if (!cast_type_to_either<DataTypeDecimalV2>(
                            remove_nullable(res_data_type).get(), [&](const auto& type_result) {
                                auto max_and_multiplier = Impl::get_max_and_multiplier(
                                        type_left, type_right, type_result);
                                res = Impl::constant_vector(
                                        column_left_ptr->template get_value<typename Impl::ArgA>(),
                                        column_right->get_ptr(), type_left, type_right,
                                        max_and_multiplier.first, max_and_multiplier.second,
                                        type_result, check_overflow_for_decimal);
                                return true;
                            })) {
                    throw Exception(ErrorCode::INTERNAL_ERROR,
                                    "Wrong type. Expected: Decimal, Actually: {}",
                                    type_to_string(res_data_type->get_primitive_type()));
                }
            } else {
                if (!cast_type_to_either<DataTypeDecimal32, DataTypeDecimal64, DataTypeDecimal128,
                                         DataTypeDecimal256>(
                            remove_nullable(res_data_type).get(), [&](const auto& type_result) {
                                auto max_and_multiplier = Impl::get_max_and_multiplier(
                                        type_left, type_right, type_result);
                                res = Impl::constant_vector(
                                        column_left_ptr->template get_value<typename Impl::ArgA>(),
                                        column_right->get_ptr(), type_left, type_right,
                                        max_and_multiplier.first, max_and_multiplier.second,
                                        type_result, check_overflow_for_decimal);
                                return true;
                            })) {
                    throw Exception(ErrorCode::INTERNAL_ERROR,
                                    "Wrong type. Expected: Decimal, Actually: {}",
                                    type_to_string(res_data_type->get_primitive_type()));
                }
            }
        } else {
            res = Impl::constant_vector(column_left_ptr->template get_value<typename Impl::ArgA>(),
                                        column_right->get_ptr());
        }
        return res;
    }

    ColumnPtr vector_vector(ColumnPtr column_left, ColumnPtr column_right,
                            const typename Impl::DataTypeA* type_left,
                            const typename Impl::DataTypeB* type_right, DataTypePtr res_data_type,
                            bool check_overflow_for_decimal) const {
        ColumnPtr res = nullptr;
        if constexpr (result_is_decimal) {
            if constexpr (Impl::DataTypeA::PType == TYPE_DECIMALV2) {
                if (!cast_type_to_either<DataTypeDecimalV2>(
                            remove_nullable(res_data_type).get(), [&](const auto& type_result) {
                                auto max_and_multiplier = Impl::get_max_and_multiplier(
                                        type_left, type_right, type_result);
                                res = Impl::vector_vector(column_left->get_ptr(),
                                                          column_right->get_ptr(), type_left,
                                                          type_right, max_and_multiplier.first,
                                                          max_and_multiplier.second, type_result,
                                                          check_overflow_for_decimal);
                                return true;
                            })) {
                    throw Exception(ErrorCode::INTERNAL_ERROR,
                                    "Wrong type. Expected: Decimal, Actually: {}",
                                    type_to_string(res_data_type->get_primitive_type()));
                }
            } else {
                if (!cast_type_to_either<DataTypeDecimal32, DataTypeDecimal64, DataTypeDecimal128,
                                         DataTypeDecimal256>(
                            remove_nullable(res_data_type).get(), [&](const auto& type_result) {
                                auto max_and_multiplier = Impl::get_max_and_multiplier(
                                        type_left, type_right, type_result);
                                res = Impl::vector_vector(column_left->get_ptr(),
                                                          column_right->get_ptr(), type_left,
                                                          type_right, max_and_multiplier.first,
                                                          max_and_multiplier.second, type_result,
                                                          check_overflow_for_decimal);
                                return true;
                            })) {
                    throw Exception(ErrorCode::INTERNAL_ERROR,
                                    "Wrong type. Expected: Decimal, Actually: {}",
                                    type_to_string(res_data_type->get_primitive_type()));
                }
            }
        } else {
            res = Impl::vector_vector(column_left->get_ptr(), column_right->get_ptr());
        }
        return res;
    }
};
#include "common/compile_check_avoid_end.h"
} // namespace doris::vectorized
