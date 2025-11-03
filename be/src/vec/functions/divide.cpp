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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/divide.cpp
// and modified by Doris

#include <string.h>

#include "runtime/decimalv2_value.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/cast_type_to_either.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct DivideFloatingImpl;

template <typename Impl>
class FunctionDiv : public IFunction {
    static constexpr bool result_is_decimal = !std::is_same_v<Impl, DivideFloatingImpl>;
    mutable bool need_replace_null_data_to_default_ = false;

public:
    static constexpr auto name = "divide";

    static FunctionPtr create() { return std::make_shared<FunctionDiv>(); }

    FunctionDiv() = default;

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
        return make_nullable(arguments[0]);
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

                                column_result = Impl::constant_constant(
                                        column_left_ptr->template get_value<typename Impl::ArgA>(),
                                        column_right_ptr->template get_value<typename Impl::ArgB>(),
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
                                res = Impl::vector_constant(
                                        column_left->get_ptr(),
                                        column_right_ptr->template get_value<typename Impl::ArgB>(),
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
                                        column_right->get_ptr(), max_and_multiplier.first,
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
                                res = Impl::constant_vector(
                                        column_left_ptr->template get_value<typename Impl::ArgA>(),
                                        column_right->get_ptr(), max_and_multiplier.first,
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
                                res = Impl::vector_vector(
                                        column_left->get_ptr(), column_right->get_ptr(),
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
                                res = Impl::vector_vector(
                                        column_left->get_ptr(), column_right->get_ptr(),
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
            res = Impl::vector_vector(column_left->get_ptr(), column_right->get_ptr());
        }
        return res;
    }
};

static const DecimalV2Value one(1, 0);

struct DivideFloatingImpl {
    using ArgA = typename PrimitiveTypeTraits<TYPE_DOUBLE>::CppNativeType;
    using ArgB = typename PrimitiveTypeTraits<TYPE_DOUBLE>::CppNativeType;
    using ColumnType = typename PrimitiveTypeTraits<TYPE_DOUBLE>::ColumnType;
    using DataTypeA = typename PrimitiveTypeTraits<TYPE_DOUBLE>::DataType;
    using DataTypeB = typename PrimitiveTypeTraits<TYPE_DOUBLE>::DataType;

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>()};
    }

    static void apply(const typename ColumnType::Container& a, ArgB b,
                      typename ColumnType::Container& c, PaddedPODArray<UInt8>& null_map) {
        size_t size = c.size();
        UInt8 is_null = b == 0;
        memset(null_map.data(), is_null, size);

        if (!is_null) {
            for (size_t i = 0; i < size; i++) {
                c[i] = (double)a[i] / (double)b;
            }
        }
    }

    static inline ArgA apply(ArgA a, ArgB b, UInt8& is_null) {
        is_null = b == 0;
        return static_cast<ArgA>(a) / (b + is_null);
    }

    static ColumnPtr constant_constant(ArgA a, ArgB b) {
        auto column_result = ColumnFloat64 ::create(1);

        auto null_map = ColumnUInt8::create(1, 0);
        column_result->get_element(0) = apply(a, b, null_map->get_element(0));
        return ColumnNullable::create(std::move(column_result), std::move(null_map));
    }

    static ColumnPtr vector_constant(ColumnPtr column_left, ArgB b) {
        const auto column_left_ptr = assert_cast<const ColumnType*>(column_left.get());
        auto column_result = ColumnFloat64::create(column_left->size());
        DCHECK(column_left_ptr != nullptr);

        auto null_map = ColumnUInt8::create(column_left->size(), 0);
        apply(column_left_ptr->get_data(), b, column_result->get_data(), null_map->get_data());
        return ColumnNullable::create(std::move(column_result), std::move(null_map));
    }

    static ColumnPtr constant_vector(ArgA a, ColumnPtr column_right) {
        const auto column_right_ptr = assert_cast<const ColumnType*>(column_right.get());
        auto column_result = ColumnFloat64::create(column_right->size());
        DCHECK(column_right_ptr != nullptr);

        auto null_map = ColumnUInt8::create(column_right->size(), 0);
        auto& b = column_right_ptr->get_data();
        auto& c = column_result->get_data();
        auto& n = null_map->get_data();
        size_t size = b.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = apply(a, b[i], n[i]);
        }
        return ColumnNullable::create(std::move(column_result), std::move(null_map));
    }

    static ColumnPtr vector_vector(ColumnPtr column_left, ColumnPtr column_right) {
        const auto* column_left_ptr = assert_cast<const ColumnType*>(column_left.get());
        const auto* column_right_ptr = assert_cast<const ColumnType*>(column_right.get());

        auto column_result = ColumnFloat64::create(column_left->size());
        DCHECK(column_left_ptr != nullptr && column_right_ptr != nullptr);

        auto null_map = ColumnUInt8::create(column_result->size(), 0);
        auto& a = column_left_ptr->get_data();
        auto& b = column_right_ptr->get_data();
        auto& c = column_result->get_data();
        auto& n = null_map->get_data();
        size_t size = a.size();
        for (size_t i = 0; i < size; ++i) {
            c[i] = apply(a[i], b[i], n[i]);
        }
        return ColumnNullable::create(std::move(column_result), std::move(null_map));
    }
};

template <PrimitiveType TypeA, PrimitiveType TypeB>
struct DivideDecimalImpl {
    static_assert(is_decimal(TypeA) && is_decimal(TypeB));
    static_assert((TypeA == TYPE_DECIMALV2 && TypeB == TYPE_DECIMALV2) ||
                  (TypeA != TYPE_DECIMALV2 && TypeB != TYPE_DECIMALV2));
    using ArgA = typename PrimitiveTypeTraits<TypeA>::ColumnItemType;
    using ArgB = typename PrimitiveTypeTraits<TypeB>::ColumnItemType;
    using ArgNativeTypeA = typename PrimitiveTypeTraits<TypeA>::CppNativeType;
    using ArgNativeTypeB = typename PrimitiveTypeTraits<TypeB>::CppNativeType;
    using DataTypeA = typename PrimitiveTypeTraits<TypeA>::DataType;
    using DataTypeB = typename PrimitiveTypeTraits<TypeB>::DataType;
    using ColumnTypeA = typename PrimitiveTypeTraits<TypeA>::ColumnType;
    using ColumnTypeB = typename PrimitiveTypeTraits<TypeB>::ColumnType;

    static DataTypes get_variadic_argument_types() {
        return {std::make_shared<typename PrimitiveTypeTraits<TypeA>::DataType>(),
                std::make_shared<typename PrimitiveTypeTraits<TypeB>::DataType>()};
    }

    static inline DecimalV2Value impl(DecimalV2Value a, DecimalV2Value b, UInt8& is_null) {
        is_null = b.is_zero();
        return a / (is_null ? one : b);
    }

    template <PrimitiveType ResultType>
        requires(is_decimal(ResultType))
    static inline typename PrimitiveTypeTraits<ResultType>::CppNativeType impl(ArgNativeTypeA a,
                                                                               ArgNativeTypeB b,
                                                                               UInt8& is_null) {
        is_null = b == 0;
        return static_cast<typename PrimitiveTypeTraits<ResultType>::CppNativeType>(a) /
               (b + is_null);
    }

    template <PrimitiveType ResultType>
        requires(is_decimal(ResultType))
    static ColumnPtr constant_constant(
            ArgA a, ArgB b,
            const typename PrimitiveTypeTraits<ResultType>::CppType& max_result_number,
            const typename PrimitiveTypeTraits<ResultType>::CppType& scale_diff_multiplier,
            const DataTypeDecimal<ResultType>& res_data_type, bool check_overflow_for_decimal) {
        auto column_result = ColumnDecimal<ResultType>::create(1, res_data_type.get_scale());

        auto null_map = ColumnUInt8::create(1, 0);
        if (check_overflow_for_decimal) {
            column_result->get_element(0) =
                    typename PrimitiveTypeTraits<ResultType>::ColumnItemType(
                            apply<true, ResultType>(a.value, b.value, null_map->get_element(0),
                                                    max_result_number));
        } else {
            column_result->get_element(0) =
                    typename PrimitiveTypeTraits<ResultType>::ColumnItemType(
                            apply<false, ResultType>(a.value, b.value, null_map->get_element(0),
                                                     max_result_number));
        }

        return ColumnNullable::create(std::move(column_result), std::move(null_map));
    }

    template <PrimitiveType ResultType>
        requires(is_decimal(ResultType))
    static ColumnPtr vector_constant(
            ColumnPtr column_left, ArgB b,
            const typename PrimitiveTypeTraits<ResultType>::CppType& max_result_number,
            const typename PrimitiveTypeTraits<ResultType>::CppType& scale_diff_multiplier,
            const DataTypeDecimal<ResultType>& res_data_type, bool check_overflow_for_decimal) {
        const auto* column_left_ptr = assert_cast<const ColumnTypeA*>(column_left.get());
        auto column_result =
                ColumnDecimal<ResultType>::create(column_left->size(), res_data_type.get_scale());
        DCHECK(column_left_ptr != nullptr);

        auto null_map = ColumnUInt8::create(column_left->size(), 0);
        const auto& a = column_left_ptr->get_data().data();
        const auto& c = column_result->get_data().data();
        auto& n = null_map->get_data();
        auto sz = column_left->size();
        if (check_overflow_for_decimal) {
            for (size_t i = 0; i < sz; ++i) {
                c[i] = typename DataTypeDecimal<ResultType>::FieldType(
                        apply<true, ResultType>(a[i].value, b.value, n[i], max_result_number));
            }
        } else {
            for (size_t i = 0; i < sz; ++i) {
                c[i] = typename DataTypeDecimal<ResultType>::FieldType(
                        apply<false, ResultType>(a[i].value, b.value, n[i], max_result_number));
            }
        }

        return ColumnNullable::create(std::move(column_result), std::move(null_map));
    }

    template <PrimitiveType ResultType>
        requires(is_decimal(ResultType))
    static ColumnPtr constant_vector(
            ArgA a, ColumnPtr column_right,
            const typename PrimitiveTypeTraits<ResultType>::CppType& max_result_number,
            const typename PrimitiveTypeTraits<ResultType>::CppType& scale_diff_multiplier,
            const DataTypeDecimal<ResultType>& res_data_type, bool check_overflow_for_decimal) {
        const auto* column_right_ptr = assert_cast<const ColumnTypeB*>(column_right.get());
        auto column_result =
                ColumnDecimal<ResultType>::create(column_right->size(), res_data_type.get_scale());
        DCHECK(column_right_ptr != nullptr);

        auto null_map = ColumnUInt8::create(column_right->size(), 0);
        const auto& b = column_right_ptr->get_data().data();
        const auto& c = column_result->get_data().data();
        auto& n = null_map->get_data();
        auto sz = column_right->size();
        if (check_overflow_for_decimal) {
            for (size_t i = 0; i < sz; ++i) {
                c[i] = typename DataTypeDecimal<ResultType>::FieldType(
                        apply<true, ResultType>(a.value, b[i].value, n[i], max_result_number));
            }
        } else {
            for (size_t i = 0; i < sz; ++i) {
                c[i] = typename DataTypeDecimal<ResultType>::FieldType(
                        apply<false, ResultType>(a.value, b[i].value, n[i], max_result_number));
            }
        }

        return ColumnNullable::create(std::move(column_result), std::move(null_map));
    }

    template <PrimitiveType ResultType>
        requires(is_decimal(ResultType))
    static ColumnPtr vector_vector(
            ColumnPtr column_left, ColumnPtr column_right,
            const typename PrimitiveTypeTraits<ResultType>::CppType& max_result_number,
            const typename PrimitiveTypeTraits<ResultType>::CppType& scale_diff_multiplier,
            const DataTypeDecimal<ResultType>& res_data_type, bool check_overflow_for_decimal) {
        const auto* column_left_ptr = assert_cast<const ColumnTypeA*>(column_left.get());
        const auto* column_right_ptr = assert_cast<const ColumnTypeB*>(column_right.get());

        auto column_result =
                ColumnDecimal<ResultType>::create(column_left->size(), res_data_type.get_scale());
        DCHECK(column_left_ptr != nullptr && column_right_ptr != nullptr);

        // function divide, modulo and pmod
        auto null_map = ColumnUInt8::create(column_result->size(), 0);
        const auto& a = column_left_ptr->get_data().data();
        const auto& b = column_right_ptr->get_data().data();
        const auto& c = column_result->get_data().data();
        auto& n = null_map->get_data();
        auto sz = column_right->size();
        if constexpr (TypeA == TYPE_DECIMALV2) {
            if (check_overflow_for_decimal) {
                for (size_t i = 0; i < sz; ++i) {
                    c[i] = Decimal128V2(apply<true, TYPE_DECIMALV2>(a[i].value, b[i].value, n[i],
                                                                    max_result_number));
                }
            } else {
                for (size_t i = 0; i < sz; ++i) {
                    c[i] = Decimal128V2(apply<false, TYPE_DECIMALV2>(a[i].value, b[i].value, n[i],
                                                                     max_result_number));
                }
            }
        } else {
            if (check_overflow_for_decimal) {
                for (size_t i = 0; i < sz; ++i) {
                    c[i] = typename DataTypeDecimal<ResultType>::FieldType(apply<true, ResultType>(
                            a[i].value, b[i].value, n[i], max_result_number));
                }
            } else {
                for (size_t i = 0; i < sz; ++i) {
                    c[i] = typename DataTypeDecimal<ResultType>::FieldType(apply<false, ResultType>(
                            a[i].value, b[i].value, n[i], max_result_number));
                }
            }
        }
        return ColumnNullable::create(std::move(column_result), std::move(null_map));
    }

    template <bool check_overflow_for_decimal, PrimitiveType ResultType>
        requires(is_decimal(ResultType))
    static ALWAYS_INLINE typename PrimitiveTypeTraits<ResultType>::CppNativeType apply(
            ArgNativeTypeA a, ArgNativeTypeB b, UInt8& is_null,
            const typename PrimitiveTypeTraits<ResultType>::CppType& max_result_number) {
        if constexpr (TypeA == TYPE_DECIMALV2) {
            DecimalV2Value l(a);
            DecimalV2Value r(b);
            auto ans = impl(l, r, is_null);
            using ANS_TYPE = std::decay_t<decltype(ans)>;
            if constexpr (check_overflow_for_decimal) {
                if constexpr (std::is_same_v<ANS_TYPE, DecimalV2Value>) {
                    if (ans.value() > max_result_number.value() ||
                        ans.value() < -max_result_number.value()) {
                        throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                        "Arithmetic overflow: {} {} {} = {}, result type: {}",
                                        DecimalV2Value(a).to_string(), "divide",
                                        DecimalV2Value(b).to_string(),
                                        DecimalV2Value(ans).to_string(),
                                        type_to_string(ResultType));
                    }
                } else if constexpr (IsDecimalNumber<ANS_TYPE>) {
                    if (ans.value > max_result_number.value ||
                        ans.value < -max_result_number.value) {
                        throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                        "Arithmetic overflow: {} {} {} = {}, result type: {}",
                                        DecimalV2Value(a).to_string(), "divide",
                                        DecimalV2Value(b).to_string(),
                                        DecimalV2Value(ans).to_string(),
                                        type_to_string(ResultType));
                    }
                } else {
                    if (ans > max_result_number.value || ans < -max_result_number.value) {
                        throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                        "Arithmetic overflow: {} {} {} = {}, result type: {}",
                                        DecimalV2Value(a).to_string(), "divide",
                                        DecimalV2Value(b).to_string(),
                                        DecimalV2Value(ans).to_string(),
                                        type_to_string(ResultType));
                    }
                }
            }
            typename PrimitiveTypeTraits<ResultType>::CppNativeType result {};
            memcpy(&result, &ans, std::min(sizeof(result), sizeof(ans)));
            return result;
        } else {
            return impl<ResultType>(a, b, is_null);
        }
    }

    template <PrimitiveType PT>
    static std::pair<typename PrimitiveTypeTraits<PT>::CppType,
                     typename PrimitiveTypeTraits<PT>::CppType>
    get_max_and_multiplier(const DataTypeA* type_left, const DataTypeB* type_right,
                           const DataTypeDecimal<PT>& type_result) {
        auto max_result_number =
                DataTypeDecimal<PT>::get_max_digits_number(type_result.get_precision());

        auto orig_result_scale = type_left->get_scale() + type_right->get_scale();
        auto result_scale = type_result.get_scale();
        DCHECK(orig_result_scale >= result_scale);
        auto scale_diff_multiplier =
                DataTypeDecimal<PT>::get_scale_multiplier(orig_result_scale - result_scale);
        return {typename PrimitiveTypeTraits<PT>::CppType(max_result_number),
                typename PrimitiveTypeTraits<PT>::CppType(scale_diff_multiplier)};
    }
};

void register_function_divide(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDiv<DivideFloatingImpl>>();
    factory.register_function<FunctionDiv<DivideDecimalImpl<TYPE_DECIMALV2, TYPE_DECIMALV2>>>();

    factory.register_function<FunctionDiv<DivideDecimalImpl<TYPE_DECIMAL32, TYPE_DECIMAL32>>>();
    factory.register_function<FunctionDiv<DivideDecimalImpl<TYPE_DECIMAL32, TYPE_DECIMAL64>>>();
    factory.register_function<FunctionDiv<DivideDecimalImpl<TYPE_DECIMAL32, TYPE_DECIMAL128I>>>();
    factory.register_function<FunctionDiv<DivideDecimalImpl<TYPE_DECIMAL32, TYPE_DECIMAL256>>>();

    factory.register_function<FunctionDiv<DivideDecimalImpl<TYPE_DECIMAL64, TYPE_DECIMAL32>>>();
    factory.register_function<FunctionDiv<DivideDecimalImpl<TYPE_DECIMAL64, TYPE_DECIMAL64>>>();
    factory.register_function<FunctionDiv<DivideDecimalImpl<TYPE_DECIMAL64, TYPE_DECIMAL128I>>>();
    factory.register_function<FunctionDiv<DivideDecimalImpl<TYPE_DECIMAL64, TYPE_DECIMAL256>>>();

    factory.register_function<FunctionDiv<DivideDecimalImpl<TYPE_DECIMAL128I, TYPE_DECIMAL32>>>();
    factory.register_function<FunctionDiv<DivideDecimalImpl<TYPE_DECIMAL128I, TYPE_DECIMAL64>>>();
    factory.register_function<FunctionDiv<DivideDecimalImpl<TYPE_DECIMAL128I, TYPE_DECIMAL128I>>>();
    factory.register_function<FunctionDiv<DivideDecimalImpl<TYPE_DECIMAL128I, TYPE_DECIMAL256>>>();

    factory.register_function<FunctionDiv<DivideDecimalImpl<TYPE_DECIMAL256, TYPE_DECIMAL32>>>();
    factory.register_function<FunctionDiv<DivideDecimalImpl<TYPE_DECIMAL256, TYPE_DECIMAL64>>>();
    factory.register_function<FunctionDiv<DivideDecimalImpl<TYPE_DECIMAL256, TYPE_DECIMAL128I>>>();
    factory.register_function<FunctionDiv<DivideDecimalImpl<TYPE_DECIMAL256, TYPE_DECIMAL256>>>();
}

} // namespace doris::vectorized
