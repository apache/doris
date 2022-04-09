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

#include "vec/functions/function_binary_arithmetic.h"

namespace doris::vectorized {

/**
 * Arithmetic operations: /, %
 * intDiv (integer division)
 */

template <template <typename, typename> class Op, typename Name,
          bool CanBeExecutedOnDefaultArguments = true>
class FunctionBinaryArithmeticToNullType : public IFunction {
    bool check_decimal_overflow = true;

    template <typename F>
    static bool cast_type(const IDataType* type, F&& f) {
        return cast_type_to_either<DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64,
                                   DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
                                   DataTypeInt128, DataTypeFloat32, DataTypeFloat64,
                                   DataTypeDecimal<Decimal32>, DataTypeDecimal<Decimal64>,
                                   DataTypeDecimal<Decimal128>>(type, std::forward<F>(f));
    }

    template <typename F>
    static bool cast_both_types(const IDataType* left, const IDataType* right, F&& f) {
        return cast_type(left, [&](const auto& left_) {
            return cast_type(right, [&](const auto& right_) { return f(left_, right_); });
        });
    }

public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionBinaryArithmeticToNullType>(); }

    FunctionBinaryArithmeticToNullType() {}
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DataTypePtr type_res;

        const IDataType* first_type = arguments[0].get();
        const IDataType* secord_type = arguments[1].get();
        if (first_type->is_nullable()) {
            first_type = static_cast<const DataTypeNullable*>(first_type)->get_nested_type().get();
        }
        if (secord_type->is_nullable()) {
            secord_type =
                    static_cast<const DataTypeNullable*>(secord_type)->get_nested_type().get();
        }

        bool valid =
                cast_both_types(first_type, secord_type, [&](const auto& left, const auto& right) {
                    using LeftDataType = std::decay_t<decltype(left)>;
                    using RightDataType = std::decay_t<decltype(right)>;
                    using ResultDataType =
                            typename BinaryOperationTraits<Op, LeftDataType,
                                                           RightDataType>::ResultDataType;
                    if constexpr (!std::is_same_v<ResultDataType, InvalidType>) {
                        if constexpr (IsDataTypeDecimal<LeftDataType> &&
                                      IsDataTypeDecimal<RightDataType>) {
                            constexpr bool is_multiply = false;
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
                        else if constexpr (IsDataTypeDecimal<ResultDataType>)
                            type_res = std::make_shared<ResultDataType>(27, 9);
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

        return make_nullable(type_res);
    }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto* left_generic = block.get_by_position(arguments[0]).type.get();
        auto* right_generic = block.get_by_position(arguments[1]).type.get();
        if (left_generic->is_nullable()) {
            left_generic =
                    static_cast<const DataTypeNullable*>(left_generic)->get_nested_type().get();
        }
        if (right_generic->is_nullable()) {
            right_generic =
                    static_cast<const DataTypeNullable*>(right_generic)->get_nested_type().get();
        }

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
                        constexpr bool is_multiply = false;
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

                        auto null_map = ColumnUInt8::create(input_rows_count, 0);
                        auto& null_map_data = null_map->get_data();
                        size_t argument_size = arguments.size();
                        ColumnPtr argument_columns[argument_size];

                        for (size_t i = 0; i < argument_size; ++i) {
                            argument_columns[i] =
                                    block.get_by_position(arguments[i])
                                            .column->convert_to_full_column_if_const();
                        }

                        auto col_left_raw = argument_columns[0].get();
                        auto col_right_raw = argument_columns[1].get();

                        typename ColVecResult::MutablePtr col_res = nullptr;
                        if constexpr (result_is_decimal) {
                            ResultDataType type =
                                    decimal_result_type(left, right, is_multiply, is_division);
                            col_res = ColVecResult::create(0, type.get_scale());
                        } else {
                            col_res = ColVecResult::create();
                        }

                        auto& vec_res = col_res->get_data();
                        vec_res.resize(block.rows());

                        if (auto col_left = check_and_get_column<ColVecT0>(col_left_raw)) {
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
                                                          scale_b, check_decimal_overflow,
                                                          null_map_data);
                                }
                            } else {
                                if (auto col_right =
                                            check_and_get_column<ColVecT1>(col_right_raw)) {
                                    OpImpl::vector_vector(col_left->get_data(),
                                                          col_right->get_data(), vec_res,
                                                          null_map_data);
                                }
                            }
                        } else {
                            return false;
                        }

                        block.get_by_position(result).column =
                                ColumnNullable::create(std::move(col_res), std::move(null_map));
                        return true;
                    } else {
                        return false;
                    }
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

#ifdef DORIS_ENABLE_JIT
protected:
    virtual bool is_compilable_impl(const DataTypes& arguments) const override {
        if (arguments.size() != 2) {
            return false;
        }

        return cast_both_types(arguments[0].get(), arguments[1].get(), [&](const auto& left, const auto& right) {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;
            if constexpr (std::is_same_v<DataTypeString, LeftDataType> || std::is_same_v<DataTypeString, RightDataType>)
                return false;
            else {
                using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;
                using OpSpec = Op<typename LeftDataType::FieldType, typename RightDataType::FieldType>;
                return !std::is_same_v<ResultDataType, InvalidType> && !IsDataTypeDecimal<ResultDataType> && OpSpec::compilable;
            }
        });
    }

    virtual Status compile_impl(llvm::IRBuilderBase& builder, const DataTypes& types, Values values, llvm::Value** result) const override {
        assert(2 == types.size() && 2 == values.size());

        *result = nullptr;
        cast_both_types(types[0].get(), types[1].get(), [&](const auto& left, const auto& right) {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;
            if constexpr (!std::is_same_v<DataTypeString, LeftDataType> && !std::is_same_v<DataTypeString, RightDataType>) {
                using ResultDataType = typename BinaryOperationTraits<Op, LeftDataType, RightDataType>::ResultDataType;
                using OpSpec = Op<typename LeftDataType::FieldType, typename RightDataType::FieldType>;
                if constexpr (!std::is_same_v<ResultDataType, InvalidType> && !IsDataTypeDecimal<ResultDataType> && OpSpec::compilable) {
                    auto& b = static_cast<llvm::IRBuilder<>&>(builder);
                    auto type = std::make_shared<ResultDataType>();
                    auto * lval = native_cast(b, types[0], values[0], type);
                    auto * rval = native_cast(b, types[1], values[1], type);
                    *result = OpSpec::compile(b, lval, rval, std::is_signed_v<typename ResultDataType::FieldType>);
                    return true;
                }
            }
            return false;
        });

        if (*result)
            return Status::OK();
        else
            return Status::RuntimeError("build failed");
    }
#endif

};

} // namespace doris::vectorized
