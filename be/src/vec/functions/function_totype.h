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
#include <fmt/format.h>

#include "vec/columns/column_complex.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

// support string->complex/primary
// support primary/complex->primary/complex
// support primary -> string
template <typename Impl, typename Name>
class FunctionUnaryToType : public IFunction {
public:
    static constexpr auto name = Name::name;
    static constexpr bool has_variadic_argument =
            !std::is_void_v<decltype(has_variadic_argument_types(std::declval<Impl>()))>;

    static FunctionPtr create() { return std::make_shared<FunctionUnaryToType>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<typename Impl::ReturnType>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        return execute_impl<typename Impl::ReturnType>(block, arguments, result, input_rows_count);
    }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (has_variadic_argument) {
            return Impl::get_variadic_argument_types();
        }
        return {};
    }

private:
    // handle result == DataTypeString
    template <typename T>
        requires std::is_same_v<T, DataTypeString>
    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t input_rows_count) const {
        const ColumnPtr column = block.get_by_position(arguments[0]).column;
        if constexpr (typeindex_is_int(Impl::TYPE_INDEX)) {
            if (auto* col = check_and_get_column<ColumnVector<typename Impl::Type>>(column.get())) {
                auto col_res = Impl::ReturnColumnType::create();
                RETURN_IF_ERROR(Impl::vector(col->get_data(), col_res->get_chars(),
                                             col_res->get_offsets()));
                block.replace_by_position(result, std::move(col_res));
                return Status::OK();
            }
        } else if constexpr (is_complex_v<typename Impl::Type>) {
            if (const auto* col = check_and_get_column<ColumnComplexType<typename Impl::Type>>(
                        column.get())) {
                auto col_res = Impl::ReturnColumnType::create();
                RETURN_IF_ERROR(Impl::vector(col->get_data(), col_res->get_chars(),
                                             col_res->get_offsets()));
                block.replace_by_position(result, std::move(col_res));
                return Status::OK();
            }
        }

        return Status::RuntimeError("Illegal column {} of argument of function {}",
                                    block.get_by_position(arguments[0]).column->get_name(),
                                    get_name());
    }
    template <typename T>
        requires(!std::is_same_v<T, DataTypeString>)
    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t input_rows_count) const {
        const ColumnPtr column = block.get_by_position(arguments[0]).column;
        if constexpr (Impl::TYPE_INDEX == TypeIndex::String) {
            if (const ColumnString* col = check_and_get_column<ColumnString>(column.get())) {
                auto col_res = Impl::ReturnColumnType::create();
                RETURN_IF_ERROR(
                        Impl::vector(col->get_chars(), col->get_offsets(), col_res->get_data()));
                block.replace_by_position(result, std::move(col_res));
                return Status::OK();
            }
        } else if constexpr (typeindex_is_int(Impl::TYPE_INDEX)) {
            if (const auto* col =
                        check_and_get_column<ColumnVector<typename Impl::Type>>(column.get())) {
                auto col_res = Impl::ReturnColumnType::create();
                RETURN_IF_ERROR(Impl::vector(col->get_data(), col_res->get_data()));
                block.replace_by_position(result, std::move(col_res));
                return Status::OK();
            }
        } else if constexpr (is_complex_v<typename Impl::Type>) {
            if (const auto* col = check_and_get_column<ColumnComplexType<typename Impl::Type>>(
                        column.get())) {
                auto col_res = Impl::ReturnColumnType::create();
                RETURN_IF_ERROR(Impl::vector(col->get_data(), col_res->get_data()));
                block.replace_by_position(result, std::move(col_res));
                return Status::OK();
            }
        }
        return Status::RuntimeError("Illegal column {} of argument of function {}",
                                    block.get_by_position(arguments[0]).column->get_name(),
                                    get_name());
    }
};

template <typename LeftDataType, typename RightDataType,
          template <typename, typename> typename Impl, typename Name>
class FunctionBinaryToType : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionBinaryToType>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        using ResultDataType = typename Impl<LeftDataType, RightDataType>::ResultDataType;
        return std::make_shared<ResultDataType>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t /*input_rows_count*/) const override {
        DCHECK_EQ(arguments.size(), 2);
        const auto& [lcol, left_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& [rcol, right_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);

        using ResultDataType = typename Impl<LeftDataType, RightDataType>::ResultDataType;

        using T0 = typename LeftDataType::FieldType;
        using T1 = typename RightDataType::FieldType;
        using ResultType = typename ResultDataType::FieldType;

        using ColVecLeft =
                std::conditional_t<is_complex_v<T0>, ColumnComplexType<T0>, ColumnVector<T0>>;
        using ColVecRight =
                std::conditional_t<is_complex_v<T1>, ColumnComplexType<T1>, ColumnVector<T1>>;

        using ColVecResult =
                std::conditional_t<is_complex_v<ResultType>, ColumnComplexType<ResultType>,
                                   ColumnVector<ResultType>>;

        typename ColVecResult::MutablePtr col_res = nullptr;

        col_res = ColVecResult::create();
        auto& vec_res = col_res->get_data();
        vec_res.resize(block.rows());

        if (auto col_left = check_and_get_column<ColVecLeft>(lcol.get())) {
            if (auto col_right = check_and_get_column<ColVecRight>(rcol.get())) {
                if (left_const) {
                    Impl<LeftDataType, RightDataType>::scalar_vector(
                            col_left->get_data()[0], col_right->get_data(), vec_res);
                } else if (right_const) {
                    Impl<LeftDataType, RightDataType>::vector_scalar(
                            col_left->get_data(), col_right->get_data()[0], vec_res);
                } else {
                    Impl<LeftDataType, RightDataType>::vector_vector(
                            col_left->get_data(), col_right->get_data(), vec_res);
                }

                block.replace_by_position(result, std::move(col_res));
                return Status::OK();
            }
        }
        return Status::RuntimeError("unimplements function {}", get_name());
    }
};

template <template <typename, typename> typename Impl, typename Name>
class FunctionBinaryToType<DataTypeString, DataTypeString, Impl, Name> : public IFunction {
public:
    using LeftDataType = DataTypeString;
    using RightDataType = DataTypeString;
    using ResultDataType = typename Impl<LeftDataType, RightDataType>::ResultDataType;

    using ColVecLeft = ColumnString;
    using ColVecRight = ColumnString;

    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionBinaryToType>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<ResultDataType>();
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()};
    }

    bool is_variadic() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t /*input_rows_count*/) const override {
        const auto& left = block.get_by_position(arguments[0]);
        const auto& right = block.get_by_position(arguments[1]);
        return execute_inner_impl<ResultDataType>(left, right, block, arguments, result);
    }

private:
    template <typename ReturnDataType>
        requires(!std::is_same_v<ResultDataType, DataTypeString>)
    Status execute_inner_impl(const ColumnWithTypeAndName& left, const ColumnWithTypeAndName& right,
                              Block& block, const ColumnNumbers& arguments, size_t result) const {
        const auto& [lcol, left_const] = unpack_if_const(left.column);
        const auto& [rcol, right_const] = unpack_if_const(right.column);

        using ResultType = typename ResultDataType::FieldType;
        using ColVecResult = ColumnVector<ResultType>;
        typename ColVecResult::MutablePtr col_res = ColVecResult::create();

        auto& vec_res = col_res->get_data();
        vec_res.resize(block.rows());

        if (auto col_left = check_and_get_column<ColVecLeft>(lcol.get())) {
            if (auto col_right = check_and_get_column<ColVecRight>(rcol.get())) {
                if (left_const) {
                    static_cast<void>(Impl<LeftDataType, RightDataType>::scalar_vector(
                            col_left->get_data_at(0), col_right->get_chars(),
                            col_right->get_offsets(), vec_res));
                } else if (right_const) {
                    static_cast<void>(Impl<LeftDataType, RightDataType>::vector_scalar(
                            col_left->get_chars(), col_left->get_offsets(),
                            col_right->get_data_at(0), vec_res));
                } else {
                    static_cast<void>(Impl<LeftDataType, RightDataType>::vector_vector(
                            col_left->get_chars(), col_left->get_offsets(), col_right->get_chars(),
                            col_right->get_offsets(), vec_res));
                }

                block.replace_by_position(result, std::move(col_res));
                return Status::OK();
            }
        }
        return Status::RuntimeError("unimplements function {}", get_name());
    }

    template <typename ReturnDataType>
        requires std::is_same_v<ResultDataType, DataTypeString>
    Status execute_inner_impl(const ColumnWithTypeAndName& left, const ColumnWithTypeAndName& right,
                              Block& block, const ColumnNumbers& arguments, size_t result) {
        const auto& [lcol, left_const] = unpack_if_const(left.column);
        const auto& [rcol, right_const] = unpack_if_const(right.column);

        using ColVecResult = ColumnString;
        typename ColVecResult::MutablePtr col_res = ColVecResult::create();
        if (auto col_left = check_and_get_column<ColVecLeft>(lcol.get())) {
            if (auto col_right = check_and_get_column<ColVecRight>(rcol.get())) {
                if (left_const) {
                    Impl<LeftDataType, RightDataType>::scalar_vector(
                            col_left->get_data_at(0), col_right->get_chars(),
                            col_right->get_offsets(), col_res->get_chars(), col_res->get_offsets());
                } else if (right_const) {
                    Impl<LeftDataType, RightDataType>::vector_scalar(
                            col_left->get_chars(), col_left->get_offsets(),
                            col_right->get_data_at(0), col_res->get_chars(),
                            col_res->get_offsets());
                } else {
                    Impl<LeftDataType, RightDataType>::vector_vector(
                            col_left->get_chars(), col_left->get_offsets(), col_right->get_chars(),
                            col_right->get_offsets(), col_res->get_chars(), col_res->get_offsets());
                }

                block.replace_by_position(result, std::move(col_res));
                return Status::OK();
            }
        }
        return Status::RuntimeError("unimplements function {}", get_name());
    }
};

// func(type,type) -> nullable(type)
template <typename LeftDataType, typename RightDataType, typename ResultDateType,
          typename ReturnType, template <typename, typename, typename, typename> typename Impl,
          typename Name>
class FunctionBinaryToNullType : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionBinaryToNullType>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        using ResultDataType = typename Impl<LeftDataType, RightDataType, ResultDateType,
                                             ReturnType>::ResultDataType;
        return make_nullable(std::make_shared<ResultDataType>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        DCHECK_EQ(arguments.size(), 2);

        ColumnPtr argument_columns[2];
        bool col_const[2];
        for (int i = 0; i < 2; ++i) {
            std::tie(argument_columns[i], col_const[i]) =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
            check_set_nullable(argument_columns[i], null_map, col_const[i]);
        }

        using ResultDataType = typename Impl<LeftDataType, RightDataType, ResultDateType,
                                             ReturnType>::ResultDataType;

        using T0 = typename LeftDataType::FieldType;
        using T1 = typename RightDataType::FieldType;
        using ResultType = typename ResultDataType::FieldType;

        using ColVecLeft =
                std::conditional_t<is_complex_v<T0>, ColumnComplexType<T0>, ColumnVector<T0>>;
        using ColVecRight =
                std::conditional_t<is_complex_v<T1>, ColumnComplexType<T1>, ColumnVector<T1>>;

        using ColVecResult =
                std::conditional_t<is_complex_v<ResultType>, ColumnComplexType<ResultType>,
                                   ColumnVector<ResultType>>;

        typename ColVecResult::MutablePtr col_res = nullptr;

        col_res = ColVecResult::create();
        auto& vec_res = col_res->get_data();
        vec_res.resize(block.rows());

        if (auto col_left = check_and_get_column<ColVecLeft>(argument_columns[0].get())) {
            if (auto col_right = check_and_get_column<ColVecRight>(argument_columns[1].get())) {
                if (col_const[0]) {
                    Impl<LeftDataType, RightDataType, ResultDateType, ReturnType>::scalar_vector(
                            col_left->get_data()[0], col_right->get_data(), vec_res,
                            null_map->get_data());
                } else if (col_const[1]) {
                    Impl<LeftDataType, RightDataType, ResultDateType, ReturnType>::vector_scalar(
                            col_left->get_data(), col_right->get_data()[0], vec_res,
                            null_map->get_data());
                } else {
                    Impl<LeftDataType, RightDataType, ResultDateType, ReturnType>::vector_vector(
                            col_left->get_data(), col_right->get_data(), vec_res,
                            null_map->get_data());
                }

                block.get_by_position(result).column =
                        ColumnNullable::create(std::move(col_res), std::move(null_map));
                return Status::OK();
            }
        }
        return Status::RuntimeError("unimplements function {}", get_name());
    }
};

// func(string,string) -> nullable(type)
template <typename Impl>
class FunctionBinaryStringOperateToNullType : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() {
        return std::make_shared<FunctionBinaryStringOperateToNullType>();
    }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<typename Impl::ReturnType>());
    }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (vectorized::HasGetVariadicArgumentTypesImpl<Impl>) {
            return Impl::get_variadic_argument_types_impl();
        } else {
            return {};
        }
    }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        ColumnPtr argument_columns[2];
        bool col_const[2];
        for (int i = 0; i < 2; ++i) {
            std::tie(argument_columns[i], col_const[i]) =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
            check_set_nullable(argument_columns[i], null_map, col_const[i]);
        }

        auto res = Impl::ColumnType::create();

        auto specific_str_column = assert_cast<const ColumnString*>(argument_columns[0].get());
        auto specific_char_column = assert_cast<const ColumnString*>(argument_columns[1].get());

        auto& ldata = specific_str_column->get_chars();
        auto& loffsets = specific_str_column->get_offsets();

        auto& rdata = specific_char_column->get_chars();
        auto& roffsets = specific_char_column->get_offsets();

        // execute Impl
        if constexpr (std::is_same_v<typename Impl::ReturnType, DataTypeString>) {
            auto& res_data = res->get_chars();
            auto& res_offsets = res->get_offsets();
            if (col_const[0]) {
                Impl::scalar_vector(context, specific_str_column->get_data_at(0), rdata, roffsets,
                                    res_data, res_offsets, null_map->get_data());
            } else if (col_const[1]) {
                Impl::vector_scalar(context, ldata, loffsets, specific_char_column->get_data_at(0),
                                    res_data, res_offsets, null_map->get_data());
            } else {
                Impl::vector_vector(context, ldata, loffsets, rdata, roffsets, res_data,
                                    res_offsets, null_map->get_data());
            }
        } else {
            if (col_const[0]) {
                Impl::scalar_vector(context, specific_str_column->get_data_at(0), rdata, roffsets,
                                    res->get_data(), null_map->get_data());
            } else if (col_const[1]) {
                Impl::vector_scalar(context, ldata, loffsets, specific_char_column->get_data_at(0),
                                    res->get_data(), null_map->get_data());
            } else {
                Impl::vector_vector(context, ldata, loffsets, rdata, roffsets, res->get_data(),
                                    null_map->get_data());
            }
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res), std::move(null_map));
        return Status::OK();
    }
};

// func(string) -> nullable(type)
template <typename Impl>
class FunctionStringOperateToNullType : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionStringOperateToNullType>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<typename Impl::ReturnType>());
    }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto null_map = ColumnUInt8::create(input_rows_count, 0);

        auto& col_ptr = block.get_by_position(arguments[0]).column;

        auto res = Impl::ColumnType::create();
        if (const auto* col = check_and_get_column<ColumnString>(col_ptr.get())) {
            auto col_res = Impl::ColumnType::create();
            static_cast<void>(Impl::vector(col->get_chars(), col->get_offsets(),
                                           col_res->get_chars(), col_res->get_offsets(),
                                           null_map->get_data()));
            block.replace_by_position(
                    result, ColumnNullable::create(std::move(col_res), std::move(null_map)));
        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        get_name());
        }
        return Status::OK();
    }
};

template <typename Impl>
class FunctionStringEncode : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionStringEncode>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<typename Impl::ReturnType>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto& col_ptr = block.get_by_position(arguments[0]).column;

        auto res = Impl::ColumnType::create();
        if (const auto* col = check_and_get_column<ColumnString>(col_ptr.get())) {
            auto col_res = Impl::ColumnType::create();
            static_cast<void>(Impl::vector(col->get_chars(), col->get_offsets(),
                                           col_res->get_chars(), col_res->get_offsets()));
            block.replace_by_position(result, std::move(col_res));
        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        get_name());
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized
