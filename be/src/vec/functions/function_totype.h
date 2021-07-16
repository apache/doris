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
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/cast_type_to_either.h"
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
    static FunctionPtr create() { return std::make_shared<FunctionUnaryToType>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<typename Impl::ReturnType>();
    }

    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t input_rows_count) override {
        return execute_impl<typename Impl::ReturnType>(block, arguments, result, input_rows_count);
    }

private:
    // handle result == DataTypeString
    template <typename T, std::enable_if_t<std::is_same_v<T, DataTypeString>, T>* = nullptr>
    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t /*input_rows_count*/) {
        const ColumnPtr column = block.get_by_position(arguments[0]).column;
        if constexpr (std::is_integer(Impl::TYPE_INDEX)) {
            if (auto* col = check_and_get_column<ColumnVector<typename Impl::Type>>(column.get())) {
                auto col_res = Impl::ReturnColumnType::create();
                RETURN_IF_ERROR(Impl::vector(col->get_data(), col_res->get_chars(),
                                             col_res->get_offsets()));
                block.replace_by_position(result, std::move(col_res));
                return Status::OK();
            }
        }

        return Status::RuntimeError(
                fmt::format("Illegal column {} of argument of function {}",
                            block.get_by_position(arguments[0]).column->get_name(), get_name()));
    }
    template <typename T, std::enable_if_t<!std::is_same_v<T, DataTypeString>, T>* = nullptr>
    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t /*input_rows_count*/) {
        const ColumnPtr column = block.get_by_position(arguments[0]).column;
        if constexpr (Impl::TYPE_INDEX == TypeIndex::String) {
            if (const ColumnString* col = check_and_get_column<ColumnString>(column.get())) {
                auto col_res = Impl::ReturnColumnType::create();
                RETURN_IF_ERROR(
                        Impl::vector(col->get_chars(), col->get_offsets(), col_res->get_data()));
                block.replace_by_position(result, std::move(col_res));
                return Status::OK();
            }
        } else if constexpr (std::is_integer(Impl::TYPE_INDEX)) {
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
        return Status::RuntimeError(
                fmt::format("Illegal column {} of argument of function {}",
                            block.get_by_position(arguments[0]).column->get_name(), get_name()));
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

    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t /*input_rows_count*/) override {
        DCHECK_EQ(arguments.size(), 2);
        const auto& left = block.get_by_position(arguments[0]);
        auto lcol = left.column->convert_to_full_column_if_const();
        const auto& right = block.get_by_position(arguments[1]);
        auto rcol = right.column->convert_to_full_column_if_const();

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
                Impl<LeftDataType, RightDataType>::vector_vector(col_left->get_data(),
                                                                 col_right->get_data(), vec_res);
                block.replace_by_position(result, std::move(col_res));
                return Status::OK();
            }
        }
        return Status::RuntimeError(fmt::format("unimplements function {}", get_name()));
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
    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t /*input_rows_count*/) override {
        const auto& left = block.get_by_position(arguments[0]);
        const auto& right = block.get_by_position(arguments[1]);
        return execute_inner_impl<ResultDataType>(left, right, block, arguments, result);
    }

private:
    template <typename ReturnDataType,
              std::enable_if_t<!std::is_same_v<ResultDataType, DataTypeString>, ReturnDataType>* =
                      nullptr>
    Status execute_inner_impl(const ColumnWithTypeAndName& left, const ColumnWithTypeAndName& right,
                              Block& block, const ColumnNumbers& arguments, size_t result) {
        auto lcol = left.column->convert_to_full_column_if_const();
        auto rcol = right.column->convert_to_full_column_if_const();

        using ResultType = typename ResultDataType::FieldType;
        using ColVecResult = ColumnVector<ResultType>;
        typename ColVecResult::MutablePtr col_res = ColVecResult::create();

        auto& vec_res = col_res->get_data();
        vec_res.resize(block.rows());

        if (auto col_left = check_and_get_column<ColVecLeft>(lcol.get())) {
            if (auto col_right = check_and_get_column<ColVecRight>(rcol.get())) {
                Impl<LeftDataType, RightDataType>::vector_vector(
                        col_left->get_chars(), col_left->get_offsets(), col_right->get_chars(),
                        col_right->get_offsets(), vec_res);
                block.replace_by_position(result, std::move(col_res));
                return Status::OK();
            }
        }
        return Status::RuntimeError(fmt::format("unimplements function {}", get_name()));
    }

    template <typename ReturnDataType,
              std::enable_if_t<std::is_same_v<ResultDataType, DataTypeString>, ReturnDataType>* =
                      nullptr>
    Status execute_inner_impl(const ColumnWithTypeAndName& left, const ColumnWithTypeAndName& right,
                              Block& block, const ColumnNumbers& arguments, size_t result) {
        auto lcol = left.column->convert_to_full_column_if_const();
        auto rcol = right.column->convert_to_full_column_if_const();

        using ColVecResult = ColumnString;
        typename ColVecResult::MutablePtr col_res = ColVecResult::create();
        if (auto col_left = check_and_get_column<ColVecLeft>(lcol.get())) {
            if (auto col_right = check_and_get_column<ColVecRight>(rcol.get())) {
                Impl<LeftDataType, RightDataType>::vector_vector(
                        col_left->get_chars(), col_left->get_offsets(), col_right->get_chars(),
                        col_right->get_offsets(), col_res->get_chars(), col_res->get_offsets());
                block.replace_by_position(result, std::move(col_res));
                return Status::OK();
            }
        }
        return Status::RuntimeError(fmt::format("unimplements function {}", get_name()));
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
    bool use_default_implementation_for_constants() const override { return true; }
    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t input_rows_count) override {
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        ColumnPtr argument_columns[2];

        // focus convert const to full column to simply execute logic
        // handle
        for (int i = 0; i < 2; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_columns[i])) {
                argument_columns[i] = nullable->get_nested_column_ptr();
                VectorizedUtils::update_null_map(null_map->get_data(),
                                                 nullable->get_null_map_data());
            }
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
            Impl::vector_vector(ldata, loffsets, rdata, roffsets, res_data, res_offsets,
                                null_map->get_data());
        } else {
            Impl::vector_vector(ldata, loffsets, rdata, roffsets, res->get_data(),
                                null_map->get_data());
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res), std::move(null_map));
        return Status::OK();
    }
};
} // namespace doris::vectorized
