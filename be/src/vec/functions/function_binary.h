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

#include "common/status.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_varbinary.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_varbinary.h"
#include "vec/functions/function.h"

namespace doris::vectorized {
#include "common/compile_check_avoid_begin.h"

template <typename Impl>
class FunctionBinaryUnary : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionBinaryUnary<Impl>>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        auto res = std::make_shared<typename Impl::ReturnType>();
        return Impl::is_nullable ? make_nullable(res) : res;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return Impl::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

struct SubBinaryUtil {
    static void sub_binary_execute(Block& block, const ColumnNumbers& arguments, uint32_t result,
                                   size_t input_rows_count) {
        DCHECK_EQ(arguments.size(), 3);
        auto res = ColumnVarbinary::create();

        bool col_const[3];
        ColumnPtr argument_columns[3];
        for (int i = 0; i < 3; ++i) {
            std::tie(argument_columns[i], col_const[i]) =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
        }

        const auto* specific_binary_column =
                assert_cast<const ColumnVarbinary*>(argument_columns[0].get());
        const auto* specific_start_column =
                assert_cast<const ColumnInt32*>(argument_columns[1].get());
        const auto* specific_len_column =
                assert_cast<const ColumnInt32*>(argument_columns[2].get());

        std::visit(
                [&](auto binary_const, auto start_const, auto len_const) {
                    vectors<binary_const, start_const, len_const>(
                            specific_binary_column, specific_start_column, specific_len_column,
                            res.get(), input_rows_count);
                },
                vectorized::make_bool_variant(col_const[0]),
                vectorized::make_bool_variant(col_const[1]),
                vectorized::make_bool_variant(col_const[2]));
        block.get_by_position(result).column = std::move(res);
    }

private:
    template <bool binary_const, bool start_const, bool len_const>
    static void vectors(const ColumnVarbinary* binarys, const ColumnInt32* start,
                        const ColumnInt32* len, ColumnVarbinary* res, size_t size) {
        if constexpr (start_const && len_const) {
            if (start->get_data()[0] == 0 || len->get_data()[0] <= 0) {
                for (size_t i = 0; i < size; ++i) {
                    res->insert_default();
                }
                return;
            }
        }

        for (size_t i = 0; i < size; ++i) {
            StringRef binary = binarys->get_data_at(index_check_const<binary_const>(i));
            int binary_size = binary.size;

            int start_value = start->get_data()[index_check_const<start_const>(i)];
            int len_value = len->get_data()[index_check_const<len_const>(i)];

            if (start_value > binary_size || start_value < -binary_size || binary_size == 0 ||
                len_value <= 0) {
                res->insert_default();
                continue;
            }
            int fixed_pos = start_value - 1;
            if (fixed_pos < 0) {
                fixed_pos = binary_size + fixed_pos + 1;
            }
            int fixed_len = std::min(binary_size - fixed_pos, len_value);

            res->insert_data(binary.data + fixed_pos, fixed_len);
        }
    }
};

template <typename Impl>
class FunctionSubBinary : public IFunction {
public:
    static constexpr auto name = "sub_binary";
    static FunctionPtr create() { return std::make_shared<FunctionSubBinary<Impl>>(); }
    String get_name() const override { return name; }
    DataTypes get_variadic_argument_types_impl() const override {
        return Impl::get_variadic_argument_types();
    }
    size_t get_number_of_arguments() const override {
        return get_variadic_argument_types_impl().size();
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeVarbinary>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return Impl::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

struct SubBinary3Impl {
    static DataTypes get_variadic_argument_types() {
        return DataTypes {std::make_shared<DataTypeVarbinary>(), std::make_shared<DataTypeInt32>(),
                          std::make_shared<DataTypeInt32>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        SubBinaryUtil::sub_binary_execute(block, arguments, result, input_rows_count);
        return Status::OK();
    }
};

struct SubBinary2Impl {
    static DataTypes get_variadic_argument_types() {
        return DataTypes {std::make_shared<DataTypeVarbinary>(), std::make_shared<DataTypeInt32>()};
    }

    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        auto col_len = ColumnInt32::create(input_rows_count);
        auto& strlen_data = col_len->get_data();

        ColumnPtr binary_col;
        bool binary_const;
        std::tie(binary_col, binary_const) =
                unpack_if_const(block.get_by_position(arguments[0]).column);

        const auto& binarys = assert_cast<const ColumnVarbinary*>(binary_col.get());

        if (binary_const) {
            std::fill(strlen_data.begin(), strlen_data.end(), binarys->get_data()[0].size());
        } else {
            for (int i = 0; i < input_rows_count; ++i) {
                strlen_data[i] = binarys->get_data()[i].size();
            }
        }

        // we complete the column2(strlen) with the default value - each row's strlen.
        block.insert({std::move(col_len), std::make_shared<DataTypeInt32>(), "strlen"});
        ColumnNumbers temp_arguments = {arguments[0], arguments[1], block.columns() - 1};

        SubBinaryUtil::sub_binary_execute(block, temp_arguments, result, input_rows_count);
        return Status::OK();
    }
};

#include "common/compile_check_avoid_end.h"
} // namespace doris::vectorized
