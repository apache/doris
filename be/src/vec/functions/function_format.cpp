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

#include <glog/logging.h>

#include <cstdio>
#include <regex>
#include <vector>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/cast_type_to_either.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

class FunctionFormatNumber : public IFunction {
public:
    static constexpr auto name = "format_number";

    static constexpr const char* UNITS[6] = {"", "K", "M", "B", "T", "Q"};

    static FunctionPtr create() { return std::make_shared<FunctionFormatNumber>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    bool is_variadic() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto column = block.get_by_position(arguments[0]).column;
        const auto& column_data = assert_cast<const ColumnFloat64*>(column.get())->get_data();
        auto col_res = ColumnString::create();
        fmt::memory_buffer buffer;

        for (auto i = 0; i < input_rows_count; ++i) {
            auto res_data = format_number(buffer, column_data[i]);
            col_res->insert_data(res_data.data(), res_data.length());
        }
        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }

    std::string format_number(fmt::memory_buffer& buffer, double number) const {
        buffer.clear();
        double abs_number = std::abs(number);
        int unit_index = 0;
        while (abs_number >= 1000 && unit_index < 5) {
            abs_number /= 1000;
            ++unit_index;
        }
        if (number < 0) {
            fmt::format_to(buffer, "-");
        }
        if (abs_number == 1) {
            //eg: 1000 ---> 1K
            fmt::format_to(buffer, "{}", abs_number);
        } else if (abs_number < 10) {
            //eg: 1239 ---> 1.24K only want to show 2 decimal
            fmt::format_to(buffer, "{:.2f}", abs_number);
        } else if (abs_number < 100) {
            //eg: 12399999 ---> 12.4M only want to show 1 decimal
            fmt::format_to(buffer, "{:.1f}", abs_number);
        } else {
            // eg: 999999999999999 ---> 1000T only want to show 0 decimal
            fmt::format_to(buffer, "{:.0f}", abs_number);
        }
        fmt::format_to(buffer, UNITS[unit_index]);
        return fmt::to_string(buffer);
    }
};

class FunctionFormat : public IFunction {
public:
    static constexpr auto name = "format";

    static FunctionPtr create() { return std::make_shared<FunctionFormat>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_GE(arguments.size(), 2);
        bool valid =
                cast_type(block.get_by_position(arguments[1]).type.get(), [&](const auto& type) {
                    using DataType = std::decay_t<decltype(type)>;
                    using ColVecData =
                            std::conditional_t<is_number(DataType::PType),
                                               ColumnVector<DataType::PType>, ColumnString>;
                    if (auto col = check_and_get_column<ColVecData>(
                                           block.get_by_position(arguments[1]).column.get()) ||
                                   is_column_const(*block.get_by_position(arguments[1]).column)) {
                        execute_inner<ColVecData, DataType::PType>(block, arguments, result,
                                                                   input_rows_count);
                        return true;
                    }
                    return false;
                });
        if (!valid) {
            return Status::RuntimeError(
                    "{}'s argument does not match the expected data type, type: {}, column: {}",
                    get_name(), block.get_by_position(arguments[1]).type->get_name(),
                    block.get_by_position(arguments[1]).column->dump_structure());
        }
        return Status::OK();
    }

    template <typename F>
    static bool cast_type(const IDataType* type, F&& f) {
        return cast_type_to_either<DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
                                   DataTypeInt128, DataTypeFloat32, DataTypeFloat64,
                                   DataTypeString>(type, std::forward<F>(f));
    }

    template <typename ColVecData, PrimitiveType T>
    void execute_inner(Block& block, const ColumnNumbers& arguments, uint32_t result,
                       size_t input_rows_count) const {
        size_t argument_size = arguments.size();
        std::vector<ColumnPtr> argument_columns(argument_size);
        auto result_column = ColumnString::create();

        // maybe most user is format(const, column), so only handle this case const column
        if (argument_size == 2) {
            std::vector<uint8_t> is_consts(argument_size);
            std::tie(argument_columns[0], is_consts[0]) =
                    unpack_if_const(block.get_by_position(arguments[0]).column);
            std::tie(argument_columns[1], is_consts[1]) =
                    unpack_if_const(block.get_by_position(arguments[1]).column);
            execute_for_two_argument<ColVecData, T>(argument_columns, is_consts,
                                                    assert_cast<ColumnString*>(result_column.get()),
                                                    input_rows_count);
        } else {
            for (size_t i = 0; i < argument_size; ++i) {
                argument_columns[i] = block.get_by_position(arguments[i])
                                              .column->convert_to_full_column_if_const();
            }
            execute_for_others_arg<ColVecData, T>(argument_columns,
                                                  assert_cast<ColumnString*>(result_column.get()),
                                                  argument_size, input_rows_count);
        }

        block.replace_by_position(result, std::move(result_column));
    }

    template <typename ColVecData, PrimitiveType T>
    void execute_for_two_argument(std::vector<ColumnPtr>& argument_columns,
                                  std::vector<uint8_t>& is_consts, ColumnString* result_data_column,
                                  size_t input_rows_count) const {
        const auto& format_column = assert_cast<const ColumnString&>(*argument_columns[0].get());
        const auto& value_column = assert_cast<const ColVecData&>(*argument_columns[1].get());
        for (int i = 0; i < input_rows_count; ++i) {
            auto format =
                    format_column.get_data_at(index_check_const(i, is_consts[0])).to_string_view();
            std::string res;
            try {
                if constexpr (is_string_type(T)) {
                    auto value = value_column.get_data_at(index_check_const(i, is_consts[1]));
                    res = fmt::format(format, value);
                } else {
                    auto value = value_column.get_data()[index_check_const(i, is_consts[1])];
                    res = fmt::format(format, value);
                }
            } catch (const std::exception& e) {
                throw doris::Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "Invalid Input argument \"{}\" of function format, error: {}", format,
                        e.what());
            }
            result_data_column->insert_data(res.data(), res.length());
        }
    }

    template <typename ColVecData, PrimitiveType T>
    void execute_for_others_arg(std::vector<ColumnPtr>& argument_columns,
                                ColumnString* result_data_column, size_t argument_size,
                                size_t input_rows_count) const {
        const auto& format_column = assert_cast<const ColumnString&>(*argument_columns[0].get());
        for (int i = 0; i < input_rows_count; ++i) {
            auto format = format_column.get_data_at(i).to_string_view();
            std::string res;
            fmt::dynamic_format_arg_store<fmt::format_context> args;
            if constexpr (is_string_type(T)) {
                for (int col = 1; col < argument_size; ++col) {
                    const auto& arg_column_data =
                            assert_cast<const ColVecData&>(*argument_columns[col].get());
                    args.push_back(arg_column_data.get_data_at(i).to_string());
                }
            } else {
                for (int col = 1; col < argument_size; ++col) {
                    const auto& arg_column_data =
                            assert_cast<const ColVecData&>(*argument_columns[col].get()).get_data();
                    args.push_back(arg_column_data[i]);
                }
            }
            try {
                res = fmt::vformat(format, args);
            } catch (const std::exception& e) {
                throw doris::Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "Invalid Input argument \"{}\" of function format, error: {}", format,
                        e.what());
            }
            result_data_column->insert_data(res.data(), res.length());
        }
    }
};

void register_function_format(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionFormatNumber>();
    factory.register_function<FunctionFormat>();
}

} // namespace doris::vectorized
