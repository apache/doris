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

#include <bit>
#include <bitset>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/cast_type_to_either.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

class FunctionBitTest : public IFunction {
public:
    static constexpr auto name = "bit_test";

    static FunctionPtr create() { return std::make_shared<FunctionBitTest>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt8>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        bool valid =
                cast_type(block.get_by_position(arguments[0]).type.get(), [&](const auto& type) {
                    using DataType = std::decay_t<decltype(type)>;
                    using T = typename DataType::FieldType;
                    if (auto col = check_and_get_column<ColumnVector<T>>(
                                           block.get_by_position(arguments[0]).column.get()) ||
                                   is_column_const(*block.get_by_position(arguments[0]).column)) {
                        execute_inner<T>(block, arguments, result, input_rows_count);
                        return true;
                    }
                    return false;
                });
        if (!valid) {
            return Status::RuntimeError(
                    "{}'s argument does not match the expected data type, type: {}, column: {}",
                    get_name(), block.get_by_position(arguments[0]).type->get_name(),
                    block.get_by_position(arguments[0]).column->dump_structure());
        }
        return Status::OK();
    }

    template <typename F>
    static bool cast_type(const IDataType* type, F&& f) {
        return cast_type_to_either<DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
                                   DataTypeInt128>(type, std::forward<F>(f));
    }

    template <typename T>
    void execute_inner(Block& block, const ColumnNumbers& arguments, uint32_t result,
                       size_t input_rows_count) const {
        size_t argument_size = arguments.size();
        std::vector<ColumnPtr> argument_columns(argument_size);
        auto result_data_column = ColumnInt8::create(input_rows_count, 1);
        auto& res_data = result_data_column->get_data();

        // maybe most user is bit_test(column, const), so only handle this case
        if (argument_size == 2) {
            std::vector<uint8_t> is_consts(argument_size);
            std::tie(argument_columns[0], is_consts[0]) =
                    unpack_if_const(block.get_by_position(arguments[0]).column);
            std::tie(argument_columns[1], is_consts[1]) =
                    unpack_if_const(block.get_by_position(arguments[1]).column);
            execute_for_two_argument<T>(argument_columns, is_consts, res_data, input_rows_count);
        } else {
            for (size_t i = 0; i < argument_size; ++i) {
                argument_columns[i] = block.get_by_position(arguments[i])
                                              .column->convert_to_full_column_if_const();
            }
            execute_for_others_arg<T>(argument_columns, res_data, argument_size, input_rows_count);
        }

        block.replace_by_position(result, std::move(result_data_column));
    }

    template <typename T>
    void execute_for_two_argument(std::vector<ColumnPtr>& argument_columns,
                                  std::vector<uint8_t>& is_consts, ColumnInt8::Container& res_data,
                                  size_t input_rows_count) const {
        const auto& first_column_data =
                assert_cast<const ColumnVector<T>&>(*argument_columns[0].get()).get_data();
        const auto& second_column_data =
                assert_cast<const ColumnVector<T>&>(*argument_columns[1].get()).get_data();
        for (int i = 0; i < input_rows_count; ++i) {
            auto first_value = first_column_data[index_check_const(i, is_consts[0])];
            auto second_value = second_column_data[index_check_const(i, is_consts[1])];
            // the pos is invalid, set result = 0
            if (second_value < 0 || second_value >= sizeof(T) * 8) {
                res_data[i] = 0;
                continue;
            }
            res_data[i] = ((first_value >> second_value) & 1);
        }
    }

    template <typename T>
    void execute_for_others_arg(std::vector<ColumnPtr>& argument_columns,
                                ColumnInt8::Container& res_data, size_t argument_size,
                                size_t input_rows_count) const {
        const auto& first_column_data =
                assert_cast<const ColumnVector<T>&>(*argument_columns[0].get()).get_data();
        for (int i = 0; i < input_rows_count; ++i) {
            auto first_value = first_column_data[i];
            for (int col = 1; col < argument_size; ++col) {
                const auto& arg_column_data =
                        assert_cast<const ColumnVector<T>&>(*argument_columns[col].get())
                                .get_data();
                // the pos is invalid, set result = 0
                if (arg_column_data[i] < 0 || arg_column_data[i] >= sizeof(T) * 8) {
                    res_data[i] = 0;
                    break;
                }
                // if one of pos & result is 0, could set res = 0, and return directly.
                if (!((first_value >> arg_column_data[i]) & 1)) {
                    res_data[i] = 0;
                    break;
                }
            }
        }
    }
};

void register_function_bit_test(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionBitTest>();
    factory.register_alias("bit_test", "bit_test_all");
}

} // namespace doris::vectorized
