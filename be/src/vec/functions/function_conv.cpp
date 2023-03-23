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

#include "exprs/math_functions.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

template <typename Impl>
class FunctionConv : public IFunction {
public:
    static constexpr auto name = "conv";
    String get_name() const override { return name; }
    static FunctionPtr create() { return std::make_shared<FunctionConv<Impl>>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }
    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<typename Impl::DataType>(), std::make_shared<DataTypeInt8>(),
                std::make_shared<DataTypeInt8>()};
    }
    size_t get_number_of_arguments() const override {
        return get_variadic_argument_types_impl().size();
    }

    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto result_column = ColumnString::create();
        auto result_null_map_column = ColumnUInt8::create(input_rows_count, 0);

        const auto& col0 = block.get_by_position(arguments[0]).column;
        bool col_const[3] = {is_column_const(*col0)};
        ColumnPtr argument_columns[3] = {
                col_const[0] ? static_cast<const ColumnConst&>(*col0).convert_to_full_column()
                             : col0};
        check_set_nullable(argument_columns[0], result_null_map_column);

        for (int i = 1; i < 3; ++i) {
            std::tie(argument_columns[i], col_const[i]) =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
            check_set_nullable(argument_columns[i], result_null_map_column);
        }

        if (col_const[1] && col_const[2]) {
            execute_const_args(
                    context,
                    assert_cast<const typename Impl::DataType::ColumnType*>(
                            argument_columns[0].get()),
                    assert_cast<const ColumnInt8*>(argument_columns[1].get())->get_element(0),
                    assert_cast<const ColumnInt8*>(argument_columns[2].get())->get_element(0),
                    assert_cast<ColumnString*>(result_column.get()),
                    assert_cast<ColumnUInt8*>(result_null_map_column.get())->get_data(),
                    input_rows_count);
        } else {
            execute_straight(context,
                             assert_cast<const typename Impl::DataType::ColumnType*>(
                                     argument_columns[0].get()),
                             assert_cast<const ColumnInt8*>(argument_columns[1].get()),
                             assert_cast<const ColumnInt8*>(argument_columns[2].get()),
                             assert_cast<ColumnString*>(result_column.get()),
                             assert_cast<ColumnUInt8*>(result_null_map_column.get())->get_data(),
                             input_rows_count);
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(result_column), std::move(result_null_map_column));
        return Status::OK();
    }

private:
    static ALWAYS_INLINE void execute_straight(
            FunctionContext* context, const typename Impl::DataType::ColumnType* data_column,
            const ColumnInt8* src_base_column, const ColumnInt8* dst_base_column,
            ColumnString* result_column, NullMap& result_null_map, size_t input_rows_count) {
        for (size_t i = 0; i < input_rows_count; i++) {
            if (result_null_map[i]) {
                result_column->insert_default();
                continue;
            }

            Int8 src_base = src_base_column->get_element(i);
            Int8 dst_base = dst_base_column->get_element(i);
            if (std::abs(src_base) < MathFunctions::MIN_BASE ||
                std::abs(src_base) > MathFunctions::MAX_BASE ||
                std::abs(dst_base) < MathFunctions::MIN_BASE ||
                std::abs(dst_base) > MathFunctions::MAX_BASE) {
                result_null_map[i] = true;
                result_column->insert_default();
                continue;
            }

            Impl::calculate_cell(context, data_column, src_base, dst_base, result_column,
                                 result_null_map, i);
        }
    }
    static ALWAYS_INLINE void execute_const_args(
            FunctionContext* context, const typename Impl::DataType::ColumnType* data_column,
            const Int8 src_base, const Int8 dst_base, ColumnString* result_column,
            NullMap& result_null_map, size_t input_rows_count) {
        for (size_t i = 0; i < input_rows_count; i++) {
            if (result_null_map[i]) {
                result_column->insert_default();
                continue;
            }

            if (std::abs(src_base) < MathFunctions::MIN_BASE ||
                std::abs(src_base) > MathFunctions::MAX_BASE ||
                std::abs(dst_base) < MathFunctions::MIN_BASE ||
                std::abs(dst_base) > MathFunctions::MAX_BASE) {
                result_null_map[i] = true;
                result_column->insert_default();
                continue;
            }

            Impl::calculate_cell(context, data_column, src_base, dst_base, result_column,
                                 result_null_map, i);
        }
    }
};

struct ConvInt64Impl {
    using DataType = DataTypeInt64;

    static void calculate_cell(FunctionContext* context, const DataType::ColumnType* data_column,
                               const Int8 src_base, const Int8 dst_base,
                               ColumnString* result_column, NullMap& result_null_map,
                               size_t index) {
        Int64 num = data_column->get_element(index);
        if (src_base < 0 && num >= 0) {
            result_null_map[index] = true;
            result_column->insert_default();
            return;
        }

        int64_t decimal_num = num;
        if (src_base != 10) {
            if (!MathFunctions::decimal_in_base_to_decimal(num, src_base, &decimal_num)) {
                MathFunctions::handle_parse_result(dst_base, &decimal_num,
                                                   StringParser::PARSE_OVERFLOW);
            }
        }
        StringRef str = MathFunctions::decimal_to_base(context, decimal_num, dst_base);
        result_column->insert_data(reinterpret_cast<const char*>(str.data), str.size);
    }
};

struct ConvStringImpl {
    using DataType = DataTypeString;

    static void calculate_cell(FunctionContext* context, const DataType::ColumnType* data_column,
                               const Int8 src_base, const Int8 dst_base,
                               ColumnString* result_column, NullMap& result_null_map,
                               size_t index) {
        StringRef str = data_column->get_data_at(index);
        StringParser::ParseResult parse_res;
        int64_t decimal_num =
                StringParser::string_to_int<int64_t>(str.data, str.size, src_base, &parse_res);
        if (src_base < 0 && decimal_num >= 0) {
            result_null_map[index] = true;
            result_column->insert_default();
            return;
        }

        if (!MathFunctions::handle_parse_result(dst_base, &decimal_num, parse_res)) {
            result_column->insert_data("0", 1);
        } else {
            StringRef str = MathFunctions::decimal_to_base(context, decimal_num, dst_base);
            result_column->insert_data(reinterpret_cast<const char*>(str.data), str.size);
        }
    }
};

void register_function_conv(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionConv<ConvInt64Impl>>();
    factory.register_function<FunctionConv<ConvStringImpl>>();
}

} // namespace doris::vectorized
