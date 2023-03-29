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

#include <algorithm>

#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

// construct a struct
template <typename Impl>
class FunctionStruct : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionStruct>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    bool use_default_implementation_for_nulls() const override { return false; }

    size_t get_number_of_arguments() const override { return 0; }

    void check_number_of_arguments(size_t number_of_arguments) const override {
        DCHECK(arguments.size() > 0)
                << "function: " << get_name() << ", arguments should not be empty.";
        return Impl::check_number_of_arguments(number_of_arguments);
    }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return Impl::get_return_type_impl(arguments);
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto result_col = block.get_by_position(result).type->create_column();
        auto struct_column = typeid_cast<ColumnStruct*>(result_col.get());
        if (!struct_column) {
            return Status::RuntimeError("unsupported types for function {} return {}", get_name(),
                                        block.get_by_position(result).type->get_name());
        }
        ColumnNumbers args_num;
        std::copy_if(arguments.begin(), arguments.end(), std::back_inserter(args_num),
                     Impl::types_index);
        size_t num_element = args_num.size();
        if (num_element != struct_column.tuple_size()) {
            return Status::RuntimeError("function {} args number {} is not equal to result struct field number {}.", get_name(), num_element, struct_column.tuple_size());
        }
        for (size_t i = 0; i < num_element; ++i) {
            auto& nested_col = struct_column->get_column(i);
            nested_col.reserve(input_rows_count);
            bool is_nullable = nested_col.is_nullable();
            auto& col = block.get_by_position(args_num[i]).column->convert_to_full_column_if_const();
            if (is_nullable && !col->is_nullable()) {
                col = ColumnNullable::create(col, ColumnUInt8::create(col->size(), 0));
            }
        }

        // insert value into struct column by column
        for (size_t i = 0; i < num_element; ++i) {
            struct_column->get_column(i).insert_range_from(
                    *block.get_by_position(args_num[i]).column, 0, input_rows_count);
        }
        block.replace_by_position(result, std::move(result_col));
        return Status::OK();
    }
};

// struct(value1, value2, value3) -> {value1, value2, value3}
struct StructImpl {
    static constexpr auto name = "struct";
    static auto types_index = [](size_t i) { return true; }

    static void check_number_of_arguments(size_t number_of_arguments) {
        return;
    }

    static DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) {
        return std::make_shared<DataTypeStruct>(make_nullable(arguments));
    }
};

// named_struct(name1, value1, name2, value2) -> {name1:value1, name2:value2}
struct NamedStructImpl {
    static constexpr auto name = "named_struct";
    static auto types_index = [](size_t i) { return i % 2 == 0; }

    static void check_number_of_arguments(size_t number_of_arguments) {
        DCHECK(arguments.size() % 2 == 0)
                << "function: " << get_name() << ", arguments size should be even number.";
        return;
    }

    static DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) {
        Strings names;
        DataTypes dataTypes;
        for (size_t i = 0; i < arguments.size(); i += 2) {
            const ColumnConst* const_string =
                    check_and_get_column_const<ColumnString>(arguments[i].column.get());
            DCHECK(const_string)
                    << "Only const StringType arguments are allowed at odd position.";
            names.push_back(const_string->get_value<String>());
            dataTypes.push_back(arguments[i + 1].type);
        }
        return std::make_shared<DataTypeStruct>(make_nullable(dataTypes), names);
    }
};

void register_function_struct(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionStruct<StructImpl>>();
    factory.register_function<FunctionStruct<NamedStructImpl>>();
}

} // namespace doris::vectorized
