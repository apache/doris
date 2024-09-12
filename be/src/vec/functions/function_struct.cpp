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
#include <stddef.h>

#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

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
        DCHECK(number_of_arguments > 0)
                << "function: " << get_name() << ", arguments should not be empty.";
        return Impl::check_number_of_arguments(number_of_arguments);
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return Impl::get_return_type_impl(arguments);
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto result_col = block.get_by_position(result).type->create_column();
        auto struct_column = assert_cast<ColumnStruct*>(result_col.get());
        ColumnNumbers args_num;
        for (size_t i = 0; i < arguments.size(); i++) {
            if (Impl::pred(i)) {
                args_num.push_back(arguments[i]);
            }
        }
        size_t num_element = args_num.size();
        if (num_element != struct_column->tuple_size()) {
            return Status::RuntimeError(
                    "function {} args number {} is not equal to result struct field number {}.",
                    get_name(), num_element, struct_column->tuple_size());
        }
        std::vector<ColumnPtr> arg(num_element);
        for (size_t i = 0; i < num_element; ++i) {
            auto& nested_col = struct_column->get_column(i);
            nested_col.reserve(input_rows_count);
            bool is_nullable = nested_col.is_nullable();
            auto& col = block.get_by_position(args_num[i]).column;
            col = col->convert_to_full_column_if_const();
            arg[i] = col;
            if (is_nullable && !col->is_nullable()) {
                arg[i] = ColumnNullable::create(col, ColumnUInt8::create(col->size(), 0));
            }
        }

        // insert value into struct column by column
        for (size_t i = 0; i < num_element; ++i) {
            struct_column->get_column(i).insert_range_from(*arg[i], 0, input_rows_count);
        }
        block.replace_by_position(result, std::move(result_col));
        return Status::OK();
    }
};

// struct(value1, value2, value3) -> {value1, value2, value3}
struct StructImpl {
    static constexpr auto name = "struct";
    static constexpr auto pred = [](size_t i) { return true; };

    static void check_number_of_arguments(size_t number_of_arguments) {}

    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        return std::make_shared<DataTypeStruct>(make_nullable(arguments));
    }
};

// named_struct(name1, value1, name2, value2) -> {name1:value1, name2:value2}
struct NamedStructImpl {
    static constexpr auto name = "named_struct";
    static constexpr auto pred = [](size_t i) { return (i & 1) == 1; };

    static void check_number_of_arguments(size_t number_of_arguments) {
        DCHECK(number_of_arguments % 2 == 0)
                << "function: " << name << ", arguments size should be even number.";
    }

    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        DataTypes data_types(arguments.size() / 2);
        size_t even_idx = 1;
        for (size_t i = 0; i < data_types.size(); i++) {
            data_types[i] = arguments[even_idx];
            even_idx += 2;
        }
        return std::make_shared<DataTypeStruct>(make_nullable(data_types));
    }
};

void register_function_struct(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionStruct<StructImpl>>();
    factory.register_function<FunctionStruct<NamedStructImpl>>();
}

} // namespace doris::vectorized
