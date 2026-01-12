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
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

class FunctionStructElement : public IFunction {
public:
    static constexpr auto name = "struct_element";
    static FunctionPtr create() { return std::make_shared<FunctionStructElement>(); }

    // Get function name.
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        DCHECK(arguments[0].type->get_primitive_type() == TYPE_STRUCT)
                << "First argument for function: " << name
                << " should be DataTypeStruct but it has type " << arguments[0].type->get_name()
                << ".";
        DCHECK(is_int_or_bool(arguments[1].type->get_primitive_type()) ||
               is_string_type(arguments[1].type->get_primitive_type()))
                << "Second argument for function: " << name
                << " should be Int or String but it has type " << arguments[1].type->get_name()
                << ".";

        const auto* struct_type = check_and_get_data_type<DataTypeStruct>(arguments[0].type.get());

        auto index_type = arguments[1].type;
        const auto& index_column = arguments[1].column;
        if (!index_column) {
            throw doris::Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "Function {}: second argument column is nullptr, but it should not be.",
                    get_name());
        }
        size_t index;
        auto st = get_element_index(*struct_type, index_column, index_type, &index);
        if (!st.ok()) {
            // will handle nullptr outside
            return nullptr;
        }
        // The struct_element is marked as AlwaysNullable in fe.
        return make_nullable(struct_type->get_elements()[index]);
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto* struct_type = check_and_get_data_type<DataTypeStruct>(
                block.get_by_position(arguments[0]).type.get());
        const auto* struct_col = check_and_get_column<ColumnStruct>(
                block.get_by_position(arguments[0]).column.get());
        if (!struct_col || !struct_type) {
            return Status::RuntimeError(
                    fmt::format("unsupported types for function {}({}, {})", get_name(),
                                block.get_by_position(arguments[0]).type->get_name(),
                                block.get_by_position(arguments[1]).type->get_name()));
        }

        auto index_column = block.get_by_position(arguments[1]).column;
        auto index_type = block.get_by_position(arguments[1]).type;
        size_t index;
        RETURN_IF_ERROR(get_element_index(*struct_type, index_column, index_type, &index));
        ColumnPtr res_column = struct_col->get_column_ptr(index);
        ColumnPtr ele_column = res_column->clone_resized(res_column->size());
        //This function must return a ColumnNullable column, so it is necessary to convert the result column into ColumnNullable.
        block.replace_by_position(result, make_nullable(ele_column));
        return Status::OK();
    }

private:
    Status get_element_index(const DataTypeStruct& struct_type, const ColumnPtr& index_column,
                             const DataTypePtr& index_type, size_t* result) const {
        size_t index;
        if (is_int_or_bool(index_type->get_primitive_type())) {
            index = index_column->get_int(0);
            size_t limit = struct_type.get_elements().size() + 1;
            if (index < 1 || index >= limit) {
                return Status::RuntimeError(
                        fmt::format("Index out of bound for function {}: index {} should base from "
                                    "1 and less than {}.",
                                    get_name(), index, limit));
            }
            index -= 1; // the index start from 1
        } else if (is_string_type(index_type->get_primitive_type())) {
            std::string field_name = index_column->get_data_at(0).to_string();
            std::optional<size_t> pos = struct_type.try_get_position_by_name(field_name);
            if (!pos.has_value()) {
                return Status::RuntimeError(
                        fmt::format("Element not found for function {}: name {} not found in {}.",
                                    get_name(), field_name, struct_type.get_name()));
            }
            index = pos.value();
        } else {
            return Status::RuntimeError(
                    fmt::format("Argument not supported for function {}: second arg type {} should "
                                "be int or string.",
                                get_name(), index_type->get_name()));
        }
        *result = index;
        return Status::OK();
    }
};

void register_function_struct_element(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionStructElement>();
}

} // namespace doris::vectorized
