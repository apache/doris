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

#include "simple_function_factory.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

// size function for size with map and array
class FunctionSize : public IFunction {
public:
    static constexpr auto name = "size";
    static FunctionPtr create() { return std::make_shared<FunctionSize>(); }
    String get_name() const override { return name; }
    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DataTypePtr datatype = arguments[0];
        if (datatype->is_nullable()) {
            datatype = assert_cast<const DataTypeNullable*>(datatype.get())->get_nested_type();
        }
        DCHECK(is_map(datatype) || is_array(datatype)) << "first argument for function: " << name
                                                       << " should be DataTypeMap or DataTypeArray";
        return std::make_shared<DataTypeInt64>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const auto& [left_column, left_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto type = block.get_by_position(arguments[0]).type;
        const ColumnArray* array_column = nullptr;
        const ColumnMap* map_column = nullptr;
        if (is_array(type)) {
            if (left_column->is_nullable()) {
                auto nullable_column = reinterpret_cast<const ColumnNullable*>(left_column.get());
                array_column =
                        check_and_get_column<ColumnArray>(nullable_column->get_nested_column());
            } else {
                array_column = check_and_get_column<ColumnArray>(*left_column.get());
            }
        } else if (is_map(type)) {
            if (left_column->is_nullable()) {
                auto nullable_column = reinterpret_cast<const ColumnNullable*>(left_column.get());
                map_column = check_and_get_column<ColumnMap>(nullable_column->get_nested_column());
            } else {
                map_column = check_and_get_column<ColumnMap>(*left_column.get());
            }
        }

        auto dst_column = ColumnInt64::create(input_rows_count);
        auto& dst_data = dst_column->get_data();

        if (left_const && map_column) {
            for (size_t i = 0; i < map_column->size(); i++) {
                dst_data[i] = map_column->size_at(0);
            }
        } else if (left_const && array_column) {
            for (size_t i = 0; i < array_column->size(); i++) {
                dst_data[i] = array_column->size_at(0);
            }
        } else if (map_column) {
            for (size_t i = 0; i < map_column->size(); i++) {
                dst_data[i] = map_column->size_at(i);
            }
        } else if (array_column) {
            for (size_t i = 0; i < array_column->size(); i++) {
                dst_data[i] = array_column->size_at(i);
            }
        } else {
            return Status::RuntimeError("unsupported types for function {}({})", get_name(),
                                        block.get_by_position(arguments[0]).type->get_name());
        }

        block.replace_by_position(result, std::move(dst_column));
        return Status::OK();
    }
};

void register_function_size(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionSize>();
    factory.register_alias(FunctionSize::name, "map_size");
    factory.register_alias(FunctionSize::name, "cardinality");
    factory.register_alias(FunctionSize::name, "array_size");
}
} // namespace doris::vectorized