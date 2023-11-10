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

#include "util/bitmap_value.h"
#include "vec/columns/column_complex.h"
#include "vec/data_types/data_type_number.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

struct FunctionBitmapMinImpl {
    static constexpr auto name = "bitmap_min";
    static Int64 calculate(const BitmapValue& value) { return value.minimum(); }
};

struct FunctionBitmapMaxImpl {
    static constexpr auto name = "bitmap_max";
    static Int64 calculate(const BitmapValue& value) { return value.maximum(); }
};

template <typename Impl>
class FunctionBitmapSingle : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionBitmapSingle>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeInt64>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto result_column = ColumnInt64::create();
        auto result_null_map_column = ColumnUInt8::create(input_rows_count, 0);

        ColumnPtr argument_column = block.get_by_position(arguments[0]).column;
        if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_column)) {
            // Danger: Here must dispose the null map data first! Because
            // argument_columns[i]=nullable->get_nested_column_ptr(); will release the mem
            // of column nullable mem of null map
            VectorizedUtils::update_null_map(result_null_map_column->get_data(),
                                             nullable->get_null_map_data());
            argument_column = nullable->get_nested_column_ptr();
        }

        execute_straight(assert_cast<const ColumnBitmap*>(argument_column.get()),
                         assert_cast<ColumnInt64*>(result_column.get()),
                         assert_cast<ColumnUInt8*>(result_null_map_column.get())->get_data(),
                         input_rows_count);

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(result_column), std::move(result_null_map_column));
        return Status::OK();
    }

private:
    void execute_straight(const ColumnBitmap* date_column, ColumnInt64* result_column,
                          NullMap& result_null_map, size_t input_rows_count) const {
        for (size_t i = 0; i < input_rows_count; i++) {
            if (result_null_map[i]) {
                result_column->insert_default();
                continue;
            }

            BitmapValue value = date_column->get_element(i);
            if (!value.cardinality()) {
                result_null_map[i] = true;
                result_column->insert_default();
                continue;
            }

            result_column->insert(Impl::calculate(value));
        }
    }
};

} // namespace doris::vectorized
