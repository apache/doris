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

#include "vec/columns/columns_number.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

class FunctionConvertTZ : public IFunction {
public:
    static constexpr auto name = "convert_tz";

    static FunctionPtr create() { return std::make_shared<FunctionConvertTZ>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeDateTime>());
    }

    bool use_default_implementation_for_constants() const override { return true; }
    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto result_column = ColumnDateTime::create();
        auto result_null_map_column = ColumnUInt8::create(input_rows_count, 0);

        ColumnPtr argument_columns[3];

        for (int i = 0; i < 3; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_columns[i])) {
                // Danger: Here must dispose the null map data first! Because
                // argument_columns[i]=nullable->get_nested_column_ptr(); will release the mem
                // of column nullable mem of null map
                VectorizedUtils::update_null_map(result_null_map_column->get_data(),
                                                 nullable->get_null_map_data());
                argument_columns[i] = nullable->get_nested_column_ptr();
            }
        }

        execute_straight(context, assert_cast<const ColumnDateTime*>(argument_columns[0].get()),
                         assert_cast<const ColumnString*>(argument_columns[1].get()),
                         assert_cast<const ColumnString*>(argument_columns[2].get()),
                         assert_cast<ColumnDateTime*>(result_column.get()),
                         assert_cast<ColumnUInt8*>(result_null_map_column.get())->get_data(),
                         input_rows_count);

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(result_column), std::move(result_null_map_column));
        return Status::OK();
    }

private:
    void execute_straight(FunctionContext* context, const ColumnDateTime* date_column,
                          const ColumnString* from_tz_column, const ColumnString* to_tz_column,
                          ColumnDateTime* result_column, NullMap& result_null_map,
                          size_t input_rows_count) {
        for (size_t i = 0; i < input_rows_count; i++) {
            if (result_null_map[i]) {
                result_column->insert_default();
                continue;
            }

            StringRef from_tz = from_tz_column->get_data_at(i);
            StringRef to_tz = to_tz_column->get_data_at(i);

            VecDateTimeValue ts_value =
                    binary_cast<Int64, VecDateTimeValue>(date_column->get_element(i));
            int64_t timestamp;

            if (!ts_value.unix_timestamp(&timestamp, from_tz.to_string())) {
                result_null_map[i] = true;
                result_column->insert_default();
                continue;
            }

            VecDateTimeValue ts_value2;
            if (!ts_value2.from_unixtime(timestamp, to_tz.to_string())) {
                result_null_map[i] = true;
                result_column->insert_default();
                continue;
            }

            result_column->insert(binary_cast<VecDateTimeValue, Int64>(ts_value2));
        }
    }
};

} // namespace doris::vectorized
