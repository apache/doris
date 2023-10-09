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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionDateOrDatetimeToString.h
// and modified by Doris

#pragma once

#include <stddef.h>

#include <memory>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/date_time_transforms.h"
#include "vec/functions/function.h"
#include "vec/utils/util.hpp"

namespace doris {
class FunctionContext;

namespace vectorized {
class DataTypeString;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

template <typename Transform>
class FunctionDateOrDateTimeToString : public IFunction {
public:
    static constexpr auto name = Transform::name;
    static constexpr bool has_variadic_argument =
            !std::is_void_v<decltype(has_variadic_argument_types(std::declval<Transform>()))>;
    static FunctionPtr create() { return std::make_shared<FunctionDateOrDateTimeToString>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }
    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        RETURN_REAL_TYPE_FOR_DATEV2_FUNCTION(DataTypeString);
    }

    bool is_variadic() const override { return true; }

    DataTypes get_variadic_argument_types_impl() const override {
        if constexpr (has_variadic_argument) return Transform::get_variadic_argument_types();
        return {};
    }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const ColumnPtr source_col = block.get_by_position(arguments[0]).column;
        const auto is_nullable = block.get_by_position(result).type->is_nullable();
        const auto* sources = check_and_get_column<ColumnVector<typename Transform::OpArgType>>(
                remove_nullable(source_col).get());
        auto col_res = ColumnString::create();

        // Support all input of datetime is valind to make sure not null return
        if (sources) {
            if (is_nullable) {
                auto null_map = ColumnVector<UInt8>::create(input_rows_count);
                TransformerToStringOneArgument<Transform>::vector(
                        context, sources->get_data(), col_res->get_chars(), col_res->get_offsets(),
                        null_map->get_data());
                if (const auto* nullable_col =
                            check_and_get_column<ColumnNullable>(source_col.get())) {
                    NullMap& result_null_map = assert_cast<ColumnUInt8&>(*null_map).get_data();
                    const NullMap& src_null_map =
                            assert_cast<const ColumnUInt8&>(nullable_col->get_null_map_column())
                                    .get_data();
                    VectorizedUtils::update_null_map(result_null_map, src_null_map);
                }
                block.replace_by_position(
                        result, ColumnNullable::create(std::move(col_res), std::move(null_map)));
            } else {
                TransformerToStringOneArgument<Transform>::vector(
                        context, sources->get_data(), col_res->get_chars(), col_res->get_offsets());
                block.replace_by_position(result, std::move(col_res));
            }
        } else {
            return Status::InternalError("Illegal column {} of first argument of function {}",
                                         block.get_by_position(arguments[0]).column->get_name(),
                                         name);
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized
