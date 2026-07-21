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

#include <cstdint>
#include <memory>

#include "common/check.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/types.h"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "runtime/runtime_state.h"
#include "storage/row_ttl.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

class FunctionRowTtlIsVisible final : public IFunction {
public:
    static constexpr auto name = "row_ttl_is_visible";

    static FunctionPtr create() { return std::make_shared<FunctionRowTtlIsVisible>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return std::make_shared<DataTypeUInt8>();
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& ttl_entry = block.get_by_position(arguments[0]);
        ColumnPtr ttl_column = ttl_entry.column->convert_to_full_column_if_const();
        const NullMap* null_map = VectorizedUtils::get_null_map(ttl_column);
        ttl_column = remove_nullable(ttl_column);
        const FieldType ttl_type = TabletColumn::get_field_type_by_type(
                remove_nullable(ttl_entry.type)->get_primitive_type());
        const bool source_time = ttl_type != FieldType::OLAP_FIELD_TYPE_BIGINT;
        const auto* direct_expiration =
                source_time ? nullptr : check_and_get_column<ColumnInt64>(ttl_column.get());
        DORIS_CHECK(source_time || direct_expiration != nullptr);
        ColumnPtr duration_column = block.get_by_position(arguments[1])
                                            .column->convert_to_full_column_if_const();
        const auto& duration = assert_cast<const ColumnInt64&>(*duration_column).get_data();

        auto visible = ColumnUInt8::create(input_rows_count);
        auto& visible_data = visible->get_data();
        const int64_t query_now_us = context->state()->timestamp_ms() / 1'000 * 1'000'000L +
                                     context->state()->nano_seconds() / 1'000;
        const cctz::time_zone local_time_zone = cctz::local_time_zone();
        for (size_t row = 0; row < input_rows_count; ++row) {
            if (null_map != nullptr && (*null_map)[row]) {
                visible_data[row] = 1;
                continue;
            }
            int64_t expiration_us = 0;
            if (source_time) {
                RETURN_IF_ERROR(calculate_row_ttl_expiration_us(
                        *ttl_column, ttl_type, row, local_time_zone, duration[row],
                        &expiration_us));
            } else {
                expiration_us = direct_expiration->get_data()[row];
            }
            visible_data[row] = expiration_us > query_now_us;
        }
        block.replace_by_position(result, std::move(visible));
        return Status::OK();
    }
};

void register_function_row_ttl(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionRowTtlIsVisible>();
}

} // namespace doris
