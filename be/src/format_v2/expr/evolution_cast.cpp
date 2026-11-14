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

#include "format_v2/expr/evolution_cast.h"

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_timestamptz.h"
#include "exprs/function/cast/cast_base.h"

namespace doris::format {

Status EvolutionCast::execute_column_impl(VExprContext* context, const Block* block,
                                          const Selector* selector, size_t count,
                                          ColumnPtr& result_column) const {
    const auto source = remove_nullable(_children[0]->execute_type(block));
    const auto target = remove_nullable(_data_type);
    const auto primitive = source->get_primitive_type();
    // An external BINARY table may overlay a GEOMETRY/GEOGRAPHY-annotated Parquet leaf.
    // Preserve its original WKB in an owning binary column without enabling a SQL spatial CAST.
    if ((primitive == TYPE_GEOMETRY || primitive == TYPE_GEOGRAPHY) &&
        target->get_primitive_type() == TYPE_VARBINARY) {
        ColumnPtr input;
        RETURN_IF_ERROR(_children[0]->execute_column(context, block, selector, count, input));
        auto result = _data_type->create_column();
        for (size_t row = 0; row < count; ++row) {
            if (input->is_null_at(row)) {
                // TableReader normally keeps the cast nullable until its final alignment. A
                // direct required cast must also reject NULL instead of replacing it with bytes.
                if (!_data_type->is_nullable()) {
                    return Status::InvalidArgument(
                            "Cannot read NULL spatial value as required VARBINARY");
                }
                result->insert_default();
            } else {
                const auto value = input->get_data_at(row);
                result->insert_data(value.data, value.size);
            }
        }
        result_column = std::move(result);
        return Status::OK();
    }
    // Paimon evolution truncates 01.600000 at p0 to 01; ordinary CAST rounds it to 02.
    // Keep this conversion out of SQL casts and other table formats' evolution paths.
    if (!_truncate_datetimev2_precision || primitive != target->get_primitive_type() ||
        (primitive != TYPE_DATETIMEV2 && primitive != TYPE_TIMESTAMPTZ) ||
        source->get_scale() <= target->get_scale()) {
        return Cast::execute_column_impl(context, block, selector, count, result_column);
    }

    ColumnPtr input;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, selector, count, input));
    auto result = _data_type->create_column();
    if (is_column_nullable(*result) && !is_column_nullable(*input)) {
        auto& nullable_result = assert_cast<ColumnNullable&>(*result);
        nullable_result.get_nested_column().insert_range_from(*input, 0, count);
        nullable_result.get_null_map_column().insert_many_vals(0, count);
    } else {
        result->insert_range_from(*input, 0, count);
    }
    IColumn* nested_column = result.get();
    if (is_column_nullable(*nested_column)) {
        nested_column = &assert_cast<ColumnNullable&>(*nested_column).get_nested_column();
    }
    uint32_t divisor = 1;
    for (uint32_t i = target->get_scale(); i < 6; ++i) {
        divisor *= 10;
    }
    if (primitive == TYPE_DATETIMEV2) {
        auto& data = assert_cast<ColumnDateTimeV2&>(*nested_column).get_data();
        for (auto& value : data) {
            value.unchecked_set_time_unit<TimeUnit::MICROSECOND>(value.microsecond() / divisor *
                                                                 divisor);
        }
    } else {
        auto& data = assert_cast<ColumnTimeStampTz&>(*nested_column).get_data();
        for (auto& value : data) {
            value.set_microsecond(value.microsecond() / divisor * divisor);
        }
    }
    result_column = std::move(result);
    return Status::OK();
}

void EvolutionCast::configure_cast_options(CastOptions& options) const {
    if (_strict) {
        options.strict_mode = true;
    } else if (remove_nullable(_children[0]->data_type())->get_primitive_type() == TYPE_DATEV2 &&
               remove_nullable(_data_type)->get_primitive_type() == TYPE_TIMESTAMP_NS) {
        // Iceberg DATE promotion must fail outside the nanosecond range, even in non-strict SQL
        // sessions. Otherwise a localized IS NOT NULL filter can silently discard retained rows.
        options.strict_mode = true;
    }
}

std::string EvolutionCast::debug_string() const {
    return "Evolution" + Cast::debug_string();
}

} // namespace doris::format
