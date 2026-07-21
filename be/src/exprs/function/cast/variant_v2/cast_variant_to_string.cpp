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
#include <utility>

#include "core/assert_cast.h"
#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_string.h"
#include "core/string_buffer.hpp"
#include "exprs/function/cast/variant_v2/cast_variant_v2_internal.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "exprs/function_context.h"
#include "runtime/runtime_state.h"

namespace doris::CastWrapper::variant_v2_internal {
namespace {

VariantJsonFormatOptions json_options(FunctionContext* context) {
    if (context == nullptr || context->state() == nullptr) {
        return {};
    }
    return {.timezone = &context->state()->timezone_obj()};
}

bool uses_concrete_string_cast(VariantRef value) {
    if (value.basic_type() == VariantBasicType::SHORT_STRING) {
        return true;
    }
    if (value.basic_type() != VariantBasicType::PRIMITIVE) {
        return false;
    }
    switch (value.primitive_id()) {
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
    case VariantPrimitiveId::INT8:
    case VariantPrimitiveId::INT16:
    case VariantPrimitiveId::INT32:
    case VariantPrimitiveId::INT64:
    case VariantPrimitiveId::DOUBLE:
    case VariantPrimitiveId::DECIMAL4:
    case VariantPrimitiveId::DECIMAL8:
    case VariantPrimitiveId::DECIMAL16:
    case VariantPrimitiveId::DATE:
    case VariantPrimitiveId::TIMESTAMP_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
    case VariantPrimitiveId::FLOAT:
    case VariantPrimitiveId::STRING:
    case VariantPrimitiveId::TIMESTAMP_NANOS:
    case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
        return true;
    case VariantPrimitiveId::NULL_VALUE:
    case VariantPrimitiveId::BINARY:
    case VariantPrimitiveId::TIME_NTZ_MICROS:
    case VariantPrimitiveId::UUID:
        return false;
    }
    throw Exception(ErrorCode::CORRUPTION, "Unknown Variant primitive id");
}

void append_fallback_string(VariantRef value, bool forced_null,
                            const VariantJsonFormatOptions& options, VectorBufferWriter* writer,
                            ColumnUInt8* nulls) {
    if (forced_null) {
        nulls->insert_value(1);
        writer->commit();
        return;
    }
    nulls->insert_value(0);
    if (value.is_null()) {
        writer->write("null", 4);
    } else {
        to_json(value, *writer, options);
    }
    writer->commit();
}

Status assemble_mixed_strings(ColumnPtr concrete, std::span<const size_t> concrete_rows,
                              ColumnString::MutablePtr fallback_strings,
                              ColumnUInt8::MutablePtr fallback_nulls,
                              std::span<const size_t> fallback_rows, size_t rows,
                              ColumnPtr* output) {
    const auto& nullable = assert_cast<const ColumnNullable&>(*concrete);
    auto strings = ColumnString::create();
    auto nulls = ColumnUInt8::create();
    strings->reserve(rows);
    nulls->reserve(rows);
    strings->insert_range_from(nullable.get_nested_column(), 0, concrete_rows.size());
    nulls->insert_range_from(nullable.get_null_map_column(), 0, concrete_rows.size());
    strings->insert_range_from(*fallback_strings, 0, fallback_rows.size());
    nulls->insert_range_from(*fallback_nulls, 0, fallback_rows.size());

    IColumn::Permutation permutation(rows);
    size_t concatenated_row = 0;
    for (size_t row : concrete_rows) {
        permutation[row] = concatenated_row++;
    }
    for (size_t row : fallback_rows) {
        permutation[row] = concatenated_row++;
    }
    if (concatenated_row != rows) {
        return Status::InternalError("Variant V2 STRING CAST produced {} rows, expected {}",
                                     concatenated_row, rows);
    }
    ColumnPtr concatenated = ColumnNullable::create(std::move(strings), std::move(nulls));
    *output = concatenated->permute(permutation, rows);
    return Status::OK();
}

template <typename ValueAt, typename CastAllConcrete>
Status cast_values_to_string(FunctionContext* context, size_t rows, ForcedNulls forced_nulls,
                             ValueAt&& value_at, CastAllConcrete&& cast_all_concrete,
                             ColumnPtr* output) {
    DorisVector<size_t> concrete_rows;
    DorisVector<size_t> fallback_rows;
    concrete_rows.reserve(rows);
    fallback_rows.reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        const bool forced = !forced_nulls.empty() && forced_nulls[row] != 0;
        if (!forced && uses_concrete_string_cast(value_at(row))) {
            concrete_rows.push_back(row);
        } else {
            fallback_rows.push_back(row);
        }
    }

    if (concrete_rows.size() == rows) {
        return cast_all_concrete(std::make_shared<DataTypeString>(), output);
    }

    auto fallback_strings = ColumnString::create();
    auto fallback_nulls = ColumnUInt8::create();
    VectorBufferWriter writer(*fallback_strings);
    const VariantJsonFormatOptions options = json_options(context);
    for (size_t row : fallback_rows) {
        const bool forced = !forced_nulls.empty() && forced_nulls[row] != 0;
        append_fallback_string(forced ? VariantRef {} : value_at(row), forced, options, &writer,
                               fallback_nulls.get());
    }
    if (concrete_rows.empty()) {
        *output = ColumnNullable::create(std::move(fallback_strings), std::move(fallback_nulls));
        return Status::OK();
    }

    DorisVector<VariantRef> concrete_values;
    concrete_values.reserve(concrete_rows.size());
    for (size_t row : concrete_rows) {
        concrete_values.push_back(value_at(row));
    }
    ColumnPtr concrete;
    RETURN_IF_ERROR(cast_variant_refs_to_scalar(context, concrete_values,
                                                std::make_shared<DataTypeString>(), {}, &concrete));
    return assemble_mixed_strings(std::move(concrete), concrete_rows, std::move(fallback_strings),
                                  std::move(fallback_nulls), fallback_rows, rows, output);
}

Status cast_typed_variant_to_string(FunctionContext* context, const ColumnVariantV2& source,
                                    size_t rows, ForcedNulls forced_nulls, ColumnPtr* output) {
    const auto& typed = assert_cast<const ColumnNullable&>(source.typed_column());
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    const NullMap& inner_nulls = typed.get_null_map_data();
    size_t concrete_rows = 0;
    for (size_t row = 0; row < rows; ++row) {
        if (inner_nulls[row] == 0 && (forced_nulls.empty() || forced_nulls[row] == 0)) {
            ++concrete_rows;
        }
    }
    if (concrete_rows == rows) {
        return cast_variant_values_to_scalar(context, source, string_type, rows, {}, output);
    }

    ColumnPtr concrete;
    if (concrete_rows != 0) {
        RETURN_IF_ERROR(cast_variant_values_to_scalar(context, source, string_type, rows,
                                                      forced_nulls, &concrete));
    }
    const ColumnNullable* converted =
            concrete ? &assert_cast<const ColumnNullable&>(*concrete) : nullptr;
    const ColumnString* converted_strings =
            converted == nullptr
                    ? nullptr
                    : &assert_cast<const ColumnString&>(converted->get_nested_column());
    auto strings = ColumnString::create();
    auto nulls = ColumnUInt8::create();
    strings->reserve(rows);
    nulls->reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        const bool forced = !forced_nulls.empty() && forced_nulls[row] != 0;
        if (forced) {
            strings->insert_default();
            nulls->insert_value(1);
        } else if (inner_nulls[row] != 0) {
            strings->insert_data("null", 4);
            nulls->insert_value(0);
        } else {
            DCHECK(converted != nullptr);
            DCHECK_EQ(converted->get_null_map_data()[row], 0);
            strings->insert_from(*converted_strings, row);
            nulls->insert_value(0);
        }
    }
    *output = ColumnNullable::create(std::move(strings), std::move(nulls));
    return Status::OK();
}

} // namespace

Status cast_variant_refs_to_string(FunctionContext* context, std::span<const VariantRef> values,
                                   ForcedNulls forced_nulls, ColumnPtr* output) {
    if (!forced_nulls.empty() && forced_nulls.size() != values.size()) {
        return Status::InvalidArgument("Variant V2 STRING CAST null map has {} rows, expected {}",
                                       forced_nulls.size(), values.size());
    }
    return cast_values_to_string(
            context, values.size(), forced_nulls, [&](size_t row) { return values[row]; },
            [&](const DataTypePtr& string_type, ColumnPtr* result) {
                return cast_variant_refs_to_scalar(context, values, string_type, {}, result);
            },
            output);
}

Status cast_variant_to_string(FunctionContext* context, const ColumnVariantV2& source, size_t rows,
                              ForcedNulls forced_nulls, ColumnPtr* output) {
    if (source.size() != rows || (!forced_nulls.empty() && forced_nulls.size() != rows)) {
        return Status::InvalidArgument("Invalid Variant V2 input shape for STRING CAST");
    }
    if (source.is_typed()) {
        return cast_typed_variant_to_string(context, source, rows, forced_nulls, output);
    }
    return cast_values_to_string(
            context, rows, forced_nulls, [&](size_t row) { return source.get_value_ref(row); },
            [&](const DataTypePtr& string_type, ColumnPtr* result) {
                return cast_encoded_variant_to_scalar(context, source, string_type, rows, {},
                                                      result);
            },
            output);
}

} // namespace doris::CastWrapper::variant_v2_internal
