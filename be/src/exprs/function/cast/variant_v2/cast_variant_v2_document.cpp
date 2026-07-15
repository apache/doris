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

#include <utility>

#include "core/assert_cast.h"
#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_string.h"
#include "core/string_buffer.hpp"
#include "exprs/function/cast/variant_v2/cast_variant_v2_internal.h"
#include "exprs/function_context.h"
#include "runtime/runtime_state.h"
#include "util/jsonb_writer.h"
#include "util/variant/variant_json.h"
#include "util/variant/variant_jsonb.h"

namespace doris::CastWrapper::variant_v2_internal {
namespace {

VariantJsonFormatOptions json_options(FunctionContext* context) {
    if (context == nullptr || context->state() == nullptr) {
        return {};
    }
    return {.timezone = &context->state()->timezone_obj()};
}

bool uses_concrete_string_cast(VariantValueRef value) {
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

const ColumnVariantV2& encoded_source(const ColumnVariantV2& source, ColumnPtr* owner) {
    if (!source.is_typed()) {
        return source;
    }
    Status status = clone_as_encoded(source, owner);
    if (!status.ok()) {
        throw Exception(status);
    }
    return assert_cast<const ColumnVariantV2&>(**owner);
}

Status append_fallback_string(VariantValueRef value, bool forced_null,
                              const VariantJsonFormatOptions& options, VectorBufferWriter* writer,
                              ColumnUInt8* nulls) {
    if (forced_null) {
        nulls->insert_value(1);
        writer->commit();
        return Status::OK();
    }
    nulls->insert_value(0);
    if (value.is_null()) {
        writer->write("null", 4);
    } else {
        to_json(value, *writer, options);
    }
    writer->commit();
    return Status::OK();
}

} // namespace

Status cast_jsonb_to_variant(const ColumnPtr& source, size_t rows, ForcedNulls forced_nulls,
                             ColumnPtr* output) {
    const auto* strings = check_and_get_column<ColumnString>(source.get());
    if (strings == nullptr || strings->size() != rows ||
        (!forced_nulls.empty() && forced_nulls.size() != rows)) {
        return Status::InvalidArgument("Invalid JSONB input shape for Variant V2 CAST");
    }
    JsonbToVariantEncoder encoder(VariantBlockBuilder::ReserveHint {.rows = rows});
    for (size_t row = 0; row < rows; ++row) {
        if (!forced_nulls.empty() && forced_nulls[row] != 0) {
            encoder.add_null();
        } else {
            encoder.add_jsonb(strings->get_data_at(row));
        }
    }
    VariantEncodedBlock block = encoder.finish_block();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_block(block.view());
    *output = std::move(result);
    return Status::OK();
}

Status cast_variant_to_string(FunctionContext* context, const ColumnVariantV2& source, size_t rows,
                              ForcedNulls forced_nulls, ColumnPtr* output) {
    if (source.size() != rows || (!forced_nulls.empty() && forced_nulls.size() != rows)) {
        return Status::InvalidArgument("Invalid Variant V2 input shape for STRING CAST");
    }
    ColumnPtr encoded_owner;
    const ColumnVariantV2& encoded = encoded_source(source, &encoded_owner);

    DorisVector<VariantValueRef> concrete_values;
    DorisVector<size_t> concrete_rows;
    DorisVector<size_t> fallback_rows;
    concrete_values.reserve(rows);
    concrete_rows.reserve(rows);
    fallback_rows.reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        const bool forced = !forced_nulls.empty() && forced_nulls[row] != 0;
        VariantValueRef value = encoded.get_value_ref(row);
        if (!forced && uses_concrete_string_cast(value)) {
            concrete_values.push_back(value);
            concrete_rows.push_back(row);
        } else {
            fallback_rows.push_back(row);
        }
    }

    MutableColumnPtr concatenated_strings = ColumnString::create();
    auto concatenated_nulls = ColumnUInt8::create();
    concatenated_strings->reserve(rows);
    concatenated_nulls->reserve(rows);
    IColumn::Permutation permutation(rows);
    size_t concatenated_row = 0;

    if (!concrete_values.empty()) {
        ColumnPtr concrete;
        RETURN_IF_ERROR(cast_variant_refs_to_scalar(
                context, concrete_values, std::make_shared<DataTypeString>(), {}, &concrete));
        const auto& nullable = assert_cast<const ColumnNullable&>(*concrete);
        concatenated_strings->insert_range_from(nullable.get_nested_column(), 0,
                                                concrete_values.size());
        concatenated_nulls->insert_range_from(nullable.get_null_map_column(), 0,
                                              concrete_values.size());
        for (size_t row : concrete_rows) {
            permutation[row] = concatenated_row++;
        }
    }

    auto fallback_strings = ColumnString::create();
    auto fallback_nulls = ColumnUInt8::create();
    VectorBufferWriter writer(*fallback_strings);
    const VariantJsonFormatOptions options = json_options(context);
    for (size_t row : fallback_rows) {
        RETURN_IF_ERROR(append_fallback_string(encoded.get_value_ref(row),
                                               !forced_nulls.empty() && forced_nulls[row] != 0,
                                               options, &writer, fallback_nulls.get()));
        permutation[row] = concatenated_row++;
    }
    concatenated_strings->insert_range_from(*fallback_strings, 0, fallback_rows.size());
    concatenated_nulls->insert_range_from(*fallback_nulls, 0, fallback_rows.size());

    if (concatenated_row != rows) {
        return Status::InternalError("Variant V2 STRING CAST produced {} rows, expected {}",
                                     concatenated_row, rows);
    }
    ColumnPtr concatenated =
            ColumnNullable::create(std::move(concatenated_strings), std::move(concatenated_nulls));
    *output = concatenated->permute(permutation, rows);
    return Status::OK();
}

Status cast_variant_to_jsonb(FunctionContext* context, const ColumnVariantV2& source, size_t rows,
                             ForcedNulls forced_nulls, ColumnPtr* output) {
    if (source.size() != rows || (!forced_nulls.empty() && forced_nulls.size() != rows)) {
        return Status::InvalidArgument("Invalid Variant V2 input shape for JSONB CAST");
    }
    ColumnPtr encoded_owner;
    const ColumnVariantV2& encoded = encoded_source(source, &encoded_owner);
    auto strings = ColumnString::create();
    auto nulls = ColumnUInt8::create(rows, 0);
    strings->reserve(rows);
    JsonbWriter writer;
    const VariantJsonFormatOptions options = json_options(context);
    for (size_t row = 0; row < rows; ++row) {
        if (!forced_nulls.empty() && forced_nulls[row] != 0) {
            strings->insert_default();
            nulls->get_data()[row] = 1;
            continue;
        }
        variant_to_jsonb(encoded.get_value_ref(row), writer, options);
        strings->insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
    }
    *output = ColumnNullable::create(std::move(strings), std::move(nulls));
    return Status::OK();
}

} // namespace doris::CastWrapper::variant_v2_internal
