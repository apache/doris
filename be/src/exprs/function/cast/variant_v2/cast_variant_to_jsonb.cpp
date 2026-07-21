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
#include "exprs/function/cast/variant_v2/cast_variant_v2_internal.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "exprs/function_context.h"
#include "runtime/runtime_state.h"
#include "util/jsonb_writer.h"

namespace doris::CastWrapper::variant_v2_internal {
namespace {

VariantJsonFormatOptions json_options(FunctionContext* context) {
    if (context == nullptr || context->state() == nullptr) {
        return {};
    }
    return {.timezone = &context->state()->timezone_obj()};
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

} // namespace

Status cast_jsonb_to_variant(const ColumnPtr& source, size_t rows, ForcedNulls forced_nulls,
                             ColumnPtr* output) {
    const auto* strings = check_and_get_column<ColumnString>(source.get());
    if (strings == nullptr || strings->size() != rows ||
        (!forced_nulls.empty() && forced_nulls.size() != rows)) {
        return Status::InvalidArgument("Invalid JSONB input shape for Variant V2 CAST");
    }
    JsonbToVariantEncoder encoder(VariantBatchBuilder::ReserveHint {.rows = rows});
    for (size_t row = 0; row < rows; ++row) {
        if (!forced_nulls.empty() && forced_nulls[row] != 0) {
            encoder.add_null();
        } else {
            encoder.add_jsonb(strings->get_data_at(row));
        }
    }
    VariantBatchBuilder block = encoder.finish_batch();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_batch(block);
    *output = std::move(result);
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
