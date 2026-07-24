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

#include <span>

#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "core/value/variant/variant_value.h"

namespace doris {

class ColumnVariantV2;
class FunctionContext;

namespace CastWrapper::variant_v2_internal {

using ForcedNulls = std::span<const NullMap::value_type>;

bool is_supported_scalar_source(const DataTypePtr& type);
bool is_supported_scalar_target(const DataTypePtr& type);

Status cast_scalar_to_variant(const ColumnPtr& source, const DataTypePtr& source_type, size_t rows,
                              ForcedNulls forced_nulls, ColumnPtr* output);
Status cast_jsonb_to_variant(const ColumnPtr& source, size_t rows, ForcedNulls forced_nulls,
                             ColumnPtr* output);
Status cast_array_to_variant(const ColumnPtr& source, const DataTypePtr& source_type, size_t rows,
                             ForcedNulls forced_nulls, ColumnPtr* output);

Status cast_typed_variant_to_scalar(FunctionContext* context, const ColumnVariantV2& source,
                                    const DataTypePtr& target_type, size_t rows,
                                    ForcedNulls forced_nulls, ColumnPtr* output);
Status cast_variant_refs_to_scalar(FunctionContext* context, std::span<const VariantRef> values,
                                   const DataTypePtr& target_type, ForcedNulls forced_nulls,
                                   ColumnPtr* output);
Status cast_encoded_variant_to_scalar(FunctionContext* context, const ColumnVariantV2& source,
                                      const DataTypePtr& target_type, size_t rows,
                                      ForcedNulls forced_nulls, ColumnPtr* output);
Status cast_variant_values_to_scalar(FunctionContext* context, const ColumnVariantV2& source,
                                     const DataTypePtr& target_type, size_t rows,
                                     ForcedNulls forced_nulls, ColumnPtr* output);
Status cast_variant_to_array(FunctionContext* context, const ColumnVariantV2& source,
                             const DataTypePtr& target_type, size_t rows, ForcedNulls forced_nulls,
                             ColumnPtr* output);
Status cast_variant_to_string(FunctionContext* context, const ColumnVariantV2& source, size_t rows,
                              ForcedNulls forced_nulls, ColumnPtr* output);
Status cast_variant_refs_to_string(FunctionContext* context, std::span<const VariantRef> values,
                                   ForcedNulls forced_nulls, ColumnPtr* output);
Status cast_variant_to_jsonb(FunctionContext* context, const ColumnVariantV2& source, size_t rows,
                             ForcedNulls forced_nulls, ColumnPtr* output);
Status cast_variant_refs_to_jsonb(FunctionContext* context, std::span<const VariantRef> values,
                                  ForcedNulls forced_nulls, ColumnPtr* output);

ColumnPtr make_all_null_column(const DataTypePtr& nested_type, size_t rows);
Status apply_forced_nulls(ColumnPtr column, ForcedNulls forced_nulls, ColumnPtr* output);

} // namespace CastWrapper::variant_v2_internal
} // namespace doris
