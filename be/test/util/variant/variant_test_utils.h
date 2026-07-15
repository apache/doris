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

#include <array>
#include <limits>
#include <span>

#include "common/check.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "util/variant/variant_field.h"
#include "util/variant/variant_value.h"

namespace doris {

// Test-only independent validator for the canonical writer subset. The span is one complete
// encoding unit, so dictionary entries must equal the union of keys referenced by all rows.
void validate_canonical(VariantMetadataRef metadata, std::span<const VariantValueRef> rows);
void validate_canonical(VariantValueRef row);

inline void insert_encoded_field(ColumnVariantV2& column, const VariantField& field) {
    const VariantValueRef value = field.ref();
    DORIS_CHECK_LE(value.metadata.size, std::numeric_limits<uint32_t>::max());
    DORIS_CHECK_LE(value.size, std::numeric_limits<uint32_t>::max());
    const std::array<uint32_t, 2> metadata_offsets {0, static_cast<uint32_t>(value.metadata.size)};
    const std::array<uint32_t, 2> value_offsets {0, static_cast<uint32_t>(value.size)};
    column.insert_encoded_rows({.metadata_bytes = {value.metadata.data, value.metadata.size},
                                .metadata_offsets = metadata_offsets,
                                .meta_ids = {},
                                .value_bytes = {value.data, value.size},
                                .value_offsets = value_offsets});
}

} // namespace doris
