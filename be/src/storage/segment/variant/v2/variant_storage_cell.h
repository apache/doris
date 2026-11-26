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

#include <cstdint>
#include <span>

#include "common/status.h"
#include "core/column/column_nullable.h"
#include "core/string_ref.h"
#include "core/value/variant/variant_batch_builder.h"

namespace doris::segment_v2::variant_v2 {

// Appends one bounded V1 storage cell to an encoded Variant row. This is the shared value-level
// primitive used by exact-cell decoding and hierarchical assembly.
Status append_v1_storage_cell(StringRef cell, VariantBatchBuilder::Row& output, uint32_t depth);

// Decodes exact persisted V1 storage cells into one nullable Variant V2 batch. Missing/SQL NULL
// belongs only to the outer null map; a present NONE or JSONB null remains a Variant payload.
// The destination is replaced only after every unmasked cell has been validated and decoded.
Status decode_v1_storage_cells(std::span<const StringRef> cells,
                               std::span<const uint8_t> outer_nulls,
                               std::span<const uint8_t> missing,
                               ColumnNullable::MutablePtr* output);

} // namespace doris::segment_v2::variant_v2
