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

#include "core/column/column_variant_v2.h"
#include "core/custom_allocator.h"
#include "exprs/function/function_variant_element_v2.h"

namespace doris::variant_native_v2_internal {

bool is_outer_null(std::span<const uint8_t> outer_nulls, size_t row) noexcept;

// Resolves object keys once per dense metadata id and traverses arrays without metadata lookup.
class VariantPathV2BatchReader {
public:
    VariantPathV2BatchReader(const ColumnVariantV2& source,
                             const ResolvedVariantElementV2Path& path);

    bool find_at(size_t row, VariantValueRef* output);
    size_t metadata_count() const noexcept;
    uint32_t metadata_id_at(size_t row) const;
    VariantMetadataRef metadata_at(uint32_t id);

private:
    void _resolve_metadata(uint32_t metadata_id);

    ColumnVariantV2::ReadView _source;
    const ResolvedVariantElementV2Path& _path;
    DorisVector<int64_t> _object_ids;
    DorisVector<uint8_t> _resolved_metadata;
};

Status execute_variant_exists_path_v2(const ColumnVariantV2& source,
                                      const ResolvedVariantElementV2Path& path,
                                      std::span<const uint8_t> outer_nulls, ColumnPtr* output);
Status execute_variant_keys_v2(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls,
                               ColumnPtr* output);
Status execute_variant_length_v2(const ColumnVariantV2& source,
                                 std::span<const uint8_t> outer_nulls, ColumnPtr* output);
Status execute_variant_contains_v2(const ColumnVariantV2& target, const ColumnVariantV2& candidate,
                                   std::span<const uint8_t> target_outer_nulls,
                                   std::span<const uint8_t> candidate_outer_nulls,
                                   ColumnPtr* output);

} // namespace doris::variant_native_v2_internal
