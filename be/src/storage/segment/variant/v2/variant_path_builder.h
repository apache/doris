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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_string.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type.h"
#include "core/value/variant/variant_value.h"
#include "util/json/path_in_data.h"

namespace doris::segment_v2 {

struct VariantPathColumn {
    PathInData path;
    DataTypePtr type;
    // Values stay compact until the storage writer consumes them. rowids[i] is the logical
    // segment row for column[i]; gaps are written as SQL NULL without constructing an N-row
    // column for every selected path.
    ColumnPtr column;
    DorisVector<uint32_t> rowids;
};

// Storage publication normalizes integer widths consistently for both shredded paths and
// already-extracted leaf writers. A Nothing nested inside ARRAY cannot be published unless a
// predefined storage type resolves it.
DataTypePtr normalize_variant_path_integer_widths(const DataTypePtr& type);
bool variant_path_type_contains_nothing(const DataTypePtr& type);

// An incrementally promotable builder for one flattened Variant path. Present leaves are stored
// compactly beside their ordered logical row ids; a storage writer fills row-id gaps with SQL NULL
// only when it consumes a selected physical subcolumn. The builder owns every byte after append
// returns.
class VariantPathBuilder final {
public:
    explicit VariantPathBuilder(PathInData path, size_t prefix_rows = 0);
    ~VariantPathBuilder();

    VariantPathBuilder(VariantPathBuilder&&) noexcept;
    VariantPathBuilder& operator=(VariantPathBuilder&&) noexcept;
    VariantPathBuilder(const VariantPathBuilder&) = delete;
    VariantPathBuilder& operator=(const VariantPathBuilder&) = delete;

    Status append(VariantRef value, size_t row);
    Status complete_rows(size_t rows);
    Status convert_to(const DataTypePtr& storage_type);

    const PathInData& path() const;
    const DataTypePtr& type() const;
    ColumnPtr column() const;
    std::span<const uint32_t> rowids() const;
    uint32_t non_null_rows() const;
#ifdef BE_TEST
    size_t rows() const;
    size_t promotion_count() const;
    bool is_null_at(size_t row) const;
    Status materialize(ColumnPtr* result) const;
#endif
    size_t byte_size() const;

    Status write_sparse_cell(size_t value_index, ColumnString::Chars* chars) const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

struct VariantPathSelectionCandidate {
    const VariantPathBuilder* builder = nullptr;
    bool is_typed_path = false;
};

struct VariantPathSelection {
    DorisVector<size_t> materialized;
    DorisVector<size_t> sparse;
};

// Typed paths are fixed unless typed_paths_to_sparse is enabled. Dynamic paths are ordered by the
// authoritative non-null-count/depth/path rule before the materialization budget is applied.
VariantPathSelection select_variant_paths(std::span<const VariantPathSelectionCandidate> candidates,
                                          size_t max_dynamic_materialized_paths,
                                          bool typed_paths_to_sparse);

} // namespace doris::segment_v2
