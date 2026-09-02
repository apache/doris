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
#include "core/column/column_nullable.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type.h"
#include "util/json/path_in_data.h"

namespace doris {

class ColumnMap;

namespace segment_v2::variant_v2 {

enum class StorageMapKind : uint8_t {
    NONE,
    SPARSE,
    DOC,
};

// Describes the physical streams available to one hierarchical reader. Paths are normalized once
// by create().
struct VariantAssemblerOptions {
    struct MaterializedPath {
        PathInData path;
        DataTypePtr type;
    };

    PathInData requested_path;
    DorisVector<MaterializedPath> materialized_paths;
    StorageMapKind storage_map_kind = StorageMapKind::NONE;
    bool has_root = false;
};

namespace variant_assembler_detail {

// Normalized materialized stream. Keeping the relative path, type, and caller batch position in
// one record prevents sorted metadata from drifting away from its source column.
struct MaterializedSlot {
    PathInData relative_path;
    DataTypePtr type;
    size_t batch_index = 0;
};

} // namespace variant_assembler_detail

// Borrowed storage batch. All spans and pointers only need to remain valid until assemble()
// returns; the assembler retains none of them. Hierarchical materialized columns stay in the same
// order as the paths supplied to create(); a nullable root_jsonb is the authoritative whole-root
// SQL NULL state, and storage_map uses persisted Map<String,String>.
struct VariantAssemblerBatchView {
    size_t num_rows = 0;
    const IColumn* root_jsonb = nullptr;
    std::span<const IColumn* const> materialized_columns;
    const ColumnMap* storage_map = nullptr;
};

// Owns normalized PathInData/DataType metadata from create(), then prepares and assembles each
// borrowed batch. A failed batch does not alter later calls or publish a partial result.
class VariantAssembler final {
public:
    static Result<std::unique_ptr<VariantAssembler>> create(VariantAssemblerOptions options);

    VariantAssembler(const VariantAssembler&) = delete;
    VariantAssembler& operator=(const VariantAssembler&) = delete;

    // The nullable wrapper is the complete owning result: its nested column is ColumnVariantV2 and
    // its null map is the assembled outer-null state. It is assigned only after the batch succeeds.
    Status assemble(const VariantAssemblerBatchView& batch,
                    ColumnNullable::MutablePtr* output) const;

private:
    VariantAssembler(StorageMapKind storage_map_kind, bool has_root, const PathInData& requested,
                     DorisVector<variant_assembler_detail::MaterializedSlot> materialized,
                     bool can_assemble_flat_materialized);

    StorageMapKind _storage_map_kind;
    bool _has_root;
    PathInData _requested;
    DorisVector<variant_assembler_detail::MaterializedSlot> _materialized;
    bool _can_assemble_flat_materialized;
};

} // namespace segment_v2::variant_v2
} // namespace doris
