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
#include <optional>
#include <span>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type.h"
#include "util/json/path_in_data.h"

namespace doris {

class ColumnMap;
class VariantShreddedColumnBuilder;

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
    // DOC storage is the authoritative value source, but footer materialized paths still describe
    // scalar identities that can be retained in S-state without reading duplicate value streams.
    DorisVector<MaterializedPath> shredded_layout_hints;
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
    // Block-local index into VariantShreddedColumnBuilder::layout(), bound once by create().
    std::optional<size_t> shredded_path_index;
};

} // namespace variant_assembler_detail

// Storage batch. Borrowed materialized_columns remain valid until assemble() returns. Alternatively,
// owned_materialized_columns explicitly transfers each mutable owner during assemble(), enabling
// native typed children to reuse their physical data without a copy. The two spans are mutually
// exclusive and stay in the same order as the paths supplied to create(). A nullable root_jsonb is
// the authoritative whole-root SQL NULL state, and storage_map uses persisted Map<String,String>.
struct VariantAssemblerBatchView {
    size_t num_rows = 0;
    const IColumn* root_jsonb = nullptr;
    std::span<const IColumn* const> materialized_columns;
    std::span<MutableColumnPtr> owned_materialized_columns;
    const ColumnMap* storage_map = nullptr;
};

// Owns normalized PathInData/DataType metadata from create(), then prepares and assembles each
// borrowed batch. A failed batch does not alter later calls or publish a partial result.
class VariantAssembler final {
public:
    static Result<std::unique_ptr<VariantAssembler>> create(VariantAssemblerOptions options);

    ~VariantAssembler();

    VariantAssembler(const VariantAssembler&) = delete;
    VariantAssembler& operator=(const VariantAssembler&) = delete;

    // The nullable wrapper is the complete owning result: its nested column is encoded for subtree
    // reads and may be shredded for a whole-root fixed scalar layout; its null map is the assembled
    // outer-null state. It is assigned only after the batch succeeds.
    Status assemble(const VariantAssemblerBatchView& batch,
                    ColumnNullable::MutablePtr* output) const;

#ifdef BE_TEST
    struct TestAccess {
        static size_t encoded_shredded_builds(const VariantAssembler& assembler);
        static size_t direct_shredded_builds(const VariantAssembler& assembler);
        static size_t max_shredded_execution_layout_paths();
    };
#endif

private:
    VariantAssembler(StorageMapKind storage_map_kind, bool has_root, const PathInData& requested,
                     DorisVector<variant_assembler_detail::MaterializedSlot> materialized,
                     std::unique_ptr<VariantShreddedColumnBuilder> shredded_builder);

    StorageMapKind _storage_map_kind;
    bool _has_root;
    PathInData _requested;
    DorisVector<variant_assembler_detail::MaterializedSlot> _materialized;
    std::unique_ptr<VariantShreddedColumnBuilder> _shredded_builder;
};

} // namespace segment_v2::variant_v2
} // namespace doris
