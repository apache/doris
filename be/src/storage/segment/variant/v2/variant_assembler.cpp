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

#include "storage/segment/variant/v2/variant_assembler.h"

#include <algorithm>
#include <cstring>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_column_utils.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/column/variant_v2/variant_shredded_column_builder.h"
#include "core/column/variant_v2/variant_shredded_path.h"
#include "core/data_type/data_type_nullable.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "storage/segment/variant/v2/variant_assembler_internal.h"
#include "storage/segment/variant/v2/variant_storage_cell.h"

namespace doris::segment_v2::variant_v2 {
namespace {

using MaterializedSlot = variant_assembler_detail::MaterializedSlot;
using MaterializedPath = VariantAssemblerOptions::MaterializedPath;

bool same_path(const PathInData& left, const PathInData& right) {
    return left.get_parts() == right.get_parts();
}

DorisVector<const MaterializedPath*> sorted_path_refs(const DorisVector<MaterializedPath>& paths) {
    DorisVector<const MaterializedPath*> sorted;
    sorted.reserve(paths.size());
    for (const MaterializedPath& path : paths) {
        sorted.push_back(&path);
    }
    std::ranges::sort(sorted, [](const MaterializedPath* left, const MaterializedPath* right) {
        return variant_shredded_path_less(left->path, right->path);
    });
    return sorted;
}

// Assembly has five stages:
// 1. create() makes materialized paths relative to the requested subtree, sorts them, and binds
//    their optional shredded field indices once.
// 2. prepare_hierarchical_batch() unwraps concrete columns once per batch.
// 3. The row loop merges already ordered materialized and sparse/doc paths. A row-local cursor
//    exposes persisted cells, while ObjectEmitter owns the open object scopes.
// 4. Non-DOC whole-root reads with a fixed scalar layout route visible scalar leaves directly into
//    owned shredded fields while ObjectEmitter writes the residual E. DOC roots are authoritative
//    encoded values and never use footer hints to construct S; subtree reads also remain encoded.
// 5. The completed E or S value column and outer null map are published atomically.
//
// StorageMapRowCursor and ObjectEmitter are deliberately local implementation state. Neither is a
// reusable reader abstraction: the former only advances one persisted map row, and the latter only
// translates this merge's ordered paths into VariantBatchBuilder calls.

void publish_assembled(VariantBatchBuilder* builder,
                       VariantShreddedColumnBuilder::Batch* shredded_batch,
                       ColumnUInt8::MutablePtr outer_nulls, ColumnNullable::MutablePtr* output) {
    VariantBatchBuilder block = builder->finish_batch();
    auto values = ColumnVariantV2::create();
    values->insert_encoded_batch(block);
    if (shredded_batch != nullptr) {
        values = shredded_batch->finish(std::move(values));
    }
    *output = ColumnNullable::create(std::move(values), std::move(outer_nulls));
}

// Row-local cursor over one persisted Map<String,String>. It owns no input data or I/O:
// bind() unwraps the concrete columns once per batch; start_row() then selects a row/subtree.
// Persisted paths are normalized dotted object paths, so the hot loop keeps only borrowed raw
// strings and checks the component boundary directly instead of building a parts vector.
struct StorageMapRowCursor {
    const ColumnMap* source = nullptr;
    const ColumnString* paths = nullptr;
    const ColumnString* values = nullptr;
    size_t index = 0;
    size_t end = 0;
    bool available = false;
    bool is_direct_subtree_value = false;
    uint32_t depth = 0;
    StringRef cell;
    StringRef sort_key;
    StringRef requested;
    bool reads_whole_variant = true;

    void bind(const ColumnMap* storage_map, size_t rows) {
        DORIS_CHECK(storage_map != nullptr);
        DORIS_CHECK_EQ(storage_map->size(), rows);
        source = storage_map;
        paths = check_and_get_column<ColumnString>(&storage_map->get_keys());
        values = check_and_get_column<ColumnString>(&storage_map->get_values());
        DORIS_CHECK(paths != nullptr);
        DORIS_CHECK(values != nullptr);
        DORIS_CHECK_EQ(paths->size(), values->size());
        DCHECK(rows == 0 || storage_map->offset_at(static_cast<ssize_t>(rows)) == paths->size());
    }

    size_t begin(size_t row) const noexcept { return source->offset_at(static_cast<ssize_t>(row)); }
    size_t row_end(size_t row) const noexcept {
        return begin(row) + source->size_at(static_cast<ssize_t>(row));
    }
    bool row_empty(size_t row) const noexcept {
        return source->size_at(static_cast<ssize_t>(row)) == 0;
    }

    Status start_row(size_t row_index, StringRef requested_raw, bool reads_whole_variant_value) {
        index = begin(row_index);
        end = row_end(row_index);
        requested = requested_raw;
        reads_whole_variant = reads_whole_variant_value;
        available = false;
        if (!reads_whole_variant) {
            index = find_variant_sparse_path_lower_bound(requested_raw, *paths, index, end);
        }
        return advance();
    }

    Status advance() {
        available = false;
        while (index < end) {
            const StringRef path = paths->get_data_at(index);
            const StringRef value = values->get_data_at(index);
            ++index;

            is_direct_subtree_value = false;
            sort_key = path;
            if (!reads_whole_variant) {
                if (path.size < requested.size ||
                    (requested.size != 0 &&
                     std::memcmp(path.data, requested.data, requested.size) != 0)) {
                    index = end;
                    return Status::OK();
                }
                if (path.size == requested.size) {
                    is_direct_subtree_value = true;
                    sort_key = {};
                } else {
                    const auto next = static_cast<unsigned char>(path.data[requested.size]);
                    if (next < static_cast<unsigned char>('.')) {
                        continue;
                    }
                    if (next > static_cast<unsigned char>('.')) {
                        index = end;
                        return Status::OK();
                    }
                    sort_key = {path.data + requested.size + 1, path.size - requested.size - 1};
                }
            }

            depth = is_direct_subtree_value ? 0 : 1;
            if (!is_direct_subtree_value) {
                for (size_t offset = 0; offset < sort_key.size; ++offset) {
                    depth += sort_key.data[offset] == '.';
                }
            }
            if (depth > VARIANT_MAX_NESTING_DEPTH) {
                return Status::Corruption("Variant sparse/doc path exceeds maximum depth {}",
                                          VARIANT_MAX_NESTING_DEPTH);
            }
            cell = value;
            available = true;
            return Status::OK();
        }
        return Status::OK();
    }
};

// Emits ordered dotted paths into one VariantBatchBuilder row. The previous borrowed raw path and
// open object scopes are sufficient to find the component LCP; components are scanned only while
// they are emitted and are never materialized into a separate parts container.
struct ObjectEmitter {
    using ObjectScope = VariantBatchBuilder::Row::ObjectScope;

    VariantBatchBuilder::Row* row = nullptr;
    bool emitted = false;
    bool previous_is_direct_subtree_value = false;
    uint32_t previous_depth = 0;
    StringRef previous_path;
    std::vector<ObjectScope> scopes;

    ObjectEmitter() { scopes.reserve(8); }

    static size_t component_end(StringRef path, size_t offset) {
        DCHECK_LE(offset, path.size);
        if (offset == path.size) {
            return path.size;
        }
        const char* path_data = path.data == nullptr ? "" : path.data;
        const char* dot =
                static_cast<const char*>(std::memchr(path_data + offset, '.', path.size - offset));
        return dot == nullptr ? path.size : static_cast<size_t>(dot - path_data);
    }

    size_t common_prefix_depth(StringRef path, uint32_t depth) const {
        if (!emitted) {
            return 0;
        }
        DCHECK(!previous_is_direct_subtree_value);
        const char* previous_data = previous_path.data == nullptr ? "" : previous_path.data;
        const char* current_data = path.data == nullptr ? "" : path.data;
        size_t previous_offset = 0;
        size_t current_offset = 0;
        size_t common_depth = 0;
        while (previous_offset <= previous_path.size && current_offset <= path.size) {
            const size_t previous_end = component_end(previous_path, previous_offset);
            const size_t current_end = component_end(path, current_offset);
            const size_t previous_size = previous_end - previous_offset;
            const size_t current_size = current_end - current_offset;
            if (previous_size != current_size ||
                (current_size != 0 &&
                 std::memcmp(previous_data + previous_offset, current_data + current_offset,
                             current_size) != 0)) {
                break;
            }
            ++common_depth;
            if (previous_end == previous_path.size || current_end == path.size) {
                break;
            }
            previous_offset = previous_end + 1;
            current_offset = current_end + 1;
        }
        DCHECK(common_depth != previous_depth || previous_depth >= depth);
        return common_depth;
    }

    void open_suffix(StringRef path, uint32_t depth, size_t common_depth) {
        const char* path_data = path.data == nullptr ? "" : path.data;
        size_t offset = 0;
        for (size_t part = 0; part < depth; ++part) {
            const size_t end = component_end(path, offset);
            if (part >= common_depth) {
                scopes.back().add_key({path_data + offset, end - offset});
                if (part + 1 < depth) {
                    scopes.push_back(row->start_object());
                }
            }
            offset = end + 1;
        }
    }

    void start_row(VariantBatchBuilder::Row* output) {
        row = output;
        previous_path = {};
        previous_is_direct_subtree_value = false;
        previous_depth = 0;
        scopes.clear();
        emitted = false;
    }

    void prepare(StringRef path, bool is_direct_subtree_value, uint32_t depth) {
        if (is_direct_subtree_value) {
            DCHECK(!emitted);
            emitted = true;
            previous_is_direct_subtree_value = true;
            return;
        }

        const size_t common_depth = common_prefix_depth(path, depth);
        if (!emitted) {
            scopes.push_back(row->start_object());
        }
        while (scopes.size() > common_depth + 1) {
            scopes.back().finish();
            scopes.pop_back();
        }
        open_suffix(path, depth, common_depth);
        previous_path = path;
        previous_is_direct_subtree_value = false;
        previous_depth = depth;
        emitted = true;
    }

    Status append_materialized(StringRef path, bool is_direct_subtree_value, uint32_t depth,
                               const variant_assembler_detail::PreparedMaterializedColumn& column,
                               size_t row_index) {
        prepare(path, is_direct_subtree_value, depth);
        return variant_assembler_detail::append_materialized_value(column, row_index, *row, depth);
    }

    // prepare() mutates the emitter state even though all storage is pointer-owned.
    // NOLINTNEXTLINE(readability-make-member-function-const)
    Status append_cell(StringRef path, bool is_direct_subtree_value, uint32_t depth,
                       StringRef value) {
        prepare(path, is_direct_subtree_value, depth);
        return append_v1_storage_cell(value, *row, depth);
    }

    void append_value(StringRef path, bool is_direct_subtree_value, uint32_t depth,
                      VariantRef value) {
        prepare(path, is_direct_subtree_value, depth);
        row->add_value(value);
    }

    // Keep the empty ancestor object left by removing one exact scalar leaf. The prefix remains
    // open so adjacent selected/non-selected siblings continue through the normal LCP path.
    void erase_leaf(StringRef path, bool is_direct_subtree_value, uint32_t depth) {
        DORIS_CHECK(!is_direct_subtree_value);
        DORIS_CHECK_GT(depth, 0);
        if (depth == 1) {
            return;
        }
        size_t prefix_size = path.size;
        while (prefix_size != 0 && path.data[prefix_size - 1] != '.') {
            --prefix_size;
        }
        DORIS_CHECK_GT(prefix_size, 0);
        --prefix_size;
        const uint32_t prefix_depth = depth - 1;
        prepare({path.data, prefix_size}, false, prefix_depth);
        if (scopes.size() == prefix_depth) {
            scopes.push_back(row->start_object());
        }
        DORIS_CHECK_EQ(scopes.size(), prefix_depth + 1);
        // The removed leaf remains the lexical frontier even though only its ancestor scopes are
        // emitted. Compare the next path with the full leaf so a residual sibling is legal.
        previous_path = path;
        previous_depth = depth;
    }

    void finish_row_object() {
        if (!emitted) {
            auto object = row->start_object();
            object.finish();
            return;
        }
        while (!scopes.empty()) {
            scopes.back().finish();
            scopes.pop_back();
        }
    }
};

struct PreparedHierarchicalBatch {
    const ColumnString* root_values = nullptr;
    const uint8_t* root_nulls = nullptr;
    DorisVector<variant_assembler_detail::PreparedMaterializedColumn> materialized;
};

bool has_materialized_value(
        std::span<const variant_assembler_detail::PreparedMaterializedColumn> materialized,
        std::span<const MaterializedSlot> materialized_slots, size_t row,
        bool reads_whole_variant) {
    DORIS_CHECK_EQ(materialized.size(), materialized_slots.size());
    for (size_t index = 0; index < materialized.size(); ++index) {
        if (variant_assembler_detail::is_materialized_value_visible(
                    materialized[index], row,
                    reads_whole_variant && materialized_slots[index].relative_path.empty())) {
            return true;
        }
    }
    return false;
}

PreparedHierarchicalBatch prepare_hierarchical_batch(
        StorageMapKind storage_map_kind, bool has_root,
        std::span<const MaterializedSlot> materialized_slots, VariantAssemblerBatch& batch,
        StorageMapRowCursor* map_cursor) {
    const bool owns_materialized = !batch.owned_materialized_columns.empty();
    DORIS_CHECK(owns_materialized ? batch.materialized_columns.empty()
                                  : batch.owned_materialized_columns.empty());
    DORIS_CHECK_EQ(owns_materialized ? batch.owned_materialized_columns.size()
                                     : batch.materialized_columns.size(),
                   materialized_slots.size());
    DORIS_CHECK_EQ(batch.storage_map != nullptr, storage_map_kind != StorageMapKind::NONE);

    PreparedHierarchicalBatch output;
    if (has_root) {
        DORIS_CHECK(batch.root_jsonb != nullptr);
        DORIS_CHECK_EQ(batch.root_jsonb->size(), batch.num_rows);
        const IColumn* root_values = batch.root_jsonb;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(root_values)) {
            output.root_nulls = nullable->get_null_map_data().data();
            root_values = &nullable->get_nested_column();
        }
        output.root_values = check_and_get_column<ColumnString>(root_values);
        DORIS_CHECK(output.root_values != nullptr);
    } else {
        DORIS_CHECK(batch.root_jsonb == nullptr);
    }

    output.materialized.reserve(materialized_slots.size());
    for (const MaterializedSlot& slot : materialized_slots) {
        const IColumn* column = owns_materialized
                                        ? batch.owned_materialized_columns[slot.batch_index].get()
                                        : batch.materialized_columns[slot.batch_index];
        output.materialized.push_back(variant_assembler_detail::prepare_materialized_column(
                slot.type, column, batch.num_rows));
    }
    if (owns_materialized) {
        for (size_t index = 0; index < materialized_slots.size(); ++index) {
            const size_t batch_index = materialized_slots[index].batch_index;
            output.materialized[index].owner =
                    std::move(batch.owned_materialized_columns[batch_index]);
        }
    }
    if (storage_map_kind != StorageMapKind::NONE) {
        map_cursor->bind(batch.storage_map, batch.num_rows);
    }
    return output;
}

struct MergeValue {
    StringRef raw_path;
    const MaterializedSlot* materialized_slot = nullptr;
    const variant_assembler_detail::PreparedMaterializedColumn* materialized = nullptr;
    StringRef cell;
    bool is_direct_subtree_value = false;
    uint32_t depth = 0;

    void set_materialized(const MaterializedSlot& slot, StringRef raw,
                          const variant_assembler_detail::PreparedMaterializedColumn* column) {
        raw_path = raw;
        materialized_slot = &slot;
        materialized = column;
        cell = {};
        is_direct_subtree_value = slot.relative_path.empty();
        DORIS_CHECK_LE(slot.relative_path.get_parts().size(), VARIANT_MAX_NESTING_DEPTH);
        depth = static_cast<uint32_t>(slot.relative_path.get_parts().size());
    }

    void set_cell(StringRef raw, StringRef value, bool is_direct_value, uint32_t cell_depth) {
        raw_path = raw;
        materialized_slot = nullptr;
        materialized = nullptr;
        cell = value;
        is_direct_subtree_value = is_direct_value;
        depth = cell_depth;
    }
};

Status append_merge_value(const MergeValue& value, size_t row, ObjectEmitter* emitter) {
    if (value.materialized != nullptr) {
        return emitter->append_materialized(value.raw_path, value.is_direct_subtree_value,
                                            value.depth, *value.materialized, row);
    }
    return emitter->append_cell(value.raw_path, value.is_direct_subtree_value, value.depth,
                                value.cell);
}

Status append_shredded_merge_value(const MergeValue& value, size_t row,
                                   VariantShreddedColumnBuilder::Batch* shredded_batch,
                                   ObjectEmitter* emitter, bool* routed) {
    DORIS_CHECK(shredded_batch != nullptr);
    DORIS_CHECK(routed != nullptr);
    *routed = false;
    const std::optional<size_t> path_index =
            value.materialized_slot != nullptr
                    ? value.materialized_slot->shredded_path_index
                    : shredded_batch->find_raw_path(value.raw_path, value.depth);
    if (!path_index.has_value()) {
        return Status::OK();
    }

    if (value.materialized != nullptr &&
        is_supported_variant_typed_identity(value.materialized->primitive)) {
        shredded_batch->append_materialized(*path_index, row);
        emitter->erase_leaf(value.raw_path, value.is_direct_subtree_value, value.depth);
        *routed = true;
        return Status::OK();
    }

    // Structural cells must remain in the residual. Decode an exact candidate once into row-local
    // scratch so both the scalar decision and residual append consume the same validated value.
    VariantBatchBuilder scratch({.rows = 1});
    auto scratch_row = scratch.begin_row();
    if (value.materialized != nullptr) {
        RETURN_IF_ERROR(variant_assembler_detail::append_materialized_value(
                *value.materialized, row, scratch_row, value.depth));
    } else {
        RETURN_IF_ERROR(append_v1_storage_cell(value.cell, scratch_row, value.depth));
    }
    scratch_row.finish();
    VariantBatchBuilder decoded = scratch.finish_batch();
    const VariantRef decoded_value = decoded.value_at(0);
    if (shredded_batch->append_value(*path_index, decoded_value)) {
        emitter->erase_leaf(value.raw_path, value.is_direct_subtree_value, value.depth);
    } else {
        emitter->append_value(value.raw_path, value.is_direct_subtree_value, value.depth,
                              decoded_value);
    }
    *routed = true;
    return Status::OK();
}

// A raw key such as "a-" sorts between "a" and "a.b". The ancestor therefore cannot be emitted
// until the merge reaches its dotted descendant range or passes it. This byte comparison retains
// that ordering rule without allocating PathInData parts in the row loop.
int compare_with_raw_descendant_start(StringRef value, StringRef ancestor) noexcept {
    const size_t common = std::min(value.size, ancestor.size);
    const int comparison = common == 0 ? 0 : std::memcmp(value.data, ancestor.data, common);
    if (comparison != 0) {
        return comparison;
    }
    if (value.size <= ancestor.size) {
        return -1;
    }
    const auto next = static_cast<unsigned char>(value.data[ancestor.size]);
    if (next < static_cast<unsigned char>('.')) {
        return -1;
    }
    if (next > static_cast<unsigned char>('.')) {
        return 1;
    }
    return 0;
}

Status append_visible_merge_values(const DorisVector<MergeValue>& values, size_t row,
                                   VariantShreddedColumnBuilder::Batch* shredded_batch,
                                   ObjectEmitter* emitter) {
    const auto end = values.end();
    for (auto current = values.begin(); current != end; ++current) {
        bool has_descendant = false;
        if (current->is_direct_subtree_value) {
            has_descendant = std::find_if(current + 1, end, [](const MergeValue& candidate) {
                                 return !candidate.is_direct_subtree_value;
                             }) != end;
        } else {
            const auto first_possible_descendant =
                    std::lower_bound(current + 1, end, *current,
                                     [](const MergeValue& candidate, const MergeValue& ancestor) {
                                         return compare_with_raw_descendant_start(
                                                        candidate.raw_path, ancestor.raw_path) < 0;
                                     });
            has_descendant = first_possible_descendant != end &&
                             compare_with_raw_descendant_start(first_possible_descendant->raw_path,
                                                               current->raw_path) == 0;
        }
        if (!has_descendant) {
            if (shredded_batch != nullptr) {
                bool routed = false;
                RETURN_IF_ERROR(append_shredded_merge_value(*current, row, shredded_batch, emitter,
                                                            &routed));
                if (routed) {
                    continue;
                }
            }
            RETURN_IF_ERROR(append_merge_value(*current, row, emitter));
        }
    }
    return Status::OK();
}

Status emit_doc_row(size_t row, StringRef requested_raw, bool reads_whole_variant,
                    StorageMapRowCursor* cursor, DorisVector<MergeValue>* pending,
                    VariantShreddedColumnBuilder::Batch* shredded_batch, ObjectEmitter* emitter) {
    RETURN_IF_ERROR(cursor->start_row(row, requested_raw, reads_whole_variant));
    pending->clear();
    while (cursor->available) {
        pending->emplace_back();
        pending->back().set_cell(cursor->sort_key, cursor->cell, cursor->is_direct_subtree_value,
                                 cursor->depth);
        RETURN_IF_ERROR(cursor->advance());
    }
    return append_visible_merge_values(*pending, row, shredded_batch, emitter);
}

Status emit_merged_row(
        std::span<const variant_assembler_detail::PreparedMaterializedColumn> materialized,
        bool has_sparse, std::span<const MaterializedSlot> materialized_slots, size_t row,
        StringRef requested_raw, bool reads_whole_variant, StorageMapRowCursor* sparse_cursor,
        DorisVector<MergeValue>* pending, VariantShreddedColumnBuilder::Batch* shredded_batch,
        ObjectEmitter* emitter) {
    if (has_sparse) {
        RETURN_IF_ERROR(sparse_cursor->start_row(row, requested_raw, reads_whole_variant));
    }
    pending->clear();
    size_t materialized_index = 0;
    while (true) {
        while (materialized_index < materialized.size()) {
            if (variant_assembler_detail::is_materialized_value_visible(
                        materialized[materialized_index], row,
                        reads_whole_variant &&
                                materialized_slots[materialized_index].relative_path.empty())) {
                break;
            }
            ++materialized_index;
        }
        const bool materialized_available = materialized_index < materialized_slots.size();
        StringRef materialized_raw_path;
        if (materialized_available) {
            const std::string& path =
                    materialized_slots[materialized_index].relative_path.get_path();
            materialized_raw_path = {path.data(), path.size()};
        }
        const bool materialized_is_root =
                materialized_available &&
                materialized_slots[materialized_index].relative_path.empty();
        const int path_comparison =
                !has_sparse || !sparse_cursor->available || !materialized_available
                        ? 0
                        : sparse_cursor->sort_key.compare(materialized_raw_path);
        const bool same_logical_path =
                has_sparse && sparse_cursor->available && materialized_available &&
                path_comparison == 0 &&
                sparse_cursor->depth ==
                        materialized_slots[materialized_index].relative_path.get_parts().size() &&
                sparse_cursor->is_direct_subtree_value == materialized_is_root;
        if (same_logical_path) {
            const StringRef conflict_path =
                    materialized_is_root ? requested_raw : materialized_raw_path;
            return Status::Corruption(
                    "Variant path '{}' is present in both materialized and sparse streams at row "
                    "{}",
                    conflict_path.to_string(), row);
        }
        const bool use_sparse = has_sparse && sparse_cursor->available &&
                                (!materialized_available || path_comparison < 0 ||
                                 (path_comparison == 0 && sparse_cursor->is_direct_subtree_value &&
                                  !materialized_is_root));
        if (!materialized_available && !use_sparse) {
            return append_visible_merge_values(*pending, row, shredded_batch, emitter);
        }
        pending->emplace_back();
        MergeValue& current = pending->back();
        if (!use_sparse) {
            current.set_materialized(materialized_slots[materialized_index], materialized_raw_path,
                                     &materialized[materialized_index]);
            ++materialized_index;
        } else {
            current.set_cell(sparse_cursor->sort_key, sparse_cursor->cell,
                             sparse_cursor->is_direct_subtree_value, sparse_cursor->depth);
            RETURN_IF_ERROR(sparse_cursor->advance());
        }
    }
}

Status assemble_hierarchical_row(StorageMapKind storage_map_kind, bool has_root,
                                 const PathInData& requested,
                                 std::span<const MaterializedSlot> materialized_slots,
                                 const PreparedHierarchicalBatch& batch, size_t row_index,
                                 VariantBatchBuilder::Row* row, StorageMapRowCursor* map_cursor,
                                 DorisVector<MergeValue>* pending, ObjectEmitter* emitter,
                                 VariantShreddedColumnBuilder::Batch* shredded_batch,
                                 bool* is_outer_null) {
    const bool has_sparse = storage_map_kind == StorageMapKind::SPARSE;
    const bool has_doc = storage_map_kind == StorageMapKind::DOC;
    const bool has_root_sidecar_value =
            has_root && (batch.root_nulls == nullptr || batch.root_nulls[row_index] == 0) &&
            batch.root_values->get_data_at(row_index).size != 0;
    const bool has_doc_row = has_doc && !map_cursor->row_empty(row_index);
    const bool reads_whole_variant = requested.empty();

    // Physical row precedence:
    // 1. A non-empty doc row exclusively defines the row.
    // 2. Visible materialized/sparse values define the row and suppress the legacy root sidecar.
    // 3. Otherwise a present root sidecar is the complete value.
    // 4. An empty whole-Variant row is {}; an absent subtree is SQL NULL.
    if (has_root_sidecar_value && !has_doc_row &&
        (!has_sparse || map_cursor->row_empty(row_index))) {
        // Only the root fast-path candidate needs this O(materialized-paths) scan. Ordinary object
        // and doc rows are scanned once later by emit_merged_row()/emit_doc_row().
        if (!has_materialized_value(batch.materialized, materialized_slots, row_index,
                                    reads_whole_variant)) {
            if (shredded_batch == nullptr) {
                jsonb_to_variant(batch.root_values->get_data_at(row_index), *row, 0, nullptr);
            } else {
                VariantBatchBuilder scratch({.rows = 1});
                auto scratch_row = scratch.begin_row();
                jsonb_to_variant(batch.root_values->get_data_at(row_index), scratch_row, 0,
                                 nullptr);
                scratch_row.finish();
                VariantBatchBuilder decoded = scratch.finish_batch();
                shredded_batch->append_root(decoded.value_at(0), *row, true);
            }
            return Status::OK();
        }
    }

    const std::string& requested_path = requested.get_path();
    const StringRef requested_raw {requested_path.data(), requested_path.size()};
    emitter->start_row(row);
    if (has_doc_row) {
        RETURN_IF_ERROR(emit_doc_row(row_index, requested_raw, reads_whole_variant, map_cursor,
                                     pending, shredded_batch, emitter));
    } else {
        RETURN_IF_ERROR(emit_merged_row(batch.materialized, has_sparse, materialized_slots,
                                        row_index, requested_raw, reads_whole_variant, map_cursor,
                                        pending, shredded_batch, emitter));
    }
    if (!emitter->emitted) {
        if (reads_whole_variant &&
            (storage_map_kind != StorageMapKind::NONE || !materialized_slots.empty())) {
            // In hierarchical root storage, an outer-non-null row with no emitted paths is the
            // empty object. Missing subtrees have no such root-object semantics.
            emitter->finish_row_object();
            return Status::OK();
        }
        row->add_null();
        *is_outer_null = true;
        return Status::OK();
    }
    emitter->finish_row_object();
    return Status::OK();
}

Status assemble_hierarchical(StorageMapKind storage_map_kind, bool has_root,
                             const PathInData& requested,
                             std::span<const MaterializedSlot> materialized_slots,
                             VariantAssemblerBatch& batch,
                             const VariantShreddedColumnBuilder* shredded_builder,
                             ColumnNullable::MutablePtr* output) {
    StorageMapRowCursor map_cursor;
    PreparedHierarchicalBatch prepared = prepare_hierarchical_batch(
            storage_map_kind, has_root, materialized_slots, batch, &map_cursor);
    VariantBatchBuilder builder(
            {.rows = batch.num_rows, .metadata_keys = materialized_slots.size() + 8});
    auto outer = ColumnUInt8::create();
    outer->reserve(batch.num_rows);
    std::optional<VariantShreddedColumnBuilder::Batch> shredded_batch;
    if (shredded_builder != nullptr) {
        shredded_batch.emplace(shredded_builder->begin_batch(batch.num_rows));
        for (size_t index = 0; index < materialized_slots.size(); ++index) {
            const auto& path_index = materialized_slots[index].shredded_path_index;
            if (!path_index.has_value()) {
                continue;
            }
            auto& materialized = prepared.materialized[index];
            shredded_batch->bind_materialized_source(*path_index, *materialized.data,
                                                     materialized.nulls,
                                                     std::move(materialized.owner));
        }
    }
    VariantShreddedColumnBuilder::Batch* shredded_batch_ptr =
            shredded_batch.has_value() ? &shredded_batch.value() : nullptr;
    DorisVector<MergeValue> pending;
    ObjectEmitter emitter;
    for (size_t row_index = 0; row_index < batch.num_rows; ++row_index) {
        auto row = builder.begin_row();
        if (prepared.root_nulls != nullptr && prepared.root_nulls[row_index] != 0) {
            outer->insert_value(1);
            row.add_null();
            row.finish();
            if (shredded_batch_ptr != nullptr) {
                shredded_batch_ptr->finish_row();
            }
            continue;
        }
        bool is_outer_null = false;
        RETURN_IF_ERROR(assemble_hierarchical_row(
                storage_map_kind, has_root, requested, materialized_slots, prepared, row_index,
                &row, &map_cursor, &pending, &emitter, shredded_batch_ptr, &is_outer_null));
        outer->insert_value(is_outer_null ? 1 : 0);
        row.finish();
        if (shredded_batch_ptr != nullptr) {
            shredded_batch_ptr->finish_row();
        }
    }
    publish_assembled(&builder, shredded_batch_ptr, std::move(outer), output);
    return Status::OK();
}

Status check_options(const VariantAssemblerOptions& options) {
    if (options.requested_path.has_nested_part()) {
        return Status::NotSupported(
                "ColumnVariantV2 does not support assembling nested array path '{}'",
                options.requested_path.get_path());
    }
    const auto validate_paths = [&](const DorisVector<MaterializedPath>& paths,
                                    std::string_view kind) -> Status {
        for (const auto& path : paths) {
            if (path.type == nullptr) {
                return Status::InvalidArgument("Variant {} path '{}' has no type", kind,
                                               path.path.get_path());
            }
            if (path.path.has_nested_part()) {
                return Status::NotSupported(
                        "ColumnVariantV2 does not support assembling nested array path '{}'",
                        path.path.get_path());
            }
            const auto& requested_parts = options.requested_path.get_parts();
            const auto& path_parts = path.path.get_parts();
            if (requested_parts.size() > path_parts.size() ||
                !std::equal(requested_parts.begin(), requested_parts.end(), path_parts.begin())) {
                return Status::InvalidArgument(
                        "Variant {} path '{}' is outside requested path '{}'", kind,
                        path.path.get_path(), options.requested_path.get_path());
            }
        }
        return Status::OK();
    };
    RETURN_IF_ERROR(validate_paths(options.materialized_paths, "materialized"));

    const auto materialized = sorted_path_refs(options.materialized_paths);
    const auto check_duplicates = [](const DorisVector<const MaterializedPath*>& sorted,
                                     std::string_view kind) -> Status {
        for (size_t index = 1; index < sorted.size(); ++index) {
            if (same_path(sorted[index - 1]->path, sorted[index]->path)) {
                return Status::InvalidArgument("Duplicate Variant {} path '{}'", kind,
                                               sorted[index]->path.get_path());
            }
        }
        return Status::OK();
    };
    RETURN_IF_ERROR(check_duplicates(materialized, "materialized"));
    DORIS_CHECK(options.requested_path.empty() || !options.has_root);
    return Status::OK();
}

// Normalize the materialized streams once when the iterator is created. The relative paths are
// sorted in the order consumed by the row merge. Each slot carries its original batch position.
DorisVector<MaterializedSlot> build_materialized_slots(const VariantAssemblerOptions& options) {
    DorisVector<MaterializedSlot> slots;
    slots.reserve(options.materialized_paths.size());
    for (size_t source_index = 0; source_index < options.materialized_paths.size();
         ++source_index) {
        const auto& source = options.materialized_paths[source_index];
        // Legacy ColumnVariant segments may materialize a scalar/array root at the empty path.
        // The ordered row merge retains it when it is the only value and drops it when descendants
        // from another physical stream form the visible object.
        DORIS_CHECK(source.type != nullptr);
        DORIS_CHECK_LE(options.requested_path.get_parts().size(), source.path.get_parts().size());
        for (size_t part = 0; part < options.requested_path.get_parts().size(); ++part) {
            DORIS_CHECK_EQ(options.requested_path.get_parts()[part].key,
                           source.path.get_parts()[part].key);
        }
        slots.push_back({.relative_path = source.path.copy_pop_nfront(
                                 options.requested_path.get_parts().size()),
                         .type = source.type,
                         .batch_index = source_index,
                         .shredded_path_index = std::nullopt});
    }
    std::ranges::sort(slots, [](const MaterializedSlot& left, const MaterializedSlot& right) {
        const std::string& left_raw = left.relative_path.get_path();
        const std::string& right_raw = right.relative_path.get_path();
        if (left_raw != right_raw) {
            return left_raw < right_raw;
        }
        // PathInData() (logical root) and PathInData("") (empty object key) share the same raw
        // bytes. The root must precede the empty key so the normal ancestor rule remains stable.
        return left.relative_path.empty() && !right.relative_path.empty();
    });

    for (const MaterializedSlot& slot : slots) {
        DORIS_CHECK_LE(slot.relative_path.get_parts().size(), VARIANT_MAX_NESTING_DEPTH);
    }
    return slots;
}

std::unique_ptr<VariantShreddedColumnBuilder> build_shredded_builder(
        const PathInData& requested, std::span<MaterializedSlot> materialized,
        size_t layout_limit) {
    if (!requested.empty()) {
        return nullptr;
    }

    const auto eligible_path = [](const PathInData& path) {
        return !path.empty() &&
               std::ranges::none_of(path.get_parts(), [](const PathInData::Part& part) {
                   return part.is_nested || part.anonymous_array_level != 0 ||
                          part.key.find('.') != std::string_view::npos;
               });
    };

    DorisVector<VariantShreddedLayoutEntry> candidates;
    candidates.reserve(materialized.size());
    for (const MaterializedSlot& slot : materialized) {
        const DataTypePtr scalar_type = remove_nullable(slot.type);
        if (eligible_path(slot.relative_path) &&
            is_supported_variant_typed_identity(scalar_type->get_primitive_type())) {
            candidates.push_back({.path = slot.relative_path, .scalar_type = scalar_type});
        }
    }
    std::ranges::sort(candidates, [](const auto& left, const auto& right) {
        return variant_shredded_path_less(left.path, right.path);
    });

    DorisVector<VariantShreddedLayoutEntry> layout;
    layout.reserve(std::min(candidates.size(), layout_limit));
    for (size_t index = 0; index < candidates.size(); ++index) {
        // Lexicographic PathInData order places the first descendant immediately after its
        // ancestor, so prefix filtering is linear after the canonical sort.
        if (index + 1 < candidates.size() &&
            candidates[index].path.get_parts().size() <
                    candidates[index + 1].path.get_parts().size() &&
            variant_shredded_path_is_prefix(candidates[index].path, candidates[index + 1].path)) {
            continue;
        }
        layout.push_back(std::move(candidates[index]));
        if (layout.size() == layout_limit) {
            break;
        }
    }
    if (layout.empty()) {
        return nullptr;
    }
    auto builder = std::make_unique<VariantShreddedColumnBuilder>(std::move(layout));
    const auto& sorted_layout = builder->layout();
    for (MaterializedSlot& slot : materialized) {
        const auto candidate = std::lower_bound(
                sorted_layout.begin(), sorted_layout.end(), slot.relative_path,
                [](const VariantShreddedLayoutEntry& entry, const PathInData& needle) {
                    return variant_shredded_path_less(entry.path, needle);
                });
        if (candidate != sorted_layout.end() &&
            candidate->path.get_parts() == slot.relative_path.get_parts()) {
            slot.shredded_path_index = static_cast<size_t>(candidate - sorted_layout.begin());
        }
    }
    return builder;
}

} // namespace

Result<std::unique_ptr<VariantAssembler>> VariantAssembler::create(
        VariantAssemblerOptions options) {
    RETURN_IF_ERROR_RESULT(check_options(options));
    DorisVector<MaterializedSlot> materialized = build_materialized_slots(options);
    std::unique_ptr<VariantShreddedColumnBuilder> shredded_builder;
    // A DOC row is the complete authoritative value. Footer/materialized path metadata must not
    // change its in-memory representation from E to S.
    if (options.storage_map_kind != StorageMapKind::DOC) {
        const int32_t configured_layout_limit =
                config::variant_max_shredded_execution_initial_layout_paths;
        DORIS_CHECK_GT(configured_layout_limit, 0);
        shredded_builder = build_shredded_builder(options.requested_path, materialized,
                                                  static_cast<size_t>(configured_layout_limit));
    }
    return std::unique_ptr<VariantAssembler>(
            new VariantAssembler(options.storage_map_kind, options.has_root, options.requested_path,
                                 std::move(materialized), std::move(shredded_builder)));
}

VariantAssembler::VariantAssembler(
        StorageMapKind storage_map_kind, bool has_root, const PathInData& requested,
        DorisVector<variant_assembler_detail::MaterializedSlot> materialized,
        std::unique_ptr<VariantShreddedColumnBuilder> shredded_builder)
        : _storage_map_kind(storage_map_kind),
          _has_root(has_root),
          _requested(requested),
          _materialized(std::move(materialized)),
          _shredded_builder(std::move(shredded_builder)) {}

VariantAssembler::~VariantAssembler() = default;

#ifdef BE_TEST
size_t VariantAssembler::TestAccess::encoded_shredded_builds(const VariantAssembler& assembler) {
    return assembler._shredded_builder == nullptr
                   ? 0
                   : assembler._shredded_builder->test_encoded_source_builds();
}

size_t VariantAssembler::TestAccess::direct_shredded_builds(const VariantAssembler& assembler) {
    return assembler._shredded_builder == nullptr
                   ? 0
                   : assembler._shredded_builder->test_direct_batches();
}

#endif

Status VariantAssembler::assemble(VariantAssemblerBatch& batch,
                                  ColumnNullable::MutablePtr* output) const {
    DORIS_CHECK(output != nullptr);
    try {
        ColumnNullable::MutablePtr result;
        const Status status =
                assemble_hierarchical(_storage_map_kind, _has_root, _requested, _materialized,
                                      batch, _shredded_builder.get(), &result);
        if (!status.ok()) {
            return status;
        }
        *output = std::move(result);
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

} // namespace doris::segment_v2::variant_v2
