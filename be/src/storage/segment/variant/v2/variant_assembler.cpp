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
#include <string_view>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_column_utils.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "storage/segment/variant/v2/variant_assembler_internal.h"
#include "storage/segment/variant/v2/variant_storage_cell.h"

namespace doris::segment_v2::variant_v2 {
namespace {

using MaterializedSlot = variant_assembler_detail::MaterializedSlot;

// Assembly has four stages:
// 1. create() makes materialized paths relative to the requested subtree and sorts them once.
// 2. prepare_hierarchical_batch() unwraps concrete columns once per batch.
// 3. The row loop merges already ordered materialized and sparse/doc paths. A row-local cursor
//    exposes persisted cells, while ObjectEmitter owns the open object scopes.
// 4. publish_encoded() finishes the batch and transfers the completed values/null map atomically.
//
// StorageMapRowCursor and ObjectEmitter are deliberately local implementation state. Neither is a
// reusable reader abstraction: the former only advances one persisted map row, and the latter only
// translates this merge's ordered paths into VariantBatchBuilder calls.

void publish_encoded(VariantBatchBuilder* builder, ColumnUInt8::MutablePtr outer_nulls,
                     ColumnNullable::MutablePtr* output) {
    VariantBatchBuilder block = builder->finish_batch();
    auto values = ColumnVariantV2::create();
    values->insert_encoded_batch(block);
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
        if (!reads_whole_variant) {
            index = find_variant_sparse_path_lower_bound(requested_raw, *paths, index, end);
        }
        available = false;
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

bool can_assemble_flat_materialized(StorageMapKind storage_map_kind, const PathInData& requested,
                                    std::span<const MaterializedSlot> materialized_slots) {
    if (storage_map_kind != StorageMapKind::NONE || !requested.empty() ||
        materialized_slots.empty()) {
        return false;
    }
    for (size_t index = 0; index < materialized_slots.size(); ++index) {
        const MaterializedSlot& slot = materialized_slots[index];
        const auto& parts = slot.relative_path.get_parts();
        if (parts.size() != 1 || parts.front().key.find('.') != std::string_view::npos ||
            remove_nullable(slot.type)->get_primitive_type() == TYPE_ARRAY ||
            (index != 0 && slot.relative_path == materialized_slots[index - 1].relative_path)) {
            return false;
        }
    }
    return true;
}

bool has_only_empty_root_payload(const PreparedHierarchicalBatch& batch, size_t rows) {
    if (batch.root_values == nullptr) {
        return true;
    }
    for (size_t row = 0; row < rows; ++row) {
        if ((batch.root_nulls == nullptr || batch.root_nulls[row] == 0) &&
            batch.root_values->get_data_at(row).size != 0) {
            return false;
        }
    }
    return true;
}

template <typename Visitor>
void visit_visible_scalar_rows(const variant_assembler_detail::PreparedMaterializedColumn& column,
                               size_t rows, Visitor&& visitor) {
    DCHECK_NE(column.primitive, TYPE_ARRAY);
    if (column.nulls == nullptr) {
        for (size_t row = 0; row < rows; ++row) {
            visitor(row);
        }
        return;
    }
    const uint8_t* current = column.nulls;
    const uint8_t* end = current + rows;
    while (current != end) {
        const auto* visible = static_cast<const uint8_t*>(std::memchr(current, 0, end - current));
        if (visible == nullptr) {
            return;
        }
        visitor(static_cast<size_t>(visible - column.nulls));
        current = visible + 1;
    }
}

struct ActiveMaterializedRows {
    DorisVector<size_t> offsets;
    DorisVector<size_t> slots;
};

bool index_active_materialized_rows(const PreparedHierarchicalBatch& batch, size_t rows,
                                    ActiveMaterializedRows* result) {
    result->offsets.resize(rows + 1);
    for (const auto& column : batch.materialized) {
        visit_visible_scalar_rows(column, rows, [&](size_t row) {
            if (batch.root_nulls == nullptr || batch.root_nulls[row] == 0) {
                ++result->offsets[row + 1];
            }
        });
    }
    for (size_t row = 0; row < rows; ++row) {
        result->offsets[row + 1] += result->offsets[row];
    }
    // The CSR index is intended only for sparse batches. At most one out of sixteen cells may
    // become an active slot, bounding both its memory and its second column scan.
    if (static_cast<unsigned __int128>(result->offsets.back()) * 16 >
        static_cast<unsigned __int128>(rows) * batch.materialized.size()) {
        return false;
    }
    result->slots.resize(result->offsets.back());
    DorisVector<size_t> positions = result->offsets;
    for (size_t slot = 0; slot < batch.materialized.size(); ++slot) {
        visit_visible_scalar_rows(batch.materialized[slot], rows, [&](size_t row) {
            if (batch.root_nulls == nullptr || batch.root_nulls[row] == 0) {
                result->slots[positions[row]++] = slot;
            }
        });
    }
    return true;
}

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
        std::span<const MaterializedSlot> materialized_slots,
        const VariantAssemblerBatchView& batch, StorageMapRowCursor* map_cursor) {
    DORIS_CHECK_EQ(batch.materialized_columns.size(), materialized_slots.size());
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
        output.materialized.push_back(variant_assembler_detail::prepare_materialized_column(
                slot.type, batch.materialized_columns[slot.batch_index], batch.num_rows));
    }
    if (storage_map_kind != StorageMapKind::NONE) {
        map_cursor->bind(batch.storage_map, batch.num_rows);
    }
    return output;
}

struct MergeValue {
    StringRef raw_path;
    const variant_assembler_detail::PreparedMaterializedColumn* materialized = nullptr;
    StringRef cell;
    bool is_direct_subtree_value = false;
    uint32_t depth = 0;

    void set_materialized(const PathInData& path, StringRef raw,
                          const variant_assembler_detail::PreparedMaterializedColumn* column) {
        raw_path = raw;
        materialized = column;
        cell = {};
        is_direct_subtree_value = path.empty();
        DORIS_CHECK_LE(path.get_parts().size(), VARIANT_MAX_NESTING_DEPTH);
        depth = static_cast<uint32_t>(path.get_parts().size());
    }

    void set_cell(StringRef raw, StringRef value, bool is_direct_value, uint32_t cell_depth) {
        raw_path = raw;
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
            RETURN_IF_ERROR(append_merge_value(*current, row, emitter));
        }
    }
    return Status::OK();
}

Status emit_doc_row(size_t row, StringRef requested_raw, bool reads_whole_variant,
                    StorageMapRowCursor* cursor, DorisVector<MergeValue>* pending,
                    ObjectEmitter* emitter) {
    RETURN_IF_ERROR(cursor->start_row(row, requested_raw, reads_whole_variant));
    pending->clear();
    while (cursor->available) {
        pending->emplace_back();
        pending->back().set_cell(cursor->sort_key, cursor->cell, cursor->is_direct_subtree_value,
                                 cursor->depth);
        RETURN_IF_ERROR(cursor->advance());
    }
    return append_visible_merge_values(*pending, row, emitter);
}

Status emit_merged_row(
        std::span<const variant_assembler_detail::PreparedMaterializedColumn> materialized,
        bool has_sparse, std::span<const MaterializedSlot> materialized_slots, size_t row,
        StringRef requested_raw, bool reads_whole_variant, StorageMapRowCursor* sparse_cursor,
        DorisVector<MergeValue>* pending, ObjectEmitter* emitter) {
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
        const bool use_sparse = has_sparse && sparse_cursor->available &&
                                (!materialized_available || path_comparison < 0 ||
                                 (path_comparison == 0 && sparse_cursor->is_direct_subtree_value &&
                                  !materialized_is_root));
        if (!materialized_available && !use_sparse) {
            return append_visible_merge_values(*pending, row, emitter);
        }
        pending->emplace_back();
        MergeValue& current = pending->back();
        if (!use_sparse) {
            current.set_materialized(materialized_slots[materialized_index].relative_path,
                                     materialized_raw_path, &materialized[materialized_index]);
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
            jsonb_to_variant(batch.root_values->get_data_at(row_index), *row, 0, nullptr);
            return Status::OK();
        }
    }

    const std::string& requested_path = requested.get_path();
    const StringRef requested_raw {requested_path.data(), requested_path.size()};
    emitter->start_row(row);
    if (has_doc_row) {
        RETURN_IF_ERROR(emit_doc_row(row_index, requested_raw, reads_whole_variant, map_cursor,
                                     pending, emitter));
    } else {
        RETURN_IF_ERROR(emit_merged_row(batch.materialized, has_sparse, materialized_slots,
                                        row_index, requested_raw, reads_whole_variant, map_cursor,
                                        pending, emitter));
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

Status assemble_flat_materialized(std::span<const MaterializedSlot> materialized_slots,
                                  const PreparedHierarchicalBatch& batch,
                                  const ActiveMaterializedRows& active, size_t rows,
                                  ColumnNullable::MutablePtr* output) {
    VariantBatchBuilder builder({.rows = rows, .metadata_keys = materialized_slots.size()});
    auto outer = ColumnUInt8::create();
    outer->reserve(rows);
    for (size_t row_index = 0; row_index < rows; ++row_index) {
        auto row = builder.begin_row();
        if (batch.root_nulls != nullptr && batch.root_nulls[row_index] != 0) {
            outer->insert_value(1);
            row.add_null();
            row.finish();
            continue;
        }
        auto object = row.start_object();
        for (size_t active_index = active.offsets[row_index];
             active_index < active.offsets[row_index + 1]; ++active_index) {
            const size_t slot_index = active.slots[active_index];
            const std::string& path = materialized_slots[slot_index].relative_path.get_path();
            object.add_key({path.data(), path.size()});
            RETURN_IF_ERROR(variant_assembler_detail::append_materialized_value(
                    batch.materialized[slot_index], row_index, row, 1));
        }
        object.finish();
        outer->insert_value(0);
        row.finish();
    }
    publish_encoded(&builder, std::move(outer), output);
    return Status::OK();
}

Status assemble_hierarchical(StorageMapKind storage_map_kind, bool has_root,
                             const PathInData& requested,
                             std::span<const MaterializedSlot> materialized_slots,
                             bool can_assemble_flat, const VariantAssemblerBatchView& batch,
                             ColumnNullable::MutablePtr* output) {
    StorageMapRowCursor map_cursor;
    PreparedHierarchicalBatch prepared = prepare_hierarchical_batch(
            storage_map_kind, has_root, materialized_slots, batch, &map_cursor);
    if (can_assemble_flat && has_only_empty_root_payload(prepared, batch.num_rows)) {
        ActiveMaterializedRows active;
        if (index_active_materialized_rows(prepared, batch.num_rows, &active)) {
            return assemble_flat_materialized(materialized_slots, prepared, active, batch.num_rows,
                                              output);
        }
    }
    VariantBatchBuilder builder(
            {.rows = batch.num_rows, .metadata_keys = materialized_slots.size() + 8});
    auto outer = ColumnUInt8::create();
    outer->reserve(batch.num_rows);
    DorisVector<MergeValue> pending;
    ObjectEmitter emitter;
    for (size_t row_index = 0; row_index < batch.num_rows; ++row_index) {
        auto row = builder.begin_row();
        if (prepared.root_nulls != nullptr && prepared.root_nulls[row_index] != 0) {
            outer->insert_value(1);
            row.add_null();
            row.finish();
            continue;
        }
        bool is_outer_null = false;
        RETURN_IF_ERROR(assemble_hierarchical_row(storage_map_kind, has_root, requested,
                                                  materialized_slots, prepared, row_index, &row,
                                                  &map_cursor, &pending, &emitter, &is_outer_null));
        outer->insert_value(is_outer_null ? 1 : 0);
        row.finish();
    }
    publish_encoded(&builder, std::move(outer), output);
    return Status::OK();
}

Status check_options(const VariantAssemblerOptions& options) {
    if (options.requested_path.has_nested_part()) {
        return Status::NotSupported(
                "ColumnVariantV2 does not support assembling nested array path '{}'",
                options.requested_path.get_path());
    }
    for (const auto& materialized : options.materialized_paths) {
        if (materialized.path.has_nested_part()) {
            return Status::NotSupported(
                    "ColumnVariantV2 does not support assembling nested array path '{}'",
                    materialized.path.get_path());
        }
    }
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
                         .batch_index = source_index});
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

} // namespace

Result<std::unique_ptr<VariantAssembler>> VariantAssembler::create(
        VariantAssemblerOptions options) {
    RETURN_IF_ERROR_RESULT(check_options(options));
    DorisVector<MaterializedSlot> materialized = build_materialized_slots(options);
    const bool can_assemble_flat = can_assemble_flat_materialized(
            options.storage_map_kind, options.requested_path, materialized);
    return std::unique_ptr<VariantAssembler>(
            new VariantAssembler(options.storage_map_kind, options.has_root, options.requested_path,
                                 std::move(materialized), can_assemble_flat));
}

VariantAssembler::VariantAssembler(
        StorageMapKind storage_map_kind, bool has_root, const PathInData& requested,
        DorisVector<variant_assembler_detail::MaterializedSlot> materialized,
        bool can_assemble_flat_materialized)
        : _storage_map_kind(storage_map_kind),
          _has_root(has_root),
          _requested(requested),
          _materialized(std::move(materialized)),
          _can_assemble_flat_materialized(can_assemble_flat_materialized) {}

Status VariantAssembler::assemble(const VariantAssemblerBatchView& batch,
                                  ColumnNullable::MutablePtr* output) const {
    DORIS_CHECK(output != nullptr);
    try {
        ColumnNullable::MutablePtr result;
        const Status status =
                assemble_hierarchical(_storage_map_kind, _has_root, _requested, _materialized,
                                      _can_assemble_flat_materialized, batch, &result);
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
