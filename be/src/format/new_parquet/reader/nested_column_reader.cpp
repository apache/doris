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

#include "format/new_parquet/reader/nested_column_reader.h"

#include <parquet/api/schema.h>

#include <cstdint>
#include <string_view>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "format/new_parquet/reader/arrow_leaf_reader_adapter.h"

namespace doris::parquet {

Status read_nested_scalar_batch(ScalarColumnReader& column_reader, int64_t batch_rows,
                                int16_t value_slot_definition_level, NestedScalarBatch* batch,
                                int16_t value_slot_repetition_level) {
    return read_nested_leaf_batch(column_reader.leaf_context(), batch_rows,
                                  value_slot_definition_level, batch, value_slot_repetition_level);
}

Status append_scalar_batch_value(const ScalarColumnReader& column_reader,
                                 const NestedScalarBatch& batch, int64_t level_idx,
                                 MutableColumnPtr& column) {
    const int64_t value_idx = batch.value_indices[level_idx];
    if (value_idx < 0) {
        return Status::Corruption("Nested parquet value is absent for column {}",
                                  column_reader.name());
    }
    auto* nullable_column = check_and_get_column<ColumnNullable>(*column);
    if (nullable_column != nullptr) {
        nullable_column->get_nested_column().insert_from(*batch.values_column,
                                                         static_cast<size_t>(value_idx));
        nullable_column->get_null_map_data().push_back(0);
        return Status::OK();
    }
    column->insert_from(*batch.values_column, static_cast<size_t>(value_idx));
    return Status::OK();
}

Status read_nested_scalar_batch_from_overflow(ScalarColumnReader& reader, int64_t batch_rows,
                                              int16_t value_slot_definition_level,
                                              NestedScalarOverflow* overflow,
                                              NestedScalarBatch* batch) {
    DORIS_CHECK(overflow != nullptr);
    DORIS_CHECK(batch != nullptr);
    if (!overflow->empty()) {
        *batch = std::move(overflow->batch);
        overflow->clear();
        return Status::OK();
    }
    return read_nested_scalar_batch(reader, batch_rows, value_slot_definition_level, batch);
}

Status read_nested_batch_from_overflow(ScalarColumnReader& reader, int64_t batch_rows,
                                       int16_t value_slot_definition_level,
                                       NestedScalarOverflow* overflow, NestedScalarBatch* batch) {
    return read_nested_scalar_batch_from_overflow(reader, batch_rows, value_slot_definition_level,
                                                  overflow, batch);
}

Status validate_nested_scalar_alignment(const std::string& column_name,
                                        const NestedScalarBatch& driver_batch,
                                        const NestedScalarBatch& candidate_batch,
                                        std::string_view candidate_name, std::string_view action) {
    return validate_nested_shape_alignment(column_name, driver_batch, candidate_batch,
                                           candidate_name, action);
}

Status validate_nested_struct_alignment(const std::string& column_name,
                                        const NestedScalarBatch& driver_batch,
                                        const NestedStructBatch& candidate_batch,
                                        std::string_view action) {
    return validate_nested_shape_alignment(column_name, driver_batch, candidate_batch, "value",
                                           action);
}

Status advance_non_scalar_struct_children(StructColumnReader& struct_reader, bool parent_is_null,
                                          std::vector<MutableColumnPtr>& child_columns) {
    for (size_t child_idx = 0; child_idx < struct_reader.child_count(); ++child_idx) {
        const int output_idx = struct_reader.child_output_index(child_idx);
        auto* child_reader = struct_reader.child_reader(child_idx);
        if (dynamic_cast<ScalarColumnReader*>(child_reader) != nullptr) {
            continue;
        }
        if (parent_is_null) {
            if (output_idx >= 0) {
                child_columns[output_idx]->insert_default();
            }
            RETURN_IF_ERROR(child_reader->skip(1));
            continue;
        }
        if (output_idx < 0) {
            RETURN_IF_ERROR(child_reader->skip(1));
            continue;
        }
        int64_t child_rows = 0;
        RETURN_IF_ERROR(child_reader->read(1, child_columns[output_idx], &child_rows));
        if (child_rows != 1) {
            return Status::Corruption(
                    "Parquet STRUCT child {} returned {} rows while reading one parent row for "
                    "column {}",
                    child_reader->name(), child_rows, struct_reader.name());
        }
    }
    return Status::OK();
}

Status read_nested_struct_batch(StructColumnReader& struct_reader, int64_t batch_rows,
                                int16_t value_slot_definition_level, NestedStructBatch* batch) {
    if (batch == nullptr) {
        return Status::InvalidArgument("Nested struct batch is null for column {}",
                                       struct_reader.name());
    }
    *batch = NestedStructBatch();

    std::vector<ScalarColumnReader*> scalar_children;
    scalar_children.reserve(struct_reader.child_count());
    std::vector<size_t> scalar_child_indices;
    scalar_child_indices.reserve(struct_reader.child_count());
    for (size_t child_idx = 0; child_idx < struct_reader.child_count(); ++child_idx) {
        auto* scalar_child =
                dynamic_cast<ScalarColumnReader*>(struct_reader.child_reader(child_idx));
        if (scalar_child == nullptr) {
            continue;
        }
        scalar_child_indices.push_back(child_idx);
        scalar_children.push_back(scalar_child);
    }
    if (scalar_children.empty()) {
        return Status::NotSupported("Parquet nested STRUCT column {} has no scalar children",
                                    struct_reader.name());
    }

    batch->scalar_child_indices = std::move(scalar_child_indices);
    batch->child_batches.resize(scalar_children.size());
    for (size_t child_idx = 0; child_idx < scalar_children.size(); ++child_idx) {
        RETURN_IF_ERROR(read_nested_scalar_batch(*scalar_children[child_idx], batch_rows,
                                                 value_slot_definition_level,
                                                 &batch->child_batches[child_idx]));
        const auto& child_batch = batch->child_batches[child_idx];
        if (child_idx == 0) {
            batch->records_read = child_batch.records_read;
            batch->levels_written = child_batch.levels_written;
            continue;
        }
        if (child_batch.records_read != batch->records_read ||
            child_batch.levels_written != batch->levels_written) {
            return Status::Corruption(
                    "Parquet STRUCT children returned different level counts in column {}",
                    struct_reader.name());
        }
        for (int64_t level_idx = 0; level_idx < batch->levels_written; ++level_idx) {
            if (child_batch.rep_levels[level_idx] !=
                batch->child_batches[0].rep_levels[level_idx]) {
                return Status::Corruption(
                        "Parquet STRUCT children returned different repetition levels in column {}",
                        struct_reader.name());
            }
        }
    }
    return Status::OK();
}

Status read_nested_struct_batch_from_overflow(StructColumnReader& reader, int64_t batch_rows,
                                              int16_t value_slot_definition_level,
                                              NestedStructOverflow* overflow,
                                              NestedStructBatch* batch) {
    DORIS_CHECK(overflow != nullptr);
    DORIS_CHECK(batch != nullptr);
    if (!overflow->empty()) {
        *batch = std::move(overflow->batch);
        overflow->clear();
        return Status::OK();
    }
    return read_nested_struct_batch(reader, batch_rows, value_slot_definition_level, batch);
}

Status read_nested_batch_from_overflow(StructColumnReader& reader, int64_t batch_rows,
                                       int16_t value_slot_definition_level,
                                       NestedStructOverflow* overflow, NestedStructBatch* batch) {
    return read_nested_struct_batch_from_overflow(reader, batch_rows, value_slot_definition_level,
                                                  overflow, batch);
}

Status append_struct_batch_value(StructColumnReader& struct_reader, const NestedStructBatch& batch,
                                 int64_t level_idx, MutableColumnPtr& column) {
    if (batch.child_batches.empty()) {
        return Status::Corruption("Parquet STRUCT column {} has no child batch",
                                  struct_reader.name());
    }

    auto* nullable_column = check_and_get_column<ColumnNullable>(*column);
    auto* struct_column =
            nullable_column == nullptr
                    ? assert_cast<ColumnStruct*>(column.get())
                    : assert_cast<ColumnStruct*>(&nullable_column->get_nested_column());
    auto* parent_null_map =
            nullable_column == nullptr ? nullptr : &nullable_column->get_null_map_data();
    DORIS_CHECK(struct_column != nullptr);

    DCHECK_EQ(batch.child_batches.size(), batch.scalar_child_indices.size());

    const bool parent_is_null = batch.child_batches[0].def_levels[level_idx] <
                                struct_reader.nullable_definition_level();
    for (size_t child_idx = 1; child_idx < batch.child_batches.size(); ++child_idx) {
        const bool child_parent_is_null = batch.child_batches[child_idx].def_levels[level_idx] <
                                          struct_reader.nullable_definition_level();
        if (child_parent_is_null != parent_is_null) {
            return Status::Corruption(
                    "Parquet STRUCT children returned different null parent shape in column {}",
                    struct_reader.name());
        }
    }

    std::vector<MutableColumnPtr> child_columns;
    child_columns.reserve(struct_column->get_columns().size());
    for (size_t child_idx = 0; child_idx < struct_column->get_columns().size(); ++child_idx) {
        child_columns.push_back(struct_column->get_column_ptr(child_idx)->assert_mutable());
    }

    if (parent_is_null) {
        if (parent_null_map == nullptr) {
            return Status::Corruption(
                    "Parquet STRUCT column {} contains null for non-nullable struct",
                    struct_reader.name());
        }
        parent_null_map->push_back(1);
        for (auto& child_column : child_columns) {
            child_column->insert_default();
        }
        RETURN_IF_ERROR(struct_reader.skip_non_scalar_children(1));
    } else {
        if (parent_null_map != nullptr) {
            parent_null_map->push_back(0);
        }
        for (size_t scalar_idx = 0; scalar_idx < batch.child_batches.size(); ++scalar_idx) {
            const size_t child_idx = batch.scalar_child_indices[scalar_idx];
            auto* scalar_child =
                    dynamic_cast<ScalarColumnReader*>(struct_reader.child_reader(child_idx));
            if (scalar_child == nullptr) {
                return Status::Corruption(
                        "Parquet STRUCT column {} child {} is not a scalar batch child",
                        struct_reader.name(), child_idx);
            }
            const int output_idx = struct_reader.child_output_index(child_idx);
            if (output_idx < 0) {
                continue;
            }
            if (!scalar_child->type()->is_nullable() &&
                batch.child_batches[scalar_idx].def_levels[level_idx] !=
                        scalar_child->descriptor()->max_definition_level()) {
                return Status::Corruption(
                        "Parquet STRUCT column {} contains null for non-nullable child {}",
                        struct_reader.name(), scalar_child->name());
            }
            RETURN_IF_ERROR(append_nullable_scalar_child(
                    struct_reader.name(), "STRUCT", scalar_child->name(), *scalar_child,
                    batch.child_batches[scalar_idx], level_idx,
                    scalar_child->descriptor()->max_definition_level(), child_columns[output_idx]));
        }
        RETURN_IF_ERROR(
                advance_non_scalar_struct_children(struct_reader, parent_is_null, child_columns));
    }

    for (size_t child_idx = 0; child_idx < child_columns.size(); ++child_idx) {
        struct_column->get_column_ptr(child_idx) = std::move(child_columns[child_idx]);
    }
    return Status::OK();
}

ColumnArray* array_column_from_output(MutableColumnPtr& column) {
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        return assert_cast<ColumnArray*>(&nullable_column->get_nested_column());
    }
    return assert_cast<ColumnArray*>(column.get());
}

ColumnMap* map_column_from_output(MutableColumnPtr& column) {
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        return assert_cast<ColumnMap*>(&nullable_column->get_nested_column());
    }
    return assert_cast<ColumnMap*>(column.get());
}

ColumnStruct* struct_column_from_output(MutableColumnPtr& column) {
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        return assert_cast<ColumnStruct*>(&nullable_column->get_nested_column());
    }
    return assert_cast<ColumnStruct*>(column.get());
}

NullMap* null_map_from_nullable_output(MutableColumnPtr& column) {
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(*column)) {
        return &nullable_column->get_null_map_data();
    }
    return nullptr;
}

void append_offsets(ColumnArray::Offsets64& offsets, const std::vector<uint64_t>& entry_counts) {
    offsets.reserve(offsets.size() + entry_counts.size());
    uint64_t current_offset = offsets.empty() ? 0 : offsets.back();
    for (const auto entry_count : entry_counts) {
        current_offset += entry_count;
        offsets.push_back(current_offset);
    }
}

void append_parent_nulls(NullMap* dst, const NullMap& src) {
    if (dst == nullptr) {
        return;
    }
    dst->insert(src.begin(), src.end());
}

Status RepeatedParentSinkState::append_null_parent(const std::string& column_name,
                                                   std::string_view parent_kind,
                                                   const DataTypePtr& type) const {
    DORIS_CHECK(entry_counts != nullptr);
    DORIS_CHECK(parent_nulls != nullptr);
    if (!type->is_nullable()) {
        return Status::Corruption("Parquet {} column {} contains null for non-nullable {}",
                                  parent_kind, column_name, parent_kind);
    }
    entry_counts->push_back(0);
    parent_nulls->push_back(1);
    return Status::OK();
}

void RepeatedParentSinkState::append_present_parent() const {
    DORIS_CHECK(entry_counts != nullptr);
    DORIS_CHECK(parent_nulls != nullptr);
    entry_counts->push_back(0);
    parent_nulls->push_back(0);
}

Status RepeatedParentSinkState::require_parent(const std::string& column_name) const {
    DORIS_CHECK(entry_counts != nullptr);
    if (entry_counts->empty()) {
        return Status::Corruption("Invalid repeated level for column {}", column_name);
    }
    return Status::OK();
}

Status RepeatedParentSinkState::add_entry(const std::string& column_name) const {
    RETURN_IF_ERROR(require_parent(column_name));
    ++entry_counts->back();
    return Status::OK();
}

Status RepeatedChildSinkState::append_null_child(const std::string& column_name,
                                                 std::string_view parent_kind,
                                                 std::string_view child_kind,
                                                 const DataTypePtr& type) const {
    DORIS_CHECK(entry_counts != nullptr);
    DORIS_CHECK(parent_nulls != nullptr);
    if (!type->is_nullable()) {
        return Status::Corruption("Parquet {} column {} contains null for non-nullable {}",
                                  parent_kind, column_name, child_kind);
    }
    entry_counts->push_back(0);
    parent_nulls->push_back(1);
    return Status::OK();
}

void RepeatedChildSinkState::append_present_child() const {
    DORIS_CHECK(entry_counts != nullptr);
    DORIS_CHECK(parent_nulls != nullptr);
    entry_counts->push_back(0);
    parent_nulls->push_back(0);
}

Status RepeatedChildSinkState::require_child(const std::string& column_name,
                                             std::string_view child_kind) const {
    DORIS_CHECK(entry_counts != nullptr);
    if (entry_counts->empty()) {
        return Status::Corruption("Invalid repeated {} level for column {}", child_kind,
                                  column_name);
    }
    return Status::OK();
}

Status RepeatedChildSinkState::add_entry(const std::string& column_name,
                                         std::string_view child_kind) const {
    RETURN_IF_ERROR(require_child(column_name, child_kind));
    ++entry_counts->back();
    return Status::OK();
}

Status append_nullable_scalar_child(const std::string& column_name, std::string_view parent_kind,
                                    std::string_view child_kind,
                                    const ScalarColumnReader& child_reader,
                                    const NestedScalarBatch& batch, int64_t level_idx,
                                    int16_t max_definition_level, MutableColumnPtr& column) {
    const int16_t def_level = batch.def_levels[level_idx];
    if (def_level == max_definition_level) {
        return append_scalar_batch_value(child_reader, batch, level_idx, column);
    }
    if (!child_reader.type()->is_nullable()) {
        return Status::Corruption("Parquet {} column {} contains null for non-nullable {}",
                                  parent_kind, column_name, child_kind);
    }
    column->insert_default();
    return Status::OK();
}

} // namespace doris::parquet
