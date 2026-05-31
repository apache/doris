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

#include "format/new_parquet/column_reader.h"

#include <parquet/api/reader.h>
#include <parquet/api/schema.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "format/new_parquet/arrow_leaf_reader_adapter.h"
#include "format/new_parquet/nested_level_assembler.h"
#include "format/new_parquet/parquet_column_schema.h"
#include "format/new_parquet/scalar_column_reader.h"
#include "format/new_parquet/shape_only_column_reader.h"
#include "format/reader/file_reader.h"

namespace doris::parquet {
namespace {

class StructColumnReader final : public ParquetColumnReader {
public:
    StructColumnReader(const ParquetColumnSchema& schema, DataTypePtr type,
                       std::vector<std::unique_ptr<ParquetColumnReader>> children,
                       std::vector<int> child_output_indices)
            : _field_id(schema.top_level_field_id),
              _nullable_definition_level(schema.nullable_definition_level),
              _repeated_repetition_level(schema.repeated_repetition_level),
              _type(std::move(type)),
              _name(schema.name),
              _children(std::move(children)),
              _child_output_indices(std::move(child_output_indices)) {
        DCHECK_EQ(_children.size(), _child_output_indices.size());
    }

    int file_column_id() const override { return _field_id; }
    int parquet_leaf_column_id() const override { return -1; }
    const DataTypePtr& type() const override { return _type; }
    const std::string& name() const override { return _name; }

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;

    Status skip_non_scalar_children(int64_t rows);
    size_t child_count() const { return _children.size(); }
    ParquetColumnReader* child_reader(size_t child_idx) const { return _children[child_idx].get(); }
    int child_output_index(size_t child_idx) const { return _child_output_indices[child_idx]; }
    int16_t nullable_definition_level() const { return _nullable_definition_level; }

private:
    int _field_id = -1;
    int16_t _nullable_definition_level = 0;
    int16_t _repeated_repetition_level = 0;
    DataTypePtr _type;
    std::string _name;
    std::vector<std::unique_ptr<ParquetColumnReader>> _children;
    std::vector<int> _child_output_indices;
};

class ListColumnReader final : public ParquetColumnReader {
public:
    ListColumnReader(const ParquetColumnSchema& schema, DataTypePtr type,
                     std::unique_ptr<ParquetColumnReader> element_reader)
            : _field_id(schema.top_level_field_id),
              _nullable_definition_level(schema.nullable_definition_level),
              _repeated_repetition_level(schema.repeated_repetition_level),
              _type(std::move(type)),
              _name(schema.name),
              _element_reader(std::move(element_reader)) {}

    int file_column_id() const override { return _field_id; }
    int parquet_leaf_column_id() const override { return -1; }
    const DataTypePtr& type() const override { return _type; }
    const std::string& name() const override { return _name; }

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;

    int16_t nullable_definition_level() const { return _nullable_definition_level; }
    int16_t repeated_repetition_level() const { return _repeated_repetition_level; }
    ParquetColumnReader* element_reader() const { return _element_reader.get(); }

private:
    int _field_id = -1;
    int16_t _nullable_definition_level = 0;
    int16_t _repeated_repetition_level = 0;
    DataTypePtr _type;
    std::string _name;
    std::unique_ptr<ParquetColumnReader> _element_reader;
    NestedScalarOverflow _element_overflow;
    NestedStructOverflow _struct_element_overflow;
};

class MapColumnReader final : public ParquetColumnReader {
public:
    MapColumnReader(const ParquetColumnSchema& schema, DataTypePtr type,
                    std::unique_ptr<ParquetColumnReader> key_reader,
                    std::unique_ptr<ParquetColumnReader> value_reader)
            : _field_id(schema.top_level_field_id),
              _nullable_definition_level(schema.nullable_definition_level),
              _repeated_repetition_level(schema.repeated_repetition_level),
              _type(std::move(type)),
              _name(schema.name),
              _key_reader(std::move(key_reader)),
              _value_reader(std::move(value_reader)) {}

    int file_column_id() const override { return _field_id; }
    int parquet_leaf_column_id() const override { return -1; }
    const DataTypePtr& type() const override { return _type; }
    const std::string& name() const override { return _name; }

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;

private:
    int _field_id = -1;
    int16_t _nullable_definition_level = 0;
    int16_t _repeated_repetition_level = 0;
    DataTypePtr _type;
    std::string _name;
    std::unique_ptr<ParquetColumnReader> _key_reader;
    std::unique_ptr<ParquetColumnReader> _value_reader;
    NestedScalarOverflow _key_overflow;
    NestedScalarOverflow _value_overflow;
    NestedStructOverflow _struct_value_overflow;
};

Status read_nested_scalar_batch(
        ScalarColumnReader& column_reader, int64_t batch_rows, int16_t value_slot_definition_level,
        NestedScalarBatch* batch,
        int16_t value_slot_repetition_level = std::numeric_limits<int16_t>::max()) {
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

Status validate_nested_scalar_alignment(const std::string& column_name,
                                        const NestedScalarBatch& driver_batch,
                                        const NestedScalarBatch& candidate_batch,
                                        std::string_view candidate_name, std::string_view action) {
    if (candidate_batch.records_read != driver_batch.records_read ||
        candidate_batch.levels_written != driver_batch.levels_written) {
        return Status::Corruption(
                "Parquet MAP key/value levels are not aligned for column {}{}: driver rows={}, "
                "driver levels={}, {} rows={}, {} levels={}",
                column_name, action, driver_batch.records_read, driver_batch.levels_written,
                candidate_name, candidate_batch.records_read, candidate_name,
                candidate_batch.levels_written);
    }
    for (int64_t level_idx = 0; level_idx < driver_batch.levels_written; ++level_idx) {
        if (candidate_batch.rep_levels[level_idx] != driver_batch.rep_levels[level_idx]) {
            return Status::Corruption(
                    "Parquet MAP key/value repetition levels are not aligned for column {}{}",
                    column_name, action);
        }
    }
    return Status::OK();
}

Status validate_nested_struct_alignment(const std::string& column_name,
                                        const NestedScalarBatch& driver_batch,
                                        const NestedStructBatch& candidate_batch,
                                        std::string_view action) {
    if (candidate_batch.child_batches.empty()) {
        return Status::Corruption("Parquet MAP value STRUCT has no child batch for column {}{}",
                                  column_name, action);
    }
    if (candidate_batch.records_read != driver_batch.records_read ||
        candidate_batch.levels_written != driver_batch.levels_written) {
        return Status::Corruption(
                "Parquet MAP key/value levels are not aligned for column {}{}: key rows={}, key "
                "levels={}, value rows={}, value levels={}",
                column_name, action, driver_batch.records_read, driver_batch.levels_written,
                candidate_batch.records_read, candidate_batch.levels_written);
    }
    const auto& value_rep_levels = candidate_batch.child_batches[0].rep_levels;
    for (int64_t level_idx = 0; level_idx < driver_batch.levels_written; ++level_idx) {
        if (value_rep_levels[level_idx] != driver_batch.rep_levels[level_idx]) {
            return Status::Corruption(
                    "Parquet MAP key/value repetition levels are not aligned for column {}{}",
                    column_name, action);
        }
    }
    return Status::OK();
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
        child_columns.push_back(struct_column->get_column_ptr(child_idx)->assume_mutable());
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
            RETURN_IF_ERROR(append_scalar_batch_value(*scalar_child,
                                                      batch.child_batches[scalar_idx], level_idx,
                                                      child_columns[output_idx]));
        }
        RETURN_IF_ERROR(
                advance_non_scalar_struct_children(struct_reader, parent_is_null, child_columns));
    }

    for (size_t child_idx = 0; child_idx < child_columns.size(); ++child_idx) {
        struct_column->get_column_ptr(child_idx) = std::move(child_columns[child_idx]);
    }
    return Status::OK();
}

bool supports_nested_scalar_record_reader(const ParquetColumnSchema& column_schema) {
    if (supports_record_reader(column_schema.type_descriptor)) {
        return true;
    }
    const auto& type_descriptor = column_schema.type_descriptor;
    if (type_descriptor.extra_type_info != ParquetExtraTypeInfo::NONE ||
        type_descriptor.is_decimal || type_descriptor.is_timestamp ||
        type_descriptor.is_string_like) {
        return false;
    }
    if (type_descriptor.converted_type != ::parquet::ConvertedType::NONE &&
        type_descriptor.converted_type != ::parquet::ConvertedType::UNDEFINED) {
        return false;
    }
    switch (type_descriptor.physical_type) {
    case ::parquet::Type::BOOLEAN:
    case ::parquet::Type::INT32:
    case ::parquet::Type::INT64:
    case ::parquet::Type::FLOAT:
    case ::parquet::Type::DOUBLE:
        return true;
    default:
        return false;
    }
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

const reader::FieldProjection* find_child_projection(const reader::FieldProjection* projection,
                                                     const ParquetColumnSchema& child_schema) {
    if (projection == nullptr || projection->project_all_children) {
        return nullptr;
    }
    auto it = std::find_if(projection->children.begin(), projection->children.end(),
                           [&](const reader::FieldProjection& child_projection) {
                               return child_projection.file_path == child_schema.file_path;
                           });
    return it == projection->children.end() ? nullptr : &*it;
}

template <typename Sink>
Status assemble_repeated_levels(ScalarColumnReader& driver_reader, int16_t repeated_level,
                                int16_t value_slot_definition_level, int64_t rows,
                                NestedScalarOverflow* overflow, Sink& sink, int64_t* rows_read) {
    auto read_batch = [&](int64_t batch_rows, NestedScalarBatch* batch) {
        return read_nested_scalar_batch(driver_reader, batch_rows, value_slot_definition_level,
                                        batch);
    };
    auto rep_level_at = [](const NestedScalarBatch& batch, int64_t level_idx) {
        return batch.rep_levels[level_idx];
    };
    return doris::parquet::assemble_repeated_levels<NestedScalarBatch>(
            driver_reader.name(), repeated_level, rows, overflow, read_batch,
            move_nested_scalar_tail, rep_level_at, sink, rows_read);
}

template <typename Sink>
Status assemble_repeated_struct_levels(StructColumnReader& driver_reader, int16_t repeated_level,
                                       int16_t value_slot_definition_level, int64_t rows,
                                       NestedStructOverflow* overflow, Sink& sink,
                                       int64_t* rows_read) {
    auto read_batch = [&](int64_t batch_rows, NestedStructBatch* batch) {
        return read_nested_struct_batch(driver_reader, batch_rows, value_slot_definition_level,
                                        batch);
    };
    auto rep_level_at = [](const NestedStructBatch& batch, int64_t level_idx) {
        return batch.child_batches[0].rep_levels[level_idx];
    };
    return doris::parquet::assemble_repeated_levels<NestedStructBatch>(
            driver_reader.name(), repeated_level, rows, overflow, read_batch,
            move_nested_struct_tail, rep_level_at, sink, rows_read);
}

} // namespace

Status StructColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    if (column.get() == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet struct read result pointer for column {}",
                                       _name);
    }
    if (_children.empty()) {
        column->resize(static_cast<size_t>(rows));
        *rows_read = rows;
        return Status::OK();
    }

    auto* struct_column = struct_column_from_output(column);
    DORIS_CHECK(struct_column != nullptr);
    auto* parent_null_map = null_map_from_nullable_output(column);

    std::vector<ScalarColumnReader*> scalar_children;
    std::vector<size_t> scalar_child_indices;
    scalar_children.reserve(_children.size());
    scalar_child_indices.reserve(_children.size());
    bool all_scalar_children = true;
    for (size_t child_idx = 0; child_idx < _children.size(); ++child_idx) {
        const auto& child_reader = _children[child_idx];
        DORIS_CHECK(child_reader != nullptr);
        auto* scalar_child = dynamic_cast<ScalarColumnReader*>(child_reader.get());
        if (scalar_child == nullptr) {
            all_scalar_children = false;
            continue;
        }
        scalar_child_indices.push_back(child_idx);
        scalar_children.push_back(scalar_child);
    }
    if (all_scalar_children) {
        std::vector<NestedScalarBatch> child_batches(scalar_children.size());
        int64_t expected_rows = -1;
        for (size_t child_idx = 0; child_idx < scalar_children.size(); ++child_idx) {
            RETURN_IF_ERROR(read_nested_scalar_batch(*scalar_children[child_idx], rows,
                                                     _nullable_definition_level,
                                                     &child_batches[child_idx]));
            if (expected_rows < 0) {
                expected_rows = child_batches[child_idx].records_read;
            } else if (child_batches[child_idx].records_read != expected_rows) {
                return Status::Corruption(
                        "Parquet struct children returned different row counts in column {}: {} "
                        "vs {}",
                        _name, expected_rows, child_batches[child_idx].records_read);
            }
            if (child_batches[child_idx].levels_written != child_batches[child_idx].records_read) {
                return Status::Corruption(
                        "Parquet struct child {} returned repeated levels in column {}",
                        scalar_children[child_idx]->name(), _name);
            }
        }

        if (expected_rows <= 0) {
            *rows_read = 0;
            return Status::OK();
        }

        std::vector<MutableColumnPtr> child_columns;
        child_columns.reserve(struct_column->get_columns().size());
        for (size_t child_idx = 0; child_idx < struct_column->get_columns().size(); ++child_idx) {
            child_columns.push_back(struct_column->get_column_ptr(child_idx)->assume_mutable());
        }

        NullMap parent_nulls;
        parent_nulls.reserve(static_cast<size_t>(expected_rows));
        for (int64_t row_idx = 0; row_idx < expected_rows; ++row_idx) {
            const bool parent_is_null =
                    child_batches[0].def_levels[row_idx] < _nullable_definition_level;
            parent_nulls.push_back(parent_is_null);
            for (size_t child_idx = 1; child_idx < child_batches.size(); ++child_idx) {
                const bool child_parent_is_null =
                        child_batches[child_idx].def_levels[row_idx] < _nullable_definition_level;
                if (child_parent_is_null != parent_is_null) {
                    return Status::Corruption(
                            "Parquet struct children returned different null parent shape in "
                            "column {}",
                            _name);
                }
            }
            for (size_t child_idx = 0; child_idx < scalar_children.size(); ++child_idx) {
                const int output_idx = _child_output_indices[scalar_child_indices[child_idx]];
                if (output_idx < 0) {
                    continue;
                }
                if (parent_is_null) {
                    child_columns[output_idx]->insert_default();
                } else {
                    if (!scalar_children[child_idx]->type()->is_nullable() &&
                        child_batches[child_idx].def_levels[row_idx] !=
                                scalar_children[child_idx]->descriptor()->max_definition_level()) {
                        return Status::Corruption(
                                "Parquet STRUCT column {} contains null for non-nullable child {}",
                                _name, scalar_children[child_idx]->name());
                    }
                    RETURN_IF_ERROR(append_scalar_batch_value(*scalar_children[child_idx],
                                                              child_batches[child_idx], row_idx,
                                                              child_columns[output_idx]));
                }
            }
        }
        for (size_t child_idx = 0; child_idx < child_columns.size(); ++child_idx) {
            struct_column->get_column_ptr(child_idx) = std::move(child_columns[child_idx]);
        }
        if (parent_null_map == nullptr) {
            for (const auto parent_is_null : parent_nulls) {
                if (parent_is_null) {
                    return Status::Corruption(
                            "Parquet STRUCT column {} contains null for non-nullable struct",
                            _name);
                }
            }
        } else {
            append_parent_nulls(parent_null_map, parent_nulls);
        }
        *rows_read = expected_rows;
        return Status::OK();
    }

    if (!scalar_children.empty()) {
        std::vector<NestedScalarBatch> child_batches(scalar_children.size());
        int64_t expected_rows = -1;
        for (size_t scalar_idx = 0; scalar_idx < scalar_children.size(); ++scalar_idx) {
            RETURN_IF_ERROR(read_nested_scalar_batch(
                    *scalar_children[scalar_idx], rows, _nullable_definition_level,
                    &child_batches[scalar_idx], _repeated_repetition_level));
            int64_t child_rows = 0;
            for (int64_t level_idx = 0; level_idx < child_batches[scalar_idx].levels_written;
                 ++level_idx) {
                if (child_batches[scalar_idx].rep_levels[level_idx] <= _repeated_repetition_level) {
                    ++child_rows;
                }
            }
            if (expected_rows < 0) {
                expected_rows = child_rows;
            } else if (child_rows != expected_rows) {
                return Status::Corruption(
                        "Parquet struct children returned different row counts in column {}: {} "
                        "vs {}",
                        _name, expected_rows, child_rows);
            }
        }
        if (expected_rows <= 0) {
            *rows_read = 0;
            return Status::OK();
        }

        std::vector<MutableColumnPtr> child_columns;
        child_columns.reserve(struct_column->get_columns().size());
        for (size_t child_idx = 0; child_idx < struct_column->get_columns().size(); ++child_idx) {
            child_columns.push_back(struct_column->get_column_ptr(child_idx)->assume_mutable());
        }

        NullMap parent_nulls;
        parent_nulls.reserve(static_cast<size_t>(expected_rows));
        std::vector<int64_t> level_indices(child_batches.size(), 0);
        for (int64_t row_idx = 0; row_idx < expected_rows; ++row_idx) {
            for (size_t scalar_idx = 0; scalar_idx < child_batches.size(); ++scalar_idx) {
                while (level_indices[scalar_idx] < child_batches[scalar_idx].levels_written &&
                       child_batches[scalar_idx].rep_levels[level_indices[scalar_idx]] >
                               _repeated_repetition_level) {
                    ++level_indices[scalar_idx];
                }
                if (level_indices[scalar_idx] >= child_batches[scalar_idx].levels_written) {
                    return Status::Corruption("Parquet struct child {} ended before column {}",
                                              scalar_children[scalar_idx]->name(), _name);
                }
            }
            const bool parent_is_null =
                    child_batches[0].def_levels[level_indices[0]] < _nullable_definition_level;
            parent_nulls.push_back(parent_is_null);
            for (size_t scalar_idx = 1; scalar_idx < child_batches.size(); ++scalar_idx) {
                const bool child_parent_is_null =
                        child_batches[scalar_idx].def_levels[level_indices[scalar_idx]] <
                        _nullable_definition_level;
                if (child_parent_is_null != parent_is_null) {
                    return Status::Corruption(
                            "Parquet struct children returned different null parent shape in "
                            "column {}",
                            _name);
                }
            }
            for (size_t scalar_idx = 0; scalar_idx < scalar_children.size(); ++scalar_idx) {
                const size_t child_idx = scalar_child_indices[scalar_idx];
                const int output_idx = _child_output_indices[child_idx];
                if (output_idx < 0) {
                    continue;
                }
                if (parent_is_null) {
                    child_columns[output_idx]->insert_default();
                    continue;
                }
                if (!scalar_children[scalar_idx]->type()->is_nullable() &&
                    child_batches[scalar_idx].def_levels[level_indices[scalar_idx]] !=
                            scalar_children[scalar_idx]->descriptor()->max_definition_level()) {
                    return Status::Corruption(
                            "Parquet STRUCT column {} contains null for non-nullable child {}",
                            _name, scalar_children[scalar_idx]->name());
                }
                RETURN_IF_ERROR(append_scalar_batch_value(
                        *scalar_children[scalar_idx], child_batches[scalar_idx],
                        level_indices[scalar_idx], child_columns[output_idx]));
            }
            RETURN_IF_ERROR(
                    advance_non_scalar_struct_children(*this, parent_is_null, child_columns));
            for (auto& level_idx : level_indices) {
                ++level_idx;
            }
        }
        for (size_t child_idx = 0; child_idx < child_columns.size(); ++child_idx) {
            struct_column->get_column_ptr(child_idx) = std::move(child_columns[child_idx]);
        }
        if (parent_null_map == nullptr) {
            for (const auto parent_is_null : parent_nulls) {
                if (parent_is_null) {
                    return Status::Corruption(
                            "Parquet STRUCT column {} contains null for non-nullable struct",
                            _name);
                }
            }
        } else {
            append_parent_nulls(parent_null_map, parent_nulls);
        }
        *rows_read = expected_rows;
        return Status::OK();
    }

    if (parent_null_map != nullptr) {
        return Status::NotSupported(
                "Current parquet nullable STRUCT reader requires at least one scalar child for "
                "column {}",
                _name);
    }

    int64_t expected_rows = -1;
    size_t child_idx = 0;
    for (auto& child_reader : _children) {
        DORIS_CHECK(child_reader != nullptr);
        int64_t child_rows = 0;
        const int output_idx = _child_output_indices[child_idx];
        if (output_idx < 0) {
            RETURN_IF_ERROR(child_reader->skip(rows));
            child_rows = rows;
        } else {
            auto child_column = struct_column->get_column_ptr(output_idx)->assume_mutable();
            RETURN_IF_ERROR(child_reader->read(rows, child_column, &child_rows));
            struct_column->get_column_ptr(output_idx) = std::move(child_column);
        }
        if (expected_rows < 0) {
            expected_rows = child_rows;
        } else if (child_rows != expected_rows) {
            return Status::Corruption(
                    "Parquet struct children returned different row counts in column {}: {} vs {}",
                    _name, expected_rows, child_rows);
        }
        child_idx++;
    }

    *rows_read = std::max<int64_t>(expected_rows, 0);
    return Status::OK();
}

Status StructColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }
    for (auto& child_reader : _children) {
        RETURN_IF_ERROR(child_reader->skip(rows));
    }
    return Status::OK();
}

Status StructColumnReader::skip_non_scalar_children(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }
    for (auto& child_reader : _children) {
        DORIS_CHECK(child_reader != nullptr);
        if (dynamic_cast<ScalarColumnReader*>(child_reader.get()) != nullptr) {
            continue;
        }
        RETURN_IF_ERROR(child_reader->skip(rows));
    }
    return Status::OK();
}

Status ListColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    if (column.get() == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet list read result pointer for column {}",
                                       _name);
    }
    if (_element_reader == nullptr) {
        return Status::InternalError("Parquet list element reader is not initialized for column {}",
                                     _name);
    }
    auto* array_column = array_column_from_output(column);
    DORIS_CHECK(array_column != nullptr);
    auto* parent_null_map = null_map_from_nullable_output(column);
    auto nested_column = array_column->get_data_ptr()->assume_mutable();
    std::vector<uint64_t> entry_counts;
    NullMap parent_nulls;
    const int16_t element_slot_definition_level = _nullable_definition_level + 1;

    if (auto* element_reader = dynamic_cast<ScalarColumnReader*>(_element_reader.get())) {
        const int16_t element_max_definition_level =
                element_reader->descriptor()->max_definition_level();

        struct ScalarListSink {
            ListColumnReader* self = nullptr;
            ScalarColumnReader* element_reader = nullptr;
            MutableColumnPtr* nested_column = nullptr;
            std::vector<uint64_t>* entry_counts = nullptr;
            NullMap* parent_nulls = nullptr;
            int16_t element_max_definition_level = 0;

            Status start_batch(const NestedScalarBatch&) { return Status::OK(); }

            Status start_parent(const NestedScalarBatch& batch, int64_t level_idx) {
                const int16_t def_level = batch.def_levels[level_idx];
                if (def_level < self->_nullable_definition_level) {
                    if (!self->_type->is_nullable()) {
                        return Status::Corruption(
                                "Parquet LIST column {} contains null for non-nullable list",
                                self->_name);
                    }
                    entry_counts->push_back(0);
                    parent_nulls->push_back(1);
                    return Status::OK();
                }
                entry_counts->push_back(0);
                parent_nulls->push_back(0);
                if (def_level == self->_nullable_definition_level) {
                    return Status::OK();
                }
                return append_element(batch, level_idx);
            }

            Status append_repeated(const NestedScalarBatch& batch, int64_t level_idx) {
                if (entry_counts->empty()) {
                    return Status::Corruption("Invalid repeated LIST level for column {}",
                                              self->_name);
                }
                return append_element(batch, level_idx);
            }

            Status append_element(const NestedScalarBatch& batch, int64_t level_idx) {
                const int16_t def_level = batch.def_levels[level_idx];
                if (def_level == element_max_definition_level) {
                    RETURN_IF_ERROR(append_scalar_batch_value(*element_reader, batch, level_idx,
                                                              *nested_column));
                } else {
                    if (!element_reader->type()->is_nullable()) {
                        return Status::Corruption(
                                "Parquet LIST column {} contains null for non-nullable element",
                                self->_name);
                    }
                    (*nested_column)->insert_default();
                }
                ++entry_counts->back();
                return Status::OK();
            }
        };

        ScalarListSink sink {this,          element_reader, &nested_column,
                             &entry_counts, &parent_nulls,  element_max_definition_level};
        RETURN_IF_ERROR(assemble_repeated_levels(*element_reader, _repeated_repetition_level,
                                                 element_slot_definition_level, rows,
                                                 &_element_overflow, sink, rows_read));

        array_column->get_data_ptr() = std::move(nested_column);
        append_offsets(array_column->get_offsets(), entry_counts);
        append_parent_nulls(parent_null_map, parent_nulls);
        return Status::OK();
    }

    if (auto* struct_element_reader = dynamic_cast<StructColumnReader*>(_element_reader.get())) {
        struct StructListSink {
            ListColumnReader* self = nullptr;
            StructColumnReader* element_reader = nullptr;
            MutableColumnPtr* nested_column = nullptr;
            std::vector<uint64_t>* entry_counts = nullptr;
            NullMap* parent_nulls = nullptr;

            Status start_batch(const NestedStructBatch&) { return Status::OK(); }

            Status start_parent(const NestedStructBatch& batch, int64_t level_idx) {
                const int16_t def_level = batch.child_batches[0].def_levels[level_idx];
                if (def_level < self->_nullable_definition_level) {
                    if (!self->_type->is_nullable()) {
                        return Status::Corruption(
                                "Parquet LIST column {} contains null for non-nullable list",
                                self->_name);
                    }
                    entry_counts->push_back(0);
                    parent_nulls->push_back(1);
                    RETURN_IF_ERROR(element_reader->skip_non_scalar_children(1));
                    return Status::OK();
                }
                entry_counts->push_back(0);
                parent_nulls->push_back(0);
                if (def_level == self->_nullable_definition_level) {
                    RETURN_IF_ERROR(element_reader->skip_non_scalar_children(1));
                    return Status::OK();
                }
                return append_element(batch, level_idx);
            }

            Status append_repeated(const NestedStructBatch& batch, int64_t level_idx) {
                if (entry_counts->empty()) {
                    return Status::Corruption("Invalid repeated LIST level for column {}",
                                              self->_name);
                }
                return append_element(batch, level_idx);
            }

            Status append_element(const NestedStructBatch& batch, int64_t level_idx) {
                RETURN_IF_ERROR(append_struct_batch_value(*element_reader, batch, level_idx,
                                                          *nested_column));
                ++entry_counts->back();
                return Status::OK();
            }
        };

        StructListSink sink {this, struct_element_reader, &nested_column, &entry_counts,
                             &parent_nulls};
        RETURN_IF_ERROR(assemble_repeated_struct_levels(
                *struct_element_reader, _repeated_repetition_level, element_slot_definition_level,
                rows, &_struct_element_overflow, sink, rows_read));

        array_column->get_data_ptr() = std::move(nested_column);
        append_offsets(array_column->get_offsets(), entry_counts);
        append_parent_nulls(parent_null_map, parent_nulls);
        return Status::OK();
    }

    auto* list_element_reader = dynamic_cast<ListColumnReader*>(_element_reader.get());
    if (list_element_reader == nullptr) {
        return Status::NotSupported(
                "Current parquet LIST reader only supports scalar, scalar-child STRUCT, or nested "
                "LIST elements for column {}",
                _name);
    }
    auto* scalar_nested_element_reader =
            dynamic_cast<ScalarColumnReader*>(list_element_reader->_element_reader.get());
    if (scalar_nested_element_reader == nullptr) {
        return Status::NotSupported(
                "Current parquet nested LIST reader only supports scalar nested elements for "
                "column "
                "{}",
                _name);
    }

    auto* inner_array_column = array_column_from_output(nested_column);
    DORIS_CHECK(inner_array_column != nullptr);
    auto* inner_null_map = null_map_from_nullable_output(nested_column);
    auto inner_nested_column = inner_array_column->get_data_ptr()->assume_mutable();
    std::vector<uint64_t> inner_entry_counts;
    NullMap inner_parent_nulls;
    const int16_t inner_element_slot_definition_level =
            list_element_reader->_nullable_definition_level + 1;
    const int16_t nested_element_max_definition_level =
            scalar_nested_element_reader->descriptor()->max_definition_level();

    struct NestedListSink {
        ListColumnReader* self = nullptr;
        ListColumnReader* element_reader = nullptr;
        ScalarColumnReader* scalar_element_reader = nullptr;
        MutableColumnPtr* inner_nested_column = nullptr;
        std::vector<uint64_t>* entry_counts = nullptr;
        NullMap* parent_nulls = nullptr;
        std::vector<uint64_t>* inner_entry_counts = nullptr;
        NullMap* inner_parent_nulls = nullptr;
        int16_t nested_element_max_definition_level = 0;

        Status start_batch(const NestedScalarBatch&) { return Status::OK(); }

        Status start_parent(const NestedScalarBatch& batch, int64_t level_idx) {
            const int16_t def_level = batch.def_levels[level_idx];
            if (def_level < self->_nullable_definition_level) {
                if (!self->_type->is_nullable()) {
                    return Status::Corruption(
                            "Parquet LIST column {} contains null for non-nullable list",
                            self->_name);
                }
                entry_counts->push_back(0);
                parent_nulls->push_back(1);
                return Status::OK();
            }
            entry_counts->push_back(0);
            parent_nulls->push_back(0);
            if (def_level == self->_nullable_definition_level) {
                return Status::OK();
            }
            return append_inner_list(batch, level_idx);
        }

        Status append_repeated(const NestedScalarBatch& batch, int64_t level_idx) {
            if (entry_counts->empty()) {
                return Status::Corruption("Invalid repeated LIST level for column {}", self->_name);
            }
            if (batch.rep_levels[level_idx] < element_reader->_repeated_repetition_level) {
                return append_inner_list(batch, level_idx);
            }
            if (inner_entry_counts->empty()) {
                return Status::Corruption("Invalid nested repeated LIST level for column {}",
                                          self->_name);
            }
            return append_scalar_element(batch, level_idx);
        }

        Status append_inner_list(const NestedScalarBatch& batch, int64_t level_idx) {
            const int16_t def_level = batch.def_levels[level_idx];
            if (def_level < element_reader->_nullable_definition_level) {
                if (!element_reader->_type->is_nullable()) {
                    return Status::Corruption(
                            "Parquet LIST column {} contains null for non-nullable nested list",
                            self->_name);
                }
                ++entry_counts->back();
                inner_entry_counts->push_back(0);
                inner_parent_nulls->push_back(1);
                return Status::OK();
            }
            ++entry_counts->back();
            inner_entry_counts->push_back(0);
            inner_parent_nulls->push_back(0);
            if (def_level == element_reader->_nullable_definition_level) {
                return Status::OK();
            }
            return append_scalar_element(batch, level_idx);
        }

        Status append_scalar_element(const NestedScalarBatch& batch, int64_t level_idx) {
            const int16_t def_level = batch.def_levels[level_idx];
            if (def_level == nested_element_max_definition_level) {
                RETURN_IF_ERROR(append_scalar_batch_value(*scalar_element_reader, batch, level_idx,
                                                          *inner_nested_column));
            } else {
                if (!scalar_element_reader->type()->is_nullable()) {
                    return Status::Corruption(
                            "Parquet LIST column {} contains null for non-nullable nested element",
                            self->_name);
                }
                (*inner_nested_column)->insert_default();
            }
            ++inner_entry_counts->back();
            return Status::OK();
        }
    };

    NestedListSink sink {this,
                         list_element_reader,
                         scalar_nested_element_reader,
                         &inner_nested_column,
                         &entry_counts,
                         &parent_nulls,
                         &inner_entry_counts,
                         &inner_parent_nulls,
                         nested_element_max_definition_level};
    RETURN_IF_ERROR(assemble_repeated_levels(
            *scalar_nested_element_reader, _repeated_repetition_level,
            inner_element_slot_definition_level, rows, &_element_overflow, sink, rows_read));

    inner_array_column->get_data_ptr() = std::move(inner_nested_column);
    append_offsets(inner_array_column->get_offsets(), inner_entry_counts);
    append_parent_nulls(inner_null_map, inner_parent_nulls);
    append_offsets(array_column->get_offsets(), entry_counts);
    array_column->get_data_ptr() = std::move(nested_column);
    append_parent_nulls(parent_null_map, parent_nulls);
    return Status::OK();
}

Status ListColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }
    struct SkipSink {
        Status start_batch(const NestedScalarBatch&) { return Status::OK(); }
        Status start_parent(const NestedScalarBatch&, int64_t) { return Status::OK(); }
        Status append_repeated(const NestedScalarBatch&, int64_t) { return Status::OK(); }
    };
    int64_t rows_read = 0;
    if (auto* element_reader = dynamic_cast<ScalarColumnReader*>(_element_reader.get())) {
        SkipSink sink;
        RETURN_IF_ERROR(assemble_repeated_levels(*element_reader, _repeated_repetition_level,
                                                 _nullable_definition_level + 1, rows,
                                                 &_element_overflow, sink, &rows_read));
    } else if (auto* struct_element_reader =
                       dynamic_cast<StructColumnReader*>(_element_reader.get())) {
        struct StructSkipSink {
            Status start_batch(const NestedStructBatch&) { return Status::OK(); }
            Status start_parent(const NestedStructBatch&, int64_t) { return Status::OK(); }
            Status append_repeated(const NestedStructBatch&, int64_t) { return Status::OK(); }
        };
        StructSkipSink sink;
        RETURN_IF_ERROR(assemble_repeated_struct_levels(
                *struct_element_reader, _repeated_repetition_level, _nullable_definition_level + 1,
                rows, &_struct_element_overflow, sink, &rows_read));
    } else if (auto* list_element_reader = dynamic_cast<ListColumnReader*>(_element_reader.get())) {
        auto* scalar_nested_element_reader =
                dynamic_cast<ScalarColumnReader*>(list_element_reader->_element_reader.get());
        if (scalar_nested_element_reader == nullptr) {
            return Status::NotSupported(
                    "Current parquet nested LIST skip only supports scalar nested elements for "
                    "column {}",
                    _name);
        }
        SkipSink sink;
        RETURN_IF_ERROR(
                assemble_repeated_levels(*scalar_nested_element_reader, _repeated_repetition_level,
                                         list_element_reader->_nullable_definition_level + 1, rows,
                                         &_element_overflow, sink, &rows_read));
    } else {
        return Status::NotSupported(
                "Current parquet LIST reader only supports scalar, scalar-child STRUCT, or nested "
                "LIST elements for column {}",
                _name);
    }
    if (rows_read != rows) {
        return Status::Corruption("Failed to skip parquet LIST column {}: skipped {} of {} rows",
                                  _name, rows_read, rows);
    }
    return Status::OK();
}

Status MapColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    if (column.get() == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet map read result pointer for column {}",
                                       _name);
    }
    if (_key_reader == nullptr || _value_reader == nullptr) {
        return Status::InternalError("Parquet map child reader is not initialized for column {}",
                                     _name);
    }
    auto* key_reader = dynamic_cast<ScalarColumnReader*>(_key_reader.get());
    auto* value_reader = dynamic_cast<ScalarColumnReader*>(_value_reader.get());
    auto* struct_value_reader = dynamic_cast<StructColumnReader*>(_value_reader.get());
    auto* list_value_reader = dynamic_cast<ListColumnReader*>(_value_reader.get());
    if (key_reader == nullptr || (value_reader == nullptr && struct_value_reader == nullptr &&
                                  list_value_reader == nullptr)) {
        return Status::NotSupported(
                "Current parquet MAP reader only supports scalar key with scalar, scalar-child "
                "STRUCT, or scalar LIST value for column {}",
                _name);
    }

    auto* map_column = map_column_from_output(column);
    DORIS_CHECK(map_column != nullptr);
    auto* parent_null_map = null_map_from_nullable_output(column);
    auto key_column = map_column->get_keys_ptr()->assume_mutable();
    auto value_column = map_column->get_values_ptr()->assume_mutable();
    std::vector<uint64_t> entry_counts;
    NullMap parent_nulls;
    const int16_t entry_definition_level = _nullable_definition_level + 1;
    const int16_t key_max_definition_level = key_reader->descriptor()->max_definition_level();

    if (value_reader != nullptr) {
        const int16_t value_max_definition_level =
                value_reader->descriptor()->max_definition_level();

        struct ScalarMapSink {
            MapColumnReader* self = nullptr;
            ScalarColumnReader* key_reader = nullptr;
            ScalarColumnReader* value_reader = nullptr;
            MutableColumnPtr* key_column = nullptr;
            MutableColumnPtr* value_column = nullptr;
            std::vector<uint64_t>* entry_counts = nullptr;
            NullMap* parent_nulls = nullptr;
            int16_t key_max_definition_level = 0;
            int16_t value_max_definition_level = 0;

            Status read_value_batch(int64_t batch_rows, NestedScalarBatch* out_value_batch) {
                return read_nested_scalar_batch_from_overflow(
                        *value_reader, batch_rows, self->_nullable_definition_level + 1,
                        &self->_value_overflow, out_value_batch);
            }

            Status validate_value_alignment(const NestedScalarBatch& key_batch,
                                            const NestedScalarBatch& candidate_value_batch) {
                return validate_nested_scalar_alignment(self->_name, key_batch,
                                                        candidate_value_batch, "value", "");
            }

            Status start_batch(const NestedScalarBatch& key_batch) {
                RETURN_IF_ERROR(read_value_batch(key_batch.records_read, &value_batch));
                RETURN_IF_ERROR(validate_value_alignment(key_batch, value_batch));
                return Status::OK();
            }

            Status start_parent(const NestedScalarBatch& key_batch, int64_t level_idx) {
                const int16_t def_level = key_batch.def_levels[level_idx];
                if (def_level < self->_nullable_definition_level) {
                    if (!self->_type->is_nullable()) {
                        return Status::Corruption(
                                "Parquet MAP column {} contains null for non-nullable map",
                                self->_name);
                    }
                    entry_counts->push_back(0);
                    parent_nulls->push_back(1);
                    return Status::OK();
                }
                entry_counts->push_back(0);
                parent_nulls->push_back(0);
                if (def_level == self->_nullable_definition_level) {
                    return Status::OK();
                }
                return append_entry(key_batch, level_idx);
            }

            Status append_repeated(const NestedScalarBatch& key_batch, int64_t level_idx) {
                if (entry_counts->empty()) {
                    return Status::Corruption("Invalid repeated MAP level for column {}",
                                              self->_name);
                }
                return append_entry(key_batch, level_idx);
            }

            Status append_entry(const NestedScalarBatch& key_batch, int64_t level_idx) {
                if (key_batch.def_levels[level_idx] != key_max_definition_level) {
                    return Status::Corruption("Parquet MAP column {} contains null map key",
                                              self->_name);
                }
                RETURN_IF_ERROR(
                        append_scalar_batch_value(*key_reader, key_batch, level_idx, *key_column));
                if (value_batch.def_levels[level_idx] == value_max_definition_level) {
                    RETURN_IF_ERROR(append_scalar_batch_value(*value_reader, value_batch, level_idx,
                                                              *value_column));
                } else {
                    if (!value_reader->type()->is_nullable()) {
                        return Status::Corruption(
                                "Parquet MAP column {} contains null for non-nullable value",
                                self->_name);
                    }
                    (*value_column)->insert_default();
                }
                ++entry_counts->back();
                return Status::OK();
            }

            NestedScalarBatch value_batch;
        };

        ScalarMapSink sink;
        sink.self = this;
        sink.key_reader = key_reader;
        sink.value_reader = value_reader;
        sink.key_column = &key_column;
        sink.value_column = &value_column;
        sink.entry_counts = &entry_counts;
        sink.parent_nulls = &parent_nulls;
        sink.key_max_definition_level = key_max_definition_level;
        sink.value_max_definition_level = value_max_definition_level;
        RETURN_IF_ERROR(assemble_repeated_levels(*key_reader, _repeated_repetition_level,
                                                 entry_definition_level, rows, &_key_overflow, sink,
                                                 rows_read));
        if (!_key_overflow.empty()) {
            move_nested_scalar_tail(
                    sink.value_batch,
                    sink.value_batch.levels_written - _key_overflow.batch.levels_written,
                    &_value_overflow);
        }

        map_column->get_keys_ptr() = std::move(key_column);
        map_column->get_values_ptr() = std::move(value_column);
        append_offsets(map_column->get_offsets(), entry_counts);
        append_parent_nulls(parent_null_map, parent_nulls);
        return Status::OK();
    }

    if (list_value_reader != nullptr) {
        auto* scalar_list_value_reader =
                dynamic_cast<ScalarColumnReader*>(list_value_reader->element_reader());
        if (scalar_list_value_reader == nullptr) {
            return Status::NotSupported(
                    "Current parquet MAP LIST value reader only supports scalar list values for "
                    "column {}",
                    _name);
        }

        auto* list_value_column = array_column_from_output(value_column);
        DORIS_CHECK(list_value_column != nullptr);
        auto* list_value_null_map = null_map_from_nullable_output(value_column);
        auto list_nested_column = list_value_column->get_data_ptr()->assume_mutable();
        std::vector<uint64_t> list_entry_counts;
        NullMap list_parent_nulls;
        const int16_t list_element_slot_definition_level =
                list_value_reader->nullable_definition_level() + 1;
        const int16_t list_element_max_definition_level =
                scalar_list_value_reader->descriptor()->max_definition_level();

        struct ListValueMapSink {
            MapColumnReader* self = nullptr;
            ScalarColumnReader* key_reader = nullptr;
            ListColumnReader* value_reader = nullptr;
            ScalarColumnReader* scalar_value_reader = nullptr;
            MutableColumnPtr* key_column = nullptr;
            MutableColumnPtr* list_nested_column = nullptr;
            std::vector<uint64_t>* entry_counts = nullptr;
            NullMap* parent_nulls = nullptr;
            std::vector<uint64_t>* list_entry_counts = nullptr;
            NullMap* list_parent_nulls = nullptr;
            int16_t key_max_definition_level = 0;
            int16_t list_element_max_definition_level = 0;
            NestedScalarBatch key_batch;
            int64_t key_level_idx = 0;

            Status read_key_batch(int64_t batch_rows, NestedScalarBatch* out_key_batch) {
                return read_nested_scalar_batch_from_overflow(*key_reader, batch_rows,
                                                              self->_nullable_definition_level + 1,
                                                              &self->_key_overflow, out_key_batch);
            }

            Status validate_key_batch(const NestedScalarBatch& value_batch,
                                      const NestedScalarBatch& candidate_key_batch) {
                if (candidate_key_batch.records_read != value_batch.records_read) {
                    return Status::Corruption(
                            "Parquet MAP key/value rows are not aligned for column {}: key "
                            "rows={}, "
                            "value rows={}",
                            self->_name, candidate_key_batch.records_read,
                            value_batch.records_read);
                }
                return Status::OK();
            }

            Status start_batch(const NestedScalarBatch& value_batch) {
                RETURN_IF_ERROR(read_key_batch(value_batch.records_read, &key_batch));
                RETURN_IF_ERROR(validate_key_batch(value_batch, key_batch));
                key_level_idx = 0;
                return Status::OK();
            }

            Status start_parent(const NestedScalarBatch& value_batch, int64_t level_idx) {
                const int16_t def_level = value_batch.def_levels[level_idx];
                if (def_level < self->_nullable_definition_level) {
                    if (!self->_type->is_nullable()) {
                        return Status::Corruption(
                                "Parquet MAP column {} contains null for non-nullable map",
                                self->_name);
                    }
                    RETURN_IF_ERROR(consume_key_slot(value_batch, level_idx, false));
                    entry_counts->push_back(0);
                    parent_nulls->push_back(1);
                    return Status::OK();
                }
                entry_counts->push_back(0);
                parent_nulls->push_back(0);
                if (def_level == self->_nullable_definition_level) {
                    RETURN_IF_ERROR(consume_key_slot(value_batch, level_idx, false));
                    return Status::OK();
                }
                return append_entry(value_batch, level_idx);
            }

            Status append_repeated(const NestedScalarBatch& value_batch, int64_t level_idx) {
                if (entry_counts->empty()) {
                    return Status::Corruption("Invalid repeated MAP level for column {}",
                                              self->_name);
                }
                if (value_batch.rep_levels[level_idx] < value_reader->repeated_repetition_level()) {
                    return append_entry(value_batch, level_idx);
                }
                if (list_entry_counts->empty()) {
                    return Status::Corruption("Invalid repeated MAP LIST value level for column {}",
                                              self->_name);
                }
                return append_list_element(value_batch, level_idx);
            }

            Status append_entry(const NestedScalarBatch& value_batch, int64_t level_idx) {
                RETURN_IF_ERROR(consume_key_slot(value_batch, level_idx, true));
                RETURN_IF_ERROR(append_scalar_batch_value(*key_reader, key_batch, key_level_idx - 1,
                                                          *key_column));
                ++entry_counts->back();
                const int16_t def_level = value_batch.def_levels[level_idx];
                if (def_level < value_reader->nullable_definition_level()) {
                    if (!value_reader->type()->is_nullable()) {
                        return Status::Corruption(
                                "Parquet MAP column {} contains null for non-nullable LIST value",
                                self->_name);
                    }
                    list_entry_counts->push_back(0);
                    list_parent_nulls->push_back(1);
                    return Status::OK();
                }
                list_entry_counts->push_back(0);
                list_parent_nulls->push_back(0);
                if (def_level == value_reader->nullable_definition_level()) {
                    return Status::OK();
                }
                return append_list_element(value_batch, level_idx);
            }

            Status consume_key_slot(const NestedScalarBatch& value_batch, int64_t value_level_idx,
                                    bool require_defined_key) {
                if (key_level_idx >= key_batch.levels_written) {
                    return Status::Corruption(
                            "Parquet MAP key stream ended before value stream for column {}",
                            self->_name);
                }
                if (key_batch.rep_levels[key_level_idx] !=
                    value_batch.rep_levels[value_level_idx]) {
                    return Status::Corruption(
                            "Parquet MAP key/value repetition levels are not aligned for column {}",
                            self->_name);
                }
                if (require_defined_key &&
                    key_batch.def_levels[key_level_idx] != key_max_definition_level) {
                    return Status::Corruption("Parquet MAP column {} contains null map key",
                                              self->_name);
                }
                ++key_level_idx;
                return Status::OK();
            }

            Status append_list_element(const NestedScalarBatch& value_batch, int64_t level_idx) {
                const int16_t def_level = value_batch.def_levels[level_idx];
                if (def_level == list_element_max_definition_level) {
                    RETURN_IF_ERROR(append_scalar_batch_value(*scalar_value_reader, value_batch,
                                                              level_idx, *list_nested_column));
                } else {
                    if (!scalar_value_reader->type()->is_nullable()) {
                        return Status::Corruption(
                                "Parquet MAP column {} contains null for non-nullable LIST value "
                                "element",
                                self->_name);
                    }
                    (*list_nested_column)->insert_default();
                }
                ++list_entry_counts->back();
                return Status::OK();
            }
        };

        ListValueMapSink sink;
        sink.self = this;
        sink.key_reader = key_reader;
        sink.value_reader = list_value_reader;
        sink.scalar_value_reader = scalar_list_value_reader;
        sink.key_column = &key_column;
        sink.list_nested_column = &list_nested_column;
        sink.entry_counts = &entry_counts;
        sink.parent_nulls = &parent_nulls;
        sink.list_entry_counts = &list_entry_counts;
        sink.list_parent_nulls = &list_parent_nulls;
        sink.key_max_definition_level = key_max_definition_level;
        sink.list_element_max_definition_level = list_element_max_definition_level;
        RETURN_IF_ERROR(assemble_repeated_levels(
                *scalar_list_value_reader, _repeated_repetition_level,
                list_element_slot_definition_level, rows, &_value_overflow, sink, rows_read));
        if (!_value_overflow.empty()) {
            move_nested_scalar_tail(sink.key_batch, sink.key_level_idx, &_key_overflow);
        }

        list_value_column->get_data_ptr() = std::move(list_nested_column);
        append_offsets(list_value_column->get_offsets(), list_entry_counts);
        append_parent_nulls(list_value_null_map, list_parent_nulls);
        map_column->get_keys_ptr() = std::move(key_column);
        map_column->get_values_ptr() = std::move(value_column);
        append_offsets(map_column->get_offsets(), entry_counts);
        append_parent_nulls(parent_null_map, parent_nulls);
        return Status::OK();
    }

    struct StructMapSink {
        MapColumnReader* self = nullptr;
        ScalarColumnReader* key_reader = nullptr;
        StructColumnReader* value_reader = nullptr;
        MutableColumnPtr* key_column = nullptr;
        MutableColumnPtr* value_column = nullptr;
        std::vector<uint64_t>* entry_counts = nullptr;
        NullMap* parent_nulls = nullptr;
        int16_t key_max_definition_level = 0;

        Status read_value_batch(int64_t batch_rows, NestedStructBatch* out_value_batch) {
            return read_nested_struct_batch_from_overflow(
                    *value_reader, batch_rows, self->_nullable_definition_level + 1,
                    &self->_struct_value_overflow, out_value_batch);
        }

        Status validate_value_alignment(const NestedScalarBatch& key_batch,
                                        const NestedStructBatch& candidate_value_batch) {
            return validate_nested_struct_alignment(self->_name, key_batch, candidate_value_batch,
                                                    "");
        }

        Status start_batch(const NestedScalarBatch& key_batch) {
            RETURN_IF_ERROR(read_value_batch(key_batch.records_read, &value_batch));
            RETURN_IF_ERROR(validate_value_alignment(key_batch, value_batch));
            return Status::OK();
        }

        Status start_parent(const NestedScalarBatch& key_batch, int64_t level_idx) {
            const int16_t def_level = key_batch.def_levels[level_idx];
            if (def_level < self->_nullable_definition_level) {
                if (!self->_type->is_nullable()) {
                    return Status::Corruption(
                            "Parquet MAP column {} contains null for non-nullable map",
                            self->_name);
                }
                entry_counts->push_back(0);
                parent_nulls->push_back(1);
                RETURN_IF_ERROR(value_reader->skip_non_scalar_children(1));
                return Status::OK();
            }
            entry_counts->push_back(0);
            parent_nulls->push_back(0);
            if (def_level == self->_nullable_definition_level) {
                RETURN_IF_ERROR(value_reader->skip_non_scalar_children(1));
                return Status::OK();
            }
            return append_entry(key_batch, level_idx);
        }

        Status append_repeated(const NestedScalarBatch& key_batch, int64_t level_idx) {
            if (entry_counts->empty()) {
                return Status::Corruption("Invalid repeated MAP level for column {}", self->_name);
            }
            return append_entry(key_batch, level_idx);
        }

        Status append_entry(const NestedScalarBatch& key_batch, int64_t level_idx) {
            if (key_batch.def_levels[level_idx] != key_max_definition_level) {
                return Status::Corruption("Parquet MAP column {} contains null map key",
                                          self->_name);
            }
            RETURN_IF_ERROR(
                    append_scalar_batch_value(*key_reader, key_batch, level_idx, *key_column));
            RETURN_IF_ERROR(append_struct_batch_value(*value_reader, value_batch, level_idx,
                                                      *value_column));
            ++entry_counts->back();
            return Status::OK();
        }

        NestedStructBatch value_batch;
    };

    StructMapSink sink;
    sink.self = this;
    sink.key_reader = key_reader;
    sink.value_reader = struct_value_reader;
    sink.key_column = &key_column;
    sink.value_column = &value_column;
    sink.entry_counts = &entry_counts;
    sink.parent_nulls = &parent_nulls;
    sink.key_max_definition_level = key_max_definition_level;
    RETURN_IF_ERROR(assemble_repeated_levels(*key_reader, _repeated_repetition_level,
                                             entry_definition_level, rows, &_key_overflow, sink,
                                             rows_read));
    if (!_key_overflow.empty()) {
        move_nested_struct_tail(
                sink.value_batch,
                sink.value_batch.levels_written - _key_overflow.batch.levels_written,
                &_struct_value_overflow);
    }

    map_column->get_keys_ptr() = std::move(key_column);
    map_column->get_values_ptr() = std::move(value_column);
    append_offsets(map_column->get_offsets(), entry_counts);
    append_parent_nulls(parent_null_map, parent_nulls);
    return Status::OK();
}

Status MapColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }
    DORIS_CHECK(_key_reader != nullptr);
    DORIS_CHECK(_value_reader != nullptr);
    auto* key_reader = dynamic_cast<ScalarColumnReader*>(_key_reader.get());
    auto* value_reader = dynamic_cast<ScalarColumnReader*>(_value_reader.get());
    auto* struct_value_reader = dynamic_cast<StructColumnReader*>(_value_reader.get());
    auto* list_value_reader = dynamic_cast<ListColumnReader*>(_value_reader.get());
    if (key_reader == nullptr || (value_reader == nullptr && struct_value_reader == nullptr &&
                                  list_value_reader == nullptr)) {
        return Status::NotSupported(
                "Current parquet MAP reader only supports scalar key with scalar, scalar-child "
                "STRUCT, or scalar LIST value for column {}",
                _name);
    }

    int64_t rows_read = 0;
    if (value_reader != nullptr) {
        struct SkipSink {
            MapColumnReader* self = nullptr;
            ScalarColumnReader* value_reader = nullptr;

            Status read_value_batch(int64_t batch_rows, NestedScalarBatch* out_value_batch) {
                return read_nested_scalar_batch_from_overflow(
                        *value_reader, batch_rows, self->_nullable_definition_level + 1,
                        &self->_value_overflow, out_value_batch);
            }

            Status validate_value_alignment(const NestedScalarBatch& key_batch,
                                            const NestedScalarBatch& candidate_value_batch) {
                return validate_nested_scalar_alignment(
                        self->_name, key_batch, candidate_value_batch, "value", " while skipping");
            }

            Status start_batch(const NestedScalarBatch& key_batch) {
                RETURN_IF_ERROR(read_value_batch(key_batch.records_read, &value_batch));
                RETURN_IF_ERROR(validate_value_alignment(key_batch, value_batch));
                return Status::OK();
            }

            Status start_parent(const NestedScalarBatch&, int64_t) { return Status::OK(); }

            Status append_repeated(const NestedScalarBatch&, int64_t) { return Status::OK(); }

            NestedScalarBatch value_batch;
        };
        SkipSink sink;
        sink.self = this;
        sink.value_reader = value_reader;
        RETURN_IF_ERROR(assemble_repeated_levels(*key_reader, _repeated_repetition_level,
                                                 _nullable_definition_level + 1, rows,
                                                 &_key_overflow, sink, &rows_read));
        if (!_key_overflow.empty()) {
            move_nested_scalar_tail(
                    sink.value_batch,
                    sink.value_batch.levels_written - _key_overflow.batch.levels_written,
                    &_value_overflow);
        }
    } else if (struct_value_reader != nullptr) {
        struct StructSkipSink {
            MapColumnReader* self = nullptr;
            StructColumnReader* value_reader = nullptr;

            Status read_value_batch(int64_t batch_rows, NestedStructBatch* out_value_batch) {
                return read_nested_struct_batch_from_overflow(
                        *value_reader, batch_rows, self->_nullable_definition_level + 1,
                        &self->_struct_value_overflow, out_value_batch);
            }

            Status validate_value_alignment(const NestedScalarBatch& key_batch,
                                            const NestedStructBatch& candidate_value_batch) {
                return validate_nested_struct_alignment(self->_name, key_batch,
                                                        candidate_value_batch, " while skipping");
            }

            Status start_batch(const NestedScalarBatch& key_batch) {
                RETURN_IF_ERROR(read_value_batch(key_batch.records_read, &value_batch));
                RETURN_IF_ERROR(validate_value_alignment(key_batch, value_batch));
                return Status::OK();
            }

            Status start_parent(const NestedScalarBatch&, int64_t) { return Status::OK(); }

            Status append_repeated(const NestedScalarBatch&, int64_t) { return Status::OK(); }

            NestedStructBatch value_batch;
        };
        StructSkipSink sink;
        sink.self = this;
        sink.value_reader = struct_value_reader;
        RETURN_IF_ERROR(assemble_repeated_levels(*key_reader, _repeated_repetition_level,
                                                 _nullable_definition_level + 1, rows,
                                                 &_key_overflow, sink, &rows_read));
        if (!_key_overflow.empty()) {
            move_nested_struct_tail(
                    sink.value_batch,
                    sink.value_batch.levels_written - _key_overflow.batch.levels_written,
                    &_struct_value_overflow);
        }
    } else {
        auto* scalar_list_value_reader =
                dynamic_cast<ScalarColumnReader*>(list_value_reader->element_reader());
        if (scalar_list_value_reader == nullptr) {
            return Status::NotSupported(
                    "Current parquet MAP LIST value skip only supports scalar list values for "
                    "column {}",
                    _name);
        }
        struct ListSkipSink {
            MapColumnReader* self = nullptr;
            ScalarColumnReader* key_reader = nullptr;
            ListColumnReader* value_reader = nullptr;
            NestedScalarBatch key_batch;
            int64_t key_level_idx = 0;

            Status read_key_batch(int64_t batch_rows, NestedScalarBatch* out_key_batch) {
                return read_nested_scalar_batch_from_overflow(*key_reader, batch_rows,
                                                              self->_nullable_definition_level + 1,
                                                              &self->_key_overflow, out_key_batch);
            }

            Status validate_key_batch(const NestedScalarBatch& value_batch,
                                      const NestedScalarBatch& candidate_key_batch) {
                if (candidate_key_batch.records_read != value_batch.records_read) {
                    return Status::Corruption(
                            "Parquet MAP key/value rows are not aligned for column {} while "
                            "skipping",
                            self->_name);
                }
                return Status::OK();
            }

            Status start_batch(const NestedScalarBatch& value_batch) {
                RETURN_IF_ERROR(read_key_batch(value_batch.records_read, &key_batch));
                RETURN_IF_ERROR(validate_key_batch(value_batch, key_batch));
                key_level_idx = 0;
                return Status::OK();
            }

            Status start_parent(const NestedScalarBatch& value_batch, int64_t level_idx) {
                return consume_key_slot(value_batch, level_idx);
            }

            Status append_repeated(const NestedScalarBatch& value_batch, int64_t level_idx) {
                if (value_batch.rep_levels[level_idx] < value_reader->repeated_repetition_level()) {
                    return consume_key_slot(value_batch, level_idx);
                }
                return Status::OK();
            }

            Status consume_key_slot(const NestedScalarBatch& value_batch, int64_t value_level_idx) {
                if (key_level_idx >= key_batch.levels_written) {
                    return Status::Corruption(
                            "Parquet MAP key stream ended before value stream for column {} while "
                            "skipping",
                            self->_name);
                }
                if (key_batch.rep_levels[key_level_idx] !=
                    value_batch.rep_levels[value_level_idx]) {
                    return Status::Corruption(
                            "Parquet MAP key/value repetition levels are not aligned for column {} "
                            "while skipping",
                            self->_name);
                }
                ++key_level_idx;
                return Status::OK();
            }
        };
        ListSkipSink sink;
        sink.self = this;
        sink.key_reader = key_reader;
        sink.value_reader = list_value_reader;
        RETURN_IF_ERROR(assemble_repeated_levels(*scalar_list_value_reader,
                                                 _repeated_repetition_level,
                                                 list_value_reader->nullable_definition_level() + 1,
                                                 rows, &_value_overflow, sink, &rows_read));
        if (!_value_overflow.empty()) {
            move_nested_scalar_tail(sink.key_batch, sink.key_level_idx, &_key_overflow);
        }
    }
    if (rows_read != rows) {
        return Status::Corruption("Failed to skip parquet MAP column {}: skipped {} of {} rows",
                                  _name, rows_read, rows);
    }
    return Status::OK();
}

Status ParquetColumnReader::skip(int64_t rows) {
    return Status::NotSupported("Parquet column skip is not implemented, rows={}", rows);
}

Status ParquetColumnReader::select(const SelectionVector& sel, uint16_t selected_rows,
                                   int64_t batch_rows, MutableColumnPtr& column) {
    if (column.get() == nullptr) {
        return Status::InvalidArgument("Parquet selected read result is null for column {}",
                                       name());
    }
    RETURN_IF_ERROR(sel.verify(selected_rows, batch_rows));

    const auto ranges = selection_to_ranges(sel, selected_rows);
    int64_t cursor = 0;
    for (const auto& range : ranges) {
        if (range.start < cursor || range.start + range.length > batch_rows) {
            return Status::InvalidArgument("Invalid parquet selection range [{}, {}) for column {}",
                                           range.start, range.start + range.length, name());
        }
        RETURN_IF_ERROR(skip(range.start - cursor));

        int64_t range_rows_read = 0;
        RETURN_IF_ERROR(read(range.length, column, &range_rows_read));
        if (range_rows_read != range.length) {
            return Status::Corruption(
                    "Parquet selected read returned {} rows, expected {} rows for column {}",
                    range_rows_read, range.length, name());
        }
        cursor = range.start + range.length;
    }
    RETURN_IF_ERROR(skip(batch_rows - cursor));
    return Status::OK();
}

ParquetColumnReaderFactory::ParquetColumnReaderFactory(
        std::shared_ptr<::parquet::RowGroupReader> row_group, int num_leaf_columns)
        : _row_group(std::move(row_group)),
          _record_readers(static_cast<size_t>(num_leaf_columns)) {}

reader::SchemaField ParquetColumnReaderFactory::row_position_schema_field() {
    reader::SchemaField field;
    field.id = ROW_POSITION_COLUMN_ID;
    field.name = ROW_POSITION_COLUMN_NAME;
    field.type = std::make_shared<DataTypeInt64>();
    field.column_type = reader::ColumnType::ROW_NUMBER;
    return field;
}

std::unique_ptr<ParquetColumnReader> ParquetColumnReaderFactory::create_row_position_column_reader(
        int64_t row_group_first_row) const {
    return std::make_unique<RowPositionColumnReader>(row_group_first_row);
}

Status ParquetColumnReaderFactory::create_scalar_reader(
        int parquet_leaf_column_id, const ParquetTypeDescriptor& type_descriptor,
        const ::parquet::ColumnDescriptor* descriptor, DataTypePtr type, std::string name,
        std::shared_ptr<::parquet::internal::RecordReader> record_reader,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (descriptor == nullptr || type == nullptr || record_reader == nullptr) {
        return Status::InvalidArgument("Invalid parquet column reader arguments for column {}",
                                       name);
    }
    *reader = std::make_unique<ScalarColumnReader>(parquet_leaf_column_id, descriptor,
                                                   type_descriptor, std::move(type),
                                                   std::move(name), std::move(record_reader));
    return Status::OK();
}

Status ParquetColumnReaderFactory::create_scalar_column_reader(
        const ParquetColumnSchema& column_schema,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (column_schema.leaf_column_id < 0 ||
        column_schema.leaf_column_id >= static_cast<int>(_record_readers.size())) {
        return Status::InvalidArgument("Invalid parquet leaf column id {} for column {}",
                                       column_schema.leaf_column_id, column_schema.name);
    }
    if (!supports_record_reader(column_schema.type_descriptor)) {
        return Status::NotSupported(
                "Current parquet reader only supports primitive columns without repetition; "
                "column {} is not supported",
                column_schema.name);
    }
    if (column_schema.descriptor == nullptr ||
        column_schema.descriptor->max_repetition_level() != 0 ||
        column_schema.descriptor->max_definition_level() > 1) {
        return Status::NotSupported(
                "Current parquet scalar reader only supports flat primitive columns; column {} is "
                "not supported",
                column_schema.name);
    }
    std::shared_ptr<::parquet::internal::RecordReader> record_reader;
    RETURN_IF_ERROR(get_record_reader(column_schema.leaf_column_id, column_schema.descriptor,
                                      column_schema.name, &record_reader));
    return create_scalar_reader(column_schema.leaf_column_id, column_schema.type_descriptor,
                                column_schema.descriptor, column_schema.type, column_schema.name,
                                std::move(record_reader), reader);
}

Status ParquetColumnReaderFactory::create_nested_scalar_column_reader(
        const ParquetColumnSchema& column_schema,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (column_schema.kind != ParquetColumnSchemaKind::PRIMITIVE) {
        return Status::InvalidArgument("Parquet nested scalar reader requires primitive column {}",
                                       column_schema.name);
    }
    if (column_schema.leaf_column_id < 0 ||
        column_schema.leaf_column_id >= static_cast<int>(_record_readers.size())) {
        return Status::InvalidArgument("Invalid parquet leaf column id {} for column {}",
                                       column_schema.leaf_column_id, column_schema.name);
    }
    if (!supports_nested_scalar_record_reader(column_schema)) {
        return Status::NotSupported(
                "Current parquet nested scalar reader does not support column {}",
                column_schema.name);
    }
    std::shared_ptr<::parquet::internal::RecordReader> record_reader;
    RETURN_IF_ERROR(get_record_reader(column_schema.leaf_column_id, column_schema.descriptor,
                                      column_schema.name, &record_reader));
    return create_scalar_reader(column_schema.leaf_column_id, column_schema.type_descriptor,
                                column_schema.descriptor, column_schema.type, column_schema.name,
                                std::move(record_reader), reader);
}

Status ParquetColumnReaderFactory::get_record_reader(
        int leaf_column_id, const ::parquet::ColumnDescriptor* descriptor, const std::string& name,
        std::shared_ptr<::parquet::internal::RecordReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (_row_group == nullptr) {
        return Status::InternalError("Parquet row group reader is not initialized for column {}",
                                     name);
    }
    if (leaf_column_id < 0 || leaf_column_id >= static_cast<int>(_record_readers.size())) {
        return Status::InvalidArgument("Invalid parquet leaf column id {} for column {}",
                                       leaf_column_id, name);
    }
    if (descriptor == nullptr) {
        return Status::InvalidArgument("Parquet column descriptor is null for column {}", name);
    }
    if (_record_readers[leaf_column_id] == nullptr) {
        try {
            _record_readers[leaf_column_id] =
                    _row_group->RecordReader(leaf_column_id, /*read_dictionary=*/false);
        } catch (const ::parquet::ParquetException& e) {
            return Status::Corruption("Failed to create parquet record reader for column {}: {}",
                                      name, e.what());
        } catch (const std::exception& e) {
            return Status::InternalError("Failed to create parquet record reader for column {}: {}",
                                         name, e.what());
        }
    }
    if (_record_readers[leaf_column_id] == nullptr) {
        return Status::Corruption("Failed to create parquet record reader for column {}", name);
    }
    *reader = _record_readers[leaf_column_id];
    return Status::OK();
}

Status ParquetColumnReaderFactory::create_struct_column_reader(
        const ParquetColumnSchema& column_schema, const reader::FieldProjection* projection,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    std::vector<std::unique_ptr<ParquetColumnReader>> child_readers;
    child_readers.reserve(column_schema.children.size());
    std::vector<int> child_output_indices;
    child_output_indices.reserve(column_schema.children.size());
    DataTypes projected_child_types;
    Strings projected_child_names;
    for (size_t child_idx = 0; child_idx < column_schema.children.size(); ++child_idx) {
        const auto& child_schema = column_schema.children[child_idx];
        const auto* child_projection = find_child_projection(projection, *child_schema);
        const bool child_is_projected = projection == nullptr || projection->project_all_children ||
                                        child_projection != nullptr;
        std::unique_ptr<ParquetColumnReader> child_reader;
        if (child_schema->kind == ParquetColumnSchemaKind::PRIMITIVE) {
            RETURN_IF_ERROR(create_nested_scalar_column_reader(*child_schema, &child_reader));
        } else {
            RETURN_IF_ERROR(create(*child_schema, child_projection, &child_reader));
        }
        if (child_is_projected) {
            child_output_indices.push_back(static_cast<int>(projected_child_types.size()));
            projected_child_types.push_back(child_reader->type());
            projected_child_names.push_back(child_reader->name());
        } else {
            if (child_schema->kind != ParquetColumnSchemaKind::PRIMITIVE) {
                child_reader = std::make_unique<ShapeOnlyColumnReader>(std::move(child_reader));
            }
            child_output_indices.push_back(-1);
        }
        child_readers.push_back(std::move(child_reader));
    }
    if (projected_child_types.empty() && !column_schema.children.empty()) {
        return Status::NotSupported("Parquet STRUCT projection for column {} contains no children",
                                    column_schema.name);
    }
    DataTypePtr type = column_schema.type;
    if (projection != nullptr && !projection->project_all_children) {
        type = std::make_shared<DataTypeStruct>(projected_child_types, projected_child_names);
        if (column_schema.type != nullptr && column_schema.type->is_nullable()) {
            type = make_nullable(type);
        }
    }
    *reader = std::make_unique<StructColumnReader>(column_schema, std::move(type),
                                                   std::move(child_readers),
                                                   std::move(child_output_indices));
    return Status::OK();
}

Status ParquetColumnReaderFactory::create_list_column_reader(
        const ParquetColumnSchema& column_schema, const reader::FieldProjection* projection,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (column_schema.children.size() != 1) {
        return Status::NotSupported("Unsupported parquet LIST layout for column {}",
                                    column_schema.name);
    }
    std::unique_ptr<ParquetColumnReader> element_reader;
    const auto& element_schema = *column_schema.children[0];
    const auto* element_projection = find_child_projection(projection, element_schema);
    if (projection != nullptr && !projection->project_all_children &&
        element_projection == nullptr) {
        return Status::NotSupported("Parquet LIST projection for column {} contains no element",
                                    column_schema.name);
    }
    if (element_schema.kind == ParquetColumnSchemaKind::PRIMITIVE) {
        if (element_projection != nullptr && !element_projection->project_all_children) {
            return Status::InvalidArgument(
                    "Parquet LIST scalar element projection is invalid for column {}",
                    column_schema.name);
        }
        RETURN_IF_ERROR(create_nested_scalar_column_reader(element_schema, &element_reader));
    } else if (element_schema.kind == ParquetColumnSchemaKind::STRUCT) {
        RETURN_IF_ERROR(
                create_struct_column_reader(element_schema, element_projection, &element_reader));
    } else if (element_schema.kind == ParquetColumnSchemaKind::LIST) {
        RETURN_IF_ERROR(
                create_list_column_reader(element_schema, element_projection, &element_reader));
    } else {
        return Status::NotSupported(
                "Current parquet LIST reader does not support nested complex element column {}",
                column_schema.name);
    }
    DataTypePtr type = column_schema.type;
    if (element_projection != nullptr && !element_projection->project_all_children) {
        type = std::make_shared<DataTypeArray>(element_reader->type());
        if (column_schema.type != nullptr && column_schema.type->is_nullable()) {
            type = make_nullable(type);
        }
    }
    *reader = std::make_unique<ListColumnReader>(column_schema, std::move(type),
                                                 std::move(element_reader));
    return Status::OK();
}

Status ParquetColumnReaderFactory::create_map_column_reader(
        const ParquetColumnSchema& column_schema, const reader::FieldProjection* projection,
        std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    if (column_schema.children.size() != 1 || column_schema.children[0]->children.size() != 2) {
        return Status::NotSupported("Unsupported parquet MAP layout for column {}",
                                    column_schema.name);
    }
    const auto& key_value_schema = *column_schema.children[0];
    const auto* key_value_projection = find_child_projection(projection, key_value_schema);
    if (projection != nullptr && !projection->project_all_children &&
        key_value_projection == nullptr) {
        return Status::NotSupported("Parquet MAP projection for column {} contains no entry",
                                    column_schema.name);
    }
    std::unique_ptr<ParquetColumnReader> key_reader;
    RETURN_IF_ERROR(create_nested_scalar_column_reader(*key_value_schema.children[0], &key_reader));
    std::unique_ptr<ParquetColumnReader> value_reader;
    const auto& value_schema = *key_value_schema.children[1];
    const auto* value_projection = find_child_projection(key_value_projection, value_schema);
    if (value_schema.kind == ParquetColumnSchemaKind::PRIMITIVE) {
        if (value_projection != nullptr && !value_projection->project_all_children) {
            return Status::InvalidArgument(
                    "Parquet MAP scalar value projection is invalid for column {}",
                    column_schema.name);
        }
        RETURN_IF_ERROR(create_nested_scalar_column_reader(value_schema, &value_reader));
    } else if (value_schema.kind == ParquetColumnSchemaKind::STRUCT) {
        RETURN_IF_ERROR(create_struct_column_reader(value_schema, value_projection, &value_reader));
    } else if (value_schema.kind == ParquetColumnSchemaKind::LIST) {
        RETURN_IF_ERROR(create_list_column_reader(value_schema, value_projection, &value_reader));
    } else {
        return Status::NotSupported(
                "Current parquet MAP reader does not support nested complex value column {}",
                column_schema.name);
    }
    DataTypePtr type = column_schema.type;
    if (value_projection != nullptr && !value_projection->project_all_children) {
        type = std::make_shared<DataTypeMap>(key_reader->type(), value_reader->type());
        if (column_schema.type != nullptr && column_schema.type->is_nullable()) {
            type = make_nullable(type);
        }
    }
    *reader = std::make_unique<MapColumnReader>(column_schema, std::move(type),
                                                std::move(key_reader), std::move(value_reader));
    return Status::OK();
}

Status ParquetColumnReaderFactory::create(const ParquetColumnSchema& column_schema,
                                          const reader::FieldProjection* projection,
                                          std::unique_ptr<ParquetColumnReader>* reader) const {
    if (reader == nullptr) {
        return Status::InvalidArgument("reader is null");
    }
    switch (column_schema.kind) {
    case ParquetColumnSchemaKind::PRIMITIVE:
        return create_scalar_column_reader(column_schema, reader);
    case ParquetColumnSchemaKind::STRUCT:
        return create_struct_column_reader(column_schema, projection, reader);
    case ParquetColumnSchemaKind::LIST:
        return create_list_column_reader(column_schema, projection, reader);
    case ParquetColumnSchemaKind::MAP:
        return create_map_column_reader(column_schema, projection, reader);
    }
    return Status::NotSupported("Unsupported parquet column schema kind for column {}",
                                column_schema.name);
}

} // namespace doris::parquet
