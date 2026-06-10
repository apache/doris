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

#include "format_v2/parquet/reader/struct_column_reader.h"

#include <parquet/api/schema.h>

#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

#include "core/column/column_struct.h"
#include "format_v2/parquet/reader/nested_column_materializer.h"
#include "format_v2/parquet/reader/scalar_column_reader.h"

namespace doris::parquet {
namespace {

ParquetColumnReader* struct_shape_source_reader(const StructColumnReader& reader) {
    for (size_t child_idx = 0; child_idx < reader.child_count(); ++child_idx) {
        auto* child_reader = reader.child_reader(child_idx);
        DORIS_CHECK(child_reader != nullptr);
        if (!child_reader->is_or_has_repeated_child()) {
            return child_reader;
        }
    }
    if (reader.child_count() == 0) {
        return nullptr;
    }
    return reader.child_reader(0);
}

} // namespace

Status StructColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    RETURN_IF_ERROR(load_nested_batch(rows));
    return build_nested_column(rows, column, rows_read);
}

Status StructColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }
    auto scratch_column = _type->create_column();
    RETURN_IF_ERROR(load_nested_batch(rows));
    int64_t rows_read = 0;
    RETURN_IF_ERROR(build_nested_column(rows, scratch_column, &rows_read));
    if (rows_read != rows) {
        return Status::Corruption("Failed to skip parquet STRUCT column {}: skipped {} of {} rows",
                                  _name, rows_read, rows);
    }
    update_reader_skip_rows(rows);
    return Status::OK();
}

Status StructColumnReader::load_nested_batch(int64_t rows) {
    reset_nested_build_level_cursor();
    for (auto& child_reader : _children) {
        DORIS_CHECK(child_reader != nullptr);
        RETURN_IF_ERROR(child_reader->load_nested_batch(rows));
    }
    return Status::OK();
}

Status StructColumnReader::build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                                               int64_t* values_read) {
    if (column.get() == nullptr || values_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet struct build result pointer for column {}",
                                       _name);
    }
    if (_children.empty()) {
        column->resize(column->size() + static_cast<size_t>(length_upper_bound));
        *values_read = length_upper_bound;
        return Status::OK();
    }
    auto* struct_column = struct_column_from_output(column);
    DORIS_CHECK(struct_column != nullptr);
    auto* parent_null_map = null_map_from_nullable_output(column);
    auto* shape_reader = struct_shape_source_reader(*this);
    DORIS_CHECK(shape_reader != nullptr);
    const auto& def_levels = shape_reader->nested_definition_levels();
    const auto& rep_levels = shape_reader->nested_repetition_levels();
    const int64_t levels_written = shape_reader->nested_levels_written();

    NullMap parent_nulls;
    std::vector<int64_t> parent_level_indices;
    *values_read = 0;
    int64_t level_idx = nested_build_level_cursor();
    while (level_idx < levels_written) {
        const int64_t current_level_idx = level_idx;
        const int16_t def_level = def_levels[level_idx];
        const int16_t rep_level = rep_levels[level_idx];
        const bool starts_parent =
                !shape_reader->is_or_has_repeated_child() || rep_level <= _repetition_level;
        if (starts_parent && *values_read >= length_upper_bound) {
            break;
        }
        ++level_idx;
        if (def_level < _repeated_ancestor_definition_level) {
            continue;
        }
        if (shape_reader->is_or_has_repeated_child() && rep_level > _repetition_level) {
            continue;
        }
        const bool parent_is_null = def_level < _nullable_definition_level;
        if (parent_is_null && parent_null_map == nullptr) {
            return Status::Corruption(
                    "Parquet STRUCT column {} contains null for non-nullable struct", _name);
        }
        parent_nulls.push_back(parent_is_null);
        parent_level_indices.push_back(current_level_idx);
        ++*values_read;
    }
    set_nested_build_level_cursor(level_idx);

    std::vector<MutableColumnPtr> child_columns;
    child_columns.reserve(struct_column->get_columns().size());
    for (size_t child_idx = 0; child_idx < struct_column->get_columns().size(); ++child_idx) {
        child_columns.push_back(struct_column->get_column_ptr(child_idx)->assert_mutable());
    }
    for (size_t child_idx = 0; child_idx < _children.size(); ++child_idx) {
        const int output_idx = _child_output_indices[child_idx];
        if (output_idx < 0) {
            continue;
        }
        // STRUCT owns row alignment. Child readers consume only present parent rows from their
        // level streams; null STRUCT parents become default placeholders in every child column.
        // This mirrors Arrow's separation between struct validity and child array materialization,
        // and avoids asking scalar/list/map children to invent values for an absent parent.
        int64_t pending_present_rows = 0;
        int64_t total_child_rows = 0;
        auto flush_present_rows = [&]() -> Status {
            if (pending_present_rows == 0) {
                return Status::OK();
            }
            int64_t child_rows = 0;
            RETURN_IF_ERROR(_children[child_idx]->build_nested_column(
                    pending_present_rows, child_columns[output_idx], &child_rows));
            if (child_rows != pending_present_rows) {
                return Status::Corruption(
                        "Parquet STRUCT child {} built {} rows, expected {} for column {}",
                        _children[child_idx]->name(), child_rows, pending_present_rows, _name);
            }
            total_child_rows += child_rows;
            pending_present_rows = 0;
            return Status::OK();
        };
        for (size_t parent_idx = 0; parent_idx < parent_nulls.size(); ++parent_idx) {
            const auto parent_is_null = parent_nulls[parent_idx];
            if (!parent_is_null) {
                ++pending_present_rows;
                continue;
            }
            RETURN_IF_ERROR(flush_present_rows());
            child_columns[output_idx]->insert_default();
            if (auto* scalar_child =
                        dynamic_cast<ScalarColumnReader*>(_children[child_idx].get())) {
                // Scalar struct children have one level slot for a null struct parent, but that
                // slot is below the scalar value threshold. Advance past the exact parent level,
                // not just one slot from the current child cursor: the cursor may be parked before
                // ancestor LIST/MAP null/empty shape slots after a previous present-row flush.
                const int64_t child_cursor = scalar_child->nested_build_level_cursor();
                const int64_t next_child_cursor = parent_level_indices[parent_idx] + 1;
                if (next_child_cursor > scalar_child->nested_levels_written()) {
                    return Status::Corruption(
                            "Parquet STRUCT child {} ended before null parent row in column {}",
                            scalar_child->name(), _name);
                }
                scalar_child->set_nested_build_level_cursor(
                        std::max(child_cursor, next_child_cursor));
            } else {
                RETURN_IF_ERROR(_children[child_idx]->skip_nested_column(1));
            }
            ++total_child_rows;
        }
        RETURN_IF_ERROR(flush_present_rows());
        if (total_child_rows != *values_read) {
            return Status::Corruption(
                    "Parquet STRUCT child {} built {} rows, expected {} for column {}",
                    _children[child_idx]->name(), total_child_rows, *values_read, _name);
        }
    }
    for (size_t child_idx = 0; child_idx < child_columns.size(); ++child_idx) {
        struct_column->get_column_ptr(child_idx) = std::move(child_columns[child_idx]);
    }
    append_parent_nulls(parent_null_map, parent_nulls);
    return Status::OK();
}

const std::vector<int16_t>& StructColumnReader::nested_definition_levels() const {
    auto* shape_reader = struct_shape_source_reader(*this);
    DORIS_CHECK(shape_reader != nullptr);
    return shape_reader->nested_definition_levels();
}

const std::vector<int16_t>& StructColumnReader::nested_repetition_levels() const {
    auto* shape_reader = struct_shape_source_reader(*this);
    DORIS_CHECK(shape_reader != nullptr);
    return shape_reader->nested_repetition_levels();
}

int64_t StructColumnReader::nested_levels_written() const {
    auto* shape_reader = struct_shape_source_reader(*this);
    DORIS_CHECK(shape_reader != nullptr);
    return shape_reader->nested_levels_written();
}

bool StructColumnReader::is_or_has_repeated_child() const {
    auto* shape_reader = struct_shape_source_reader(*this);
    return shape_reader != nullptr && shape_reader->is_or_has_repeated_child();
}

} // namespace doris::parquet
