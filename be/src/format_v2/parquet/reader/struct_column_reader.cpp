// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "format_v2/parquet/reader/struct_column_reader.h"

#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

#include "core/column/column_struct.h"
#include "format_v2/parquet/reader/nested_column_materializer.h"
#include "format_v2/parquet/reader/scalar_column_reader.h"

namespace doris::format::parquet {

ParquetColumnReader* StructColumnReader::shape_source_reader() const {
    for (const auto& child : _children) {
        auto* child_reader = child.get();
        DORIS_CHECK(child_reader != nullptr);
        if (!child_reader->is_or_has_repeated_child()) {
            return child_reader;
        }
    }
    if (_children.empty()) {
        return nullptr;
    }
    return _children[0].get();
}

Status StructColumnReader::advance_child_past_null_parent(ParquetColumnReader* child_reader,
                                                          int64_t parent_level_idx) const {
    DORIS_CHECK(child_reader != nullptr);
    const int64_t next_child_cursor = parent_level_idx + 1;
    if (auto* scalar_child = dynamic_cast<ScalarColumnReader*>(child_reader)) {
        if (next_child_cursor > scalar_child->nested_levels_written()) {
            return Status::Corruption(
                    "Parquet STRUCT child {} ended before null parent row in column {}",
                    scalar_child->name(), _name);
        }
        scalar_child->set_nested_build_level_cursor(
                std::max(scalar_child->nested_build_level_cursor(), next_child_cursor));
        return Status::OK();
    }
    if (auto* struct_child = dynamic_cast<StructColumnReader*>(child_reader);
        struct_child != nullptr && !struct_child->is_or_has_repeated_child()) {
        if (next_child_cursor > struct_child->nested_levels_written()) {
            return Status::Corruption(
                    "Parquet STRUCT child {} ended before null parent row in column {}",
                    struct_child->name(), _name);
        }
        struct_child->set_nested_build_level_cursor(
                std::max(struct_child->nested_build_level_cursor(), next_child_cursor));
        for (auto& grandchild : struct_child->_children) {
            RETURN_IF_ERROR(struct_child->advance_child_past_null_parent(grandchild.get(),
                                                                         parent_level_idx));
        }
        return Status::OK();
    }

    int64_t child_cursor = child_reader->nested_build_level_cursor();
    const auto& child_rep_levels = child_reader->nested_repetition_levels();
    const int64_t child_levels_written = child_reader->nested_levels_written();
    while (child_cursor < child_levels_written) {
        const int16_t child_rep_level = child_rep_levels[child_cursor];
        ++child_cursor;
        if (!child_reader->is_or_has_repeated_child() || child_rep_level <= _repetition_level) {
            break;
        }
    }
    child_reader->set_nested_build_level_cursor(child_cursor);
    return Status::OK();
}

Status StructColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    RETURN_IF_ERROR(load_nested_batch(rows));
    return build_nested_column(rows, column, rows_read);
}

Status StructColumnReader::skip(int64_t rows) {
    return skip_nested_rows(rows);
}

Status StructColumnReader::load_nested_batch(int64_t rows) {
    reset_nested_build_level_cursor();
    for (auto& child_reader : _children) {
        DORIS_CHECK(child_reader != nullptr);
        RETURN_IF_ERROR(child_reader->load_nested_batch(rows));
    }
    return Status::OK();
}

Status StructColumnReader::load_nested_levels_batch(int64_t rows) {
    reset_nested_build_level_cursor();
    for (auto& child_reader : _children) {
        DORIS_CHECK(child_reader != nullptr);
        RETURN_IF_ERROR(child_reader->load_nested_levels_batch(rows));
    }
    return Status::OK();
}

Status StructColumnReader::build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                                               int64_t* values_read) {
    if (column.get() == nullptr) {
        return Status::InvalidArgument("Invalid parquet struct build result pointer for column {}",
                                       _name);
    }
    return _consume_or_build_nested_column(length_upper_bound, &column, values_read);
}

Status StructColumnReader::consume_nested_column(int64_t length_upper_bound,
                                                 int64_t* values_consumed) {
    return _consume_or_build_nested_column(length_upper_bound, nullptr, values_consumed);
}

Status StructColumnReader::_consume_or_build_nested_column(int64_t length_upper_bound,
                                                           MutableColumnPtr* column,
                                                           int64_t* values_processed) {
    if (values_processed == nullptr) {
        return Status::InvalidArgument(
                "Invalid parquet struct process result pointer for column {}", _name);
    }
    if (_children.empty()) {
        if (column != nullptr) {
            (*column)->resize((*column)->size() + static_cast<size_t>(length_upper_bound));
        }
        *values_processed = length_upper_bound;
        return Status::OK();
    }
    ColumnStruct* struct_column = nullptr;
    NullMap* parent_null_map = nullptr;
    if (column != nullptr) {
        struct_column = struct_column_from_output(*column);
        DORIS_CHECK(struct_column != nullptr);
        parent_null_map = null_map_from_nullable_output(*column);
    }
    auto* shape_reader = shape_source_reader();
    DORIS_CHECK(shape_reader != nullptr);
    const auto& def_levels = shape_reader->nested_definition_levels();
    const auto& rep_levels = shape_reader->nested_repetition_levels();
    const int64_t levels_written = shape_reader->nested_levels_written();

    _parent_nulls.clear();
    _parent_level_indices.clear();
    *values_processed = 0;
    int64_t level_idx = nested_build_level_cursor();
    while (level_idx < levels_written) {
        const int64_t current_level_idx = level_idx;
        const int16_t def_level = def_levels[level_idx];
        const int16_t rep_level = rep_levels[level_idx];
        const bool starts_parent =
                !shape_reader->is_or_has_repeated_child() || rep_level <= _repetition_level;
        if (starts_parent && *values_processed >= length_upper_bound) {
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
        if (parent_is_null && !_type->is_nullable()) {
            return Status::Corruption(
                    "Parquet STRUCT column {} contains null for non-nullable struct", _name);
        }
        _parent_nulls.push_back(parent_is_null);
        _parent_level_indices.push_back(current_level_idx);
        ++*values_processed;
    }
    set_nested_build_level_cursor(level_idx);

    _child_columns.clear();
    if (column != nullptr) {
        _child_columns.reserve(struct_column->get_columns().size());
        for (size_t child_idx = 0; child_idx < struct_column->get_columns().size(); ++child_idx) {
            _child_columns.push_back(struct_column->get_column_ptr(child_idx)->assert_mutable());
        }
    }
    for (size_t child_idx = 0; child_idx < _children.size(); ++child_idx) {
        const int output_idx = _child_output_indices[child_idx];
        if (column != nullptr && output_idx < 0) {
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
            if (column != nullptr) {
                RETURN_IF_ERROR(_children[child_idx]->build_nested_column(
                        pending_present_rows, _child_columns[output_idx], &child_rows));
            } else {
                RETURN_IF_ERROR(_children[child_idx]->consume_nested_column(pending_present_rows,
                                                                            &child_rows));
            }
            if (child_rows != pending_present_rows) {
                return Status::Corruption(
                        "Parquet STRUCT child {} built {} rows, expected {} for column {}",
                        _children[child_idx]->name(), child_rows, pending_present_rows, _name);
            }
            total_child_rows += child_rows;
            pending_present_rows = 0;
            return Status::OK();
        };
        for (size_t parent_idx = 0; parent_idx < _parent_nulls.size(); ++parent_idx) {
            const auto parent_is_null = _parent_nulls[parent_idx];
            if (!parent_is_null) {
                ++pending_present_rows;
                continue;
            }
            RETURN_IF_ERROR(flush_present_rows());
            if (column != nullptr) {
                _child_columns[output_idx]->insert_default();
            }
            RETURN_IF_ERROR(advance_child_past_null_parent(_children[child_idx].get(),
                                                           _parent_level_indices[parent_idx]));
            ++total_child_rows;
        }
        RETURN_IF_ERROR(flush_present_rows());
        if (total_child_rows != *values_processed) {
            return Status::Corruption(
                    "Parquet STRUCT child {} built {} rows, expected {} for column {}",
                    _children[child_idx]->name(), total_child_rows, *values_processed, _name);
        }
    }
    if (column != nullptr) {
        for (size_t child_idx = 0; child_idx < _child_columns.size(); ++child_idx) {
            struct_column->get_column_ptr(child_idx) = std::move(_child_columns[child_idx]);
        }
        append_parent_nulls(parent_null_map, _parent_nulls);
    }
    return Status::OK();
}

const std::vector<int16_t>& StructColumnReader::nested_definition_levels() const {
    auto* shape_reader = shape_source_reader();
    DORIS_CHECK(shape_reader != nullptr);
    return shape_reader->nested_definition_levels();
}

const std::vector<int16_t>& StructColumnReader::nested_repetition_levels() const {
    auto* shape_reader = shape_source_reader();
    DORIS_CHECK(shape_reader != nullptr);
    return shape_reader->nested_repetition_levels();
}

int64_t StructColumnReader::nested_levels_written() const {
    auto* shape_reader = shape_source_reader();
    DORIS_CHECK(shape_reader != nullptr);
    return shape_reader->nested_levels_written();
}

bool StructColumnReader::is_or_has_repeated_child() const {
    auto* shape_reader = shape_source_reader();
    return shape_reader != nullptr && shape_reader->is_or_has_repeated_child();
}

void StructColumnReader::advance_nested_build_level_cursor_past_parent(
        int16_t parent_repetition_level) {
    ParquetColumnReader::advance_nested_build_level_cursor_past_parent(parent_repetition_level);
    for (auto& child : _children) {
        DORIS_CHECK(child != nullptr);
        child->advance_nested_build_level_cursor_past_parent(parent_repetition_level);
    }
}

} // namespace doris::format::parquet
