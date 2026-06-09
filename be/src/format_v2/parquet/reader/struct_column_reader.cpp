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
#include "format_v2/parquet/reader/nested_column_reader.h"
#include "format_v2/parquet/reader/scalar_column_reader.h"

namespace doris::parquet {

Status StructColumnReader::read_internal(int64_t rows, MutableColumnPtr& column, int64_t* rows_read,
                                         const std::vector<ParquetNullShapeSink>* ancestor_shapes) {
    if (column.get() == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet struct read result pointer for column {}",
                                       _name);
    }
    if (_children.empty()) {
        if (ancestor_shapes != nullptr && !ancestor_shapes->empty()) {
            return Status::NotSupported(
                    "Parquet STRUCT column {} cannot expose ancestor shape without a physical "
                    "descendant",
                    _name);
        }
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
        std::vector<NestedScalarValueCursor> value_cursors(child_batches.size());
        for (size_t child_idx = 0; child_idx < child_batches.size(); ++child_idx) {
            value_cursors[child_idx].reset(&child_batches[child_idx]);
        }

        std::vector<MutableColumnPtr> child_columns;
        child_columns.reserve(struct_column->get_columns().size());
        for (size_t child_idx = 0; child_idx < struct_column->get_columns().size(); ++child_idx) {
            child_columns.push_back(struct_column->get_column_ptr(child_idx)->assert_mutable());
        }

        NullMap parent_nulls;
        parent_nulls.reserve(static_cast<size_t>(expected_rows));
        for (int64_t row_idx = 0; row_idx < expected_rows; ++row_idx) {
            const bool parent_is_null =
                    child_batches[0].def_levels[row_idx] < _nullable_definition_level;
            parent_nulls.push_back(parent_is_null);
            if (ancestor_shapes != nullptr) {
                append_null_shapes(ancestor_shapes, child_batches[0].def_levels[row_idx]);
            }
            for (size_t child_idx = 1; child_idx < child_batches.size(); ++child_idx) {
                const bool child_parent_is_null =
                        child_batches[child_idx].def_levels[row_idx] < _nullable_definition_level;
                if (child_parent_is_null != parent_is_null) {
                    return Status::Corruption(
                            "Parquet struct children returned different null parent shape in "
                            "column {}",
                            _name);
                }
                if (ancestor_shapes != nullptr) {
                    for (const auto& ancestor_shape : *ancestor_shapes) {
                        const bool ancestor_is_null = child_batches[0].def_levels[row_idx] <
                                                      ancestor_shape.nullable_definition_level;
                        const bool child_ancestor_is_null =
                                child_batches[child_idx].def_levels[row_idx] <
                                ancestor_shape.nullable_definition_level;
                        if (child_ancestor_is_null != ancestor_is_null) {
                            return Status::Corruption(
                                    "Parquet struct children returned different ancestor shape in "
                                    "column {}",
                                    _name);
                        }
                    }
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
                    RETURN_IF_ERROR(append_nullable_scalar_child(
                            _name, "STRUCT", scalar_children[child_idx]->name(),
                            *scalar_children[child_idx], child_batches[child_idx], row_idx,
                            scalar_children[child_idx]->descriptor()->max_definition_level(),
                            &value_cursors[child_idx], child_columns[output_idx]));
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
        std::vector<NestedScalarValueCursor> value_cursors(child_batches.size());
        for (size_t scalar_idx = 0; scalar_idx < child_batches.size(); ++scalar_idx) {
            value_cursors[scalar_idx].reset(&child_batches[scalar_idx]);
        }

        std::vector<MutableColumnPtr> child_columns;
        child_columns.reserve(struct_column->get_columns().size());
        for (size_t child_idx = 0; child_idx < struct_column->get_columns().size(); ++child_idx) {
            child_columns.push_back(struct_column->get_column_ptr(child_idx)->assert_mutable());
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
            if (ancestor_shapes != nullptr) {
                append_null_shapes(ancestor_shapes, child_batches[0].def_levels[level_indices[0]]);
            }
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
                if (ancestor_shapes != nullptr) {
                    for (const auto& ancestor_shape : *ancestor_shapes) {
                        const bool ancestor_is_null =
                                child_batches[0].def_levels[level_indices[0]] <
                                ancestor_shape.nullable_definition_level;
                        const bool child_ancestor_is_null =
                                child_batches[scalar_idx].def_levels[level_indices[scalar_idx]] <
                                ancestor_shape.nullable_definition_level;
                        if (child_ancestor_is_null != ancestor_is_null) {
                            return Status::Corruption(
                                    "Parquet struct children returned different ancestor shape in "
                                    "column {}",
                                    _name);
                        }
                    }
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
                RETURN_IF_ERROR(append_nullable_scalar_child(
                        _name, "STRUCT", scalar_children[scalar_idx]->name(),
                        *scalar_children[scalar_idx], child_batches[scalar_idx],
                        level_indices[scalar_idx],
                        scalar_children[scalar_idx]->descriptor()->max_definition_level(),
                        &value_cursors[scalar_idx], child_columns[output_idx]));
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

    int64_t expected_rows = -1;
    std::vector<int16_t> shape_definition_levels;
    int parent_shape_index = -1;
    if (parent_null_map != nullptr) {
        parent_shape_index = 0;
        shape_definition_levels.push_back(_nullable_definition_level);
    }
    if (ancestor_shapes != nullptr) {
        shape_definition_levels.reserve(shape_definition_levels.size() + ancestor_shapes->size());
        for (const auto& ancestor_shape : *ancestor_shapes) {
            DORIS_CHECK(ancestor_shape.null_map != nullptr);
            shape_definition_levels.push_back(ancestor_shape.nullable_definition_level);
        }
    }
    const bool need_shape = !shape_definition_levels.empty();
    std::vector<NullMap> expected_shape_maps;
    size_t child_idx = 0;
    for (auto& child_reader : _children) {
        DORIS_CHECK(child_reader != nullptr);
        int64_t child_rows = 0;
        const int output_idx = _child_output_indices[child_idx];
        std::vector<NullMap> child_shape_maps(shape_definition_levels.size());
        if (output_idx < 0) {
            if (!need_shape) {
                RETURN_IF_ERROR(child_reader->skip(rows));
                child_rows = rows;
            } else {
                std::vector<ParquetNullShapeSink> child_shape_sinks;
                child_shape_sinks.reserve(shape_definition_levels.size());
                for (size_t shape_idx = 0; shape_idx < shape_definition_levels.size();
                     ++shape_idx) {
                    child_shape_sinks.push_back(
                            {shape_definition_levels[shape_idx], &child_shape_maps[shape_idx]});
                }
                auto scratch_column = child_reader->type()->create_column();
                RETURN_IF_ERROR(child_reader->read_with_ancestor_shapes(
                        rows, child_shape_sinks, scratch_column, &child_rows));
            }
        } else {
            auto child_column = struct_column->get_column_ptr(output_idx)->assert_mutable();
            if (!need_shape) {
                RETURN_IF_ERROR(child_reader->read(rows, child_column, &child_rows));
            } else {
                std::vector<ParquetNullShapeSink> child_shape_sinks;
                child_shape_sinks.reserve(shape_definition_levels.size());
                for (size_t shape_idx = 0; shape_idx < shape_definition_levels.size();
                     ++shape_idx) {
                    child_shape_sinks.push_back(
                            {shape_definition_levels[shape_idx], &child_shape_maps[shape_idx]});
                }
                // Phase-1 shape source: a nullable STRUCT whose selected children are all complex
                // has no scalar child levels to derive parent validity from. Ask each complex child
                // to materialize normally and expose the requested ancestor STRUCT shapes into
                // scratch maps, then validate sibling shapes before appending them to real outputs.
                // This keeps the file block as a top-level ColumnStruct and avoids hidden child
                // slots while leaving room for the later unified shape builder.
                RETURN_IF_ERROR(child_reader->read_with_ancestor_shapes(rows, child_shape_sinks,
                                                                        child_column, &child_rows));
            }
            struct_column->get_column_ptr(output_idx) = std::move(child_column);
        }
        if (expected_rows < 0) {
            expected_rows = child_rows;
            expected_shape_maps = std::move(child_shape_maps);
        } else if (child_rows != expected_rows) {
            return Status::Corruption(
                    "Parquet struct children returned different row counts in column {}: {} vs {}",
                    _name, expected_rows, child_rows);
        } else if (need_shape && child_shape_maps != expected_shape_maps) {
            return Status::Corruption(
                    "Parquet struct children returned different null parent shape in column {}",
                    _name);
        }
        child_idx++;
    }

    *rows_read = std::max<int64_t>(expected_rows, 0);
    if (parent_null_map != nullptr) {
        DORIS_CHECK(parent_shape_index >= 0);
        const auto& parent_nulls = expected_shape_maps[static_cast<size_t>(parent_shape_index)];
        if (parent_nulls.size() != static_cast<size_t>(*rows_read)) {
            return Status::Corruption(
                    "Parquet STRUCT column {} returned {} parent shape rows, expected {}", _name,
                    parent_nulls.size(), *rows_read);
        }
        append_parent_nulls(parent_null_map, parent_nulls);
    }
    if (ancestor_shapes != nullptr) {
        const size_t ancestor_shape_offset = parent_shape_index >= 0 ? 1 : 0;
        for (size_t shape_idx = 0; shape_idx < ancestor_shapes->size(); ++shape_idx) {
            auto* ancestor_nulls = (*ancestor_shapes)[shape_idx].null_map;
            const auto& shape_map = expected_shape_maps[ancestor_shape_offset + shape_idx];
            ancestor_nulls->insert(ancestor_nulls->end(), shape_map.begin(), shape_map.end());
        }
    }
    return Status::OK();
}

Status StructColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    return read_internal(rows, column, rows_read, nullptr);
}

Status StructColumnReader::read_with_ancestor_shape(int64_t rows,
                                                    int16_t ancestor_nullable_definition_level,
                                                    MutableColumnPtr& column, int64_t* rows_read,
                                                    NullMap* ancestor_nulls) {
    if (ancestor_nulls == nullptr) {
        return Status::InvalidArgument("Ancestor shape output is null for parquet STRUCT column {}",
                                       _name);
    }
    const auto initial_null_count = ancestor_nulls->size();
    std::vector<ParquetNullShapeSink> ancestor_shapes {
            {ancestor_nullable_definition_level, ancestor_nulls}};
    RETURN_IF_ERROR(read_with_ancestor_shapes(rows, ancestor_shapes, column, rows_read));
    if (ancestor_nulls->size() - initial_null_count != static_cast<size_t>(*rows_read)) {
        return Status::Corruption(
                "Parquet STRUCT column {} returned {} ancestor shape rows, expected {}", _name,
                ancestor_nulls->size() - initial_null_count, *rows_read);
    }
    return Status::OK();
}

Status StructColumnReader::read_with_ancestor_shapes(
        int64_t rows, const std::vector<ParquetNullShapeSink>& ancestor_shapes,
        MutableColumnPtr& column, int64_t* rows_read) {
    std::vector<size_t> initial_null_counts;
    capture_null_shape_sizes(ancestor_shapes, &initial_null_counts);
    RETURN_IF_ERROR(read_internal(rows, column, rows_read, &ancestor_shapes));
    return validate_null_shape_rows(_name, "STRUCT", ancestor_shapes, initial_null_counts,
                                    *rows_read);
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

} // namespace doris::parquet
