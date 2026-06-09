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

#include "format_v2/parquet/reader/list_column_reader.h"

#include <parquet/api/schema.h>

#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

#include "core/column/column_nullable.h"
#include "format_v2/parquet/reader/map_column_reader.h"
#include "format_v2/parquet/reader/nested_column_reader.h"
#include "format_v2/parquet/reader/scalar_column_reader.h"
#include "format_v2/parquet/reader/struct_column_reader.h"

namespace doris::parquet {
namespace {

void remove_nullable_wrapper_if_required(const ParquetColumnReader& reader,
                                         MutableColumnPtr* column) {
    DORIS_CHECK(column != nullptr);
    if (reader.type()->is_nullable()) {
        return;
    }
    if (auto* nullable_column = check_and_get_column<ColumnNullable>(**column)) {
        *column = nullable_column->get_nested_column_ptr();
    }
}

// LIST keeps its Dremel traversal local because it materializes Doris ColumnArray directly.
// This helper only owns the common stream mechanics: read-ahead, overflow, parent row
// boundaries, and the rule that a repeated child must stay in the current parent row.
// The caller still owns LIST semantics such as null parent, empty parent, and child append.
template <typename Batch, typename Overflow, typename ReadBatchFn, typename MoveTailFn,
          typename StartParentFn, typename AppendRepeatedFn>
Status consume_list_level_stream(const std::string& column_name, int16_t repeated_level,
                                 int64_t rows, Overflow* overflow, ReadBatchFn&& read_batch,
                                 MoveTailFn&& move_tail, StartParentFn&& start_parent,
                                 AppendRepeatedFn&& append_repeated, int64_t* rows_read) {
    DORIS_CHECK(overflow != nullptr);
    DORIS_CHECK(rows_read != nullptr);
    *rows_read = 0;

    while (*rows_read < rows) {
        Batch batch_from_reader;
        Batch* batch = nullptr;
        bool from_overflow = false;
        if (!overflow->empty()) {
            batch = &overflow->batch;
            from_overflow = true;
        } else {
            const int64_t batch_rows = std::max<int64_t>(rows - *rows_read, NESTED_READ_BATCH_ROWS);
            RETURN_IF_ERROR(read_batch(batch_rows, &batch_from_reader));
            if (batch_from_reader.empty()) {
                break;
            }
            batch = &batch_from_reader;
        }

        NestedShapeCursor<Batch> cursor(*batch);
        for (int64_t level_idx = 0; level_idx < cursor.levels_written(); ++level_idx) {
            const bool starts_parent = cursor.starts_parent(level_idx, repeated_level);
            if (starts_parent && *rows_read >= rows) {
                move_tail(*batch, level_idx, overflow);
                return Status::OK();
            }
            if (starts_parent) {
                RETURN_IF_ERROR(start_parent(*batch, level_idx));
                ++*rows_read;
                continue;
            }
            if (*rows_read == 0) {
                return Status::Corruption(
                        "Repeated parquet stream starts with repeated level for column {}",
                        column_name);
            }
            RETURN_IF_ERROR(append_repeated(*batch, level_idx));
        }

        if (from_overflow) {
            overflow->clear();
        }
    }
    return Status::OK();
}

Status read_scalar_list_values(const std::string& column_name, const DataTypePtr& list_type,
                               int16_t list_nullable_definition_level, int16_t repeated_level,
                               ScalarColumnReader& element_reader,
                               NestedScalarOverflow* element_overflow, int64_t rows,
                               MutableColumnPtr& element_column,
                               std::vector<uint64_t>* entry_counts, NullMap* parent_nulls,
                               const std::vector<ParquetNullShapeSink>* ancestor_shapes,
                               int64_t* rows_read) {
    DORIS_CHECK(element_overflow != nullptr);
    DORIS_CHECK(entry_counts != nullptr);
    DORIS_CHECK(parent_nulls != nullptr);
    DORIS_CHECK(rows_read != nullptr);
    *rows_read = 0;
    RepeatedParentSinkState parent_state {entry_counts, parent_nulls};
    NestedScalarValueAppender value_appender {
            &element_reader, "LIST", "element",
            static_cast<int16_t>(element_reader.descriptor()->max_definition_level())};
    NestedScalarValueCursor value_cursor;
    const NestedScalarBatch* value_cursor_batch = nullptr;
    value_appender.value_cursor = &value_cursor;
    const int16_t element_slot_definition_level = list_nullable_definition_level + 1;

    auto reset_value_cursor = [&](const NestedScalarBatch& batch, int64_t level_idx) {
        if (level_idx == 0 || value_cursor_batch != &batch) {
            value_cursor.reset(&batch);
            value_cursor_batch = &batch;
        }
    };
    auto read_batch = [&](int64_t batch_rows, NestedScalarBatch* batch) {
        return read_nested_scalar_batch(element_reader, batch_rows, element_slot_definition_level,
                                        batch);
    };
    auto start_parent = [&](const NestedScalarBatch& batch, int64_t level_idx) -> Status {
        reset_value_cursor(batch, level_idx);
        const int16_t def_level = batch.def_levels[level_idx];
        if (ancestor_shapes != nullptr) {
            append_null_shapes(ancestor_shapes, def_level);
        }
        if (def_level < list_nullable_definition_level) {
            RETURN_IF_ERROR(parent_state.append_null_parent(column_name, "LIST", list_type));
            return value_appender.skip_shape_only_slot();
        }
        parent_state.append_present_parent();
        if (def_level == list_nullable_definition_level) {
            return value_appender.skip_shape_only_slot();
        }
        RETURN_IF_ERROR(parent_state.require_parent(column_name));
        RETURN_IF_ERROR(value_appender.append(column_name, batch, level_idx, element_column));
        return parent_state.add_entry(column_name);
    };
    auto append_repeated = [&](const NestedScalarBatch& batch, int64_t level_idx) -> Status {
        reset_value_cursor(batch, level_idx);
        RETURN_IF_ERROR(parent_state.require_parent(column_name));
        RETURN_IF_ERROR(value_appender.append(column_name, batch, level_idx, element_column));
        return parent_state.add_entry(column_name);
    };
    return consume_list_level_stream<NestedScalarBatch>(
            column_name, repeated_level, rows, element_overflow, read_batch,
            move_nested_scalar_tail, start_parent, append_repeated, rows_read);
}

Status read_struct_list_values(const std::string& column_name, const DataTypePtr& list_type,
                               int16_t list_nullable_definition_level, int16_t repeated_level,
                               StructColumnReader& element_reader,
                               NestedStructOverflow* element_overflow, int64_t rows,
                               MutableColumnPtr& element_column,
                               std::vector<uint64_t>* entry_counts, NullMap* parent_nulls,
                               const std::vector<ParquetNullShapeSink>* ancestor_shapes,
                               int64_t* rows_read) {
    DORIS_CHECK(element_overflow != nullptr);
    DORIS_CHECK(entry_counts != nullptr);
    DORIS_CHECK(parent_nulls != nullptr);
    DORIS_CHECK(rows_read != nullptr);
    *rows_read = 0;
    RepeatedParentSinkState parent_state {entry_counts, parent_nulls};
    NestedStructValueAppender value_appender {&element_reader};
    const int16_t element_slot_definition_level = list_nullable_definition_level + 1;

    auto read_batch = [&](int64_t batch_rows, NestedStructBatch* batch) {
        return read_nested_struct_batch(element_reader, batch_rows, element_slot_definition_level,
                                        batch);
    };
    auto start_parent = [&](const NestedStructBatch& batch, int64_t level_idx) -> Status {
        const int16_t def_level = nested_shape_definition_level(batch, level_idx);
        if (ancestor_shapes != nullptr) {
            append_null_shapes(ancestor_shapes, def_level);
        }
        if (def_level < list_nullable_definition_level) {
            RETURN_IF_ERROR(parent_state.append_null_parent(column_name, "LIST", list_type));
            return value_appender.skip_shape_only_slot();
        }
        parent_state.append_present_parent();
        if (def_level == list_nullable_definition_level) {
            return value_appender.skip_shape_only_slot();
        }
        RETURN_IF_ERROR(parent_state.require_parent(column_name));
        RETURN_IF_ERROR(value_appender.append(column_name, batch, level_idx, element_column));
        return parent_state.add_entry(column_name);
    };
    auto append_repeated = [&](const NestedStructBatch& batch, int64_t level_idx) -> Status {
        RETURN_IF_ERROR(parent_state.require_parent(column_name));
        RETURN_IF_ERROR(value_appender.append(column_name, batch, level_idx, element_column));
        return parent_state.add_entry(column_name);
    };
    return consume_list_level_stream<NestedStructBatch>(
            column_name, repeated_level, rows, element_overflow, read_batch,
            move_nested_struct_tail, start_parent, append_repeated, rows_read);
}

Status read_nested_list_values(const std::string& column_name, const DataTypePtr& outer_list_type,
                               int16_t outer_nullable_definition_level,
                               int16_t outer_repeated_level, ListColumnReader& inner_list_reader,
                               ScalarColumnReader& nested_element_reader,
                               NestedScalarOverflow* element_overflow, int64_t rows,
                               MutableColumnPtr& inner_element_column,
                               std::vector<uint64_t>* outer_entry_counts,
                               NullMap* outer_parent_nulls,
                               const std::vector<ParquetNullShapeSink>* ancestor_shapes,
                               std::vector<uint64_t>* inner_entry_counts,
                               NullMap* inner_parent_nulls, int64_t* rows_read) {
    DORIS_CHECK(element_overflow != nullptr);
    DORIS_CHECK(outer_entry_counts != nullptr);
    DORIS_CHECK(outer_parent_nulls != nullptr);
    DORIS_CHECK(inner_entry_counts != nullptr);
    DORIS_CHECK(inner_parent_nulls != nullptr);
    DORIS_CHECK(rows_read != nullptr);
    *rows_read = 0;
    RepeatedParentSinkState outer_state {outer_entry_counts, outer_parent_nulls};
    RepeatedChildSinkState inner_state {inner_entry_counts, inner_parent_nulls};
    NestedScalarValueAppender value_appender {
            &nested_element_reader, "LIST", "nested element",
            static_cast<int16_t>(nested_element_reader.descriptor()->max_definition_level())};
    NestedScalarValueCursor value_cursor;
    const NestedScalarBatch* value_cursor_batch = nullptr;
    value_appender.value_cursor = &value_cursor;
    const int16_t inner_nullable_definition_level = inner_list_reader.nullable_definition_level();
    const int16_t inner_element_slot_definition_level = inner_nullable_definition_level + 1;

    auto reset_value_cursor = [&](const NestedScalarBatch& batch, int64_t level_idx) {
        if (level_idx == 0 || value_cursor_batch != &batch) {
            value_cursor.reset(&batch);
            value_cursor_batch = &batch;
        }
    };
    auto append_element = [&](const NestedScalarBatch& batch, int64_t level_idx) -> Status {
        reset_value_cursor(batch, level_idx);
        RETURN_IF_ERROR(inner_state.require_child(column_name, "nested LIST"));
        RETURN_IF_ERROR(value_appender.append(column_name, batch, level_idx, inner_element_column));
        return inner_state.add_entry(column_name, "nested LIST");
    };
    auto append_inner_list = [&](const NestedScalarBatch& batch, int64_t level_idx) -> Status {
        RETURN_IF_ERROR(outer_state.add_entry(column_name));
        const int16_t def_level = batch.def_levels[level_idx];
        if (def_level < inner_nullable_definition_level) {
            return inner_state.append_null_child(column_name, "LIST", "nested list",
                                                 inner_list_reader.type());
        }
        inner_state.append_present_child();
        if (def_level == inner_nullable_definition_level) {
            return Status::OK();
        }
        return append_element(batch, level_idx);
    };

    auto read_batch = [&](int64_t batch_rows, NestedScalarBatch* batch) {
        return read_nested_scalar_batch(nested_element_reader, batch_rows,
                                        inner_element_slot_definition_level, batch);
    };
    auto start_parent = [&](const NestedScalarBatch& batch, int64_t level_idx) -> Status {
        reset_value_cursor(batch, level_idx);
        const int16_t def_level = batch.def_levels[level_idx];
        if (ancestor_shapes != nullptr) {
            append_null_shapes(ancestor_shapes, def_level);
        }
        if (def_level < outer_nullable_definition_level) {
            return outer_state.append_null_parent(column_name, "LIST", outer_list_type);
        }
        outer_state.append_present_parent();
        if (def_level > outer_nullable_definition_level) {
            RETURN_IF_ERROR(append_inner_list(batch, level_idx));
        }
        return Status::OK();
    };
    auto append_repeated = [&](const NestedScalarBatch& batch, int64_t level_idx) -> Status {
        if (batch.rep_levels[level_idx] < inner_list_reader.repeated_repetition_level()) {
            return append_inner_list(batch, level_idx);
        }
        return append_element(batch, level_idx);
    };
    return consume_list_level_stream<NestedScalarBatch>(
            column_name, outer_repeated_level, rows, element_overflow, read_batch,
            move_nested_scalar_tail, start_parent, append_repeated, rows_read);
}

Status skip_scalar_list_values(const std::string& column_name, int16_t repeated_level,
                               int16_t value_slot_definition_level,
                               ScalarColumnReader& element_reader,
                               NestedScalarOverflow* element_overflow, int64_t rows,
                               int64_t* rows_read) {
    DORIS_CHECK(element_overflow != nullptr);
    DORIS_CHECK(rows_read != nullptr);
    auto read_batch = [&](int64_t batch_rows, NestedScalarBatch* batch) {
        return read_nested_scalar_batch(element_reader, batch_rows, value_slot_definition_level,
                                        batch);
    };
    auto start_parent = [](const NestedScalarBatch&, int64_t) { return Status::OK(); };
    auto append_repeated = [](const NestedScalarBatch&, int64_t) { return Status::OK(); };
    return consume_list_level_stream<NestedScalarBatch>(
            column_name, repeated_level, rows, element_overflow, read_batch,
            move_nested_scalar_tail, start_parent, append_repeated, rows_read);
}

Status skip_struct_list_values(const std::string& column_name, int16_t repeated_level,
                               int16_t value_slot_definition_level,
                               StructColumnReader& element_reader,
                               NestedStructOverflow* element_overflow, int64_t rows,
                               int64_t* rows_read) {
    DORIS_CHECK(element_overflow != nullptr);
    DORIS_CHECK(rows_read != nullptr);
    auto read_batch = [&](int64_t batch_rows, NestedStructBatch* batch) {
        return read_nested_struct_batch(element_reader, batch_rows, value_slot_definition_level,
                                        batch);
    };
    auto start_parent = [](const NestedStructBatch&, int64_t) { return Status::OK(); };
    auto append_repeated = [](const NestedStructBatch&, int64_t) { return Status::OK(); };
    return consume_list_level_stream<NestedStructBatch>(
            column_name, repeated_level, rows, element_overflow, read_batch,
            move_nested_struct_tail, start_parent, append_repeated, rows_read);
}

} // namespace

Status ListColumnReader::read_internal(int64_t rows, MutableColumnPtr& column, int64_t* rows_read,
                                       const std::vector<ParquetNullShapeSink>* ancestor_shapes) {
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
    auto nested_column = array_column->get_data_ptr()->assert_mutable();
    remove_nullable_wrapper_if_required(*_element_reader, &nested_column);
    std::vector<uint64_t> entry_counts;
    NullMap parent_nulls;

    if (auto* element_reader = dynamic_cast<ScalarColumnReader*>(_element_reader.get())) {
        RETURN_IF_ERROR(read_scalar_list_values(
                _name, _type, _nullable_definition_level, _repeated_repetition_level,
                *element_reader, &_element_overflow, rows, nested_column, &entry_counts,
                &parent_nulls, ancestor_shapes, rows_read));
        array_column->get_data_ptr() = std::move(nested_column);
        append_offsets(array_column->get_offsets(), entry_counts);
        append_parent_nulls(parent_null_map, parent_nulls);
        return Status::OK();
    }

    if (auto* struct_element_reader = dynamic_cast<StructColumnReader*>(_element_reader.get())) {
        RETURN_IF_ERROR(read_struct_list_values(
                _name, _type, _nullable_definition_level, _repeated_repetition_level,
                *struct_element_reader, &_struct_element_overflow, rows, nested_column,
                &entry_counts, &parent_nulls, ancestor_shapes, rows_read));
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
    auto inner_nested_column = inner_array_column->get_data_ptr()->assert_mutable();
    remove_nullable_wrapper_if_required(*scalar_nested_element_reader, &inner_nested_column);
    std::vector<uint64_t> inner_entry_counts;
    NullMap inner_parent_nulls;

    RETURN_IF_ERROR(read_nested_list_values(
            _name, _type, _nullable_definition_level, _repeated_repetition_level,
            *list_element_reader, *scalar_nested_element_reader, &_element_overflow, rows,
            inner_nested_column, &entry_counts, &parent_nulls, ancestor_shapes, &inner_entry_counts,
            &inner_parent_nulls, rows_read));
    inner_array_column->get_data_ptr() = std::move(inner_nested_column);
    append_offsets(inner_array_column->get_offsets(), inner_entry_counts);
    append_parent_nulls(inner_null_map, inner_parent_nulls);
    append_offsets(array_column->get_offsets(), entry_counts);
    array_column->get_data_ptr() = std::move(nested_column);
    append_parent_nulls(parent_null_map, parent_nulls);
    return Status::OK();
}

Status ListColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    if (dynamic_cast<MapColumnReader*>(_element_reader.get()) == nullptr) {
        return read_internal(rows, column, rows_read, nullptr);
    }
    RETURN_IF_ERROR(load_nested_batch(rows));
    return build_nested_column(rows, column, rows_read);
}

Status ListColumnReader::read_with_ancestor_shape(int64_t rows,
                                                  int16_t ancestor_nullable_definition_level,
                                                  MutableColumnPtr& column, int64_t* rows_read,
                                                  NullMap* ancestor_nulls) {
    if (ancestor_nulls == nullptr) {
        return Status::InvalidArgument("Ancestor shape output is null for parquet LIST column {}",
                                       _name);
    }
    const auto initial_null_count = ancestor_nulls->size();
    std::vector<ParquetNullShapeSink> ancestor_shapes {
            {ancestor_nullable_definition_level, ancestor_nulls}};
    RETURN_IF_ERROR(read_with_ancestor_shapes(rows, ancestor_shapes, column, rows_read));
    if (ancestor_nulls->size() - initial_null_count != static_cast<size_t>(*rows_read)) {
        return Status::Corruption(
                "Parquet LIST column {} returned {} ancestor shape rows, expected {}", _name,
                ancestor_nulls->size() - initial_null_count, *rows_read);
    }
    return Status::OK();
}

Status ListColumnReader::read_with_ancestor_shapes(
        int64_t rows, const std::vector<ParquetNullShapeSink>& ancestor_shapes,
        MutableColumnPtr& column, int64_t* rows_read) {
    std::vector<size_t> initial_null_counts;
    capture_null_shape_sizes(ancestor_shapes, &initial_null_counts);
    RETURN_IF_ERROR(read_internal(rows, column, rows_read, &ancestor_shapes));
    return validate_null_shape_rows(_name, "LIST", ancestor_shapes, initial_null_counts,
                                    *rows_read);
}

Status ListColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }
    int64_t rows_read = 0;
    if (auto* element_reader = dynamic_cast<ScalarColumnReader*>(_element_reader.get())) {
        RETURN_IF_ERROR(skip_scalar_list_values(_name, _repeated_repetition_level,
                                                _nullable_definition_level + 1, *element_reader,
                                                &_element_overflow, rows, &rows_read));
    } else if (auto* struct_element_reader =
                       dynamic_cast<StructColumnReader*>(_element_reader.get())) {
        RETURN_IF_ERROR(skip_struct_list_values(
                _name, _repeated_repetition_level, _nullable_definition_level + 1,
                *struct_element_reader, &_struct_element_overflow, rows, &rows_read));
    } else if (auto* list_element_reader = dynamic_cast<ListColumnReader*>(_element_reader.get())) {
        auto* scalar_nested_element_reader =
                dynamic_cast<ScalarColumnReader*>(list_element_reader->_element_reader.get());
        if (scalar_nested_element_reader == nullptr) {
            return Status::NotSupported(
                    "Current parquet nested LIST skip only supports scalar nested elements for "
                    "column {}",
                    _name);
        }
        RETURN_IF_ERROR(skip_scalar_list_values(_name, _repeated_repetition_level,
                                                list_element_reader->_nullable_definition_level + 1,
                                                *scalar_nested_element_reader, &_element_overflow,
                                                rows, &rows_read));
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

Status ListColumnReader::load_nested_batch(int64_t rows) {
    DORIS_CHECK(_element_reader != nullptr);
    return _element_reader->load_nested_batch(rows);
}

Status ListColumnReader::build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                                             int64_t* values_read) {
    if (column.get() == nullptr || values_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet list build result pointer for column {}",
                                       _name);
    }
    DORIS_CHECK(_element_reader != nullptr);
    auto* array_column = array_column_from_output(column);
    DORIS_CHECK(array_column != nullptr);
    auto* parent_null_map = null_map_from_nullable_output(column);
    auto nested_column = array_column->get_data_ptr()->assert_mutable();
    remove_nullable_wrapper_if_required(*_element_reader, &nested_column);

    const auto& def_levels = _element_reader->nested_definition_levels();
    const auto& rep_levels = _element_reader->nested_repetition_levels();
    const int64_t levels_written = _element_reader->nested_levels_written();
    std::vector<uint64_t> entry_counts;
    NullMap parent_nulls;
    *values_read = 0;
    for (int64_t level_idx = 0; level_idx < levels_written && *values_read < length_upper_bound;
         ++level_idx) {
        const int16_t def_level = def_levels[level_idx];
        const int16_t rep_level = rep_levels[level_idx];
        if (def_level < _repeated_ancestor_definition_level || rep_level > _repetition_level) {
            continue;
        }
        if (rep_level == _repetition_level) {
            if (entry_counts.empty()) {
                return Status::Corruption("Invalid repeated level for parquet LIST column {}",
                                          _name);
            }
            if (def_level >= _definition_level) {
                ++entry_counts.back();
            }
            continue;
        }

        const bool parent_is_null = def_level < _definition_level - 1;
        if (parent_is_null && !_type->is_nullable() && def_level >= _nullable_definition_level) {
            return Status::Corruption("Parquet LIST column {} contains null for non-nullable LIST",
                                      _name);
        }
        parent_nulls.push_back(parent_is_null);
        entry_counts.push_back(def_level >= _definition_level ? 1 : 0);
        ++*values_read;
    }

    int64_t child_value_count = 0;
    uint64_t total_entries = 0;
    for (const auto entry_count : entry_counts) {
        total_entries += entry_count;
    }
    RETURN_IF_ERROR(_element_reader->build_nested_column(static_cast<int64_t>(total_entries),
                                                         nested_column, &child_value_count));
    if (child_value_count != static_cast<int64_t>(total_entries)) {
        return Status::Corruption("Parquet LIST column {} built {} child values, expected {}",
                                  _name, child_value_count, total_entries);
    }
    array_column->get_data_ptr() = std::move(nested_column);
    append_offsets(array_column->get_offsets(), entry_counts);
    append_parent_nulls(parent_null_map, parent_nulls);
    return Status::OK();
}

const std::vector<int16_t>& ListColumnReader::nested_definition_levels() const {
    DORIS_CHECK(_element_reader != nullptr);
    return _element_reader->nested_definition_levels();
}

const std::vector<int16_t>& ListColumnReader::nested_repetition_levels() const {
    DORIS_CHECK(_element_reader != nullptr);
    return _element_reader->nested_repetition_levels();
}

int64_t ListColumnReader::nested_levels_written() const {
    DORIS_CHECK(_element_reader != nullptr);
    return _element_reader->nested_levels_written();
}

bool ListColumnReader::is_or_has_repeated_child() const {
    return true;
}

} // namespace doris::parquet
