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

#include "format/new_parquet/reader/list_column_reader.h"

#include <parquet/api/schema.h>

#include <cstdint>
#include <utility>
#include <vector>

#include "format/new_parquet/reader/map_column_reader.h"
#include "format/new_parquet/reader/nested_column_reader.h"
#include "format/new_parquet/reader/scalar_column_reader.h"
#include "format/new_parquet/reader/struct_column_reader.h"

namespace doris::parquet {
namespace {

template <typename ValueBatch, typename ValueOverflow, typename ValueReader, typename ValueAppender>
struct RepeatedListMapValueSink {
    const std::string* column_name = nullptr;
    const DataTypePtr* list_type = nullptr;
    const DataTypePtr* map_type = nullptr;
    int16_t list_nullable_definition_level = 0;
    int16_t map_nullable_definition_level = 0;
    int16_t map_repeated_repetition_level = 0;
    ScalarColumnReader* key_reader = nullptr;
    MutableColumnPtr* key_column = nullptr;
    MutableColumnPtr* value_column = nullptr;
    RepeatedParentSinkState list_state;
    RepeatedChildSinkState map_state;
    int16_t key_max_definition_level = 0;
    NestedValueStream<ValueBatch, ValueOverflow, ValueReader> value_stream;
    ValueAppender value_appender;

    Status start_batch(const NestedScalarBatch& key_batch) {
        DORIS_CHECK(column_name != nullptr);
        return value_stream.read_aligned_to_driver(*column_name, key_batch, "");
    }

    Status start_parent(const NestedScalarBatch& key_batch, int64_t level_idx) {
        DORIS_CHECK(column_name != nullptr);
        DORIS_CHECK(list_type != nullptr);
        const int16_t def_level = key_batch.def_levels[level_idx];
        if (def_level < list_nullable_definition_level) {
            RETURN_IF_ERROR(list_state.append_null_parent(*column_name, "LIST", *list_type));
            return value_appender.skip_shape_only_slot();
        }
        list_state.append_present_parent();
        if (def_level == list_nullable_definition_level) {
            return value_appender.skip_shape_only_slot();
        }
        return append_map(key_batch, level_idx);
    }

    Status append_repeated(const NestedScalarBatch& key_batch, int64_t level_idx) {
        if (key_batch.rep_levels[level_idx] < map_repeated_repetition_level) {
            return append_map(key_batch, level_idx);
        }
        return append_entry(key_batch, level_idx);
    }

    Status append_map(const NestedScalarBatch& key_batch, int64_t level_idx) {
        DORIS_CHECK(column_name != nullptr);
        DORIS_CHECK(map_type != nullptr);
        RETURN_IF_ERROR(list_state.add_entry(*column_name));
        const int16_t def_level = key_batch.def_levels[level_idx];
        if (def_level < map_nullable_definition_level) {
            RETURN_IF_ERROR(value_appender.skip_shape_only_slot());
            return map_state.append_null_child(*column_name, "LIST", "MAP element", *map_type);
        }
        map_state.append_present_child();
        if (def_level == map_nullable_definition_level) {
            return value_appender.skip_shape_only_slot();
        }
        return append_entry(key_batch, level_idx);
    }

    Status append_entry(const NestedScalarBatch& key_batch, int64_t level_idx) {
        DORIS_CHECK(column_name != nullptr);
        DORIS_CHECK(key_reader != nullptr);
        DORIS_CHECK(key_column != nullptr);
        DORIS_CHECK(value_column != nullptr);
        RETURN_IF_ERROR(map_state.require_child(*column_name, "LIST MAP element"));
        if (key_batch.def_levels[level_idx] != key_max_definition_level) {
            return Status::Corruption("Parquet LIST column {} contains null map key", *column_name);
        }
        RETURN_IF_ERROR(append_scalar_batch_value(*key_reader, key_batch, level_idx, *key_column));
        RETURN_IF_ERROR(
                value_appender.append(*column_name, value_stream.batch, level_idx, *value_column));
        return map_state.add_entry(*column_name, "LIST MAP element");
    }
};

} // namespace

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

        RepeatedListValueSink<NestedScalarBatch, NestedScalarValueAppender> sink {
                &_name,
                &_type,
                _nullable_definition_level,
                &nested_column,
                {&entry_counts, &parent_nulls},
                {element_reader, "LIST", "element", element_max_definition_level}};
        RETURN_IF_ERROR(assemble_repeated_levels(*element_reader, _repeated_repetition_level,
                                                 element_slot_definition_level, rows,
                                                 &_element_overflow, sink, rows_read));

        array_column->get_data_ptr() = std::move(nested_column);
        append_offsets(array_column->get_offsets(), entry_counts);
        append_parent_nulls(parent_null_map, parent_nulls);
        return Status::OK();
    }

    if (auto* struct_element_reader = dynamic_cast<StructColumnReader*>(_element_reader.get())) {
        RepeatedListValueSink<NestedStructBatch, NestedStructValueAppender> sink {
                &_name,
                &_type,
                _nullable_definition_level,
                &nested_column,
                {&entry_counts, &parent_nulls},
                {struct_element_reader}};
        RETURN_IF_ERROR(assemble_repeated_struct_levels(
                *struct_element_reader, _repeated_repetition_level, element_slot_definition_level,
                rows, &_struct_element_overflow, sink, rows_read));

        array_column->get_data_ptr() = std::move(nested_column);
        append_offsets(array_column->get_offsets(), entry_counts);
        append_parent_nulls(parent_null_map, parent_nulls);
        return Status::OK();
    }

    if (auto* map_element_reader = dynamic_cast<MapColumnReader*>(_element_reader.get())) {
        auto* key_reader = dynamic_cast<ScalarColumnReader*>(map_element_reader->_key_reader.get());
        auto* value_reader =
                dynamic_cast<ScalarColumnReader*>(map_element_reader->_value_reader.get());
        if (key_reader == nullptr || value_reader == nullptr) {
            return Status::NotSupported(
                    "Current parquet LIST MAP element reader only supports scalar key/value for "
                    "column {}",
                    _name);
        }

        auto* element_map_column = map_column_from_output(nested_column);
        DORIS_CHECK(element_map_column != nullptr);
        auto* element_map_null_map = null_map_from_nullable_output(nested_column);
        auto key_column = element_map_column->get_keys_ptr()->assume_mutable();
        auto value_column = element_map_column->get_values_ptr()->assume_mutable();
        std::vector<uint64_t> map_entry_counts;
        NullMap map_parent_nulls;
        const int16_t map_entry_definition_level =
                map_element_reader->_nullable_definition_level + 1;
        const int16_t key_max_definition_level = key_reader->descriptor()->max_definition_level();
        const int16_t value_max_definition_level =
                value_reader->descriptor()->max_definition_level();

        RepeatedListMapValueSink<NestedScalarBatch, NestedScalarOverflow, ScalarColumnReader,
                                 NestedScalarValueAppender>
                sink;
        sink.column_name = &_name;
        sink.list_type = &_type;
        sink.map_type = &map_element_reader->_type;
        sink.list_nullable_definition_level = _nullable_definition_level;
        sink.map_nullable_definition_level = map_element_reader->_nullable_definition_level;
        sink.map_repeated_repetition_level = map_element_reader->_repeated_repetition_level;
        sink.key_reader = key_reader;
        sink.key_column = &key_column;
        sink.value_column = &value_column;
        sink.list_state = {&entry_counts, &parent_nulls};
        sink.map_state = {&map_entry_counts, &map_parent_nulls};
        sink.key_max_definition_level = key_max_definition_level;
        sink.value_stream = {value_reader, &map_element_reader->_value_overflow,
                             map_entry_definition_level, "value"};
        sink.value_appender = {value_reader, "LIST", "MAP value", value_max_definition_level};
        RETURN_IF_ERROR(assemble_repeated_levels(
                *key_reader, _repeated_repetition_level, map_entry_definition_level, rows,
                &map_element_reader->_key_overflow, sink, rows_read));
        sink.value_stream.move_tail_from_driver_overflow(map_element_reader->_key_overflow);

        element_map_column->get_keys_ptr() = std::move(key_column);
        element_map_column->get_values_ptr() = std::move(value_column);
        append_offsets(element_map_column->get_offsets(), map_entry_counts);
        append_parent_nulls(element_map_null_map, map_parent_nulls);
        array_column->get_data_ptr() = std::move(nested_column);
        append_offsets(array_column->get_offsets(), entry_counts);
        append_parent_nulls(parent_null_map, parent_nulls);
        return Status::OK();
    }

    auto* list_element_reader = dynamic_cast<ListColumnReader*>(_element_reader.get());
    if (list_element_reader == nullptr) {
        return Status::NotSupported(
                "Current parquet LIST reader only supports scalar, scalar-child STRUCT, nested "
                "LIST, or scalar MAP elements for column {}",
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

    RepeatedNestedListValueSink<NestedScalarValueAppender> sink {
            &_name,
            &_type,
            &list_element_reader->_type,
            _nullable_definition_level,
            list_element_reader->_nullable_definition_level,
            list_element_reader->_repeated_repetition_level,
            &inner_nested_column,
            {&entry_counts, &parent_nulls},
            {&inner_entry_counts, &inner_parent_nulls},
            {scalar_nested_element_reader, "LIST", "nested element",
             nested_element_max_definition_level}};
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
    int64_t rows_read = 0;
    if (auto* element_reader = dynamic_cast<ScalarColumnReader*>(_element_reader.get())) {
        RepeatedShapeSkipSink<NestedScalarBatch> sink;
        RETURN_IF_ERROR(assemble_repeated_levels(*element_reader, _repeated_repetition_level,
                                                 _nullable_definition_level + 1, rows,
                                                 &_element_overflow, sink, &rows_read));
    } else if (auto* struct_element_reader =
                       dynamic_cast<StructColumnReader*>(_element_reader.get())) {
        RepeatedShapeSkipSink<NestedStructBatch> sink;
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
        RepeatedShapeSkipSink<NestedScalarBatch> sink;
        RETURN_IF_ERROR(
                assemble_repeated_levels(*scalar_nested_element_reader, _repeated_repetition_level,
                                         list_element_reader->_nullable_definition_level + 1, rows,
                                         &_element_overflow, sink, &rows_read));
    } else if (auto* map_element_reader = dynamic_cast<MapColumnReader*>(_element_reader.get())) {
        auto* key_reader = dynamic_cast<ScalarColumnReader*>(map_element_reader->_key_reader.get());
        auto* value_reader =
                dynamic_cast<ScalarColumnReader*>(map_element_reader->_value_reader.get());
        if (key_reader == nullptr || value_reader == nullptr) {
            return Status::NotSupported(
                    "Current parquet LIST MAP element skip only supports scalar key/value for "
                    "column {}",
                    _name);
        }
        RepeatedAlignedValueSkipSink<NestedScalarBatch, NestedScalarOverflow, ScalarColumnReader>
                sink;
        sink.column_name = &_name;
        sink.value_stream = {
                value_reader, &map_element_reader->_value_overflow,
                static_cast<int16_t>(map_element_reader->_nullable_definition_level + 1), "value"};
        RETURN_IF_ERROR(assemble_repeated_levels(
                *key_reader, _repeated_repetition_level,
                static_cast<int16_t>(map_element_reader->_nullable_definition_level + 1), rows,
                &map_element_reader->_key_overflow, sink, &rows_read));
        sink.value_stream.move_tail_from_driver_overflow(map_element_reader->_key_overflow);
    } else {
        return Status::NotSupported(
                "Current parquet LIST reader only supports scalar, scalar-child STRUCT, nested "
                "LIST, or scalar MAP elements for column {}",
                _name);
    }
    if (rows_read != rows) {
        return Status::Corruption("Failed to skip parquet LIST column {}: skipped {} of {} rows",
                                  _name, rows_read, rows);
    }
    return Status::OK();
}

} // namespace doris::parquet
