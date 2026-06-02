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

#include "format/new_parquet/reader/map_column_reader.h"

#include <parquet/api/schema.h>

#include <cstdint>
#include <utility>
#include <vector>

#include "format/new_parquet/reader/list_column_reader.h"
#include "format/new_parquet/reader/nested_column_reader.h"
#include "format/new_parquet/reader/scalar_column_reader.h"
#include "format/new_parquet/reader/struct_column_reader.h"

namespace doris::parquet {

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

        RepeatedMapValueSink<NestedScalarBatch, NestedScalarOverflow, ScalarColumnReader,
                             NestedScalarValueAppender>
                sink;
        sink.column_name = &_name;
        sink.map_type = &_type;
        sink.map_nullable_definition_level = _nullable_definition_level;
        sink.key_reader = key_reader;
        sink.key_column = &key_column;
        sink.value_column = &value_column;
        sink.parent_state = {&entry_counts, &parent_nulls};
        sink.key_max_definition_level = key_max_definition_level;
        sink.value_stream = {value_reader, &_value_overflow,
                             static_cast<int16_t>(_nullable_definition_level + 1), "value"};
        sink.value_appender = {value_reader, "MAP", "value", value_max_definition_level};
        RETURN_IF_ERROR(assemble_repeated_levels(*key_reader, _repeated_repetition_level,
                                                 entry_definition_level, rows, &_key_overflow, sink,
                                                 rows_read));
        sink.value_stream.move_tail_from_driver_overflow(_key_overflow);

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

        RepeatedMapListValueSink<NestedScalarValueAppender> sink;
        sink.column_name = &_name;
        sink.map_type = &_type;
        sink.list_type = &list_value_reader->type();
        sink.map_nullable_definition_level = _nullable_definition_level;
        sink.list_nullable_definition_level = list_value_reader->nullable_definition_level();
        sink.list_repeated_repetition_level = list_value_reader->repeated_repetition_level();
        sink.key_reader = key_reader;
        sink.key_column = &key_column;
        sink.list_element_column = &list_nested_column;
        sink.map_state = {&entry_counts, &parent_nulls};
        sink.list_state = {&list_entry_counts, &list_parent_nulls};
        sink.key_max_definition_level = key_max_definition_level;
        sink.key_stream = {key_reader, &_key_overflow,
                           static_cast<int16_t>(_nullable_definition_level + 1), "key"};
        sink.value_appender = {scalar_list_value_reader, "MAP", "LIST value element",
                               list_element_max_definition_level};
        RETURN_IF_ERROR(assemble_repeated_levels(
                *scalar_list_value_reader, _repeated_repetition_level,
                list_element_slot_definition_level, rows, &_value_overflow, sink, rows_read));
        if (!_value_overflow.empty()) {
            sink.key_stream.move_tail_to_overflow();
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

    RepeatedMapValueSink<NestedStructBatch, NestedStructOverflow, StructColumnReader,
                         NestedStructValueAppender>
            sink;
    sink.column_name = &_name;
    sink.map_type = &_type;
    sink.map_nullable_definition_level = _nullable_definition_level;
    sink.key_reader = key_reader;
    sink.key_column = &key_column;
    sink.value_column = &value_column;
    sink.parent_state = {&entry_counts, &parent_nulls};
    sink.key_max_definition_level = key_max_definition_level;
    sink.value_stream = {struct_value_reader, &_struct_value_overflow,
                         static_cast<int16_t>(_nullable_definition_level + 1), "value"};
    sink.value_appender = {struct_value_reader};
    RETURN_IF_ERROR(assemble_repeated_levels(*key_reader, _repeated_repetition_level,
                                             entry_definition_level, rows, &_key_overflow, sink,
                                             rows_read));
    sink.value_stream.move_tail_from_driver_overflow(_key_overflow);

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
        RepeatedAlignedValueSkipSink<NestedScalarBatch, NestedScalarOverflow, ScalarColumnReader>
                sink;
        sink.column_name = &_name;
        sink.value_stream = {value_reader, &_value_overflow,
                             static_cast<int16_t>(_nullable_definition_level + 1), "value"};
        RETURN_IF_ERROR(assemble_repeated_levels(*key_reader, _repeated_repetition_level,
                                                 _nullable_definition_level + 1, rows,
                                                 &_key_overflow, sink, &rows_read));
        sink.value_stream.move_tail_from_driver_overflow(_key_overflow);
    } else if (struct_value_reader != nullptr) {
        RepeatedAlignedValueSkipSink<NestedStructBatch, NestedStructOverflow, StructColumnReader>
                sink;
        sink.column_name = &_name;
        sink.value_stream = {struct_value_reader, &_struct_value_overflow,
                             static_cast<int16_t>(_nullable_definition_level + 1), "value"};
        RETURN_IF_ERROR(assemble_repeated_levels(*key_reader, _repeated_repetition_level,
                                                 _nullable_definition_level + 1, rows,
                                                 &_key_overflow, sink, &rows_read));
        sink.value_stream.move_tail_from_driver_overflow(_key_overflow);
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
            NestedScalarSlotStream key_stream;

            Status start_batch(const NestedScalarBatch& value_batch) {
                return key_stream.read_records(self->_name, value_batch.records_read,
                                               " while skipping");
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
                return key_stream.consume_aligned_slot(self->_name, value_batch, value_level_idx,
                                                       " while skipping");
            }
        };
        ListSkipSink sink;
        sink.self = this;
        sink.key_reader = key_reader;
        sink.value_reader = list_value_reader;
        sink.key_stream = {key_reader, &_key_overflow,
                           static_cast<int16_t>(_nullable_definition_level + 1), "key"};
        RETURN_IF_ERROR(assemble_repeated_levels(*scalar_list_value_reader,
                                                 _repeated_repetition_level,
                                                 list_value_reader->nullable_definition_level() + 1,
                                                 rows, &_value_overflow, sink, &rows_read));
        if (!_value_overflow.empty()) {
            sink.key_stream.move_tail_to_overflow();
        }
    }
    if (rows_read != rows) {
        return Status::Corruption("Failed to skip parquet MAP column {}: skipped {} of {} rows",
                                  _name, rows_read, rows);
    }
    return Status::OK();
}

} // namespace doris::parquet
