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

#include "format/new_parquet/reader/nested_column_reader.h"
#include "format/new_parquet/reader/scalar_column_reader.h"
#include "format/new_parquet/reader/struct_column_reader.h"

namespace doris::parquet {

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

} // namespace doris::parquet
