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
#include <string>
#include <utility>
#include <vector>

#include "core/column/column_nullable.h"
#include "format/new_parquet/reader/list_column_reader.h"
#include "format/new_parquet/reader/nested_column_reader.h"
#include "format/new_parquet/reader/scalar_column_reader.h"
#include "format/new_parquet/reader/struct_column_reader.h"

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

struct MapChildReaders {
    ScalarColumnReader* key = nullptr;
    ScalarColumnReader* scalar_value = nullptr;
    StructColumnReader* struct_value = nullptr;
    ListColumnReader* list_value = nullptr;

    bool has_supported_value() const {
        return scalar_value != nullptr || struct_value != nullptr || list_value != nullptr;
    }
};

Status resolve_map_child_readers(const std::string& column_name, ParquetColumnReader* key_reader,
                                 ParquetColumnReader* value_reader, MapChildReaders* readers) {
    DORIS_CHECK(readers != nullptr);
    readers->key = dynamic_cast<ScalarColumnReader*>(key_reader);
    readers->scalar_value = dynamic_cast<ScalarColumnReader*>(value_reader);
    readers->struct_value = dynamic_cast<StructColumnReader*>(value_reader);
    readers->list_value = dynamic_cast<ListColumnReader*>(value_reader);
    if (readers->key == nullptr || !readers->has_supported_value()) {
        return Status::NotSupported(
                "Current parquet MAP reader only supports scalar key with scalar, scalar-child "
                "STRUCT, or scalar LIST value for column {}",
                column_name);
    }
    return Status::OK();
}

struct MapReadContext {
    ColumnMap* map_column = nullptr;
    NullMap* parent_null_map = nullptr;
    MutableColumnPtr key_column;
    MutableColumnPtr value_column;
    std::vector<uint64_t> entry_counts;
    NullMap parent_nulls;

    explicit MapReadContext(MutableColumnPtr& column)
            : map_column(map_column_from_output(column)),
              parent_null_map(null_map_from_nullable_output(column)) {
        DORIS_CHECK(map_column != nullptr);
        key_column = map_column->get_keys_ptr()->assert_mutable();
        value_column = map_column->get_values_ptr()->assert_mutable();
    }

    void finish() {
        map_column->get_keys_ptr() = std::move(key_column);
        map_column->get_values_ptr() = std::move(value_column);
        append_offsets(map_column->get_offsets(), entry_counts);
        append_parent_nulls(parent_null_map, parent_nulls);
    }
};

template <typename ValueBatch, typename ValueOverflow, typename ValueReader, typename ValueAppender>
Status read_aligned_map_entries(const std::string& column_name, const DataTypePtr& map_type,
                                int16_t map_nullable_definition_level, int16_t repeated_level,
                                ScalarColumnReader& key_reader, int16_t key_max_definition_level,
                                ValueReader& value_reader, ValueOverflow* value_overflow,
                                NestedScalarOverflow* key_overflow, ValueAppender value_appender,
                                int64_t rows, MapReadContext* context, int64_t* rows_read) {
    DORIS_CHECK(context != nullptr);
    RepeatedMapValueSink<ValueBatch, ValueOverflow, ValueReader, ValueAppender> sink;
    sink.column_name = &column_name;
    sink.map_type = &map_type;
    sink.map_nullable_definition_level = map_nullable_definition_level;
    sink.key_reader = &key_reader;
    sink.key_column = &context->key_column;
    sink.value_column = &context->value_column;
    sink.parent_state = {&context->entry_counts, &context->parent_nulls};
    sink.key_max_definition_level = key_max_definition_level;
    sink.value_stream = {&value_reader, value_overflow,
                         static_cast<int16_t>(map_nullable_definition_level + 1), "value"};
    sink.value_appender = value_appender;
    RETURN_IF_ERROR(assemble_repeated_levels(
            key_reader, repeated_level, static_cast<int16_t>(map_nullable_definition_level + 1),
            rows, key_overflow, sink, rows_read));
    sink.value_stream.move_tail_from_driver_overflow(*key_overflow);
    context->finish();
    return Status::OK();
}

template <typename ValueBatch, typename ValueOverflow, typename ValueReader>
Status skip_aligned_map_entries(const std::string& column_name,
                                int16_t map_nullable_definition_level, int16_t repeated_level,
                                ScalarColumnReader& key_reader, ValueReader& value_reader,
                                ValueOverflow* value_overflow, NestedScalarOverflow* key_overflow,
                                int64_t rows, int64_t* rows_read) {
    RepeatedAlignedValueSkipSink<ValueBatch, ValueOverflow, ValueReader> sink;
    sink.column_name = &column_name;
    sink.value_stream = {&value_reader, value_overflow,
                         static_cast<int16_t>(map_nullable_definition_level + 1), "value"};
    RETURN_IF_ERROR(assemble_repeated_levels(
            key_reader, repeated_level, static_cast<int16_t>(map_nullable_definition_level + 1),
            rows, key_overflow, sink, rows_read));
    sink.value_stream.move_tail_from_driver_overflow(*key_overflow);
    return Status::OK();
}

} // namespace

Status MapColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    if (column.get() == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet map read result pointer for column {}",
                                       _name);
    }
    if (_key_reader == nullptr || _value_reader == nullptr) {
        return Status::InternalError("Parquet map child reader is not initialized for column {}",
                                     _name);
    }
    MapChildReaders readers;
    RETURN_IF_ERROR(
            resolve_map_child_readers(_name, _key_reader.get(), _value_reader.get(), &readers));

    MapReadContext context(column);
    const int16_t key_max_definition_level = readers.key->descriptor()->max_definition_level();

    if (readers.scalar_value != nullptr) {
        const int16_t value_max_definition_level =
                readers.scalar_value->descriptor()->max_definition_level();
        return read_aligned_map_entries<NestedScalarBatch>(
                _name, _type, _nullable_definition_level, _repeated_repetition_level, *readers.key,
                key_max_definition_level, *readers.scalar_value, &_value_overflow, &_key_overflow,
                NestedScalarValueAppender {readers.scalar_value, "MAP", "value",
                                           value_max_definition_level},
                rows, &context, rows_read);
    }

    if (readers.list_value != nullptr) {
        auto* scalar_list_value_reader =
                dynamic_cast<ScalarColumnReader*>(readers.list_value->element_reader());
        if (scalar_list_value_reader == nullptr) {
            return Status::NotSupported(
                    "Current parquet MAP LIST value reader only supports scalar list values for "
                    "column {}",
                    _name);
        }

        auto* list_value_column = array_column_from_output(context.value_column);
        DORIS_CHECK(list_value_column != nullptr);
        auto* list_value_null_map = null_map_from_nullable_output(context.value_column);
        auto list_nested_column = list_value_column->get_data_ptr()->assert_mutable();
        remove_nullable_wrapper_if_required(*scalar_list_value_reader, &list_nested_column);
        std::vector<uint64_t> list_entry_counts;
        NullMap list_parent_nulls;
        const int16_t list_element_slot_definition_level =
                readers.list_value->nullable_definition_level() + 1;
        const int16_t list_element_max_definition_level =
                scalar_list_value_reader->descriptor()->max_definition_level();

        RepeatedMapListValueSink<NestedScalarValueAppender> sink;
        sink.column_name = &_name;
        sink.map_type = &_type;
        sink.list_type = &readers.list_value->type();
        sink.map_nullable_definition_level = _nullable_definition_level;
        sink.list_nullable_definition_level = readers.list_value->nullable_definition_level();
        sink.list_repeated_repetition_level = readers.list_value->repeated_repetition_level();
        sink.key_reader = readers.key;
        sink.key_column = &context.key_column;
        sink.list_element_column = &list_nested_column;
        sink.map_state = {&context.entry_counts, &context.parent_nulls};
        sink.list_state = {&list_entry_counts, &list_parent_nulls};
        sink.key_max_definition_level = key_max_definition_level;
        sink.key_stream = {readers.key, &_key_overflow,
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
        context.finish();
        return Status::OK();
    }

    return read_aligned_map_entries<NestedStructBatch>(
            _name, _type, _nullable_definition_level, _repeated_repetition_level, *readers.key,
            key_max_definition_level, *readers.struct_value, &_struct_value_overflow,
            &_key_overflow, NestedStructValueAppender {readers.struct_value}, rows, &context,
            rows_read);
}

Status MapColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }
    DORIS_CHECK(_key_reader != nullptr);
    DORIS_CHECK(_value_reader != nullptr);
    MapChildReaders readers;
    RETURN_IF_ERROR(
            resolve_map_child_readers(_name, _key_reader.get(), _value_reader.get(), &readers));

    int64_t rows_read = 0;
    if (readers.scalar_value != nullptr) {
        RETURN_IF_ERROR(skip_aligned_map_entries<NestedScalarBatch>(
                _name, _nullable_definition_level, _repeated_repetition_level, *readers.key,
                *readers.scalar_value, &_value_overflow, &_key_overflow, rows, &rows_read));
    } else if (readers.struct_value != nullptr) {
        RETURN_IF_ERROR(skip_aligned_map_entries<NestedStructBatch>(
                _name, _nullable_definition_level, _repeated_repetition_level, *readers.key,
                *readers.struct_value, &_struct_value_overflow, &_key_overflow, rows, &rows_read));
    } else {
        auto* scalar_list_value_reader =
                dynamic_cast<ScalarColumnReader*>(readers.list_value->element_reader());
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
        sink.key_reader = readers.key;
        sink.value_reader = readers.list_value;
        sink.key_stream = {readers.key, &_key_overflow,
                           static_cast<int16_t>(_nullable_definition_level + 1), "key"};
        RETURN_IF_ERROR(
                assemble_repeated_levels(*scalar_list_value_reader, _repeated_repetition_level,
                                         readers.list_value->nullable_definition_level() + 1, rows,
                                         &_value_overflow, sink, &rows_read));
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
