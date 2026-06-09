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

#include "format_v2/parquet/reader/map_column_reader.h"

#include <parquet/api/schema.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/column/column_nullable.h"
#include "format_v2/parquet/reader/list_column_reader.h"
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

struct MapListValueReadContext {
    ColumnArray* list_value_column = nullptr;
    NullMap* list_value_null_map = nullptr;
    MutableColumnPtr list_nested_column;
    std::vector<uint64_t> list_entry_counts;
    NullMap list_parent_nulls;

    MapListValueReadContext(MutableColumnPtr& value_column,
                            const ScalarColumnReader& list_element_reader)
            : list_value_column(array_column_from_output(value_column)),
              list_value_null_map(null_map_from_nullable_output(value_column)) {
        DORIS_CHECK(list_value_column != nullptr);
        list_nested_column = list_value_column->get_data_ptr()->assert_mutable();
        remove_nullable_wrapper_if_required(list_element_reader, &list_nested_column);
    }

    void finish() {
        list_value_column->get_data_ptr() = std::move(list_nested_column);
        append_offsets(list_value_column->get_offsets(), list_entry_counts);
        append_parent_nulls(list_value_null_map, list_parent_nulls);
    }
};

template <typename Batch, typename Overflow, typename ReadBatchFn, typename MoveTailFn,
          typename Sink>
Status assemble_map_repeated_levels(const std::string& column_name, int16_t repeated_level,
                                    int64_t rows, Overflow* overflow, ReadBatchFn&& read_batch,
                                    MoveTailFn&& move_tail, Sink& sink, int64_t* rows_read) {
    // MAP keeps a reader-local repeated assembler because Doris materializes MAP as
    // ColumnMap, not as LIST<STRUCT<key,value>>. The stream mechanics are similar
    // to LIST, but the sink must align key/value streams and write keys, values,
    // and map offsets independently.
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
        RETURN_IF_ERROR(sink.start_batch(*batch));

        NestedShapeCursor<Batch> cursor(*batch);
        int64_t level_idx = 0;
        while (level_idx < cursor.levels_written()) {
            const bool starts_parent = cursor.starts_parent(level_idx, repeated_level);
            if (starts_parent && *rows_read >= rows) {
                move_tail(*batch, level_idx, overflow);
                return Status::OK();
            }
            if (starts_parent) {
                RETURN_IF_ERROR(sink.start_parent(*batch, level_idx));
                ++*rows_read;
            } else {
                if (*rows_read == 0) {
                    return Status::Corruption(
                            "Repeated parquet stream starts with repeated level for column {}",
                            column_name);
                }
                RETURN_IF_ERROR(sink.append_repeated(*batch, level_idx));
            }
            ++level_idx;
        }

        if (from_overflow) {
            overflow->clear();
        }
    }
    return Status::OK();
}

template <typename Batch, typename Overflow, typename Reader>
struct MapValueStream {
    MapValueStream() = default;

    MapValueStream(Reader* reader_, Overflow* overflow_, int16_t value_slot_definition_level_,
                   std::string_view value_name_)
            : reader(reader_),
              overflow(overflow_),
              value_slot_definition_level(value_slot_definition_level_),
              value_name(value_name_) {}

    Reader* reader = nullptr;
    Overflow* overflow = nullptr;
    int16_t value_slot_definition_level = 0;
    std::string_view value_name;
    Batch batch;

    Status read_aligned_to_driver(const std::string& column_name,
                                  const NestedScalarBatch& driver_batch, std::string_view action) {
        DORIS_CHECK(reader != nullptr);
        DORIS_CHECK(overflow != nullptr);
        RETURN_IF_ERROR(read_nested_batch_from_overflow(
                *reader, driver_batch.records_read, value_slot_definition_level, overflow, &batch));
        return validate_nested_shape_alignment(column_name, driver_batch, batch, value_name,
                                               action);
    }

    void move_tail_from_driver_overflow(const NestedScalarOverflow& driver_overflow) {
        if (driver_overflow.empty()) {
            return;
        }
        const int64_t levels_written = nested_shape_levels_written(batch);
        DORIS_CHECK(levels_written >= driver_overflow.batch.levels_written);
        move_nested_tail(batch, levels_written - driver_overflow.batch.levels_written, overflow);
    }
};

struct MapScalarSlotStream {
    MapScalarSlotStream() = default;

    MapScalarSlotStream(ScalarColumnReader* reader_, NestedScalarOverflow* overflow_,
                        int16_t value_slot_definition_level_, std::string_view slot_name_)
            : reader(reader_),
              overflow(overflow_),
              value_slot_definition_level(value_slot_definition_level_),
              slot_name(slot_name_) {}

    ScalarColumnReader* reader = nullptr;
    NestedScalarOverflow* overflow = nullptr;
    int16_t value_slot_definition_level = 0;
    std::string_view slot_name;
    NestedScalarBatch batch;
    int64_t level_idx = 0;
    NestedScalarValueCursor value_cursor;

    Status read_records(const std::string& column_name, int64_t records, std::string_view action) {
        DORIS_CHECK(reader != nullptr);
        DORIS_CHECK(overflow != nullptr);
        RETURN_IF_ERROR(read_nested_scalar_batch_from_overflow(
                *reader, records, value_slot_definition_level, overflow, &batch));
        if (batch.records_read != records) {
            return Status::Corruption(
                    "Parquet nested stream {} rows are not aligned for column {}{}: expected "
                    "rows={}, actual rows={}",
                    slot_name, column_name, action, records, batch.records_read);
        }
        level_idx = 0;
        value_cursor.reset(&batch);
        return Status::OK();
    }

    Status consume_aligned_slot(const std::string& column_name,
                                const NestedScalarBatch& driver_batch, int64_t driver_level_idx,
                                std::string_view action) {
        if (level_idx >= batch.levels_written) {
            return Status::Corruption(
                    "Parquet nested stream {} ended before driver stream for column {}{}",
                    slot_name, column_name, action);
        }
        if (batch.rep_levels[level_idx] != driver_batch.rep_levels[driver_level_idx]) {
            return Status::Corruption(
                    "Parquet nested stream {} repetition levels are not aligned for column {}{}",
                    slot_name, column_name, action);
        }
        ++level_idx;
        return Status::OK();
    }

    Status require_last_slot_defined(const std::string& column_name, int16_t max_definition_level,
                                     std::string_view slot_kind) const {
        DORIS_CHECK(level_idx > 0);
        if (batch.def_levels[level_idx - 1] != max_definition_level) {
            return Status::Corruption("Parquet MAP column {} contains null {}", column_name,
                                      slot_kind);
        }
        return Status::OK();
    }

    void move_tail_to_overflow() { move_nested_scalar_tail(batch, level_idx, overflow); }
};

template <typename ValueBatch, typename ValueOverflow, typename ValueReader, typename ValueAppender>
struct MapValueSink {
    const std::string* column_name = nullptr;
    const DataTypePtr* map_type = nullptr;
    int16_t map_nullable_definition_level = 0;
    ScalarColumnReader* key_reader = nullptr;
    MutableColumnPtr* key_column = nullptr;
    MutableColumnPtr* value_column = nullptr;
    RepeatedParentSinkState parent_state;
    int16_t key_max_definition_level = 0;
    MapValueStream<ValueBatch, ValueOverflow, ValueReader> value_stream;
    ValueAppender value_appender;
    NestedScalarValueCursor key_cursor;
    NestedScalarValueCursor value_cursor;

    Status start_batch(const NestedScalarBatch& key_batch) {
        DORIS_CHECK(column_name != nullptr);
        key_cursor.reset(&key_batch);
        RETURN_IF_ERROR(value_stream.read_aligned_to_driver(*column_name, key_batch, ""));
        reset_value_appender_cursor(value_stream.batch);
        return Status::OK();
    }

    Status start_parent(const NestedScalarBatch& key_batch, int64_t level_idx) {
        DORIS_CHECK(column_name != nullptr);
        DORIS_CHECK(map_type != nullptr);
        const int16_t def_level = key_batch.def_levels[level_idx];
        if (def_level < map_nullable_definition_level) {
            RETURN_IF_ERROR(parent_state.append_null_parent(*column_name, "MAP", *map_type));
            return value_appender.skip_shape_only_slot();
        }
        parent_state.append_present_parent();
        if (def_level == map_nullable_definition_level) {
            return value_appender.skip_shape_only_slot();
        }
        return append_entry(key_batch, level_idx);
    }

    Status append_repeated(const NestedScalarBatch& key_batch, int64_t level_idx) {
        return append_entry(key_batch, level_idx);
    }

    Status append_entry(const NestedScalarBatch& key_batch, int64_t level_idx) {
        DORIS_CHECK(column_name != nullptr);
        DORIS_CHECK(key_reader != nullptr);
        DORIS_CHECK(key_column != nullptr);
        DORIS_CHECK(value_column != nullptr);
        RETURN_IF_ERROR(parent_state.require_parent(*column_name));
        if (key_batch.def_levels[level_idx] != key_max_definition_level) {
            return Status::Corruption("Parquet MAP column {} contains null map key", *column_name);
        }
        RETURN_IF_ERROR(append_scalar_batch_value(*key_reader, key_batch, level_idx, &key_cursor,
                                                  *key_column));
        RETURN_IF_ERROR(
                value_appender.append(*column_name, value_stream.batch, level_idx, *value_column));
        return parent_state.add_entry(*column_name);
    }

    void reset_value_appender_cursor(const NestedScalarBatch& batch) {
        value_cursor.reset(&batch);
        value_appender.value_cursor = &value_cursor;
    }

    void reset_value_appender_cursor(const NestedStructBatch&) {}
};

template <typename ValueAppender>
struct MapListValueSink {
    const std::string* column_name = nullptr;
    const DataTypePtr* map_type = nullptr;
    const DataTypePtr* list_type = nullptr;
    int16_t map_nullable_definition_level = 0;
    int16_t list_nullable_definition_level = 0;
    int16_t list_repeated_repetition_level = 0;
    ScalarColumnReader* key_reader = nullptr;
    MutableColumnPtr* key_column = nullptr;
    MutableColumnPtr* list_element_column = nullptr;
    RepeatedParentSinkState map_state;
    RepeatedChildSinkState list_state;
    int16_t key_max_definition_level = 0;
    MapScalarSlotStream key_stream;
    ValueAppender value_appender;
    NestedScalarValueCursor value_cursor;

    Status start_batch(const NestedScalarBatch& value_batch) {
        DORIS_CHECK(column_name != nullptr);
        value_cursor.reset(&value_batch);
        value_appender.value_cursor = &value_cursor;
        return key_stream.read_records(*column_name, value_batch.records_read, "");
    }

    Status start_parent(const NestedScalarBatch& value_batch, int64_t level_idx) {
        DORIS_CHECK(column_name != nullptr);
        DORIS_CHECK(map_type != nullptr);
        const int16_t def_level = value_batch.def_levels[level_idx];
        if (def_level < map_nullable_definition_level) {
            RETURN_IF_ERROR(consume_key_slot(value_batch, level_idx, false));
            return map_state.append_null_parent(*column_name, "MAP", *map_type);
        }
        map_state.append_present_parent();
        if (def_level == map_nullable_definition_level) {
            RETURN_IF_ERROR(consume_key_slot(value_batch, level_idx, false));
            return Status::OK();
        }
        return append_entry(value_batch, level_idx);
    }

    Status append_repeated(const NestedScalarBatch& value_batch, int64_t level_idx) {
        if (value_batch.rep_levels[level_idx] < list_repeated_repetition_level) {
            return append_entry(value_batch, level_idx);
        }
        return append_list_element(value_batch, level_idx);
    }

    Status append_entry(const NestedScalarBatch& value_batch, int64_t level_idx) {
        DORIS_CHECK(column_name != nullptr);
        DORIS_CHECK(key_reader != nullptr);
        DORIS_CHECK(key_column != nullptr);
        DORIS_CHECK(list_type != nullptr);
        RETURN_IF_ERROR(consume_key_slot(value_batch, level_idx, true));
        RETURN_IF_ERROR(append_scalar_batch_value(*key_reader, key_stream.batch,
                                                  key_stream.level_idx - 1,
                                                  &key_stream.value_cursor, *key_column));
        RETURN_IF_ERROR(map_state.add_entry(*column_name));
        const int16_t def_level = value_batch.def_levels[level_idx];
        if (def_level < list_nullable_definition_level) {
            return list_state.append_null_child(*column_name, "MAP", "LIST value", *list_type);
        }
        list_state.append_present_child();
        if (def_level == list_nullable_definition_level) {
            return Status::OK();
        }
        return append_list_element(value_batch, level_idx);
    }

    Status consume_key_slot(const NestedScalarBatch& value_batch, int64_t value_level_idx,
                            bool require_defined_key) {
        DORIS_CHECK(column_name != nullptr);
        RETURN_IF_ERROR(
                key_stream.consume_aligned_slot(*column_name, value_batch, value_level_idx, ""));
        if (require_defined_key) {
            RETURN_IF_ERROR(key_stream.require_last_slot_defined(
                    *column_name, key_max_definition_level, "map key"));
        }
        return Status::OK();
    }

    Status append_list_element(const NestedScalarBatch& value_batch, int64_t level_idx) {
        DORIS_CHECK(column_name != nullptr);
        DORIS_CHECK(list_element_column != nullptr);
        RETURN_IF_ERROR(list_state.require_child(*column_name, "MAP LIST value"));
        RETURN_IF_ERROR(
                value_appender.append(*column_name, value_batch, level_idx, *list_element_column));
        return list_state.add_entry(*column_name, "MAP LIST value");
    }
};

template <typename ValueBatch, typename ValueOverflow, typename ValueReader>
struct MapAlignedValueSkipSink {
    const std::string* column_name = nullptr;
    MapValueStream<ValueBatch, ValueOverflow, ValueReader> value_stream;

    Status start_batch(const NestedScalarBatch& key_batch) {
        DORIS_CHECK(column_name != nullptr);
        return value_stream.read_aligned_to_driver(*column_name, key_batch, " while skipping");
    }

    Status start_parent(const NestedScalarBatch&, int64_t) { return Status::OK(); }

    Status append_repeated(const NestedScalarBatch&, int64_t) { return Status::OK(); }
};

template <typename Sink>
Status assemble_map_repeated_levels(ScalarColumnReader& driver_reader, int16_t repeated_level,
                                    int16_t value_slot_definition_level, int64_t rows,
                                    NestedScalarOverflow* overflow, Sink& sink,
                                    int64_t* rows_read) {
    auto read_batch = [&](int64_t batch_rows, NestedScalarBatch* batch) {
        return read_nested_scalar_batch(driver_reader, batch_rows, value_slot_definition_level,
                                        batch);
    };
    return assemble_map_repeated_levels<NestedScalarBatch>(
            driver_reader.name(), repeated_level, rows, overflow, read_batch,
            move_nested_scalar_tail, sink, rows_read);
}

template <typename ValueBatch, typename ValueOverflow, typename ValueReader, typename ValueAppender>
Status read_aligned_map_entries(const std::string& column_name, const DataTypePtr& map_type,
                                int16_t map_nullable_definition_level, int16_t repeated_level,
                                ScalarColumnReader& key_reader, int16_t key_max_definition_level,
                                ValueReader& value_reader, ValueOverflow* value_overflow,
                                NestedScalarOverflow* key_overflow, ValueAppender value_appender,
                                int64_t rows, MapReadContext* context, int64_t* rows_read) {
    DORIS_CHECK(context != nullptr);
    MapValueSink<ValueBatch, ValueOverflow, ValueReader, ValueAppender> sink;
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
    RETURN_IF_ERROR(assemble_map_repeated_levels(
            key_reader, repeated_level, static_cast<int16_t>(map_nullable_definition_level + 1),
            rows, key_overflow, sink, rows_read));
    sink.value_stream.move_tail_from_driver_overflow(*key_overflow);
    context->finish();
    return Status::OK();
}

Status read_map_list_value_entries(const std::string& column_name, const DataTypePtr& map_type,
                                   int16_t map_nullable_definition_level, int16_t repeated_level,
                                   ScalarColumnReader& key_reader, int16_t key_max_definition_level,
                                   ListColumnReader& list_value_reader,
                                   ScalarColumnReader& list_element_reader,
                                   NestedScalarOverflow* key_overflow,
                                   NestedScalarOverflow* value_overflow, int64_t rows,
                                   MapReadContext* context, int64_t* rows_read) {
    DORIS_CHECK(context != nullptr);
    MapListValueReadContext list_context(context->value_column, list_element_reader);
    const int16_t list_element_slot_definition_level =
            list_value_reader.nullable_definition_level() + 1;
    const int16_t list_element_max_definition_level =
            list_element_reader.descriptor()->max_definition_level();

    MapListValueSink<NestedScalarValueAppender> sink;
    sink.column_name = &column_name;
    sink.map_type = &map_type;
    sink.list_type = &list_value_reader.type();
    sink.map_nullable_definition_level = map_nullable_definition_level;
    sink.list_nullable_definition_level = list_value_reader.nullable_definition_level();
    sink.list_repeated_repetition_level = list_value_reader.repeated_repetition_level();
    sink.key_reader = &key_reader;
    sink.key_column = &context->key_column;
    sink.list_element_column = &list_context.list_nested_column;
    sink.map_state = {&context->entry_counts, &context->parent_nulls};
    sink.list_state = {&list_context.list_entry_counts, &list_context.list_parent_nulls};
    sink.key_max_definition_level = key_max_definition_level;
    sink.key_stream = {&key_reader, key_overflow,
                       static_cast<int16_t>(map_nullable_definition_level + 1), "key"};
    sink.value_appender = {&list_element_reader, "MAP", "LIST value element",
                           list_element_max_definition_level};
    RETURN_IF_ERROR(assemble_map_repeated_levels(list_element_reader, repeated_level,
                                                 list_element_slot_definition_level, rows,
                                                 value_overflow, sink, rows_read));
    if (!value_overflow->empty()) {
        sink.key_stream.move_tail_to_overflow();
    }

    list_context.finish();
    context->finish();
    return Status::OK();
}

template <typename ValueBatch, typename ValueOverflow, typename ValueReader>
Status skip_aligned_map_entries(const std::string& column_name,
                                int16_t map_nullable_definition_level, int16_t repeated_level,
                                ScalarColumnReader& key_reader, ValueReader& value_reader,
                                ValueOverflow* value_overflow, NestedScalarOverflow* key_overflow,
                                int64_t rows, int64_t* rows_read) {
    MapAlignedValueSkipSink<ValueBatch, ValueOverflow, ValueReader> sink;
    sink.column_name = &column_name;
    sink.value_stream = {&value_reader, value_overflow,
                         static_cast<int16_t>(map_nullable_definition_level + 1), "value"};
    RETURN_IF_ERROR(assemble_map_repeated_levels(
            key_reader, repeated_level, static_cast<int16_t>(map_nullable_definition_level + 1),
            rows, key_overflow, sink, rows_read));
    sink.value_stream.move_tail_from_driver_overflow(*key_overflow);
    return Status::OK();
}

struct MapListValueSkipSink {
    const std::string* column_name = nullptr;
    ListColumnReader* value_reader = nullptr;
    MapScalarSlotStream key_stream;

    Status start_batch(const NestedScalarBatch& value_batch) {
        DORIS_CHECK(column_name != nullptr);
        return key_stream.read_records(*column_name, value_batch.records_read, " while skipping");
    }

    Status start_parent(const NestedScalarBatch& value_batch, int64_t level_idx) {
        return consume_key_slot(value_batch, level_idx);
    }

    Status append_repeated(const NestedScalarBatch& value_batch, int64_t level_idx) {
        DORIS_CHECK(value_reader != nullptr);
        if (value_batch.rep_levels[level_idx] < value_reader->repeated_repetition_level()) {
            return consume_key_slot(value_batch, level_idx);
        }
        return Status::OK();
    }

    Status consume_key_slot(const NestedScalarBatch& value_batch, int64_t value_level_idx) {
        DORIS_CHECK(column_name != nullptr);
        return key_stream.consume_aligned_slot(*column_name, value_batch, value_level_idx,
                                               " while skipping");
    }
};

Status skip_map_list_value_entries(
        const std::string& column_name, int16_t map_nullable_definition_level,
        int16_t repeated_level, ScalarColumnReader& key_reader, ListColumnReader& list_value_reader,
        ScalarColumnReader& list_element_reader, NestedScalarOverflow* key_overflow,
        NestedScalarOverflow* value_overflow, int64_t rows, int64_t* rows_read) {
    MapListValueSkipSink sink;
    sink.column_name = &column_name;
    sink.value_reader = &list_value_reader;
    sink.key_stream = {&key_reader, key_overflow,
                       static_cast<int16_t>(map_nullable_definition_level + 1), "key"};
    RETURN_IF_ERROR(assemble_map_repeated_levels(list_element_reader, repeated_level,
                                                 list_value_reader.nullable_definition_level() + 1,
                                                 rows, value_overflow, sink, rows_read));
    if (!value_overflow->empty()) {
        sink.key_stream.move_tail_to_overflow();
    }
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
        RETURN_IF_ERROR(read_aligned_map_entries<NestedScalarBatch>(
                _name, _type, _nullable_definition_level, _repeated_repetition_level, *readers.key,
                key_max_definition_level, *readers.scalar_value, &_value_overflow, &_key_overflow,
                NestedScalarValueAppender {readers.scalar_value, "MAP", "value",
                                           value_max_definition_level},
                rows, &context, rows_read));
        return Status::OK();
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

        RETURN_IF_ERROR(read_map_list_value_entries(
                _name, _type, _nullable_definition_level, _repeated_repetition_level, *readers.key,
                key_max_definition_level, *readers.list_value, *scalar_list_value_reader,
                &_key_overflow, &_value_overflow, rows, &context, rows_read));
        return Status::OK();
    }

    RETURN_IF_ERROR(read_aligned_map_entries<NestedStructBatch>(
            _name, _type, _nullable_definition_level, _repeated_repetition_level, *readers.key,
            key_max_definition_level, *readers.struct_value, &_struct_value_overflow,
            &_key_overflow, NestedStructValueAppender {readers.struct_value}, rows, &context,
            rows_read));
    return Status::OK();
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
        RETURN_IF_ERROR(skip_map_list_value_entries(
                _name, _nullable_definition_level, _repeated_repetition_level, *readers.key,
                *readers.list_value, *scalar_list_value_reader, &_key_overflow, &_value_overflow,
                rows, &rows_read));
    }
    if (rows_read != rows) {
        return Status::Corruption("Failed to skip parquet MAP column {}: skipped {} of {} rows",
                                  _name, rows_read, rows);
    }
    return Status::OK();
}

} // namespace doris::parquet
