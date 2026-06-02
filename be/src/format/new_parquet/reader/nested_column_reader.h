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

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "format/new_parquet/reader/scalar_column_reader.h"
#include "format/new_parquet/reader/struct_column_reader.h"

namespace doris::parquet {

constexpr int64_t NESTED_READ_BATCH_ROWS = 4096;

struct NestedScalarBatch {
    int64_t records_read = 0;
    int64_t levels_written = 0;
    int64_t values_written = 0;
    std::vector<int16_t> def_levels;
    std::vector<int16_t> rep_levels;
    std::vector<int64_t> value_indices;
    MutableColumnPtr values_column;

    bool empty() const { return levels_written == 0; }
};

struct NestedScalarOverflow {
    NestedScalarBatch batch;

    bool empty() const { return batch.empty(); }
    void clear() { batch = NestedScalarBatch(); }
};

struct NestedStructBatch {
    int64_t records_read = 0;
    int64_t levels_written = 0;
    std::vector<size_t> scalar_child_indices;
    std::vector<NestedScalarBatch> child_batches;

    bool empty() const { return levels_written == 0; }
};

struct NestedStructOverflow {
    NestedStructBatch batch;

    bool empty() const { return batch.empty(); }
    void clear() { batch = NestedStructBatch(); }
};

inline int64_t nested_shape_records_read(const NestedScalarBatch& batch) {
    return batch.records_read;
}

inline int64_t nested_shape_records_read(const NestedStructBatch& batch) {
    return batch.records_read;
}

inline int64_t nested_shape_levels_written(const NestedScalarBatch& batch) {
    return batch.levels_written;
}

inline int64_t nested_shape_levels_written(const NestedStructBatch& batch) {
    return batch.levels_written;
}

inline int16_t nested_shape_definition_level(const NestedScalarBatch& batch, int64_t level_idx) {
    return batch.def_levels[level_idx];
}

inline int16_t nested_shape_definition_level(const NestedStructBatch& batch, int64_t level_idx) {
    DORIS_CHECK(!batch.child_batches.empty());
    return batch.child_batches[0].def_levels[level_idx];
}

inline int16_t nested_shape_repetition_level(const NestedScalarBatch& batch, int64_t level_idx) {
    return batch.rep_levels[level_idx];
}

inline int16_t nested_shape_repetition_level(const NestedStructBatch& batch, int64_t level_idx) {
    DORIS_CHECK(!batch.child_batches.empty());
    return batch.child_batches[0].rep_levels[level_idx];
}

template <typename Batch>
class NestedShapeCursor {
public:
    explicit NestedShapeCursor(const Batch& batch) : _batch(batch) {}

    int64_t records_read() const { return nested_shape_records_read(_batch); }
    int64_t levels_written() const { return nested_shape_levels_written(_batch); }
    int16_t definition_level(int64_t level_idx) const {
        return nested_shape_definition_level(_batch, level_idx);
    }
    int16_t repetition_level(int64_t level_idx) const {
        return nested_shape_repetition_level(_batch, level_idx);
    }
    bool starts_parent(int64_t level_idx, int16_t repeated_level) const {
        return repetition_level(level_idx) < repeated_level;
    }

private:
    const Batch& _batch;
};

inline void move_nested_scalar_tail(const NestedScalarBatch& src, int64_t start_level,
                                    NestedScalarOverflow* overflow) {
    DORIS_CHECK(overflow != nullptr);
    if (start_level >= src.levels_written) {
        overflow->clear();
        return;
    }

    NestedScalarBatch dst;
    dst.records_read = 0;
    dst.levels_written = src.levels_written - start_level;
    dst.def_levels.assign(src.def_levels.begin() + start_level, src.def_levels.end());
    dst.rep_levels.assign(src.rep_levels.begin() + start_level, src.rep_levels.end());
    dst.value_indices.resize(static_cast<size_t>(dst.levels_written), -1);
    if (src.values_column.get() != nullptr) {
        dst.values_column = src.values_column->clone_empty();
    }

    for (int64_t level_idx = start_level; level_idx < src.levels_written; ++level_idx) {
        const int64_t value_idx = src.value_indices[level_idx];
        if (value_idx < 0) {
            continue;
        }
        DORIS_CHECK(dst.values_column.get() != nullptr);
        dst.value_indices[static_cast<size_t>(level_idx - start_level)] = dst.values_written;
        dst.values_column->insert_from(*src.values_column, static_cast<size_t>(value_idx));
        dst.values_written++;
    }
    overflow->batch = std::move(dst);
}

inline void move_nested_struct_tail(const NestedStructBatch& src, int64_t start_level,
                                    NestedStructOverflow* overflow) {
    DORIS_CHECK(overflow != nullptr);
    if (start_level >= src.levels_written) {
        overflow->clear();
        return;
    }

    NestedStructBatch dst;
    dst.records_read = 0;
    dst.levels_written = src.levels_written - start_level;
    dst.scalar_child_indices = src.scalar_child_indices;
    dst.child_batches.reserve(src.child_batches.size());
    for (const auto& child_batch : src.child_batches) {
        NestedScalarOverflow child_overflow;
        move_nested_scalar_tail(child_batch, start_level, &child_overflow);
        dst.child_batches.push_back(std::move(child_overflow.batch));
    }
    overflow->batch = std::move(dst);
}

inline void move_nested_tail(const NestedScalarBatch& src, int64_t start_level,
                             NestedScalarOverflow* overflow) {
    move_nested_scalar_tail(src, start_level, overflow);
}

inline void move_nested_tail(const NestedStructBatch& src, int64_t start_level,
                             NestedStructOverflow* overflow) {
    move_nested_struct_tail(src, start_level, overflow);
}

inline bool nested_shape_has_levels(const NestedScalarBatch&) {
    return true;
}

inline bool nested_shape_has_levels(const NestedStructBatch& batch) {
    return !batch.child_batches.empty();
}

template <typename DriverBatch, typename CandidateBatch>
Status validate_nested_shape_alignment(const std::string& column_name,
                                       const DriverBatch& driver_batch,
                                       const CandidateBatch& candidate_batch,
                                       std::string_view candidate_name, std::string_view action) {
    NestedShapeCursor driver_cursor(driver_batch);
    NestedShapeCursor candidate_cursor(candidate_batch);
    if (candidate_cursor.records_read() != driver_cursor.records_read() ||
        candidate_cursor.levels_written() != driver_cursor.levels_written()) {
        return Status::Corruption(
                "Parquet nested streams are not aligned for column {}{}: driver rows={}, "
                "driver levels={}, {} rows={}, {} levels={}",
                column_name, action, driver_cursor.records_read(), driver_cursor.levels_written(),
                candidate_name, candidate_cursor.records_read(), candidate_name,
                candidate_cursor.levels_written());
    }
    if (candidate_cursor.levels_written() > 0 && !nested_shape_has_levels(candidate_batch)) {
        return Status::Corruption("Parquet nested stream {} has no shape levels for column {}{}",
                                  candidate_name, column_name, action);
    }
    for (int64_t level_idx = 0; level_idx < driver_cursor.levels_written(); ++level_idx) {
        if (candidate_cursor.repetition_level(level_idx) !=
            driver_cursor.repetition_level(level_idx)) {
            return Status::Corruption(
                    "Parquet nested repetition levels are not aligned for column {}{}", column_name,
                    action);
        }
    }
    return Status::OK();
}

template <typename Batch, typename Overflow, typename ReadBatchFn, typename MoveTailFn,
          typename Sink>
Status assemble_repeated_levels(const std::string& column_name, int16_t repeated_level,
                                int64_t rows, Overflow* overflow, ReadBatchFn&& read_batch,
                                MoveTailFn&& move_tail, Sink& sink, int64_t* rows_read) {
    if (overflow == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid repeated level assembler arguments for column {}",
                                       column_name);
    }
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

Status read_nested_scalar_batch(
        ScalarColumnReader& column_reader, int64_t batch_rows, int16_t value_slot_definition_level,
        NestedScalarBatch* batch,
        int16_t value_slot_repetition_level = std::numeric_limits<int16_t>::max(),
        bool materialize_values = true);

Status append_scalar_batch_value(const ScalarColumnReader& column_reader,
                                 const NestedScalarBatch& batch, int64_t level_idx,
                                 MutableColumnPtr& column);

Status read_nested_scalar_batch_from_overflow(ScalarColumnReader& reader, int64_t batch_rows,
                                              int16_t value_slot_definition_level,
                                              NestedScalarOverflow* overflow,
                                              NestedScalarBatch* batch);

Status read_nested_batch_from_overflow(ScalarColumnReader& reader, int64_t batch_rows,
                                       int16_t value_slot_definition_level,
                                       NestedScalarOverflow* overflow, NestedScalarBatch* batch);

Status validate_nested_scalar_alignment(const std::string& column_name,
                                        const NestedScalarBatch& driver_batch,
                                        const NestedScalarBatch& candidate_batch,
                                        std::string_view candidate_name, std::string_view action);

Status validate_nested_struct_alignment(const std::string& column_name,
                                        const NestedScalarBatch& driver_batch,
                                        const NestedStructBatch& candidate_batch,
                                        std::string_view action);

Status advance_non_scalar_struct_children(StructColumnReader& struct_reader, bool parent_is_null,
                                          std::vector<MutableColumnPtr>& child_columns);

Status read_nested_struct_batch(StructColumnReader& struct_reader, int64_t batch_rows,
                                int16_t value_slot_definition_level, NestedStructBatch* batch);

Status read_nested_struct_batch_from_overflow(StructColumnReader& reader, int64_t batch_rows,
                                              int16_t value_slot_definition_level,
                                              NestedStructOverflow* overflow,
                                              NestedStructBatch* batch);

Status read_nested_batch_from_overflow(StructColumnReader& reader, int64_t batch_rows,
                                       int16_t value_slot_definition_level,
                                       NestedStructOverflow* overflow, NestedStructBatch* batch);

Status append_struct_batch_value(StructColumnReader& struct_reader, const NestedStructBatch& batch,
                                 int64_t level_idx, MutableColumnPtr& column);

ColumnArray* array_column_from_output(MutableColumnPtr& column);
ColumnMap* map_column_from_output(MutableColumnPtr& column);
ColumnStruct* struct_column_from_output(MutableColumnPtr& column);
NullMap* null_map_from_nullable_output(MutableColumnPtr& column);

void append_offsets(ColumnArray::Offsets64& offsets, const std::vector<uint64_t>& entry_counts);
void append_parent_nulls(NullMap* dst, const NullMap& src);

struct RepeatedParentSinkState {
    std::vector<uint64_t>* entry_counts = nullptr;
    NullMap* parent_nulls = nullptr;

    Status append_null_parent(const std::string& column_name, std::string_view parent_kind,
                              const DataTypePtr& type) const;
    void append_present_parent() const;
    Status require_parent(const std::string& column_name) const;
    Status add_entry(const std::string& column_name) const;
};

struct RepeatedChildSinkState {
    std::vector<uint64_t>* entry_counts = nullptr;
    NullMap* parent_nulls = nullptr;

    Status append_null_child(const std::string& column_name, std::string_view parent_kind,
                             std::string_view child_kind, const DataTypePtr& type) const;
    void append_present_child() const;
    Status require_child(const std::string& column_name, std::string_view child_kind) const;
    Status add_entry(const std::string& column_name, std::string_view child_kind) const;
};

Status append_nullable_scalar_child(const std::string& column_name, std::string_view parent_kind,
                                    std::string_view child_kind,
                                    const ScalarColumnReader& child_reader,
                                    const NestedScalarBatch& batch, int64_t level_idx,
                                    int16_t max_definition_level, MutableColumnPtr& column);

struct NestedScalarValueAppender {
    ScalarColumnReader* reader = nullptr;
    std::string_view parent_kind;
    std::string_view child_kind;
    int16_t max_definition_level = 0;

    Status append(const std::string& column_name, const NestedScalarBatch& batch, int64_t level_idx,
                  MutableColumnPtr& column) const {
        DORIS_CHECK(reader != nullptr);
        return append_nullable_scalar_child(column_name, parent_kind, child_kind, *reader, batch,
                                            level_idx, max_definition_level, column);
    }

    Status skip_shape_only_slot() const { return Status::OK(); }
};

struct NestedStructValueAppender {
    StructColumnReader* reader = nullptr;

    Status append(const std::string&, const NestedStructBatch& batch, int64_t level_idx,
                  MutableColumnPtr& column) const {
        DORIS_CHECK(reader != nullptr);
        return append_struct_batch_value(*reader, batch, level_idx, column);
    }

    Status skip_shape_only_slot() const {
        DORIS_CHECK(reader != nullptr);
        return reader->skip_non_scalar_children(1);
    }
};

template <typename Batch>
struct RepeatedShapeSkipSink {
    Status start_batch(const Batch&) { return Status::OK(); }
    Status start_parent(const Batch&, int64_t) { return Status::OK(); }
    Status append_repeated(const Batch&, int64_t) { return Status::OK(); }
};

template <typename Batch, typename ValueAppender>
struct RepeatedListValueSink {
    const std::string* column_name = nullptr;
    const DataTypePtr* list_type = nullptr;
    int16_t list_nullable_definition_level = 0;
    MutableColumnPtr* element_column = nullptr;
    RepeatedParentSinkState parent_state;
    ValueAppender value_appender;

    Status start_batch(const Batch&) { return Status::OK(); }

    Status start_parent(const Batch& batch, int64_t level_idx) {
        DORIS_CHECK(column_name != nullptr);
        DORIS_CHECK(list_type != nullptr);
        const int16_t def_level = nested_shape_definition_level(batch, level_idx);
        if (def_level < list_nullable_definition_level) {
            RETURN_IF_ERROR(parent_state.append_null_parent(*column_name, "LIST", *list_type));
            return value_appender.skip_shape_only_slot();
        }
        parent_state.append_present_parent();
        if (def_level == list_nullable_definition_level) {
            return value_appender.skip_shape_only_slot();
        }
        return append_element(batch, level_idx);
    }

    Status append_repeated(const Batch& batch, int64_t level_idx) {
        return append_element(batch, level_idx);
    }

    Status append_element(const Batch& batch, int64_t level_idx) {
        DORIS_CHECK(column_name != nullptr);
        DORIS_CHECK(element_column != nullptr);
        RETURN_IF_ERROR(parent_state.require_parent(*column_name));
        RETURN_IF_ERROR(value_appender.append(*column_name, batch, level_idx, *element_column));
        return parent_state.add_entry(*column_name);
    }
};

template <typename ValueAppender>
struct RepeatedNestedListValueSink {
    const std::string* column_name = nullptr;
    const DataTypePtr* outer_list_type = nullptr;
    const DataTypePtr* inner_list_type = nullptr;
    int16_t outer_nullable_definition_level = 0;
    int16_t inner_nullable_definition_level = 0;
    int16_t inner_repeated_repetition_level = 0;
    MutableColumnPtr* inner_element_column = nullptr;
    RepeatedParentSinkState outer_state;
    RepeatedChildSinkState inner_state;
    ValueAppender value_appender;

    Status start_batch(const NestedScalarBatch&) { return Status::OK(); }

    Status start_parent(const NestedScalarBatch& batch, int64_t level_idx) {
        DORIS_CHECK(column_name != nullptr);
        DORIS_CHECK(outer_list_type != nullptr);
        const int16_t def_level = batch.def_levels[level_idx];
        if (def_level < outer_nullable_definition_level) {
            return outer_state.append_null_parent(*column_name, "LIST", *outer_list_type);
        }
        outer_state.append_present_parent();
        if (def_level == outer_nullable_definition_level) {
            return Status::OK();
        }
        return append_inner_list(batch, level_idx);
    }

    Status append_repeated(const NestedScalarBatch& batch, int64_t level_idx) {
        if (batch.rep_levels[level_idx] < inner_repeated_repetition_level) {
            return append_inner_list(batch, level_idx);
        }
        return append_element(batch, level_idx);
    }

    Status append_inner_list(const NestedScalarBatch& batch, int64_t level_idx) {
        DORIS_CHECK(column_name != nullptr);
        DORIS_CHECK(inner_list_type != nullptr);
        const int16_t def_level = batch.def_levels[level_idx];
        if (def_level < inner_nullable_definition_level) {
            RETURN_IF_ERROR(outer_state.add_entry(*column_name));
            return inner_state.append_null_child(*column_name, "LIST", "nested list",
                                                 *inner_list_type);
        }
        RETURN_IF_ERROR(outer_state.add_entry(*column_name));
        inner_state.append_present_child();
        if (def_level == inner_nullable_definition_level) {
            return Status::OK();
        }
        return append_element(batch, level_idx);
    }

    Status append_element(const NestedScalarBatch& batch, int64_t level_idx) {
        DORIS_CHECK(column_name != nullptr);
        DORIS_CHECK(inner_element_column != nullptr);
        RETURN_IF_ERROR(inner_state.require_child(*column_name, "nested LIST"));
        RETURN_IF_ERROR(
                value_appender.append(*column_name, batch, level_idx, *inner_element_column));
        return inner_state.add_entry(*column_name, "nested LIST");
    }
};

template <typename Batch, typename Overflow, typename Reader>
struct NestedValueStream {
    NestedValueStream() = default;

    NestedValueStream(Reader* reader_, Overflow* overflow_, int16_t value_slot_definition_level_,
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

struct NestedScalarSlotStream {
    NestedScalarSlotStream() = default;

    NestedScalarSlotStream(ScalarColumnReader* reader_, NestedScalarOverflow* overflow_,
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
struct RepeatedMapValueSink {
    const std::string* column_name = nullptr;
    const DataTypePtr* map_type = nullptr;
    int16_t map_nullable_definition_level = 0;
    ScalarColumnReader* key_reader = nullptr;
    MutableColumnPtr* key_column = nullptr;
    MutableColumnPtr* value_column = nullptr;
    RepeatedParentSinkState parent_state;
    int16_t key_max_definition_level = 0;
    NestedValueStream<ValueBatch, ValueOverflow, ValueReader> value_stream;
    ValueAppender value_appender;

    Status start_batch(const NestedScalarBatch& key_batch) {
        DORIS_CHECK(column_name != nullptr);
        return value_stream.read_aligned_to_driver(*column_name, key_batch, "");
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
        RETURN_IF_ERROR(append_scalar_batch_value(*key_reader, key_batch, level_idx, *key_column));
        RETURN_IF_ERROR(
                value_appender.append(*column_name, value_stream.batch, level_idx, *value_column));
        return parent_state.add_entry(*column_name);
    }
};

template <typename ValueAppender>
struct RepeatedMapListValueSink {
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
    NestedScalarSlotStream key_stream;
    ValueAppender value_appender;

    Status start_batch(const NestedScalarBatch& value_batch) {
        DORIS_CHECK(column_name != nullptr);
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
                                                  key_stream.level_idx - 1, *key_column));
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

template <typename ValueBatch, typename ValueOverflow, typename ValueReader, typename ValueAppender>
struct RepeatedMapMapValueSink {
    const std::string* column_name = nullptr;
    const DataTypePtr* map_type = nullptr;
    const DataTypePtr* inner_map_type = nullptr;
    int16_t map_nullable_definition_level = 0;
    int16_t inner_map_nullable_definition_level = 0;
    int16_t inner_map_repeated_repetition_level = 0;
    ScalarColumnReader* key_reader = nullptr;
    ScalarColumnReader* inner_key_reader = nullptr;
    MutableColumnPtr* key_column = nullptr;
    MutableColumnPtr* inner_key_column = nullptr;
    MutableColumnPtr* inner_value_column = nullptr;
    RepeatedParentSinkState map_state;
    RepeatedChildSinkState inner_map_state;
    int16_t key_max_definition_level = 0;
    int16_t inner_key_max_definition_level = 0;
    NestedScalarSlotStream key_stream;
    NestedValueStream<ValueBatch, ValueOverflow, ValueReader> value_stream;
    ValueAppender value_appender;

    Status start_batch(const NestedScalarBatch& inner_key_batch) {
        DORIS_CHECK(column_name != nullptr);
        RETURN_IF_ERROR(key_stream.read_records(*column_name, inner_key_batch.records_read, ""));
        return value_stream.read_aligned_to_driver(*column_name, inner_key_batch, "");
    }

    Status start_parent(const NestedScalarBatch& inner_key_batch, int64_t level_idx) {
        DORIS_CHECK(column_name != nullptr);
        DORIS_CHECK(map_type != nullptr);
        const int16_t def_level = inner_key_batch.def_levels[level_idx];
        if (def_level < map_nullable_definition_level) {
            RETURN_IF_ERROR(consume_key_slot(inner_key_batch, level_idx, false));
            RETURN_IF_ERROR(value_appender.skip_shape_only_slot());
            return map_state.append_null_parent(*column_name, "MAP", *map_type);
        }
        map_state.append_present_parent();
        if (def_level == map_nullable_definition_level) {
            RETURN_IF_ERROR(consume_key_slot(inner_key_batch, level_idx, false));
            return value_appender.skip_shape_only_slot();
        }
        return append_entry(inner_key_batch, level_idx);
    }

    Status append_repeated(const NestedScalarBatch& inner_key_batch, int64_t level_idx) {
        if (inner_key_batch.rep_levels[level_idx] < inner_map_repeated_repetition_level) {
            return append_entry(inner_key_batch, level_idx);
        }
        return append_inner_entry(inner_key_batch, level_idx);
    }

    Status append_entry(const NestedScalarBatch& inner_key_batch, int64_t level_idx) {
        DORIS_CHECK(column_name != nullptr);
        DORIS_CHECK(key_reader != nullptr);
        DORIS_CHECK(key_column != nullptr);
        DORIS_CHECK(inner_map_type != nullptr);
        RETURN_IF_ERROR(consume_key_slot(inner_key_batch, level_idx, true));
        RETURN_IF_ERROR(append_scalar_batch_value(*key_reader, key_stream.batch,
                                                  key_stream.level_idx - 1, *key_column));
        RETURN_IF_ERROR(map_state.add_entry(*column_name));
        const int16_t def_level = inner_key_batch.def_levels[level_idx];
        if (def_level < inner_map_nullable_definition_level) {
            RETURN_IF_ERROR(value_appender.skip_shape_only_slot());
            return inner_map_state.append_null_child(*column_name, "MAP", "nested MAP value",
                                                     *inner_map_type);
        }
        inner_map_state.append_present_child();
        if (def_level == inner_map_nullable_definition_level) {
            return value_appender.skip_shape_only_slot();
        }
        return append_inner_entry(inner_key_batch, level_idx);
    }

    Status consume_key_slot(const NestedScalarBatch& inner_key_batch, int64_t inner_key_level_idx,
                            bool require_defined_key) {
        DORIS_CHECK(column_name != nullptr);
        RETURN_IF_ERROR(key_stream.consume_aligned_slot(*column_name, inner_key_batch,
                                                        inner_key_level_idx, ""));
        if (require_defined_key) {
            RETURN_IF_ERROR(key_stream.require_last_slot_defined(
                    *column_name, key_max_definition_level, "map key"));
        }
        return Status::OK();
    }

    Status append_inner_entry(const NestedScalarBatch& inner_key_batch, int64_t level_idx) {
        DORIS_CHECK(column_name != nullptr);
        DORIS_CHECK(inner_key_reader != nullptr);
        DORIS_CHECK(inner_key_column != nullptr);
        DORIS_CHECK(inner_value_column != nullptr);
        RETURN_IF_ERROR(inner_map_state.require_child(*column_name, "nested MAP value"));
        if (inner_key_batch.def_levels[level_idx] != inner_key_max_definition_level) {
            return Status::Corruption("Parquet MAP column {} contains null nested map key",
                                      *column_name);
        }
        RETURN_IF_ERROR(append_scalar_batch_value(*inner_key_reader, inner_key_batch, level_idx,
                                                  *inner_key_column));
        RETURN_IF_ERROR(value_appender.append(*column_name, value_stream.batch, level_idx,
                                              *inner_value_column));
        return inner_map_state.add_entry(*column_name, "nested MAP value");
    }
};

template <typename ValueBatch, typename ValueOverflow, typename ValueReader>
struct RepeatedAlignedValueSkipSink {
    const std::string* column_name = nullptr;
    NestedValueStream<ValueBatch, ValueOverflow, ValueReader> value_stream;

    Status start_batch(const NestedScalarBatch& key_batch) {
        DORIS_CHECK(column_name != nullptr);
        return value_stream.read_aligned_to_driver(*column_name, key_batch, " while skipping");
    }

    Status start_parent(const NestedScalarBatch&, int64_t) { return Status::OK(); }

    Status append_repeated(const NestedScalarBatch&, int64_t) { return Status::OK(); }
};

template <typename Sink>
Status assemble_repeated_levels(ScalarColumnReader& driver_reader, int16_t repeated_level,
                                int16_t value_slot_definition_level, int64_t rows,
                                NestedScalarOverflow* overflow, Sink& sink, int64_t* rows_read) {
    auto read_batch = [&](int64_t batch_rows, NestedScalarBatch* batch) {
        return read_nested_scalar_batch(driver_reader, batch_rows, value_slot_definition_level,
                                        batch);
    };
    return doris::parquet::assemble_repeated_levels<NestedScalarBatch>(
            driver_reader.name(), repeated_level, rows, overflow, read_batch,
            move_nested_scalar_tail, sink, rows_read);
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
    return doris::parquet::assemble_repeated_levels<NestedStructBatch>(
            driver_reader.name(), repeated_level, rows, overflow, read_batch,
            move_nested_struct_tail, sink, rows_read);
}

} // namespace doris::parquet
