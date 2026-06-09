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
#include "format_v2/parquet/reader/scalar_column_reader.h"
#include "format_v2/parquet/reader/struct_column_reader.h"

namespace doris::parquet {

constexpr int64_t NESTED_READ_BATCH_ROWS = 4096;

struct NestedScalarBatch {
    int64_t records_read = 0;
    int64_t levels_written = 0;
    int16_t value_slot_definition_level = 0;
    int16_t value_slot_repetition_level = std::numeric_limits<int16_t>::max();
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

class NestedScalarValueCursor {
public:
    NestedScalarValueCursor() = default;

    explicit NestedScalarValueCursor(const NestedScalarBatch* batch) { reset(batch); }

    void reset(const NestedScalarBatch* batch) {
        DORIS_CHECK(batch != nullptr);
        _batch = batch;
    }

    Status value_index(const std::string& column_name, int64_t level_idx, int64_t* value_idx) {
        DORIS_CHECK(_batch != nullptr);
        DORIS_CHECK(value_idx != nullptr);
        DORIS_CHECK(level_idx < _batch->levels_written);
        DORIS_CHECK(level_idx >= 0);
        DORIS_CHECK(static_cast<size_t>(level_idx) < _batch->value_indices.size());
        const int64_t computed_value_idx = _batch->value_indices[static_cast<size_t>(level_idx)];
        if (computed_value_idx < 0) {
            return Status::Corruption("Nested parquet value is absent for column {}", column_name);
        }
        DORIS_CHECK(_batch->values_column.get() != nullptr);
        if (computed_value_idx >= _batch->values_column->size()) {
            return Status::Corruption("Nested parquet value index is out of range for column {}",
                                      column_name);
        }
        *value_idx = computed_value_idx;
        return Status::OK();
    }

    bool has_value_slot(int64_t level_idx) const {
        DORIS_CHECK(_batch != nullptr);
        DORIS_CHECK(level_idx >= 0);
        DORIS_CHECK(static_cast<size_t>(level_idx) < _batch->value_indices.size());
        return _batch->value_indices[static_cast<size_t>(level_idx)] >= 0;
    }

private:
    const NestedScalarBatch* _batch = nullptr;
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
    dst.value_slot_definition_level = src.value_slot_definition_level;
    dst.value_slot_repetition_level = src.value_slot_repetition_level;
    dst.value_indices.resize(static_cast<size_t>(dst.levels_written), -1);
    dst.values_column = src.values_column->clone_empty();

    int64_t values_written = 0;
    for (int64_t level_idx = start_level; level_idx < src.levels_written; ++level_idx) {
        const int64_t value_idx = src.value_indices[static_cast<size_t>(level_idx)];
        if (value_idx < 0) {
            continue;
        }
        dst.value_indices[static_cast<size_t>(level_idx - start_level)] = values_written;
        dst.values_column->insert_from(*src.values_column, static_cast<size_t>(value_idx));
        values_written++;
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

Status read_nested_scalar_batch(
        ScalarColumnReader& column_reader, int64_t batch_rows, int16_t value_slot_definition_level,
        NestedScalarBatch* batch,
        int16_t value_slot_repetition_level = std::numeric_limits<int16_t>::max());

Status append_scalar_batch_value(const ScalarColumnReader& column_reader,
                                 const NestedScalarBatch& batch, int64_t level_idx,
                                 NestedScalarValueCursor* value_cursor, MutableColumnPtr& column);

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
                                    int16_t max_definition_level,
                                    NestedScalarValueCursor* value_cursor,
                                    MutableColumnPtr& column);

struct NestedScalarValueAppender {
    ScalarColumnReader* reader = nullptr;
    std::string_view parent_kind;
    std::string_view child_kind;
    int16_t max_definition_level = 0;
    NestedScalarValueCursor* value_cursor = nullptr;

    Status append(const std::string& column_name, const NestedScalarBatch& batch, int64_t level_idx,
                  MutableColumnPtr& column) const {
        DORIS_CHECK(reader != nullptr);
        return append_nullable_scalar_child(column_name, parent_kind, child_kind, *reader, batch,
                                            level_idx, max_definition_level, value_cursor, column);
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

} // namespace doris::parquet
