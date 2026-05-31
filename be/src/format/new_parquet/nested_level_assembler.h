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
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"

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
    dst.values_column = src.values_column->clone_empty();

    for (int64_t level_idx = start_level; level_idx < src.levels_written; ++level_idx) {
        const int64_t value_idx = src.value_indices[level_idx];
        if (value_idx < 0) {
            continue;
        }
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

} // namespace doris::parquet
