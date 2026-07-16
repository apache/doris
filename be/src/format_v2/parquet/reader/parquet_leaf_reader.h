// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/data_type_serde/decoded_column_view.h"
#include "core/string_ref.h"
#include "format_v2/parquet/parquet_profile.h"
#include "format_v2/parquet/parquet_type.h"

namespace parquet {
class ColumnDescriptor;

namespace internal {
class RecordReader;
} // namespace internal
} // namespace parquet

namespace cctz {
class time_zone;
} // namespace cctz

namespace arrow {
class Array;
} // namespace arrow

namespace doris::format::parquet {

struct ParquetLeafReaderTestAccess;

// Read result for a nested scalar leaf, separating Dremel-encoded shape from actual values.
// The COUNT(col) aggregation fast path consumes only records_read, levels_written, def_levels, and rep_levels.
// That path does not populate value_indices or values_column, so callers must not call build_nested_column() afterwards.
struct ParquetNestedScalarBatch {
    int64_t records_read = 0;
    int64_t levels_written = 0;
    int16_t value_slot_definition_level = 0;
    int16_t value_slot_repetition_level = std::numeric_limits<int16_t>::max();
    std::vector<int16_t> def_levels;
    std::vector<int16_t> rep_levels;
    std::vector<int64_t> value_indices;
    MutableColumnPtr values_column;

    bool empty() const { return levels_written == 0; }

    // Reset logical contents without replacing the vectors/Column object. A reader repeatedly
    // sees the same physical leaf type, so retaining capacity is safe and avoids one allocation
    // family per nested batch. A levels-only read leaves values_column empty but may keep the
    // reusable object allocated.
    void reset() {
        records_read = 0;
        levels_written = 0;
        value_slot_definition_level = 0;
        value_slot_repetition_level = std::numeric_limits<int16_t>::max();
        def_levels.clear();
        rep_levels.clear();
        value_indices.clear();
        if (values_column.get() != nullptr) {
            values_column->clear();
        }
    }
};

class ParquetLeafBatch {
public:
    int64_t consumed_level_count() const { return _consumed_level_count; }
    int64_t decoded_level_count() const { return _decoded_level_count; }
    int64_t values_written() const { return _values_written; }
    bool read_dense_for_nullable() const { return _read_dense_for_nullable; }
    const int16_t* def_levels() const { return _def_levels; }
    const int16_t* rep_levels() const { return _rep_levels; }
    const std::vector<std::shared_ptr<::arrow::Array>>& binary_chunks() const {
        return _binary_chunks;
    }

    // Release Arrow payload ownership as soon as the synchronous Doris materialization finishes.
    // clear() keeps vector capacity, so the batch remains allocation-reusable without extending
    // the lifetime of what can be a very large BinaryArray/DictionaryArray payload.
    void release_binary_chunks() { _binary_chunks.clear(); }

private:
    friend class ParquetLeafReader;

    bool is_binary_value() const;

    DecodedValueKind _value_kind = DecodedValueKind::INT32;
    int64_t _consumed_level_count = 0;
    int64_t _decoded_level_count = 0;
    int64_t _values_written = 0;
    const int16_t* _def_levels = nullptr;
    const int16_t* _rep_levels = nullptr;
    const uint8_t* _fixed_values = nullptr;
    bool _read_dense_for_nullable = false;
    std::vector<std::shared_ptr<::arrow::Array>> _binary_chunks;
};

//      read_batch() -> build_null_map() + append_values()
//      read_nested_batch()
class ParquetLeafReader {
public:
    ParquetLeafReader(const ::parquet::ColumnDescriptor* descriptor,
                      ParquetTypeDescriptor type_descriptor, DataTypePtr type, std::string name,
                      std::shared_ptr<::parquet::internal::RecordReader> record_reader,
                      ParquetColumnReaderProfile profile = {},
                      const cctz::time_zone* timezone = nullptr, bool enable_strict_mode = false,
                      std::function<Status(MutableColumnPtr&, const DecodedColumnView&)>
                              decoded_value_appender = nullptr);

    Status read_batch(int64_t batch_rows, ParquetLeafBatch* batch, int64_t* rows_read) const;

    Status build_null_map(const ParquetLeafBatch& batch, int64_t records_read,
                          NullMap* null_map) const;

    Status append_values(const ParquetLeafBatch& batch, int64_t row_count, const NullMap* null_map,
                         MutableColumnPtr& column) const;

    // LEVELS / VALUE_SLOTS / LEAF_VALUES / PAYLOAD_VALUE_SLOTS.
    Status read_nested_batch(
            int64_t batch_rows, int16_t value_slot_definition_level,
            ParquetNestedScalarBatch* batch,
            int16_t value_slot_repetition_level = std::numeric_limits<int16_t>::max()) const;

    // COUNT(col) and nested-skip shape-only read path. It still calls Arrow
    // RecordReader::ReadRecords() to advance the Parquet cursor and obtain def/rep levels, but
    // Doris only copies levels:
    // - it does not build value_indices or values_column
    // - it does not enter DataTypeSerde::read_column_from_decoded_values()
    // - for Binary/FLBA, it releases and immediately discards Arrow builder chunks because that is
    //   the RecordReader's required reset operation; it never copies them into a Doris Column
    // This lets COUNT(col) on MAP/ARRAY/STRUCT evaluate top-level NULL state and lets skip advance
    // nested shape without Doris-side STRING/BINARY materialization. Arrow RecordReader does not
    // expose a public levels-only API, so ReadRecords may still perform required page decoding.
    Status read_nested_levels_batch(int64_t batch_rows, ParquetNestedScalarBatch* batch) const;

private:
    friend struct ParquetLeafReaderTestAccess;

    Status collect_batch(::parquet::internal::RecordReader& record_reader,
                         ParquetLeafBatch* batch) const;

    // Levels-only variant of collect_batch(). It snapshots only def/rep level state and does not
    // expose value buffers. Binary chunks are released only to reset Arrow's builder and are
    // immediately discarded. Used by COUNT(col) and nested skip.
    Status collect_levels_batch(::parquet::internal::RecordReader& record_reader,
                                ParquetLeafBatch* batch) const;

    Status build_spaced_fixed_values(const ParquetLeafBatch& batch, int64_t row_count,
                                     const NullMap* null_map,
                                     std::vector<uint8_t>* spaced_values) const;

    Status append_values_with_type(const ParquetLeafBatch& batch, int64_t row_count,
                                   const NullMap* null_map, const DataTypePtr& materialization_type,
                                   const DataTypeSerDeSPtr& serde, MutableColumnPtr& column) const;

    Status build_nested_batch_from_leaf_batch(const ParquetLeafBatch& leaf_batch,
                                              int64_t records_read,
                                              int16_t value_slot_definition_level,
                                              ParquetNestedScalarBatch* batch,
                                              int16_t value_slot_repetition_level) const;
    Status build_nested_levels_batch_from_leaf_batch(const ParquetLeafBatch& leaf_batch,
                                                     int64_t records_read,
                                                     ParquetNestedScalarBatch* batch) const;

    const ::parquet::ColumnDescriptor* _descriptor =
            nullptr; // Arrow column descriptor (physical_type, max_dl, max_rl)
    ParquetTypeDescriptor
            _type_descriptor; // type encoding information (decimal precision, timestamp unit, etc.)
    DataTypePtr _type;        // Doris target type
    std::string _name;        // column name for error messages
    std::shared_ptr<::parquet::internal::RecordReader>
            _record_reader;                     // Arrow physical column reader (shared ownership)
    ParquetColumnReaderProfile _profile;        // profile counters
    const cctz::time_zone* _timezone = nullptr; // timezone for timestamp conversion
    bool _enable_strict_mode = false;           // strict mode for type mismatch errors
    std::function<Status(MutableColumnPtr&, const DecodedColumnView&)> _decoded_value_appender;
    // Logical scratch. append_values() resets sizes but preserves capacity across batches.
    // StringRef entries never outlive the Arrow chunks retained by the current leaf batch.
    DataTypeSerDeSPtr _serde;
    DataTypePtr _nested_value_type;
    DataTypeSerDeSPtr _nested_value_serde;
    mutable ParquetLeafBatch _nested_leaf_batch;
    mutable NullMap _nested_value_nulls;
    mutable std::vector<StringRef> _binary_values;
    mutable std::vector<StringRef> _compact_binary_values;
    mutable std::vector<uint8_t> _spaced_values;
    mutable std::vector<float> _float_values;
    mutable std::vector<std::shared_ptr<::arrow::Array>> _discarded_binary_chunks;
};

} // namespace doris::format::parquet
