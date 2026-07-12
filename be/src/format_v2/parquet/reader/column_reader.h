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
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "format_v2/column_data.h"
#include "format_v2/parquet/parquet_profile.h"
#include "format_v2/parquet/parquet_type.h"
#include "format_v2/parquet/selection_vector.h"
#include "runtime/runtime_profile.h"

namespace parquet {
class ColumnDescriptor;
class RowGroupReader;

namespace internal {
class RecordReader;
} // namespace internal
} // namespace parquet

namespace cctz {
class time_zone;
} // namespace cctz

namespace doris {
class IColumn;
} // namespace doris

namespace doris::format::parquet {
struct ParquetColumnSchema;

class ParquetColumnReader {
public:
    virtual ~ParquetColumnReader() = default;

    virtual int file_column_id() const { return _field_id; }

    virtual int parquet_leaf_column_id() const { return _leaf_column_id; }

    int16_t nullable_definition_level() const { return _nullable_definition_level; }
    int16_t repeated_repetition_level() const { return _repeated_repetition_level; }

    virtual const DataTypePtr& type() const { return _type; }
    virtual const std::string& name() const { return _name; }
    const ParquetColumnReaderProfile& profile() const { return _profile; }

    virtual Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) = 0;

    virtual Status skip(int64_t rows);

    virtual Status select(const SelectionVector& sel, uint16_t selected_rows, int64_t batch_rows,
                          MutableColumnPtr& column);

    virtual Status select_with_dictionary_filter(const SelectionVector& sel, uint16_t selected_rows,
                                                 int64_t batch_rows,
                                                 const IColumn::Filter& dictionary_filter,
                                                 MutableColumnPtr& column,
                                                 IColumn::Filter* row_filter, bool* used_filter);

    virtual Status load_nested_batch(int64_t rows);

    // Shape-only load interface for COUNT(col) and skip. Implementations guarantee only that
    // nested_definition_levels(), nested_repetition_levels(), and nested_levels_written() are
    // available; value indices and payload columns may be absent. Callers may inspect the levels or
    // call consume_nested_column(), but must not call build_nested_column() afterwards. For example,
    // skipping ARRAY<STRING> uses this method to find ARRAY boundaries without decoding strings.
    // Normal scans that need output values use load_nested_batch() instead.
    virtual Status load_nested_levels_batch(int64_t rows);

    virtual Status build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                                       int64_t* values_read);

    // Consume logical values from a batch previously loaded by load_nested_batch() or
    // load_nested_levels_batch() without appending them to an output Column. Implementations must
    // advance exactly the same nested level cursors and perform the same shape/null/alignment
    // validation as build_nested_column(). The levels-only form is preferred for skip paths because
    // it also avoids decoding leaf payloads that will be discarded.
    //
    // `length_upper_bound` is expressed at this reader's logical level, not in physical leaf
    // values. For example, consuming two rows from ARRAY [[1, 2], []] consumes two parent ARRAY
    // rows but only two element values. A MAP implementation must also consume key/value streams
    // in lockstep, while a nullable STRUCT consumes no child value for a null parent.
    //
    // Callers must not use the ordinary skip() after either load call: the leaf stream has already
    // advanced into an in-memory nested batch, and doing so would advance it twice.
    // `values_consumed` may be smaller than the requested bound only when the loaded batch ends.
    virtual Status consume_nested_column(int64_t length_upper_bound, int64_t* values_consumed);

    virtual const std::vector<int16_t>& nested_definition_levels() const;
    virtual const std::vector<int16_t>& nested_repetition_levels() const;
    virtual int64_t nested_levels_written() const;
    virtual bool is_or_has_repeated_child() const;
    virtual void advance_nested_build_level_cursor_past_parent(int16_t parent_repetition_level);

    int64_t nested_build_level_cursor() const { return _nested_build_level_cursor; }
    void set_nested_build_level_cursor(int64_t cursor) {
        DORIS_CHECK(cursor >= 0);
        _nested_build_level_cursor = cursor;
    }
    void reset_nested_build_level_cursor() { _nested_build_level_cursor = 0; }

protected:
    ParquetColumnReader(const ParquetColumnSchema& schema, const DataTypePtr type,
                        ParquetColumnReaderProfile profile = {});
    ParquetColumnReader() = default;
    // Load shape levels and consume skipped parent rows in bounded batches. The bound limits level
    // memory when a parent expands to many children; the levels-only load plus
    // consume_nested_column() avoids both payload decoding and output Columns.
    Status skip_nested_rows(int64_t rows);
    void update_reader_read_rows(int64_t rows) const;
    void update_reader_skip_rows(int64_t rows) const;

    ParquetColumnReaderProfile _profile;
    const int _field_id = -1;       // child ordinal in the parent node
    const int _leaf_column_id = -1; // Parquet physical leaf column id (-1 = non-leaf)
    const int16_t _nullable_definition_level =
            0; // definition-level threshold where this node becomes nullable
    const int16_t _repeated_repetition_level =
            0;                           // repetition level of the nearest repeated ancestor
    const int16_t _definition_level = 0; // definition level accumulated to this node
    const int16_t _repetition_level = 0; // repetition level accumulated to this node
    const int16_t _repeated_ancestor_definition_level =
            0;                              // definition level of the nearest repeated ancestor
    const DataTypePtr _type;                // Doris target type
    const std::string _name;                // column name for error messages
    int64_t _nested_build_level_cursor = 0; // nested build cursor (current level position)
};

class ParquetColumnReaderFactory {
public:
    ParquetColumnReaderFactory(std::shared_ptr<::parquet::RowGroupReader> row_group,
                               int num_leaf_columns,
                               const std::map<int, ParquetPageSkipPlan>* page_skip_plans = nullptr,
                               ParquetPageSkipProfile page_skip_profile = {},
                               const cctz::time_zone* timezone = nullptr,
                               bool enable_strict_mode = false,
                               ParquetColumnReaderProfile column_reader_profile = {});

    Status create(const ParquetColumnSchema& column_schema,
                  const format::LocalColumnIndex* projection,
                  std::unique_ptr<ParquetColumnReader>* reader, bool read_dictionary = false) const;

    // Create a scalar reader for one representative leaf that carries the top-level column shape.
    // This is used by COUNT(col): the caller needs definition/repetition levels to decide whether
    // the top-level value is NULL, but must not materialize heavy payload leaves. MAP deliberately
    // uses the key leaf because the key stream owns entry existence and avoids reading value pages.
    Status create_count_shape_reader(const ParquetColumnSchema& column_schema,
                                     const format::LocalColumnIndex* projection,
                                     std::unique_ptr<ParquetColumnReader>* reader) const;

    Status create(const ParquetColumnSchema& column_schema,
                  std::unique_ptr<ParquetColumnReader>* reader) const {
        return create(column_schema, nullptr, reader);
    }

    std::unique_ptr<ParquetColumnReader> create_row_position_column_reader(
            int64_t row_group_first_row) const;
    std::unique_ptr<ParquetColumnReader> create_global_rowid_column_reader(
            const format::GlobalRowIdContext& context, int64_t row_group_first_row) const;

private:
    Status create_scalar_column_reader(const ParquetColumnSchema& column_schema, bool is_nested,
                                       bool read_dictionary,
                                       std::unique_ptr<ParquetColumnReader>* reader) const;

    Status create_struct_column_reader(const ParquetColumnSchema& column_schema,
                                       const format::LocalColumnIndex* projection,
                                       std::unique_ptr<ParquetColumnReader>* reader) const;

    Status create_list_column_reader(const ParquetColumnSchema& column_schema,
                                     const format::LocalColumnIndex* projection,
                                     std::unique_ptr<ParquetColumnReader>* reader) const;

    Status create_map_column_reader(const ParquetColumnSchema& column_schema,
                                    const format::LocalColumnIndex* projection,
                                    std::unique_ptr<ParquetColumnReader>* reader) const;

    Status create_column_reader(const ParquetColumnSchema& column_schema,
                                const format::LocalColumnIndex* projection, bool is_nested,
                                bool read_dictionary,
                                std::unique_ptr<ParquetColumnReader>* reader) const;
    Status create_count_shape_reader_impl(const ParquetColumnSchema& column_schema,
                                          const format::LocalColumnIndex* projection,
                                          bool is_nested,
                                          std::unique_ptr<ParquetColumnReader>* reader) const;

    Status get_record_reader(int leaf_column_id, const ::parquet::ColumnDescriptor* descriptor,
                             const std::string& name, bool install_page_filter,
                             bool read_dictionary,
                             std::shared_ptr<::parquet::internal::RecordReader>* reader) const;

    Status make_scalar_column_reader(
            const ParquetColumnSchema& column_schema,
            std::shared_ptr<::parquet::internal::RecordReader> record_reader,
            bool use_page_skip_plan, std::unique_ptr<ParquetColumnReader>* reader) const;

    std::shared_ptr<::parquet::RowGroupReader> _row_group; // Arrow RowGroup reader
    mutable std::vector<std::shared_ptr<::parquet::internal::RecordReader>>
            _record_readers; // RecordReader cache by leaf_column_id
    mutable std::vector<std::shared_ptr<::parquet::internal::RecordReader>>
            _dictionary_record_readers; // dictionary-exposing RecordReader cache by leaf_column_id
    const std::map<int, ParquetPageSkipPlan>* _page_skip_plans =
            nullptr;                                   // page-index pruning result
    ParquetPageSkipProfile _page_skip_profile;         // page skip profile
    const cctz::time_zone* _timezone = nullptr;        // timezone
    bool _enable_strict_mode = false;                  // strict mode
    ParquetColumnReaderProfile _column_reader_profile; // column reader profile
};
} // namespace doris::format::parquet
