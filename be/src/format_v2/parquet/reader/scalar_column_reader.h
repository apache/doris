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

#include <memory>
#include <string>
#include <vector>

#include "core/string_ref.h"
#include "format_v2/parquet/parquet_type.h"
#include "format_v2/parquet/reader/column_reader.h"
#include "format_v2/parquet/reader/parquet_leaf_reader.h"

namespace parquet {
class ColumnDescriptor;

namespace internal {
class RecordReader;
} // namespace internal
} // namespace parquet

namespace cctz {
class time_zone;
} // namespace cctz

namespace doris::format::parquet {

struct ScalarColumnReaderTestAccess;

//      load_nested_batch() / build_nested_column()
class ScalarColumnReader final : public ParquetColumnReader {
    friend class MapColumnReader;
    friend struct ScalarColumnReaderTestAccess;

public:
    ScalarColumnReader(const ParquetColumnSchema& column_schema,
                       std::shared_ptr<::parquet::internal::RecordReader> record_reader,
                       const ParquetPageSkipPlan* page_skip_plan = nullptr,
                       const cctz::time_zone* timezone = nullptr, bool enable_strict_mode = false,
                       ParquetColumnReaderProfile profile = {});
    ~ScalarColumnReader() override;

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;
    Status select_with_dictionary_filter(const SelectionVector& sel, uint16_t selected_rows,
                                         int64_t batch_rows,
                                         const IColumn::Filter& dictionary_filter,
                                         MutableColumnPtr& column, IColumn::Filter* row_filter,
                                         bool* used_filter) override;

    Status load_nested_batch(int64_t rows) override;
    Status load_nested_levels_batch(int64_t rows) override;
    Status build_nested_column(int64_t length_upper_bound, MutableColumnPtr& column,
                               int64_t* values_read) override;
    const std::vector<int16_t>& nested_definition_levels() const override;
    const std::vector<int16_t>& nested_repetition_levels() const override;
    int64_t nested_levels_written() const override;
    bool is_or_has_repeated_child() const override;

private:
    Status append_nested_value(int64_t level_idx, MutableColumnPtr& column) const;
    Status read_range_with_dictionary_filter(int64_t rows, const IColumn::Filter& dictionary_filter,
                                             MutableColumnPtr& column, IColumn::Filter* row_filter,
                                             int64_t* rows_read, bool* used_filter);
    Status append_dictionary_filtered_values(
            const std::vector<std::shared_ptr<::arrow::Array>>& chunks,
            const IColumn::Filter& dictionary_filter, MutableColumnPtr& column,
            IColumn::Filter* row_filter, int64_t* matched_rows, bool* used_filter) const;
    Status append_decoded_binary_values(const std::vector<StringRef>& values,
                                        MutableColumnPtr& column) const;

    const ::parquet::ColumnDescriptor* descriptor() const { return _descriptor; }

    ParquetLeafReader leaf_reader() const {
        return ParquetLeafReader(_descriptor, _type_descriptor, _type, _name, _record_reader,
                                 _profile, _timezone, _enable_strict_mode);
    }

    void advance_rows_read(int64_t rows);
    Status skip_records(int64_t rows);
    int64_t page_filtered_rows_to_skip(int64_t rows) const;

    const ::parquet::ColumnDescriptor* _descriptor = nullptr; // Arrow column descriptor
    ParquetTypeDescriptor _type_descriptor;                   // type encoding information
    std::shared_ptr<::parquet::internal::RecordReader>
            _record_reader; // Arrow physical column reader
    const ParquetPageSkipPlan* _page_skip_plan =
            nullptr;                            // page-index pruning result (may be nullptr)
    const cctz::time_zone* _timezone = nullptr; // timezone
    bool _enable_strict_mode = false;           // strict mode
    int64_t _row_group_rows_read = 0;           // rows read in the current row group (cursor)
    std::unique_ptr<ParquetNestedScalarBatch> _nested_batch; // intermediate result for nested reads
};

} // namespace doris::format::parquet
