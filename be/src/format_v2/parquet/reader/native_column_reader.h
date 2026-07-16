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

#include <gen_cpp/parquet_types.h>

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <unordered_map>
#include <vector>

#include "format/parquet/parquet_common.h"
#include "format/table/table_schema_change_helper.h"
#include "format_v2/column_data.h"
#include "format_v2/parquet/reader/column_reader.h"
#include "format_v2/parquet/reader/native/column_reader.h"

namespace doris {
class FileMetaData;
class RuntimeState;
namespace io {
struct IOContext;
}
} // namespace doris

namespace doris::format::parquet {

// Production adapter from FileScannerV2's selection-oriented reader contract to Doris' native
// Parquet page/encoding reader. The owned native reader decodes page bytes directly into the final
// Doris column. It never creates an Arrow Array/Builder, DecodedColumnView, or intermediate nested
// values_column.
//
// Cursor contract:
// - read(rows) consumes and appends exactly `rows` logical top-level rows;
// - select(selection, batch_rows) consumes `batch_rows` and appends only selected rows;
// - skip(rows) consumes `rows` with an all-false FilterMap and appends no payload;
// - one adapter lives for one top-level column in one Row Group, so decoder dictionaries,
//   decompression buffers, level buffers, converters, and destination capacity survive adaptive
//   batch-size changes.
class NativeColumnReader final : public ParquetColumnReader {
public:
    static Status create(const ParquetColumnSchema& column_schema,
                         const format::LocalColumnIndex* projection, io::FileReaderSPtr file,
                         const FileMetaData* metadata, int row_group_id,
                         const std::vector<RowRange>& selected_ranges,
                         const std::unordered_map<int, tparquet::OffsetIndex>& offset_indexes,
                         const cctz::time_zone* timezone, io::IOContext* io_ctx,
                         RuntimeState* runtime_state, bool enable_page_cache,
                         const std::string& page_cache_file_key, bool enable_dictionary_filter,
                         ParquetColumnReaderProfile profile,
                         std::unique_ptr<ParquetColumnReader>* reader);

    ~NativeColumnReader() override;

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;
    Status select(const SelectionVector& selection, uint16_t selected_rows, int64_t batch_rows,
                  MutableColumnPtr& column) override;
    Status select_with_dictionary_filter(const SelectionVector& selection, uint16_t selected_rows,
                                         int64_t batch_rows,
                                         const IColumn::Filter& dictionary_filter,
                                         MutableColumnPtr& column, IColumn::Filter* row_filter,
                                         bool* used_filter) override;

private:
    NativeColumnReader(const ParquetColumnSchema& schema, DataTypePtr projected_type,
                       ParquetColumnReaderProfile profile);

    Status init(io::FileReaderSPtr file, const FileMetaData* metadata, int row_group_id,
                FieldSchema* field, std::shared_ptr<TableSchemaChangeHelper::Node> schema_node,
                std::set<uint64_t> projected_column_ids,
                const std::vector<RowRange>& selected_ranges,
                const std::unordered_map<int, tparquet::OffsetIndex>& offset_indexes,
                const cctz::time_zone* timezone, io::IOContext* io_ctx, RuntimeState* runtime_state,
                bool enable_page_cache, const std::string& page_cache_file_key,
                bool enable_dictionary_filter);

    Status read_with_filter(int64_t rows, const uint8_t* filter_data, bool filter_all,
                            MutableColumnPtr& column, const DataTypePtr& output_type,
                            bool dictionary_ids, int64_t* rows_read);
    int64_t sync_native_profile(int64_t* max_leaf_page_reads = nullptr);
    void record_page_fragments(int64_t page_fragments, int64_t max_leaf_page_reads);
    Status validate_selected_span(int64_t rows);
    void advance_selected_span(int64_t rows);

    // Native ParquetColumnReader keeps a reference to RowRanges; declare it before the reader.
    ::doris::RowRanges _row_ranges;
    std::set<uint64_t> _projected_column_ids;
    std::set<uint64_t> _filter_column_ids;
    const std::unordered_map<int, tparquet::OffsetIndex>* _offset_indexes = nullptr;
    std::shared_ptr<TableSchemaChangeHelper::Node> _schema_node;
    std::unique_ptr<native::ColumnReader> _native_reader;
    std::unique_ptr<RuntimeState> _page_cache_runtime_state;
    std::vector<RowRange> _selected_ranges;
    size_t _selected_range_idx = 0;
    int64_t _logical_row_position = 0;
    int64_t _row_group_rows = 0;

    bool _dictionary_filter_enabled = false;
    bool _nested = false;
    // The native tree exposes cumulative statistics. Keep the last reported snapshot so each
    // FileScannerV2 batch contributes only its delta to RuntimeProfile.
    native::ColumnReader::ColumnStatistics _reported_native_stats;
    std::vector<uint8_t> _filter_scratch;
    MutableColumnPtr _skip_column;
    MutableColumnPtr _dictionary_id_column;
    MutableColumnPtr _matched_dictionary_ids;
};

} // namespace doris::format::parquet
