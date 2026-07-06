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

#include <memory>
#include <optional>
#include <orc/Reader.hh>
#include <set>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"
#include "format_v2/file_reader.h"
#include "runtime/runtime_profile.h"

namespace cctz {
class time_zone;
} // namespace cctz

namespace doris::format::orc {

struct OrcReaderScanState;

// ORC implementation of the format-v2 FileReader contract. The reader consumes
// file-local scan requests; table schema mapping is handled before open().
class OrcReader final : public format::FileReader {
public:
    OrcReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
              std::unique_ptr<io::FileDescription>& file_description,
              std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
              std::optional<format::GlobalRowIdContext> global_rowid_context = std::nullopt);
    ~OrcReader() override;

    static format::ColumnDefinition row_position_column_definition();

    Status init(RuntimeState* state) override;
    Status get_schema(std::vector<format::ColumnDefinition>* const file_schema) const override;
    std::unique_ptr<format::TableColumnMapper> create_column_mapper(
            format::TableColumnMapperOptions options) const override;
    Status open(std::shared_ptr<format::FileScanRequest> request) override;
    Status get_block(Block* file_block, size_t* rows, bool* eof) override;
    Status get_aggregate_result(const format::FileAggregateRequest& request,
                                format::FileAggregateResult* result) override;
    void set_condition_cache_context(std::shared_ptr<ConditionCacheContext> ctx) override;
    int64_t get_total_rows() const override;
    Status close() override;

    const format::FileReader::ReaderStatistics& reader_statistics() const {
        return _reader_statistics;
    }

private:
    struct OrcProfile {
        RuntimeProfile::Counter* reader_call = nullptr;                 // ReaderCall
        RuntimeProfile::Counter* reader_inclusive_latency_us = nullptr; // ReaderInclusiveLatencyUs
        RuntimeProfile::Counter* decompression_call = nullptr;          // DecompressionCall
        RuntimeProfile::Counter* decompression_latency_us = nullptr;    // DecompressionLatencyUs
        RuntimeProfile::Counter* decoding_call = nullptr;               // DecodingCall
        RuntimeProfile::Counter* decoding_latency_us = nullptr;         // DecodingLatencyUs
        RuntimeProfile::Counter* byte_decoding_call = nullptr;          // ByteDecodingCall
        RuntimeProfile::Counter* byte_decoding_latency_us = nullptr;    // ByteDecodingLatencyUs
        RuntimeProfile::Counter* io_count = nullptr;                    // IOCount
        RuntimeProfile::Counter* io_blocking_latency_us = nullptr;      // IOBlockingLatencyUs
        RuntimeProfile::Counter* selected_row_group_count = nullptr;    // SelectedRowGroupCount
        RuntimeProfile::Counter* evaluated_row_group_count = nullptr;   // EvaluatedRowGroupCount
        RuntimeProfile::Counter* read_row_count = nullptr;              // ReadRowCount
        RuntimeProfile::Counter* filtered_row_groups = nullptr;         // RowGroupsFiltered
        RuntimeProfile::Counter* filtered_row_groups_by_min_max = nullptr;
        RuntimeProfile::Counter* filtered_group_rows = nullptr; // FilteredRowsByGroup
        RuntimeProfile::Counter* filtered_bytes = nullptr;
        RuntimeProfile::Counter* read_row_groups = nullptr; // RowGroupsReadNum
        RuntimeProfile::Counter* lazy_read_filtered_rows = nullptr;
        RuntimeProfile::Counter* orc_lazy_read_filtered_rows = nullptr;
        RuntimeProfile::Counter* open_file_num = nullptr;
    };

    class OrcFilterImpl;

    void _init_profile() override;
    void _collect_profile() const;

    DataTypePtr _convert_to_doris_type(const ::orc::Type& type) const;
    DataTypePtr _convert_list_to_doris_type(const ::orc::Type& type) const;
    DataTypePtr _convert_map_to_doris_type(const ::orc::Type& type) const;
    DataTypePtr _convert_struct_to_doris_type(const ::orc::Type& type) const;
    Status _fill_schema_field(const ::orc::Type& type, int32_t local_id,
                              const std::string& field_name,
                              format::ColumnDefinition* const field) const;
    Status _fill_struct_schema_children(const ::orc::Type& type,
                                        format::ColumnDefinition* const field) const;
    Status _fill_list_schema_children(const ::orc::Type& type,
                                      format::ColumnDefinition* const field) const;
    Status _fill_map_schema_children(const ::orc::Type& type,
                                     format::ColumnDefinition* const field) const;

    // RowReader setup, SARG pruning, and ORC lazy callback.
    Status _configure_row_reader_projection();
    bool _can_apply_orc_lazy_callback() const;
    Status _init_search_argument_from_local_filters();
    Status _select_stripe_ranges_by_statistics();
    void _apply_current_stripe_range();
    Status _advance_to_next_stripe_range(bool* advanced);
    Status _create_row_reader();

    Status _filter_orc_batch(::orc::ColumnVectorBatch& data, uint16_t* sel, uint16_t size,
                             void* arg);

    Status _decode_column(const ::orc::Type& file_type, const ::orc::Type& selected_type,
                          const ::orc::ColumnVectorBatch& batch, MutableColumnPtr& column,
                          size_t rows, const std::vector<size_t>* selected_rows = nullptr) const;
    Status _decode_timestamp_column(const ::orc::ColumnVectorBatch& batch,
                                    const cctz::time_zone& timezone,
                                    MutableColumnPtr& nested_column, size_t rows,
                                    const std::vector<size_t>* selected_rows) const;
    Status _decode_list_column(const ::orc::Type& file_type, const ::orc::Type& selected_type,
                               const ::orc::ColumnVectorBatch& batch,
                               MutableColumnPtr& nested_column, size_t rows,
                               const std::vector<size_t>* selected_rows) const;
    Status _decode_map_column(const ::orc::Type& file_type, const ::orc::Type& selected_type,
                              const ::orc::ColumnVectorBatch& batch,
                              MutableColumnPtr& nested_column, size_t rows,
                              const std::vector<size_t>* selected_rows) const;
    Status _decode_struct_column(const ::orc::Type& file_type, const ::orc::Type& selected_type,
                                 const ::orc::ColumnVectorBatch& batch,
                                 MutableColumnPtr& nested_column, size_t rows,
                                 const std::vector<size_t>* selected_rows) const;
    Status _decode_column_into_block(const ::orc::StructVectorBatch& struct_batch,
                                     format::LocalColumnId file_column_id, size_t rows,
                                     Block* file_block,
                                     const std::vector<size_t>* selected_rows = nullptr) const;
    Status _decode_columns(const ::orc::StructVectorBatch& struct_batch,
                           const std::vector<format::LocalColumnIndex>& projections, size_t rows,
                           Block* file_block, std::set<format::LocalColumnId>* decoded_columns,
                           const std::vector<size_t>* selected_rows = nullptr) const;

    void _fill_row_position_column(Block* file_block, size_t rows,
                                   const std::vector<size_t>* selected_rows = nullptr) const;
    Status _fill_global_rowid_column(Block* file_block, size_t rows,
                                     const std::vector<size_t>* selected_rows = nullptr) const;

    // Row-level filtering on decoded Doris columns.
    bool _can_filter_with_decoded_columns(
            const std::set<format::LocalColumnId>& decoded_columns) const;
    bool _filter_has_row_level_predicates() const;
    Status _build_keep_filter(Block* file_block, size_t rows, IColumn::Filter* keep_filter) const;
    Status _filter_block(Block* file_block, size_t* rows) const;
    void _skip_condition_cache_false_granules(size_t* rows, bool* eof);
    void _mark_condition_cache_surviving_rows(const IColumn::Filter& keep_filter,
                                              size_t rows) const;
    Status _execute_conjuncts(Block* file_block, size_t rows, IColumn::Filter* keep_filter) const;
    Status _execute_delete_conjuncts(Block* file_block, size_t rows,
                                     IColumn::Filter* keep_filter) const;
    void _filter_block_with_keep_filter(Block* file_block, const IColumn::Filter& keep_filter,
                                        size_t selected_rows, size_t* rows) const;
    void _filter_decoded_columns(Block* file_block, const IColumn::Filter& keep_filter,
                                 size_t selected_rows,
                                 const std::set<format::LocalColumnId>& decoded_columns) const;
    void _filter_requested_columns(Block* file_block, const IColumn::Filter& keep_filter,
                                   size_t selected_rows) const;

    std::unique_ptr<OrcFilterImpl> _orc_filter;
    std::unique_ptr<OrcReaderScanState> _state;
    OrcProfile _orc_profile; // RuntimeProfile counters
    std::optional<format::GlobalRowIdContext> _global_rowid_context;
};

} // namespace doris::format::orc
