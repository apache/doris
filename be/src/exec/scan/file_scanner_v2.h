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

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "core/block/block.h"
#include "exec/operator/file_scan_operator.h"
#include "exec/scan/scanner.h"
#include "exec/scan/split_source_connector.h"
#include "exprs/vexpr_fwd.h"
#include "format_v2/column_mapper.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"
#include "storage/segment/adaptive_block_size_predictor.h"

namespace doris {

class RuntimeState;
class SlotDescriptor;
class TFileRangeDesc;
class TFileScanRangeParams;
class ShardedKVCache;

class FileScannerV2 final : public Scanner {
    ENABLE_FACTORY_CREATOR(FileScannerV2);

public:
    static constexpr const char* NAME = "FileScannerV2";
    static constexpr size_t ADAPTIVE_BATCH_INITIAL_PROBE_ROWS = 32;

    struct RealtimeCounterDeltas {
        int64_t scan_rows = 0;
        int64_t scan_bytes = 0;
        int64_t scan_bytes_from_local_storage = 0;
        int64_t scan_bytes_from_remote_storage = 0;
    };

    enum class UncachedReaderBytesStorage { LOCAL, REMOTE, NONE };

    static bool is_supported(const TFileScanRangeParams& params, const TFileRangeDesc& range);
#ifdef BE_TEST
    FileScannerV2(RuntimeState* state, RuntimeProfile* profile,
                  std::unique_ptr<format::TableReader> table_reader);
    static Status TEST_validate_scan_range(const TFileScanRangeParams& params,
                                           const TFileRangeDesc& range);
    static Status TEST_to_file_format(TFileFormatType::type format_type,
                                      format::FileFormat* file_format);
    static bool TEST_is_partition_slot(const TFileScanSlotInfo& slot_info,
                                       const std::string& column_name);
    static bool TEST_is_data_file_slot(const TFileScanSlotInfo& slot_info,
                                       const std::string& column_name);
    static Status TEST_rewrite_slot_refs_to_global_index(
            VExprSPtr* expr,
            const std::unordered_map<int32_t, format::GlobalIndex>& slot_id_to_global_index);
    static RealtimeCounterDeltas TEST_collect_realtime_counter_deltas(
            const io::FileReaderStats& file_reader_stats,
            const io::FileCacheStatistics& file_cache_statistics,
            UncachedReaderBytesStorage uncached_reader_bytes_storage, int64_t* last_read_bytes,
            int64_t* last_read_rows, int64_t* last_bytes_read_from_local,
            int64_t* last_bytes_read_from_remote);
    static void TEST_report_file_cache_profile(
            RuntimeProfile* profile, const io::FileCacheStatistics& file_cache_statistics);
    static bool TEST_should_skip_not_found(const Status& status, bool ignore_not_found);
    static bool TEST_should_skip_empty(const Status& status, bool stopped);
    static Status TEST_contextualize_output_filter_status(Status status,
                                                          TFileFormatType::type format_type) {
        return _contextualize_output_filter_status(std::move(status), format_type);
    }
    static bool TEST_should_run_adaptive_batch_size(bool predictor_initialized,
                                                    bool current_split_uses_metadata_count) {
        return _should_run_adaptive_batch_size(predictor_initialized,
                                               current_split_uses_metadata_count);
    }
#endif

    FileScannerV2(RuntimeState* state, FileScanLocalState* parent, int64_t limit,
                  std::shared_ptr<SplitSourceConnector> split_source, RuntimeProfile* profile,
                  ShardedKVCache* kv_cache,
                  const std::unordered_map<std::string, int>* colname_to_slot_id);

    Status init(RuntimeState* state, const VExprContextSPtrs& conjuncts) override;
    Status _open_impl(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    void try_stop() override;
    std::string get_name() override { return FileScannerV2::NAME; }
    std::string get_current_scan_range_name() override { return _current_range_path; }
    void update_realtime_counters() override;

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eof) override;
    Status _filter_output_block(Block* block) override;
    void _collect_profile_before_close() override;
    bool _should_update_load_counters() const override;

private:
    static Status _validate_scan_range(const TFileScanRangeParams& params,
                                       const TFileRangeDesc& range);
    Status _get_next_scan_range(bool* has_next);
    TFileFormatType::type _get_current_format_type() const;
    Status _init_io_ctx();
    Status _init_expr_ctxes();
    Status _prepare_next_split(bool* eos);
    Status _init_table_reader(const TFileRangeDesc& range);
    Status _create_table_reader_for_format(const TFileRangeDesc& range,
                                           std::unique_ptr<format::TableReader>* reader) const;
    Status _prepare_table_reader_split(const TFileRangeDesc& range,
                                       std::map<std::string, Field> partition_values);
    static bool _should_skip_not_found(const Status& status, bool ignore_not_found);
    static bool _should_skip_empty(const Status& status, bool stopped);
    static Status _contextualize_output_filter_status(Status status,
                                                      TFileFormatType::type format_type);
    bool _should_enable_file_meta_cache() const;
    std::optional<format::GlobalRowIdContext> _create_global_rowid_context(
            const TFileRangeDesc& range) const;
    Status _generate_partition_values(const TFileRangeDesc& range,
                                      std::map<std::string, Field>* partition_values) const;
    Status _parse_partition_value(const SlotDescriptor* slot_desc, const std::string& value,
                                  bool is_null, Field* field) const;
    Status _build_projected_columns(const format::TableReader& table_reader);
    Status _build_default_expr(const TFileScanSlotInfo& slot_info, VExprContextSPtr* ctx) const;
    static format::ColumnDefinition _build_table_column(const SlotDescriptor* slot_desc);
    Status _build_table_conjuncts(VExprContextSPtrs* conjuncts) const;
    static Status _to_file_format(TFileFormatType::type format_type,
                                  format::FileFormat* file_format);
    void _reset_adaptive_batch_size_state();
    void _init_adaptive_batch_size_state(TFileFormatType::type format_type);
    bool _should_enable_adaptive_batch_size(TFileFormatType::type format_type) const;
    bool _should_run_adaptive_batch_size() const;
    static bool _should_run_adaptive_batch_size(bool predictor_initialized,
                                                bool current_split_uses_metadata_count);
    size_t _predict_reader_batch_rows();
    void _update_adaptive_batch_size(const Block& block);
    static RealtimeCounterDeltas _collect_realtime_counter_deltas(
            const io::FileReaderStats& file_reader_stats,
            const io::FileCacheStatistics& file_cache_statistics,
            UncachedReaderBytesStorage uncached_reader_bytes_storage, int64_t* last_read_bytes,
            int64_t* last_read_rows, int64_t* last_bytes_read_from_local,
            int64_t* last_bytes_read_from_remote);
    static UncachedReaderBytesStorage _uncached_reader_bytes_storage(TFileType::type file_type);
    static void _report_file_cache_profile(RuntimeProfile* profile,
                                           const io::FileCacheStatistics& file_cache_statistics);
    void _report_file_reader_predicate_filtered_rows();
    void _report_condition_cache_profile();

    struct PartitionSlotInfo {
        const SlotDescriptor* slot_desc = nullptr;
        std::string canonical_name;
    };

    const TFileScanRangeParams* _params = nullptr;
    std::shared_ptr<SplitSourceConnector> _split_source;
    bool _first_scan_range = false;
    bool _has_prepared_split = false;
    TFileRangeDesc _current_range;
    std::string _current_range_path;

    std::unique_ptr<format::TableReader> _table_reader;
    std::vector<format::ColumnDefinition> _projected_columns;
    // File formats without embedded schema, such as CSV, still need the FE slot descriptors in
    // file-column order. This mirrors old FileScanner::_file_slot_descs and is passed only to
    // readers that cannot derive their schema from file metadata.
    std::vector<SlotDescriptor*> _file_slot_descs;
    bool _need_global_rowid_column = false;
    std::unordered_map<int32_t, const SlotDescriptor*> _slot_id_to_desc;
    std::unordered_map<int32_t, format::GlobalIndex> _slot_id_to_global_index;
    std::unordered_map<std::string, PartitionSlotInfo> _partition_slot_descs;

    std::unique_ptr<io::FileCacheStatistics> _file_cache_statistics;
    io::FileCacheStatistics _reported_file_cache_statistics;
    std::unique_ptr<io::FileReaderStats> _file_reader_stats;
    std::shared_ptr<io::IOContext> _io_ctx;
    ShardedKVCache* _kv_cache = nullptr;

    RuntimeProfile::Counter* _scanner_total_timer = nullptr;
    RuntimeProfile::Counter* _init_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _get_block_timer = nullptr;
    RuntimeProfile::Counter* _empty_file_counter = nullptr;
    RuntimeProfile::Counter* _prepare_split_timer = nullptr;
    RuntimeProfile::Counter* _get_next_range_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _io_timer = nullptr;
    RuntimeProfile::Counter* _not_found_file_counter = nullptr;
    RuntimeProfile::Counter* _file_counter = nullptr;
    RuntimeProfile::Counter* _file_read_bytes_counter = nullptr;
    RuntimeProfile::Counter* _file_read_calls_counter = nullptr;
    RuntimeProfile::Counter* _file_read_time_counter = nullptr;
    RuntimeProfile::Counter* _adaptive_batch_predicted_rows_counter = nullptr;
    RuntimeProfile::Counter* _adaptive_batch_actual_bytes_counter = nullptr;
    RuntimeProfile::Counter* _adaptive_batch_probe_count_counter = nullptr;
    std::unique_ptr<AdaptiveBlockSizePredictor> _block_size_predictor;
    int64_t _reported_predicate_filtered_rows = 0;
    int64_t _reported_condition_cache_hit_count = 0;
    int64_t _reported_condition_cache_filtered_rows = 0;
    int64_t _last_read_bytes = 0;
    int64_t _last_read_rows = 0;
    int64_t _last_bytes_read_from_local = 0;
    int64_t _last_bytes_read_from_remote = 0;
    int64_t _reported_io_read_time = 0;
};

} // namespace doris
