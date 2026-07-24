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

#include "exec/scan/file_scanner_v2.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PlanNodes_types.h>

#include <algorithm>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <utility>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/consts.h"
#include "common/metrics/doris_metrics.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/string_ref.h"
#include "exec/common/util.hpp"
#include "exec/operator/scan_operator.h"
#include "exec/scan/access_path_parser.h"
#include "exec/scan/file_scan_io_context.h"
#include "exprs/runtime_filter_expr.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "format/format_common.h"
#include "format/table/iceberg_scan_semantics.h"
#include "format_v2/column_mapper.h"
#include "format_v2/jni/iceberg_sys_table_reader.h"
#include "format_v2/jni/jdbc_reader.h"
#include "format_v2/jni/max_compute_jni_reader.h"
#include "format_v2/jni/trino_connector_jni_reader.h"
#include "format_v2/table/hive_reader.h"
#include "format_v2/table/hudi_reader.h"
#include "format_v2/table/iceberg_position_delete_sys_table_reader.h"
#include "format_v2/table/iceberg_reader.h"
#include "format_v2/table/paimon_reader.h"
#include "format_v2/table/remote_doris_reader.h"
#include "format_v2/table_reader.h"
#include "format_v2/wal/wal_table_reader.h"
#include "io/cache/block_file_cache_profile.h"
#include "io/fs/file_meta_cache.h"
#include "io/io_common.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/file_scan_profile.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "storage/id_manager.h"

namespace doris {
namespace {

constexpr int kIcebergPositionDeleteContent = 1;
constexpr int kIcebergDeletionVectorContent = 3;

std::string table_format_name(const TFileRangeDesc& range) {
    return range.__isset.table_format_params ? range.table_format_params.table_format_type
                                             : "NotSet";
}

TFileFormatType::type get_range_format_type(const TFileScanRangeParams& params,
                                            const TFileRangeDesc& range) {
    return range.__isset.format_type ? range.format_type : params.format_type;
}

bool is_supported_table_format(const TFileRangeDesc& range) {
    const auto table_format = table_format_name(range);
    if (table_format == "hudi" && range.__isset.table_format_params &&
        range.table_format_params.__isset.hudi_params &&
        range.table_format_params.hudi_params.__isset.delta_logs &&
        !range.table_format_params.hudi_params.delta_logs.empty()) {
        // Hudi MOR splits need log-file merge semantics and must stay on the existing JNI path.
        // FileScannerV2 currently supports native Parquet data files only.
        return false;
    }
    return table_format == "NotSet" || table_format == "tvf" || table_format == "hive" ||
           table_format == "iceberg" || table_format == "paimon" || table_format == "hudi";
}

bool is_supported_arrow_table_format(const TFileRangeDesc& range) {
    return table_format_name(range) == "remote_doris";
}

bool is_supported_jni_table_format(const TFileRangeDesc& range) {
    const auto table_format = table_format_name(range);
    if (table_format == "paimon") {
        if (!range.__isset.table_format_params ||
            !range.table_format_params.__isset.paimon_params) {
            return false;
        }
        const auto& params = range.table_format_params.paimon_params;
        if (params.__isset.reader_type) {
            if (params.reader_type == TPaimonReaderType::PAIMON_JNI) {
                return params.__isset.paimon_split;
            }
            // Paimon's C++ path is a native Parquet/ORC child of the V2 hybrid reader. Requiring
            // its physical format here prevents an ambiguous FORMAT_JNI split from being routed
            // to a reader whose file semantics cannot be determined.
            return params.reader_type == TPaimonReaderType::PAIMON_CPP &&
                   params.__isset.file_format &&
                   (params.file_format == "parquet" || params.file_format == "orc");
        }
        if (params.__isset.paimon_split) {
            // Before reader_type was added, an encoded split unambiguously selected the Java
            // reader; native scans carried only their physical Parquet or ORC range.
            return true;
        }
        return params.__isset.file_format &&
               (params.file_format == "parquet" || params.file_format == "orc");
    }
    return table_format == "jdbc" || table_format == "iceberg" || table_format == "hudi" ||
           table_format == "max_compute" || table_format == "trino_connector";
}

bool is_iceberg_position_deletes_sys_table(const TFileRangeDesc& range) {
    return range.__isset.table_format_params &&
           range.table_format_params.table_format_type == "iceberg" &&
           range.table_format_params.__isset.iceberg_params &&
           range.table_format_params.iceberg_params.__isset.content &&
           (range.table_format_params.iceberg_params.content == kIcebergPositionDeleteContent ||
            range.table_format_params.iceberg_params.content == kIcebergDeletionVectorContent);
}

bool is_csv_format(TFileFormatType::type format_type) {
    switch (format_type) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
    case TFileFormatType::FORMAT_CSV_GZ:
    case TFileFormatType::FORMAT_CSV_BZ2:
    case TFileFormatType::FORMAT_CSV_LZ4FRAME:
    case TFileFormatType::FORMAT_CSV_LZ4BLOCK:
    case TFileFormatType::FORMAT_CSV_LZOP:
    case TFileFormatType::FORMAT_CSV_DEFLATE:
    case TFileFormatType::FORMAT_CSV_SNAPPYBLOCK:
    case TFileFormatType::FORMAT_PROTO:
        return true;
    default:
        return false;
    }
}

bool is_text_format(TFileFormatType::type format_type) {
    return format_type == TFileFormatType::FORMAT_TEXT;
}

bool is_json_format(TFileFormatType::type format_type) {
    return format_type == TFileFormatType::FORMAT_JSON;
}

bool is_native_format(TFileFormatType::type format_type) {
    return format_type == TFileFormatType::FORMAT_NATIVE;
}

bool is_wal_format(TFileFormatType::type format_type) {
    return format_type == TFileFormatType::FORMAT_WAL;
}

bool is_partition_slot(const TFileScanSlotInfo& slot_info, const std::string& column_name) {
    if (column_name.starts_with(BeConsts::GLOBAL_ROWID_COL) ||
        column_name == BeConsts::ICEBERG_ROWID_COL) {
        return false;
    }
    return slot_info.__isset.category ? slot_info.category == TColumnCategory::PARTITION_KEY
                                      : !slot_info.is_file_slot;
}

bool is_data_file_slot(const TFileScanSlotInfo& slot_info, const std::string& column_name) {
    if (column_name.starts_with(BeConsts::GLOBAL_ROWID_COL) ||
        column_name == BeConsts::ICEBERG_ROWID_COL) {
        return false;
    }
    // CSV and other non-self-describing formats need FE slot descriptors for only the columns that
    // are physically read from the file. Partition/default/virtual columns stay in TableReader's
    // mapping layer and are materialized after the file-local block is read. New FE provides an
    // explicit category; old FE falls back to `is_file_slot`.
    if (slot_info.__isset.category) {
        return slot_info.category == TColumnCategory::REGULAR ||
               slot_info.category == TColumnCategory::GENERATED;
    }
    return slot_info.is_file_slot;
}

Status rewrite_slot_refs_to_global_index(
        VExprSPtr* expr,
        const std::unordered_map<int32_t, format::GlobalIndex>& slot_id_to_global_index) {
    DORIS_CHECK(expr != nullptr);
    if (*expr == nullptr) {
        return Status::OK();
    }
    if (auto* runtime_filter = dynamic_cast<RuntimeFilterExpr*>(expr->get());
        runtime_filter != nullptr) {
        auto impl = runtime_filter->get_impl();
        DORIS_CHECK(impl != nullptr);
        RETURN_IF_ERROR(rewrite_slot_refs_to_global_index(&impl, slot_id_to_global_index));
        runtime_filter->set_impl(std::move(impl));
        return Status::OK();
    }
    if ((*expr)->is_slot_ref()) {
        const auto* slot_ref = assert_cast<const VSlotRef*>(expr->get());
        const auto global_index_it = slot_id_to_global_index.find(slot_ref->slot_id());
        if (global_index_it == slot_id_to_global_index.end()) {
            return Status::InternalError(
                    "Can not resolve source slot id {} to a table global index for column {}",
                    slot_ref->slot_id(), slot_ref->column_name());
        }
        const auto global_index = global_index_it->second;
        *expr = VSlotRef::create_shared(cast_set<int>(global_index.value()),
                                        cast_set<int>(global_index.value()), -1,
                                        slot_ref->data_type(), slot_ref->column_name());
        RETURN_IF_ERROR(expr->get()->prepare(nullptr, RowDescriptor(), nullptr));
        return Status::OK();
    }
    auto children = (*expr)->children();
    for (auto& child : children) {
        if (child == nullptr) {
            continue;
        }
        RETURN_IF_ERROR(rewrite_slot_refs_to_global_index(&child, slot_id_to_global_index));
    }
    (*expr)->set_children(std::move(children));
    return Status::OK();
}

} // namespace

#ifdef BE_TEST
FileScannerV2::FileScannerV2(RuntimeState* state, RuntimeProfile* profile,
                             std::unique_ptr<format::TableReader> table_reader)
        : Scanner(state, profile),
          _table_reader(std::move(table_reader)),
          _scanner_profile(profile) {}

Status FileScannerV2::TEST_validate_scan_range(const TFileScanRangeParams& params,
                                               const TFileRangeDesc& range) {
    return _validate_scan_range(params, range);
}

Status FileScannerV2::TEST_to_file_format(TFileFormatType::type format_type,
                                          format::FileFormat* file_format) {
    return _to_file_format(format_type, file_format);
}

bool FileScannerV2::TEST_is_partition_slot(const TFileScanSlotInfo& slot_info,
                                           const std::string& column_name) {
    return is_partition_slot(slot_info, column_name);
}

bool FileScannerV2::TEST_is_data_file_slot(const TFileScanSlotInfo& slot_info,
                                           const std::string& column_name) {
    return is_data_file_slot(slot_info, column_name);
}

Status FileScannerV2::TEST_rewrite_slot_refs_to_global_index(
        VExprSPtr* expr,
        const std::unordered_map<int32_t, format::GlobalIndex>& slot_id_to_global_index) {
    return rewrite_slot_refs_to_global_index(expr, slot_id_to_global_index);
}

FileScannerV2::RealtimeCounterDeltas FileScannerV2::TEST_collect_realtime_counter_deltas(
        const io::FileReaderStats& file_reader_stats,
        const io::FileCacheStatistics& file_cache_statistics,
        UncachedReaderBytesStorage uncached_reader_bytes_storage, int64_t* last_read_bytes,
        int64_t* last_read_rows, int64_t* last_bytes_read_from_local,
        int64_t* last_bytes_read_from_remote) {
    return _collect_realtime_counter_deltas(file_reader_stats, file_cache_statistics,
                                            uncached_reader_bytes_storage, last_read_bytes,
                                            last_read_rows, last_bytes_read_from_local,
                                            last_bytes_read_from_remote);
}

void FileScannerV2::TEST_report_file_cache_profile(
        RuntimeProfile* profile, const io::FileCacheStatistics& file_cache_statistics) {
    _report_file_cache_profile(profile, file_cache_statistics);
}

bool FileScannerV2::TEST_should_skip_not_found(const Status& status, bool ignore_not_found) {
    return _should_skip_not_found(status, ignore_not_found);
}

bool FileScannerV2::TEST_should_skip_empty(const Status& status, bool stopped) {
    return _should_skip_empty(status, stopped);
}
#endif

bool FileScannerV2::is_supported(const TFileScanRangeParams& params, const TFileRangeDesc& range) {
    const auto format_type = get_range_format_type(params, range);
    if (format_type == TFileFormatType::FORMAT_PARQUET ||
        format_type == TFileFormatType::FORMAT_ORC) {
        return is_supported_table_format(range);
    } else if (format_type == TFileFormatType::FORMAT_ARROW) {
        return is_supported_arrow_table_format(range);
    } else if (format_type == TFileFormatType::FORMAT_JNI) {
        return is_supported_jni_table_format(range);
    } else if (is_wal_format(format_type)) {
        return table_format_name(range) == "NotSet";
    } else if (is_csv_format(format_type) || is_text_format(format_type) ||
               is_json_format(format_type) || is_native_format(format_type)) {
        return is_supported_table_format(range);
    } else {
        LOG(WARNING) << "Unsupported file format type " << format_type << " for file scanner v2";
        return false;
    }
}

Status FileScannerV2::_validate_scan_range(const TFileScanRangeParams& params,
                                           const TFileRangeDesc& range) {
    if (!is_supported(params, range)) {
        return Status::NotSupported(
                "FileScannerV2 does not support table format {} with file format {}",
                table_format_name(range), to_string(get_range_format_type(params, range)));
    }
    return Status::OK();
}

FileScannerV2::FileScannerV2(RuntimeState* state, FileScanLocalState* local_state, int64_t limit,
                             std::shared_ptr<SplitSourceConnector> split_source,
                             RuntimeProfile* profile, ShardedKVCache* kv_cache,
                             const std::unordered_map<std::string, int>* colname_to_slot_id)
        : Scanner(state, local_state, limit, profile),
          _split_source(std::move(split_source)),
          _kv_cache(kv_cache) {
    (void)colname_to_slot_id;
    if (state->get_query_ctx() != nullptr &&
        state->get_query_ctx()->file_scan_range_params_map.count(local_state->parent_id()) > 0) {
        _params = &(state->get_query_ctx()->file_scan_range_params_map[local_state->parent_id()]);
    } else {
        _params = _split_source->get_params();
    }
}

Status FileScannerV2::init(RuntimeState* state, const VExprContextSPtrs& conjuncts) {
    RETURN_IF_ERROR(Scanner::init(state, conjuncts));
    _initialize_scanner_residual_conjuncts();
    auto* profile = _local_state->scanner_profile();
    _scanner_profile = profile;
    const auto hierarchy = file_scan_profile::ensure_hierarchy(profile);
    _scanner_total_timer = hierarchy.scanner;
    _io_timer = hierarchy.io;
    _init_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile, "FileScannerV2InitTime",
                                             file_scan_profile::SCANNER, 1);
    _open_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile, "FileScannerV2OpenTime",
                                             file_scan_profile::SCANNER, 1);
    _get_block_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile, "FileScannerV2GetBlockTime",
                                                  file_scan_profile::SCANNER, 1);
    _prepare_split_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile, "FileScannerV2PrepareSplitTime",
                                                      file_scan_profile::SCANNER, 1);
    _get_next_range_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile, "FileScannerV2GetNextRangeTime",
                                                       file_scan_profile::SCANNER, 1);
    _close_timer = ADD_CHILD_TIMER_WITH_LEVEL(profile, "FileScannerV2CloseTime",
                                              file_scan_profile::SCANNER, 1);
    _empty_file_counter = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "EmptyFileNum", TUnit::UNIT,
                                                       file_scan_profile::SCANNER, 1);
    _not_found_file_counter = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "NotFoundFileNum", TUnit::UNIT,
                                                           file_scan_profile::SCANNER, 1);
    _file_counter = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "FileNumber", TUnit::UNIT,
                                                 file_scan_profile::SCANNER, 1);
    _file_read_bytes_counter = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "FileReadBytes", TUnit::BYTES,
                                                            file_scan_profile::IO, 1);
    _file_read_calls_counter = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "FileReadCalls", TUnit::UNIT,
                                                            file_scan_profile::IO, 1);
    _file_read_time_counter =
            ADD_CHILD_TIMER_WITH_LEVEL(profile, "FileReadTime", file_scan_profile::IO, 1);
    _adaptive_batch_predicted_rows_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "AdaptiveBatchPredictedRows", TUnit::UNIT, file_scan_profile::SCANNER, 1);
    _adaptive_batch_actual_bytes_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "AdaptiveBatchActualBytes", TUnit::BYTES, file_scan_profile::SCANNER, 1);
    _adaptive_batch_probe_count_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "AdaptiveBatchProbeCount", TUnit::UNIT, file_scan_profile::SCANNER, 1);
    _scanner_residual_filter_timer = ADD_CHILD_TIMER_WITH_LEVEL(
            profile, "ScannerResidualFilterTime", file_scan_profile::SCANNER, 1);
    _scanner_residual_rows_filtered_counter = ADD_CHILD_COUNTER_WITH_LEVEL(
            profile, "ScannerResidualRowsFiltered", TUnit::UNIT, file_scan_profile::SCANNER, 1);
    _refresh_scanner_residual_profile();
    SCOPED_TIMER(_scanner_total_timer);
    SCOPED_TIMER(_init_timer);
    _file_cache_statistics = std::make_unique<io::FileCacheStatistics>();
    _file_reader_stats = std::make_unique<io::FileReaderStats>();
    RETURN_IF_ERROR(_init_io_ctx());
    _io_ctx->file_cache_stats = _file_cache_statistics.get();
    _io_ctx->file_reader_stats = _file_reader_stats.get();
    _io_ctx->is_disposable = _state->query_options().disable_file_cache;
    return Status::OK();
}

Status FileScannerV2::_open_impl(RuntimeState* state) {
    SCOPED_TIMER(_scanner_total_timer);
    SCOPED_TIMER(_open_timer);
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(Scanner::_open_impl(state));
    RETURN_IF_ERROR(_get_next_scan_range(&_first_scan_range));
    if (_first_scan_range) {
        RETURN_IF_ERROR(_create_table_reader_for_format(_current_range, &_table_reader));
        DORIS_CHECK(_table_reader != nullptr);
        RETURN_IF_ERROR(_init_expr_ctxes());
        RETURN_IF_ERROR(_init_table_reader(_current_range));
    }
    return Status::OK();
}

Status FileScannerV2::_get_next_scan_range(bool* has_next) {
    SCOPED_TIMER(_get_next_range_timer);
    DORIS_CHECK(has_next != nullptr);
    RETURN_IF_ERROR(_split_source->get_next(has_next, &_current_range));
    if (*has_next) {
        RETURN_IF_ERROR(_validate_scan_range(*_params, _current_range));
    }
    return Status::OK();
}

Status FileScannerV2::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    SCOPED_TIMER(_scanner_total_timer);
    SCOPED_TIMER(_get_block_timer);
    while (true) {
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(_sync_table_reader_conjuncts());
        if (!_has_prepared_split) {
            RETURN_IF_ERROR(_prepare_next_split(eof));
            if (*eof) {
                return Status::OK();
            }
        }

        {
            if (_should_run_adaptive_batch_size()) {
                _table_reader->set_batch_size(_predict_reader_batch_rows());
            }
            const auto status = _table_reader->get_block(block, eof);
            if (_should_skip_not_found(status, config::ignore_not_found_file_in_external_table)) {
                RETURN_IF_ERROR(_table_reader->abort_split());
                COUNTER_UPDATE(_not_found_file_counter, 1);
                _state->update_num_finished_scan_range(1);
                _has_prepared_split = false;
                block->clear_column_data(cast_set<int64_t>(_projected_columns.size()));
                *eof = false;
                continue;
            }
            if (_should_skip_empty(status, _should_stop || _io_ctx->should_stop)) {
                // END_OF_FILE here means the reader discovered a valid split with no data while
                // opening or probing it, not that the Scanner has exhausted all splits. Examples
                // are a zero-byte CSV with an explicit schema and a Doris Native file containing
                // only its 12-byte header. Treat it like V1's empty-file path: finish this range,
                // discard partial reader state, and let the loop fetch the next split.
                RETURN_IF_ERROR(_table_reader->abort_split());
                COUNTER_UPDATE(_empty_file_counter, 1);
                _state->update_num_finished_scan_range(1);
                _has_prepared_split = false;
                block->clear_column_data(cast_set<int64_t>(_projected_columns.size()));
                *eof = false;
                continue;
            }
            RETURN_IF_ERROR(status);
        }
        if (*eof) {
            _state->update_num_finished_scan_range(1);
            _has_prepared_split = false;
            *eof = false;
            continue;
        }
        _update_adaptive_batch_size(*block);
        return Status::OK();
    }
}

Status FileScannerV2::_filter_output_block(Block* block) {
    if (_scanner_residual_conjuncts.empty() || block->rows() == 0) {
        return Status::OK();
    }
    SCOPED_TIMER(_scanner_residual_filter_timer);
    const size_t rows_before_filter = block->rows();
    auto status = VExprContext::filter_block(_scanner_residual_conjuncts, block, block->columns());
    if (!status.ok() && _params != nullptr &&
        _get_current_format_type() == TFileFormatType::FORMAT_ORC) {
        status.prepend("Orc row reader nextBatch failed. reason = ");
    }
    RETURN_IF_ERROR(status);
    const int64_t filtered_rows = cast_set<int64_t>(rows_before_filter - block->rows());
    _counter.num_rows_unselected += filtered_rows;
    if (_scanner_residual_rows_filtered_counter != nullptr) {
        COUNTER_UPDATE(_scanner_residual_rows_filtered_counter, filtered_rows);
    }
    return Status::OK();
}

size_t FileScannerV2::_last_block_rows_read(const Block& block) const {
    const auto& stats = _table_reader->last_materialized_block_stats();
    return stats.has_materialized_input ? stats.rows : block.rows();
}

size_t FileScannerV2::_last_block_bytes_read(const Block& block) const {
    const auto& stats = _table_reader->last_materialized_block_stats();
    return stats.has_materialized_input ? stats.allocated_bytes : block.allocated_bytes();
}

Status FileScannerV2::_prepare_next_split(bool* eos) {
    SCOPED_TIMER(_prepare_split_timer);
    while (true) {
        bool has_next = _first_scan_range;
        if (!_first_scan_range) {
            RETURN_IF_ERROR(_get_next_scan_range(&has_next));
        }
        _first_scan_range = false;
        if (!has_next || _should_stop) {
            *eos = true;
            return Status::OK();
        }
        DORIS_CHECK(_table_reader != nullptr);
        _current_range_path = _current_range.path;

        const auto format_type = get_range_format_type(*_params, _current_range);
        _init_adaptive_batch_size_state(format_type);
        if (_block_size_predictor != nullptr) {
            // JNI readers open eagerly in prepare_split(). Always seed the probe before preparing
            // the next split: its metadata-COUNT decision is not available yet, and the state
            // exposed by TableReader can still describe the preceding split. Metadata shortcuts
            // ignore this batch size, while row-scan fallbacks need it for their first physical
            // read batch.
            _table_reader->set_batch_size(_predict_reader_batch_rows());
        }
        std::map<std::string, Field> partition_values;
        RETURN_IF_ERROR(_generate_partition_values(_current_range, &partition_values));
        const auto status =
                _prepare_table_reader_split(_current_range, std::move(partition_values));
        if (_should_skip_not_found(status, config::ignore_not_found_file_in_external_table)) {
            RETURN_IF_ERROR(_table_reader->abort_split());
            COUNTER_UPDATE(_not_found_file_counter, 1);
            _state->update_num_finished_scan_range(1);
            continue;
        }
        if (_should_skip_empty(status, _should_stop || _io_ctx->should_stop)) {
            // Schema discovery can reach EOF before a split becomes prepared. A header-only Native
            // file follows this path, while a reader that discovers emptiness on its first
            // get_block() follows the symmetric branch in _get_block_impl(). Both paths must
            // advance exactly one scan range and preserve later files in the same scan.
            RETURN_IF_ERROR(_table_reader->abort_split());
            COUNTER_UPDATE(_empty_file_counter, 1);
            _state->update_num_finished_scan_range(1);
            continue;
        }
        RETURN_IF_ERROR(status);
        if (_table_reader->current_split_pruned()) {
            _state->update_num_finished_scan_range(1);
            continue;
        }
        COUNTER_UPDATE(_file_counter, 1);
        _has_prepared_split = true;
        *eos = false;
        return Status::OK();
    }
}

Status FileScannerV2::_init_table_reader(const TFileRangeDesc& range) {
    const auto format_type = get_range_format_type(*_params, range);
    format::FileFormat file_format;
    RETURN_IF_ERROR(_to_file_format(format_type, &file_format));
    DORIS_CHECK(_table_reader != nullptr);

    VExprContextSPtrs table_conjuncts;
    RETURN_IF_ERROR(_build_table_conjuncts(&table_conjuncts));
    std::optional<std::vector<format::GlobalIndex>> push_down_count_columns;
    const auto& push_down_count_slot_ids = _local_state->get_push_down_count_slot_ids();
    if (push_down_count_slot_ids.has_value()) {
        push_down_count_columns.emplace();
        push_down_count_columns->reserve(push_down_count_slot_ids->size());
        for (const auto slot_id : *push_down_count_slot_ids) {
            const auto global_index_it = _slot_id_to_global_index.find(slot_id);
            if (global_index_it == _slot_id_to_global_index.end()) {
                return Status::InternalError(
                        "Pushed-down COUNT argument is not a projected file scan slot, slot_id={}",
                        slot_id);
            }
            push_down_count_columns->push_back(global_index_it->second);
        }
    }
    RETURN_IF_ERROR(_table_reader->init({
            .projected_columns = _projected_columns,
            .conjuncts = std::move(table_conjuncts),
            .table_reader_owned_conjunct_count = _table_reader_owned_conjunct_count,
            .format = file_format,
            .scan_params = const_cast<TFileScanRangeParams*>(_params),
            .io_ctx = _io_ctx,
            .runtime_state = _state,
            .scanner_profile = _local_state->scanner_profile(),
            .file_slot_descs = &_file_slot_descs,
            .push_down_agg_type = _local_state->get_push_down_agg_type(),
            .push_down_count_columns = std::move(push_down_count_columns),
            .condition_cache_digest = _local_state->get_condition_cache_digest(),
    }));
    _table_reader_applied_rf_num = _applied_rf_num;
    // RFs collected before TableReader initialization are already present in the full snapshot.
    _late_arrival_rf_conjuncts.clear();
    return Status::OK();
}

Status FileScannerV2::_create_table_reader_for_format(
        const TFileRangeDesc& range, std::unique_ptr<format::TableReader>* reader) const {
    DORIS_CHECK(reader != nullptr);
    const auto file_format = get_range_format_type(*_params, range);
    if (file_format == TFileFormatType::FORMAT_WAL) {
        *reader = std::make_unique<format::wal::WalTableReader>();
        return Status::OK();
    }
    const auto table_format = table_format_name(range);
    if (table_format == "NotSet" || table_format == "tvf") {
        *reader = std::make_unique<format::TableReader>();
    } else if (table_format == "hive") {
        *reader = format::hive::HiveReader::create_unique();
    } else if (table_format == "iceberg") {
        if (is_iceberg_position_deletes_sys_table(range)) {
            *reader = std::make_unique<format::iceberg::IcebergPositionDeleteSysTableV2Reader>();
        } else if (get_range_format_type(*_params, range) == TFileFormatType::FORMAT_JNI) {
            *reader = std::make_unique<format::iceberg::IcebergSysTableJniReader>();
        } else {
            *reader = std::make_unique<format::iceberg::IcebergTableReader>();
        }
    } else if (table_format == "paimon") {
        *reader = std::make_unique<format::paimon::PaimonHybridReader>();
    } else if (table_format == "hudi") {
        *reader = std::make_unique<format::hudi::HudiHybridReader>();
    } else if (table_format == "jdbc") {
        *reader = std::make_unique<format::jdbc::JdbcJniReader>();
    } else if (table_format == "max_compute") {
        const auto* mc_desc =
                static_cast<const MaxComputeTableDescriptor*>(_output_tuple_desc->table_desc());
        RETURN_IF_ERROR(mc_desc->init_status());
        *reader = std::make_unique<format::max_compute::MaxComputeJniReader>(mc_desc);
    } else if (table_format == "trino_connector") {
        *reader = std::make_unique<format::trino_connector::TrinoConnectorJniReader>();
    } else if (table_format == "remote_doris") {
        *reader = std::make_unique<format::remote_doris::RemoteDorisReader>();
    } else {
        return Status::NotSupported("FileScannerV2 does not support table format {}", table_format);
    }
    return Status::OK();
}

Status FileScannerV2::_prepare_table_reader_split(const TFileRangeDesc& range,
                                                  std::map<std::string, Field> partition_values) {
    format::FileFormat current_split_format;
    RETURN_IF_ERROR(_to_file_format(get_range_format_type(*_params, range), &current_split_format));
    VExprContextSPtrs partition_prune_conjuncts;
    if (_state->query_options().enable_runtime_filter_partition_prune) {
        RETURN_IF_ERROR(_build_table_conjuncts(&partition_prune_conjuncts));
    }
    RETURN_IF_ERROR(_table_reader->prepare_split({
            .partition_values = std::move(partition_values),
            .partition_prune_conjuncts = std::move(partition_prune_conjuncts),
            // A metadata COUNT split may span scheduler turns. Do not enter that irreversible
            // synthetic-row path while a runtime filter can still arrive between batches.
            .all_runtime_filters_applied = _applied_rf_num == _total_rf_num,
            .condition_cache_digest = _current_condition_cache_digest(),
            .cache = _kv_cache,
            .current_range = range,
            .current_split_format = current_split_format,
            .global_rowid_context = _create_global_rowid_context(range),
    }));
    return Status::OK();
}

bool FileScannerV2::_should_skip_not_found(const Status& status, bool ignore_not_found) {
    return ignore_not_found && status.is<ErrorCode::NOT_FOUND>();
}

bool FileScannerV2::_should_skip_empty(const Status& status, bool stopped) {
    // Several readers use END_OF_FILE both for a valid zero-row split and for an interrupted IO.
    // For example, DeletionVectorReader returns END_OF_FILE("stop read.") after try_stop() marks
    // the shared IOContext. That status must unwind the stopped scanner; counting it as an empty
    // file would incorrectly finish the scan range and increment EmptyFileNum.
    return !stopped && status.is<ErrorCode::END_OF_FILE>();
}

bool FileScannerV2::_should_enable_file_meta_cache() const {
    return ExecEnv::GetInstance()->file_meta_cache()->enabled() &&
           _split_source->num_scan_ranges() < config::max_external_file_meta_cache_num / 3;
}

std::optional<format::GlobalRowIdContext> FileScannerV2::_create_global_rowid_context(
        const TFileRangeDesc& range) const {
    if (!_need_global_rowid_column) {
        return std::nullopt;
    }
    auto& id_file_map = _state->get_id_file_map();
    DORIS_CHECK(id_file_map != nullptr);
    const auto file_id = id_file_map->get_file_mapping_id(
            std::make_shared<FileMapping>(_local_state->cast<FileScanLocalState>().parent_id(),
                                          range, _should_enable_file_meta_cache()));
    return format::GlobalRowIdContext {
            .version = IdManager::ID_VERSION,
            .backend_id = BackendOptions::get_backend_id(),
            .file_id = file_id,
    };
}

Status FileScannerV2::_generate_partition_values(
        const TFileRangeDesc& range, std::map<std::string, Field>* partition_values) const {
    DORIS_CHECK(partition_values != nullptr);
    partition_values->clear();
    if (!range.__isset.columns_from_path_keys || !range.__isset.columns_from_path) {
        return Status::OK();
    }
    DORIS_CHECK(range.columns_from_path_keys.size() == range.columns_from_path.size());
    for (size_t idx = 0; idx < range.columns_from_path_keys.size(); ++idx) {
        const auto& key = range.columns_from_path_keys[idx];
        const auto it = _partition_slot_descs.find(key);
        if (it == _partition_slot_descs.end()) {
            continue;
        }
        const auto& value = range.columns_from_path[idx];
        const bool is_null = range.__isset.columns_from_path_is_null &&
                             idx < range.columns_from_path_is_null.size() &&
                             range.columns_from_path_is_null[idx];
        Field field;
        DORIS_CHECK(it->second.slot_desc != nullptr);
        RETURN_IF_ERROR(_parse_partition_value(it->second.slot_desc, value, is_null, &field));
        partition_values->emplace(it->second.canonical_name, std::move(field));
    }
    return Status::OK();
}

Status FileScannerV2::_parse_partition_value(const SlotDescriptor* slot_desc,
                                             const std::string& value, bool is_null,
                                             Field* field) const {
    DORIS_CHECK(slot_desc != nullptr);
    DORIS_CHECK(field != nullptr);
    if (is_null) {
        *field = Field::create_field<TYPE_NULL>(Null());
        return Status::OK();
    }
    const auto data_type = remove_nullable(slot_desc->get_data_type_ptr());
    auto column = data_type->create_column();
    auto serde = data_type->get_serde();
    DataTypeSerDe::FormatOptions options;
    options.converted_from_string = true;
    StringRef ref(value.data(), value.size());
    RETURN_IF_ERROR(serde->from_string(ref, *column, options));
    DORIS_CHECK(column->size() == 1);
    *field = (*column)[0];
    return Status::OK();
}

Status FileScannerV2::_init_expr_ctxes() {
    _slot_id_to_desc.clear();
    _slot_id_to_global_index.clear();
    _partition_slot_descs.clear();
    _file_slot_descs.clear();
    for (const auto* slot_desc : _output_tuple_desc->slots()) {
        _slot_id_to_desc.emplace(slot_desc->id(), slot_desc);
    }
    DORIS_CHECK(_table_reader != nullptr);
    RETURN_IF_ERROR(_build_projected_columns(*_table_reader));
    return Status::OK();
}

Status FileScannerV2::_build_projected_columns(const format::TableReader& table_reader) {
    _projected_columns.clear();
    _projected_columns.reserve(_params->required_slots.size());
    _need_global_rowid_column = false;
    format::ProjectedColumnBuildContext build_context {
            .scan_params = _params,
            .range = &_current_range,
            .runtime_state = _state,
    };
    // Field 34 is the rollout boundary for root and nested exact-name precedence.
    const bool prefer_exact_name_match =
            !_params->__isset.history_schema_info || supports_iceberg_scan_semantics_v1(_params);

    for (size_t slot_idx = 0; slot_idx < _params->required_slots.size(); ++slot_idx) {
        const auto& slot_info = _params->required_slots[slot_idx];
        const auto it = _slot_id_to_desc.find(slot_info.slot_id);
        if (it == _slot_id_to_desc.end()) {
            return Status::InternalError("Unknown source slot descriptor, slot_id={}",
                                         slot_info.slot_id);
        }
        auto column = _build_table_column(it->second);
        build_context.slot_desc = it->second;
        if (column.name.starts_with(BeConsts::GLOBAL_ROWID_COL)) {
            _need_global_rowid_column = true;
        }
        RETURN_IF_ERROR(_build_default_expr(slot_info, &column.default_expr));
        build_context.schema_column.reset();
        RETURN_IF_ERROR(table_reader.annotate_projected_column(slot_info, &build_context, &column));
        // Build nested children from access paths generated by the slot's access-path
        // expressions. A projected column can therefore contain only a subset of the schema
        // column's nested children.
        RETURN_IF_ERROR(AccessPathParser::build_nested_children(
                &column, it->second,
                build_context.schema_column.has_value() ? &*build_context.schema_column : nullptr,
                prefer_exact_name_match));
        if (is_partition_slot(slot_info, column.name)) {
            column.is_partition_key = true;
            _partition_slot_descs.emplace(
                    column.name,
                    PartitionSlotInfo {.slot_desc = it->second, .canonical_name = column.name});
            for (const auto& alias : column.name_mapping) {
                _partition_slot_descs.emplace(
                        alias,
                        PartitionSlotInfo {.slot_desc = it->second, .canonical_name = column.name});
            }
        } else if (is_data_file_slot(slot_info, column.name)) {
            _file_slot_descs.push_back(const_cast<SlotDescriptor*>(it->second));
        }
        const auto global_index = format::GlobalIndex(slot_idx);
        _slot_id_to_global_index.emplace(slot_info.slot_id, global_index);
        _projected_columns.push_back(std::move(column));
    }
    RETURN_IF_ERROR(table_reader.validate_projected_columns(build_context));
    return Status::OK();
}

Status FileScannerV2::_build_default_expr(const TFileScanSlotInfo& slot_info,
                                          VExprContextSPtr* ctx) const {
    DORIS_CHECK(ctx != nullptr);
    if (slot_info.__isset.default_value_expr && !slot_info.default_value_expr.nodes.empty()) {
        return VExpr::create_expr_tree(slot_info.default_value_expr, *ctx);
    }

    if (_params->__isset.default_value_of_src_slot) {
        const auto it = _params->default_value_of_src_slot.find(slot_info.slot_id);
        if (it != _params->default_value_of_src_slot.end() && !it->second.nodes.empty()) {
            return VExpr::create_expr_tree(it->second, *ctx);
        }
    }
    return Status::OK();
}

format::ColumnDefinition FileScannerV2::_build_table_column(const SlotDescriptor* slot_desc) {
    DORIS_CHECK(slot_desc != nullptr);
    format::ColumnDefinition column;
    // TODO(gabriel): why always BY_NAME here?
    column.identifier = Field::create_field<TYPE_STRING>(slot_desc->col_name());
    column.name = slot_desc->col_name();
    column.type = slot_desc->get_data_type_ptr();
    return column;
}

Status FileScannerV2::_build_table_conjuncts(VExprContextSPtrs* conjuncts) const {
    return _build_table_conjuncts(_conjuncts, conjuncts);
}

Status FileScannerV2::_build_table_conjuncts(const VExprContextSPtrs& source,
                                             VExprContextSPtrs* conjuncts) const {
    DORIS_CHECK(conjuncts != nullptr);
    conjuncts->clear();
    conjuncts->reserve(source.size());
    for (const auto& conjunct : source) {
        VExprSPtr root;
        RETURN_IF_ERROR(format::clone_table_expr_tree(conjunct->root(), &root));
        RETURN_IF_ERROR(rewrite_slot_refs_to_global_index(&root, _slot_id_to_global_index));
        conjuncts->push_back(VExprContext::create_shared(std::move(root)));
    }
    return Status::OK();
}

size_t FileScannerV2::_safe_conjunct_prefix_size(const VExprContextSPtrs& conjuncts) {
    for (size_t conjunct_index = 0; conjunct_index < conjuncts.size(); ++conjunct_index) {
        if (!format::TableReader::is_safe_to_pre_execute(conjuncts[conjunct_index])) {
            return conjunct_index;
        }
    }
    return conjuncts.size();
}

void FileScannerV2::_initialize_scanner_residual_conjuncts() {
    _table_reader_owned_conjunct_count = _safe_conjunct_prefix_size(_conjuncts);
    // Preserve the entire suffix, not only the unsafe expression. Otherwise a later safe
    // predicate could run below Scanner before a stateful/error-preserving ordering barrier.
    _scanner_residual_conjuncts.assign(
            _conjuncts.begin() + cast_set<ptrdiff_t>(_table_reader_owned_conjunct_count),
            _conjuncts.end());
    _refresh_scanner_residual_profile();
}

void FileScannerV2::_refresh_scanner_residual_profile() {
    if (_scanner_profile == nullptr || _scanner_residual_conjuncts.empty()) {
        return;
    }
    std::ostringstream predicates;
    predicates << "[";
    for (size_t conjunct_index = 0; conjunct_index < _scanner_residual_conjuncts.size();
         ++conjunct_index) {
        if (conjunct_index > 0) {
            predicates << ", ";
        }
        predicates << _scanner_residual_conjuncts[conjunct_index]->root()->debug_string();
    }
    predicates << "]";
    _scanner_profile->add_info_string("ScannerResidualPredicates", predicates.str());
}

Status FileScannerV2::_sync_table_reader_conjuncts() {
    if (_table_reader == nullptr) {
        return Status::OK();
    }
    if (_table_reader_applied_rf_num == _applied_rf_num) {
        return Status::OK();
    }
    VExprContextSPtrs appended;
    RETURN_IF_ERROR(_build_table_conjuncts(_late_arrival_rf_conjuncts, &appended));
    const size_t owned_count = _scanner_residual_conjuncts.empty()
                                       ? _safe_conjunct_prefix_size(_late_arrival_rf_conjuncts)
                                       : 0;
    // Preserve existing expression state and append the identity-tracked RF delta. Cost sorting
    // may move a late RF ahead of an old stateful predicate in the full scanner snapshot.
    RETURN_IF_ERROR(_table_reader->append_conjuncts_with_ownership(appended, owned_count));
    _table_reader_owned_conjunct_count += owned_count;
    _scanner_residual_conjuncts.insert(
            _scanner_residual_conjuncts.end(),
            _late_arrival_rf_conjuncts.begin() + cast_set<ptrdiff_t>(owned_count),
            _late_arrival_rf_conjuncts.end());
    _refresh_scanner_residual_profile();
    _late_arrival_rf_conjuncts.clear();
    _table_reader_applied_rf_num = _applied_rf_num;
    return Status::OK();
}

TFileFormatType::type FileScannerV2::_get_current_format_type() const {
    return get_range_format_type(*_params, _current_range);
}

Status FileScannerV2::_to_file_format(TFileFormatType::type format_type,
                                      format::FileFormat* file_format) {
    DORIS_CHECK(file_format != nullptr);
    switch (format_type) {
    case TFileFormatType::FORMAT_PARQUET:
        *file_format = format::FileFormat::PARQUET;
        return Status::OK();
    case TFileFormatType::FORMAT_ORC:
        *file_format = format::FileFormat::ORC;
        return Status::OK();
    case TFileFormatType::FORMAT_JNI:
        *file_format = format::FileFormat::JNI;
        return Status::OK();
    case TFileFormatType::FORMAT_CSV_PLAIN:
    case TFileFormatType::FORMAT_CSV_GZ:
    case TFileFormatType::FORMAT_CSV_BZ2:
    case TFileFormatType::FORMAT_CSV_LZ4FRAME:
    case TFileFormatType::FORMAT_CSV_LZ4BLOCK:
    case TFileFormatType::FORMAT_CSV_LZOP:
    case TFileFormatType::FORMAT_CSV_DEFLATE:
    case TFileFormatType::FORMAT_CSV_SNAPPYBLOCK:
    case TFileFormatType::FORMAT_PROTO:
        *file_format = format::FileFormat::CSV;
        return Status::OK();
    case TFileFormatType::FORMAT_TEXT:
        *file_format = format::FileFormat::TEXT;
        return Status::OK();
    case TFileFormatType::FORMAT_JSON:
        *file_format = format::FileFormat::JSON;
        return Status::OK();
    case TFileFormatType::FORMAT_NATIVE:
        *file_format = format::FileFormat::NATIVE;
        return Status::OK();
    case TFileFormatType::FORMAT_ARROW:
        *file_format = format::FileFormat::ARROW;
        return Status::OK();
    case TFileFormatType::FORMAT_WAL:
        *file_format = format::FileFormat::WAL;
        return Status::OK();
    default:
        return Status::NotSupported("FileScannerV2 does not support file format {}",
                                    to_string(format_type));
    }
}

Status FileScannerV2::_init_io_ctx() {
    _io_ctx = create_file_scan_io_context(_state);
    return Status::OK();
}

void FileScannerV2::_reset_adaptive_batch_size_state() {
    _block_size_predictor.reset();
    COUNTER_SET(_adaptive_batch_predicted_rows_counter, int64_t(0));
    COUNTER_SET(_adaptive_batch_actual_bytes_counter, int64_t(0));
}

void FileScannerV2::_init_adaptive_batch_size_state(TFileFormatType::type format_type) {
    _reset_adaptive_batch_size_state();
    if (!_should_enable_adaptive_batch_size(format_type)) {
        return;
    }

    // V2 native file readers do not have reliable row-width hints before the first batch. Start
    // every split with a small probe, then learn bytes-per-row from the materialized table block
    // and keep later batches close to RuntimeState::preferred_block_size_bytes().
    _block_size_predictor = std::make_unique<AdaptiveBlockSizePredictor>(
            _state->preferred_block_size_bytes(), 0.0, ADAPTIVE_BATCH_INITIAL_PROBE_ROWS,
            _state->batch_size());
}

bool FileScannerV2::_should_enable_adaptive_batch_size(TFileFormatType::type format_type) const {
    if (!config::enable_adaptive_batch_size) {
        return false;
    }
    switch (format_type) {
    case TFileFormatType::FORMAT_PARQUET:
    case TFileFormatType::FORMAT_ORC:
    case TFileFormatType::FORMAT_CSV_PLAIN:
    case TFileFormatType::FORMAT_CSV_GZ:
    case TFileFormatType::FORMAT_CSV_BZ2:
    case TFileFormatType::FORMAT_CSV_LZ4FRAME:
    case TFileFormatType::FORMAT_CSV_LZ4BLOCK:
    case TFileFormatType::FORMAT_CSV_LZOP:
    case TFileFormatType::FORMAT_CSV_DEFLATE:
    case TFileFormatType::FORMAT_CSV_SNAPPYBLOCK:
    case TFileFormatType::FORMAT_PROTO:
    case TFileFormatType::FORMAT_TEXT:
    case TFileFormatType::FORMAT_JSON:
    case TFileFormatType::FORMAT_JNI:
        return true;
    default:
        return false;
    }
}

bool FileScannerV2::_should_run_adaptive_batch_size() const {
    DORIS_CHECK(_table_reader != nullptr);
    return _should_run_adaptive_batch_size(_block_size_predictor != nullptr,
                                           _table_reader->current_split_uses_metadata_count());
}

bool FileScannerV2::_should_run_adaptive_batch_size(bool predictor_initialized,
                                                    bool current_split_uses_metadata_count) {
    // Metadata COUNT emits synthetic rows and has no physical row width to learn from. A raw COUNT
    // opcode is not sufficient here: unsupported argument counts, mappings, filters, or deletes
    // make TableReader fall back to materializing normal rows, which still need adaptive batching.
    return predictor_initialized && !current_split_uses_metadata_count;
}

size_t FileScannerV2::_predict_reader_batch_rows() {
    DORIS_CHECK(_block_size_predictor != nullptr);
    // Before history exists this returns the probe row count; after update(), it returns roughly
    // preferred_block_size_bytes / EWMA(bytes_per_row), capped by RuntimeState::batch_size().
    const size_t predicted_rows = _block_size_predictor->predict_next_rows();
    COUNTER_SET(_adaptive_batch_predicted_rows_counter, static_cast<int64_t>(predicted_rows));
    return predicted_rows;
}

void FileScannerV2::_update_adaptive_batch_size(const Block& block) {
    if (!_should_run_adaptive_batch_size()) {
        return;
    }
    const auto& stats = _table_reader->last_materialized_block_stats();
    const size_t rows = stats.has_materialized_input ? stats.rows : block.rows();
    const size_t bytes = stats.has_materialized_input ? stats.bytes : block.bytes();
    COUNTER_SET(_adaptive_batch_actual_bytes_counter, static_cast<int64_t>(bytes));
    if (rows == 0) {
        return;
    }
    // Residual predicates run after wide table columns are materialized. Learn from that pre-filter
    // shape so selective predicates cannot make the next reader batch dangerously large.
    if (!_block_size_predictor->has_history()) {
        COUNTER_UPDATE(_adaptive_batch_probe_count_counter, 1);
    }
    _block_size_predictor->update(rows, bytes);
}

Status FileScannerV2::close(RuntimeState* state) {
    SCOPED_TIMER(_scanner_total_timer);
    SCOPED_TIMER(_close_timer);
    if (!_try_close()) {
        return Status::OK();
    }
    if (_table_reader != nullptr) {
        const auto close_status = _table_reader->close();
        if (!close_status.ok()) {
            // Reserve the close attempt with _try_close(), but commit the scanner-level closed
            // state only after the retained table reader has completed its retryable cleanup.
            _is_closed.store(false);
            return close_status;
        }
        _report_condition_cache_profile();
        _table_reader.reset();
    }
    return Scanner::close(state);
}

void FileScannerV2::try_stop() {
    Scanner::try_stop();
    if (_io_ctx) {
        _io_ctx->should_stop = true;
    }
}

void FileScannerV2::update_realtime_counters() {
    if (_file_reader_stats == nullptr) {
        return;
    }
    DORIS_CHECK(_file_cache_statistics != nullptr);
    const int64_t bytes_read = cast_set<int64_t>(_file_reader_stats->read_bytes);
    auto* local_state = static_cast<FileScanLocalState*>(_local_state);
    const auto file_type =
            _current_range.__isset.file_type
                    ? _current_range.file_type
                    : (_params != nullptr && _params->__isset.file_type ? _params->file_type
                                                                        : TFileType::FILE_LOCAL);
    const auto deltas = _collect_realtime_counter_deltas(
            *_file_reader_stats, *_file_cache_statistics, _uncached_reader_bytes_storage(file_type),
            &_last_read_bytes, &_last_read_rows, &_last_bytes_read_from_local,
            &_last_bytes_read_from_remote);

    COUNTER_UPDATE(local_state->_scan_bytes, deltas.scan_bytes);
    COUNTER_UPDATE(local_state->_scan_rows, deltas.scan_rows);

    _state->get_query_ctx()->resource_ctx()->io_context()->update_scan_rows(deltas.scan_rows);
    _state->get_query_ctx()->resource_ctx()->io_context()->update_scan_bytes(deltas.scan_bytes);
    _state->get_query_ctx()->resource_ctx()->io_context()->update_scan_bytes_from_local_storage(
            deltas.scan_bytes_from_local_storage);
    _state->get_query_ctx()->resource_ctx()->io_context()->update_scan_bytes_from_remote_storage(
            deltas.scan_bytes_from_remote_storage);

    COUNTER_SET(_file_read_bytes_counter, bytes_read);
    COUNTER_SET(_file_read_calls_counter, cast_set<int64_t>(_file_reader_stats->read_calls));
    COUNTER_SET(_file_read_time_counter, cast_set<int64_t>(_file_reader_stats->read_time_ns));

    DorisMetrics::instance()->query_scan_bytes->increment(deltas.scan_bytes);
    DorisMetrics::instance()->query_scan_rows->increment(deltas.scan_rows);
    DorisMetrics::instance()->query_scan_bytes_from_local->increment(
            deltas.scan_bytes_from_local_storage);
    DorisMetrics::instance()->query_scan_bytes_from_remote->increment(
            deltas.scan_bytes_from_remote_storage);
}

FileScannerV2::RealtimeCounterDeltas FileScannerV2::_collect_realtime_counter_deltas(
        const io::FileReaderStats& file_reader_stats,
        const io::FileCacheStatistics& file_cache_statistics,
        UncachedReaderBytesStorage uncached_reader_bytes_storage, int64_t* last_read_bytes,
        int64_t* last_read_rows, int64_t* last_bytes_read_from_local,
        int64_t* last_bytes_read_from_remote) {
    DORIS_CHECK(last_read_bytes != nullptr);
    DORIS_CHECK(last_read_rows != nullptr);
    DORIS_CHECK(last_bytes_read_from_local != nullptr);
    DORIS_CHECK(last_bytes_read_from_remote != nullptr);

    const int64_t read_bytes = cast_set<int64_t>(file_reader_stats.read_bytes);
    const int64_t read_rows = cast_set<int64_t>(file_reader_stats.read_rows);
    const int64_t bytes_read_from_local = file_cache_statistics.bytes_read_from_local;
    const int64_t bytes_read_from_remote = file_cache_statistics.bytes_read_from_remote;
    DORIS_CHECK(read_bytes >= *last_read_bytes);
    DORIS_CHECK(read_rows >= *last_read_rows);
    DORIS_CHECK(bytes_read_from_local >= *last_bytes_read_from_local);
    DORIS_CHECK(bytes_read_from_remote >= *last_bytes_read_from_remote);

    RealtimeCounterDeltas deltas;
    deltas.scan_rows = read_rows - *last_read_rows;
    deltas.scan_bytes = read_bytes - *last_read_bytes;
    // Peer cache is a known cache source, but it is not remote object storage.
    const bool has_cache_source_stats = file_cache_statistics.num_local_io_total != 0 ||
                                        file_cache_statistics.num_remote_io_total != 0 ||
                                        file_cache_statistics.num_peer_io_total != 0 ||
                                        bytes_read_from_local != 0 || bytes_read_from_remote != 0 ||
                                        file_cache_statistics.bytes_read_from_peer != 0;
    if (!has_cache_source_stats) {
        switch (uncached_reader_bytes_storage) {
        case UncachedReaderBytesStorage::LOCAL:
            deltas.scan_bytes_from_local_storage = deltas.scan_bytes;
            break;
        case UncachedReaderBytesStorage::REMOTE:
            deltas.scan_bytes_from_remote_storage = deltas.scan_bytes;
            break;
        case UncachedReaderBytesStorage::NONE:
            break;
        }
    } else {
        deltas.scan_bytes_from_local_storage = bytes_read_from_local - *last_bytes_read_from_local;
        deltas.scan_bytes_from_remote_storage =
                bytes_read_from_remote - *last_bytes_read_from_remote;
    }

    *last_read_bytes = read_bytes;
    *last_read_rows = read_rows;
    *last_bytes_read_from_local = bytes_read_from_local;
    *last_bytes_read_from_remote = bytes_read_from_remote;
    return deltas;
}

FileScannerV2::UncachedReaderBytesStorage FileScannerV2::_uncached_reader_bytes_storage(
        TFileType::type file_type) {
    switch (file_type) {
    case TFileType::FILE_LOCAL:
        return UncachedReaderBytesStorage::LOCAL;
    case TFileType::FILE_STREAM:
        return UncachedReaderBytesStorage::NONE;
    case TFileType::FILE_BROKER:
    case TFileType::FILE_S3:
    case TFileType::FILE_HDFS:
    case TFileType::FILE_NET:
    case TFileType::FILE_HTTP:
        return UncachedReaderBytesStorage::REMOTE;
    }
    DORIS_CHECK(false) << "unknown file type: " << file_type;
    return UncachedReaderBytesStorage::NONE;
}

void FileScannerV2::_collect_profile_before_close() {
    _report_file_reader_predicate_filtered_rows();
    Scanner::_collect_profile_before_close();
    if (config::enable_file_cache && _state->query_options().enable_file_cache &&
        _profile != nullptr) {
        auto file_cache_delta = io::diff_file_cache_statistics(*_file_cache_statistics,
                                                               _reported_file_cache_statistics);
        // Profile collection can run more than once. Keep additive fields incremental while
        // publishing high-water gauges and peer identities from the latest complete snapshot.
        file_cache_delta.remote_only_on_miss_triggered =
                _file_cache_statistics->remote_only_on_miss_triggered;
        file_cache_delta.remote_only_on_miss_threshold_bytes =
                _file_cache_statistics->remote_only_on_miss_threshold_bytes;
        file_cache_delta.peer_hosts = _file_cache_statistics->peer_hosts;
        _report_file_cache_profile(_profile, file_cache_delta);
        _state->get_query_ctx()->resource_ctx()->io_context()->update_bytes_write_into_cache(
                file_cache_delta.bytes_write_into_cache);
        _reported_file_cache_statistics = *_file_cache_statistics;
    }
    if (_file_reader_stats != nullptr) {
        COUNTER_SET(_file_read_bytes_counter, cast_set<int64_t>(_file_reader_stats->read_bytes));
        COUNTER_SET(_file_read_calls_counter, cast_set<int64_t>(_file_reader_stats->read_calls));
        COUNTER_SET(_file_read_time_counter, cast_set<int64_t>(_file_reader_stats->read_time_ns));
        const auto read_time = cast_set<int64_t>(_file_reader_stats->read_time_ns);
        DORIS_CHECK(read_time >= _reported_io_read_time);
        // Some transports (for example Arrow Flight) record directly into IO, while filesystem
        // reads arrive through FileReaderStats. Add only the new traced delta so both paths remain
        // visible without double counting repeated profile publication.
        COUNTER_UPDATE(_io_timer, read_time - _reported_io_read_time);
        _reported_io_read_time = read_time;
    }
    // Query profiles can be collected before Scanner::close() runs. Publish condition-cache
    // counters here as well, using deltas so this method and close() cannot double count.
    _report_condition_cache_profile();
}

void FileScannerV2::_report_file_cache_profile(
        RuntimeProfile* profile, const io::FileCacheStatistics& file_cache_statistics) {
    file_scan_profile::ensure_hierarchy(profile);
    io::FileCacheProfileReporter cache_profile(profile, file_scan_profile::IO);
    cache_profile.update(&file_cache_statistics);
}

bool FileScannerV2::_should_update_load_counters() const {
    if (_is_load) {
        return true;
    }
    // TVF based loads (e.g. http_stream, group commit relay) plan the load source as a
    // tvf query scan without src tuple desc, so _is_load is false. But rows filtered by
    // the load's WHERE clause still need to be reported as unselected rows. FILE_STREAM
    // is only reachable from such load entries, never from normal queries, so use it to
    // identify these scanners.
    return (_params != nullptr && _params->__isset.file_type &&
            _params->file_type == TFileType::FILE_STREAM) ||
           (_current_range.__isset.file_type && _current_range.file_type == TFileType::FILE_STREAM);
}

void FileScannerV2::_report_file_reader_predicate_filtered_rows() {
    const int64_t filtered_rows = _io_ctx != nullptr ? _io_ctx->predicate_filtered_rows : 0;
    const int64_t filtered_delta = filtered_rows - _reported_predicate_filtered_rows;
    if (filtered_delta > 0) {
        // FileReader and TableReader both report their owned predicate rows through the shared IO
        // context. Preserve scanner-level load statistics without re-evaluating either predicate.
        _counter.num_rows_unselected += filtered_delta;
        _reported_predicate_filtered_rows = filtered_rows;
    }
}

void FileScannerV2::_report_condition_cache_profile() {
    auto* local_state = static_cast<FileScanLocalState*>(_local_state);
    const int64_t hit_count =
            _table_reader != nullptr ? _table_reader->condition_cache_hit_count() : 0;
    const int64_t hit_delta = hit_count - _reported_condition_cache_hit_count;
    if (hit_delta > 0) {
        COUNTER_UPDATE(local_state->_condition_cache_hit_counter, hit_delta);
        _reported_condition_cache_hit_count = hit_count;
    }
    const int64_t filtered_rows = _io_ctx != nullptr ? _io_ctx->condition_cache_filtered_rows : 0;
    const int64_t filtered_delta = filtered_rows - _reported_condition_cache_filtered_rows;
    if (filtered_delta > 0) {
        COUNTER_UPDATE(local_state->_condition_cache_filtered_rows_counter, filtered_delta);
        _reported_condition_cache_filtered_rows = filtered_rows;
    }
}

} // namespace doris
