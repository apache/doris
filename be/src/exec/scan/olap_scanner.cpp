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

#include "exec/scan/olap_scanner.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <stdlib.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstdint>
#include <iterator>
#include <limits>
#include <map>
#include <new>
#include <ostream>
#include <set>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet_hotspot.h"
#include "cloud/config.h"
#include "common/config.h"
#include "common/consts.h"
#include "common/logging.h"
#include "common/metrics/doris_metrics.h"
#include "core/block/block.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exec/common/variant_util.h"
#include "exec/operator/olap_scan_operator.h"
#include "exec/scan/scan_node.h"
#include "exprs/function_filter.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "io/cache/block_file_cache_profile.h"
#include "io/io_common.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "storage/binlog.h"
#include "storage/id_manager.h"
#include "storage/index/inverted/inverted_index_profile.h"
#include "storage/index/inverted/similarity/collection_statistics.h"
#include "storage/iterator/block_reader.h"
#include "storage/olap_common.h"
#include "storage/olap_tuple.h"
#include "storage/olap_utils.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_schema.h"
#ifndef NDEBUG
#include "util/debug_points.h"
#endif
#include "util/defer_op.h"
#include "util/json/path_in_data.h"

namespace doris {
#include "common/compile_check_avoid_begin.h"

using ReadSource = TabletReadSource;

static constexpr size_t MAX_SEQ_MAP_CANDIDATE_KEY_BYTES = 32 * 1024 * 1024;
static constexpr size_t MAX_SEQ_MAP_CANDIDATE_WORKSPACE_BYTES = 8 * 1024 * 1024;
static constexpr size_t MAX_SEQ_MAP_CANDIDATE_RESERVATION_BYTES =
        MAX_SEQ_MAP_CANDIDATE_KEY_BYTES + MAX_SEQ_MAP_CANDIDATE_WORKSPACE_BYTES;
static constexpr size_t MIN_SEQ_MAP_CANDIDATE_WORKSPACE_BYTES = 1 * 1024 * 1024;
static constexpr size_t MIN_SEQ_MAP_CANDIDATE_RESERVATION_HEADROOM_BYTES = 1 * 1024 * 1024;

OlapScanner::OlapScanner(ScanLocalStateBase* parent, OlapScanner::Params&& params)
        : Scanner(params.state, parent, params.limit, params.profile),
          _key_ranges(std::move(params.key_ranges)),
          _tablet_reader_params({.tablet = std::move(params.tablet),
                                 .tablet_schema {},
                                 .reader_type = ReaderType::READER_QUERY,
                                 .read_row_binlog = params.read_row_binlog,
                                 .aggregation = params.aggregation,
                                 .version = {0, params.version},
                                 .start_key {},
                                 .end_key {},
                                 .point_keys {},
                                 .predicates {},
                                 .function_filters {},
                                 .delete_predicates {},
                                 .target_cast_type_for_variants {},
                                 .all_access_paths {},
                                 .predicate_access_paths {},
                                 .rs_splits {},
                                 .return_columns {},
                                 .tso_predicate_column_id {},
                                 .output_columns {},
                                 .extra_columns {},
                                 .common_expr_ctxs_push_down {},
                                 .topn_filter_source_node_ids {},
                                 .key_group_cluster_key_idxes {},
                                 .virtual_column_exprs {},
                                 .score_runtime {},
                                 .collection_statistics {},
                                 .ann_topn_runtime {},
                                 .condition_cache_digest = parent->get_condition_cache_digest(),
                                 .binlog_scan_type = params.binlog_scan_type,
                                 .start_tso = std::nullopt,
                                 .end_tso = std::nullopt}),
          _start_tso(params.start_tso),
          _end_tso(params.end_tso),
          _initial_file_cache_stats(std::move(params.initial_file_cache_stats)) {
    _tablet_reader_params.set_read_source(std::move(params.read_source),
                                          _state->skip_delete_bitmap());
    _has_prepared = false;
    _vector_search_params = params.state->get_vector_search_params();
}

static std::string read_columns_to_string(TabletSchemaSPtr tablet_schema,
                                          const std::vector<uint32_t>& read_columns) {
    // avoid too long for one line,
    // it is hard to display in `show profile` stmt if one line is too long.
    const int col_per_line = 10;
    int i = 0;
    std::string read_columns_string;
    read_columns_string += "[";
    for (auto it = read_columns.cbegin(); it != read_columns.cend(); it++) {
        if (it != read_columns.cbegin()) {
            read_columns_string += ", ";
        }
        read_columns_string += tablet_schema->columns().at(*it)->name();
        if (i >= col_per_line) {
            read_columns_string += "\n";
            i = 0;
        } else {
            ++i;
        }
    }
    read_columns_string += "]";
    return read_columns_string;
}

static bool has_file_cache_statistics(const io::FileCacheStatistics& stats) {
    return stats.num_local_io_total != 0 || stats.num_remote_io_total != 0 ||
           stats.num_peer_io_total != 0 || stats.local_io_timer != 0 ||
           stats.bytes_read_from_local != 0 || stats.bytes_read_from_remote != 0 ||
           stats.bytes_read_from_peer != 0 || stats.remote_io_timer != 0 ||
           stats.peer_io_timer != 0 || stats.remote_wait_timer != 0 ||
           stats.write_cache_io_timer != 0 || stats.bytes_write_into_cache != 0 ||
           stats.num_skip_cache_io_total != 0 || stats.read_cache_file_directly_timer != 0 ||
           stats.cache_get_or_set_timer != 0 || stats.lock_wait_timer != 0 ||
           stats.get_timer != 0 || stats.set_timer != 0 || stats.async_cache_write_submitted != 0 ||
           stats.async_cache_write_rejected != 0 ||
           stats.async_cache_write_buffer_alloc_fail != 0 ||
           stats.async_cache_write_drop_stale_epoch != 0 ||
           stats.inflight_write_buffer_index_hit != 0 ||
           stats.inflight_write_buffer_index_miss != 0 || stats.probe_downloaded_hit != 0 ||
           stats.probe_downloading_hit != 0 || stats.probe_miss != 0 ||
           stats.block_wait_success != 0 || stats.block_wait_timeout != 0 ||
           stats.inverted_index_num_local_io_total != 0 ||
           stats.inverted_index_num_remote_io_total != 0 ||
           stats.inverted_index_num_peer_io_total != 0 ||
           stats.inverted_index_bytes_read_from_local != 0 ||
           stats.inverted_index_bytes_read_from_remote != 0 ||
           stats.inverted_index_bytes_read_from_peer != 0 ||
           stats.inverted_index_local_io_timer != 0 || stats.inverted_index_remote_io_timer != 0 ||
           stats.inverted_index_peer_io_timer != 0 || stats.inverted_index_io_timer != 0 ||
           stats.inverted_index_request_bytes != 0 || stats.inverted_index_read_bytes != 0 ||
           stats.inverted_index_range_read_count != 0 ||
           stats.inverted_index_serial_read_rounds != 0;
}

std::vector<RowSetSplits> OlapScanner::_clone_rowset_splits() const {
    std::vector<RowSetSplits> cloned;
    cloned.reserve(_tablet_reader_params.rs_splits.size());
    for (const auto& split : _tablet_reader_params.rs_splits) {
        RowSetSplits copy(split.rs_reader->clone());
        copy.segment_offsets = split.segment_offsets;
        copy.segment_row_ranges = split.segment_row_ranges;
        cloned.emplace_back(std::move(copy));
    }
    return cloned;
}

std::string OlapScanner::_encode_candidate_key(const OlapTuple& key) {
    std::string encoded;
    for (size_t i = 0; i < key.size(); ++i) {
        const auto& field = key.get_field(i);
        const auto type = static_cast<int32_t>(field.get_type());
        encoded.append(reinterpret_cast<const char*>(&type), sizeof(type));
        if (field.is_null()) {
            continue;
        }
        const auto value = field.as_string_view();
        const auto size = static_cast<uint64_t>(value.size());
        encoded.append(reinterpret_cast<const char*>(&size), sizeof(size));
        encoded.append(value);
    }
    return encoded;
}

OlapScanner::CandidateMemoryBudget OlapScanner::_split_candidate_memory_budget(
        size_t reservation_bytes) {
    if (reservation_bytes <= MIN_SEQ_MAP_CANDIDATE_WORKSPACE_BYTES) {
        return {};
    }
    const size_t workspace_bytes =
            std::clamp(reservation_bytes / 5, MIN_SEQ_MAP_CANDIDATE_WORKSPACE_BYTES,
                       MAX_SEQ_MAP_CANDIDATE_WORKSPACE_BYTES);
    const size_t key_bytes =
            std::min(MAX_SEQ_MAP_CANDIDATE_KEY_BYTES, reservation_bytes - workspace_bytes);
    return {
            .reservation_bytes = key_bytes + workspace_bytes,
            .key_bytes = key_bytes,
            .workspace_bytes = workspace_bytes,
    };
}

OlapScanner::CandidateMemoryBudget OlapScanner::_candidate_memory_budget() const {
    const auto tracker = _state->query_mem_tracker();
    if (tracker->limit() < 0) {
        return _split_candidate_memory_budget(MAX_SEQ_MAP_CANDIDATE_RESERVATION_BYTES);
    }
    if (tracker->consumption() >= tracker->limit()) {
        return {};
    }
    const auto remaining = static_cast<size_t>(tracker->limit() - tracker->consumption());
    return _split_candidate_memory_budget(
            std::min(MAX_SEQ_MAP_CANDIDATE_RESERVATION_BYTES, remaining / 8));
}

size_t OlapScanner::_estimate_candidate_key_bytes(const std::string& encoded_key,
                                                  size_t key_column_count) {
    // encoded_key contains the complete variable-length payload. Count it once for the map key
    // and once as a conservative proxy for payload owned by string-like Fields.
    const size_t fixed_bytes = sizeof(CandidateKeyMap::value_type) + 4 * sizeof(void*) +
                               sizeof(RowCursor) + key_column_count * sizeof(Field);
    if (encoded_key.size() > (std::numeric_limits<size_t>::max() - fixed_bytes) / 2) {
        return std::numeric_limits<size_t>::max();
    }
    return fixed_bytes + 2 * encoded_key.size();
}

OlapScanner::CandidateKeyInsertResult OlapScanner::_try_add_seq_map_candidate_key(
        std::string encoded_key, OlapTuple&& key, size_t key_column_count,
        size_t max_candidate_bytes, size_t reservation_headroom_bytes,
        CandidateKeyMap* candidate_keys, size_t* candidate_bytes) {
    DCHECK(candidate_keys != nullptr);
    DCHECK(candidate_bytes != nullptr);
    if (candidate_keys->contains(encoded_key)) {
        return CandidateKeyInsertResult::OK;
    }

    const size_t key_bytes = _estimate_candidate_key_bytes(encoded_key, key_column_count);
    if (*candidate_bytes > max_candidate_bytes ||
        key_bytes > max_candidate_bytes - *candidate_bytes) {
        return CandidateKeyInsertResult::KEY_BYTES_LIMIT;
    }
    if (key_bytes > reservation_headroom_bytes) {
        return CandidateKeyInsertResult::RESERVATION_LIMIT;
    }
    candidate_keys->emplace(std::move(encoded_key), std::move(key));
    *candidate_bytes += key_bytes;
    return CandidateKeyInsertResult::OK;
}

size_t OlapScanner::_estimate_candidate_map_bytes(const CandidateKeyMap& candidate_keys) const {
    size_t bytes = 0;
    const size_t key_column_count = _tablet_reader_params.tablet_schema->num_key_columns();
    for (const auto& entry : candidate_keys) {
        const size_t key_bytes = _estimate_candidate_key_bytes(entry.first, key_column_count);
        if (key_bytes > std::numeric_limits<size_t>::max() - bytes) {
            return std::numeric_limits<size_t>::max();
        }
        bytes += key_bytes;
    }
    return bytes;
}

static size_t saturating_add_size(size_t lhs, size_t rhs) {
    return rhs > std::numeric_limits<size_t>::max() - lhs ? std::numeric_limits<size_t>::max()
                                                          : lhs + rhs;
}

static size_t saturating_multiply_size(size_t lhs, size_t rhs) {
    return lhs != 0 && rhs > std::numeric_limits<size_t>::max() / lhs
                   ? std::numeric_limits<size_t>::max()
                   : lhs * rhs;
}

bool OlapScanner::CandidateScanCostLimit::exceeded(int64_t previous_candidate_scan_rows,
                                                   int64_t current_candidate_scan_rows,
                                                   size_t candidate_key_count) const {
    if (!enabled || full_scan_rows <= 0 || point_probe_cost_per_key == 0 ||
        previous_candidate_scan_rows < 0 || current_candidate_scan_rows < 0) {
        return false;
    }
    if (previous_candidate_scan_rows >= full_scan_rows ||
        current_candidate_scan_rows >= full_scan_rows - previous_candidate_scan_rows) {
        return true;
    }

    // Compare against the remaining row budget without multiplying candidate count by the
    // weighted lower/upper short-key probe cost.
    const auto remaining_rows = static_cast<uint64_t>(
            full_scan_rows - previous_candidate_scan_rows - current_candidate_scan_rows);
    return candidate_key_count > (remaining_rows - 1) / point_probe_cost_per_key;
}

void OlapScanner::_add_seq_map_candidate_cost(uint64_t row_count, size_t segment_count,
                                              CandidateScanCostLimit* cost_limit) {
    DCHECK(cost_limit != nullptr);
    if (cost_limit->full_scan_rows != std::numeric_limits<int64_t>::max()) {
        if (row_count > static_cast<uint64_t>(std::numeric_limits<int64_t>::max() -
                                              cost_limit->full_scan_rows)) {
            cost_limit->full_scan_rows = std::numeric_limits<int64_t>::max();
        } else {
            cost_limit->full_scan_rows += static_cast<int64_t>(row_count);
        }
    }

    // MOR point lookup uses the short-key path. Each lower/upper ordinal lookup can binary-search
    // up to the rowset row count, which is a conservative upper bound for every segment.
    const size_t binary_search_steps =
            std::max<size_t>(1, std::bit_width(std::max<uint64_t>(1, row_count)));
    const size_t rowset_probe_cost = saturating_multiply_size(
            saturating_multiply_size(2, segment_count), binary_search_steps);
    cost_limit->point_probe_cost_per_key =
            saturating_add_size(cost_limit->point_probe_cost_per_key, rowset_probe_cost);
}

void OlapScanner::_merge_seq_map_candidate_stats(const OlapReaderStatistics& candidate_stats,
                                                 OlapReaderStatistics* total_stats) {
    DCHECK(total_stats != nullptr);
    total_stats->seq_map_candidate_scan_rows += candidate_stats.raw_rows_read;
    total_stats->seq_map_candidate_scan_bytes += candidate_stats.uncompressed_bytes_read;
    total_stats->seq_map_candidate_index_filtered_rows +=
            candidate_stats.rows_inverted_index_filtered;
    total_stats->seq_map_candidate_index_downgrades +=
            candidate_stats.inverted_index_downgrade_count;
    total_stats->seq_map_candidate_index_lookup_ns += candidate_stats.inverted_index_lookup_timer;
    total_stats->seq_map_candidate_cache_local_bytes +=
            candidate_stats.file_cache_stats.bytes_read_from_local;
    total_stats->seq_map_candidate_cache_remote_bytes +=
            candidate_stats.file_cache_stats.bytes_read_from_remote;
    total_stats->file_cache_stats.merge_from(candidate_stats.file_cache_stats);

    total_stats->io_ns += candidate_stats.io_ns;
    total_stats->compressed_bytes_read += candidate_stats.compressed_bytes_read;
    total_stats->decompress_ns += candidate_stats.decompress_ns;
    total_stats->uncompressed_bytes_read += candidate_stats.uncompressed_bytes_read;
    total_stats->bytes_read += candidate_stats.bytes_read;
    total_stats->raw_rows_read += candidate_stats.raw_rows_read;
}

Status OlapScanner::_collect_seq_map_candidate_keys(
        const std::vector<std::shared_ptr<ColumnPredicate>>& driver_predicates,
        const std::vector<std::shared_ptr<ColumnPredicate>>& key_predicates,
        int64_t previous_candidate_scan_rows, bool price_point_lookups, int64_t max_candidate_keys,
        size_t max_candidate_bytes, size_t candidate_workspace_bytes,
        const CandidateScanCostLimit& cost_limit, CandidateKeyMap* candidate_keys,
        size_t* candidate_bytes, bool* limit_exceeded, bool* bytes_exceeded,
        bool* reservation_exceeded, bool* cost_exceeded) {
    DCHECK(candidate_keys != nullptr);
    DCHECK(candidate_bytes != nullptr);
    DCHECK(limit_exceeded != nullptr);
    DCHECK(bytes_exceeded != nullptr);
    DCHECK(reservation_exceeded != nullptr);
    DCHECK(cost_exceeded != nullptr);
    *candidate_bytes = 0;
    *limit_exceeded = false;
    *bytes_exceeded = false;
    *reservation_exceeded = false;
    *cost_exceeded = false;
    candidate_keys->clear();

    auto candidate_params = _tablet_reader_params;
    candidate_params.rs_splits = _clone_rowset_splits();
    candidate_params.predicates.clear();
    for (const auto& predicate : key_predicates) {
        candidate_params.predicates.emplace_back(predicate->clone(predicate->column_id()));
    }
    for (const auto& predicate : driver_predicates) {
        candidate_params.predicates.emplace_back(predicate->clone(predicate->column_id()));
    }
    candidate_params.function_filters.clear();
    candidate_params.all_access_paths.clear();
    candidate_params.predicate_access_paths.clear();
    candidate_params.output_columns.clear();
    candidate_params.extra_columns.clear();
    candidate_params.common_expr_ctxs_push_down.clear();
    candidate_params.topn_filter_source_node_ids.clear();
    candidate_params.key_group_cluster_key_idxes.clear();
    candidate_params.virtual_column_exprs.clear();
    candidate_params.score_runtime.reset();
    candidate_params.collection_statistics.reset();
    candidate_params.ann_topn_runtime.reset();
    candidate_params.direct_mode = true;
    candidate_params.aggregation = false;
    candidate_params.is_seq_map_candidate_scan = true;
    candidate_params.seq_map_candidate_pruned = false;
    candidate_params.push_down_agg_type_opt = TPushAggOp::NONE;
    candidate_params.read_orderby_key = false;
    candidate_params.read_orderby_key_reverse = false;
    candidate_params.read_orderby_key_num_prefix_columns = 0;
    candidate_params.read_orderby_key_limit = 0;
    candidate_params.condition_cache_digest = 0;
    candidate_params.general_read_limit = -1;
    candidate_params.read_row_binlog = false;
    candidate_params.binlog_scan_type = TBinlogScanType::NONE;
    candidate_params.start_tso.reset();
    candidate_params.end_tso.reset();
    candidate_params.tso_predicate_column_id.reset();

    std::vector<ColumnId> candidate_columns;
    candidate_columns.reserve(_tablet_reader_params.tablet_schema->num_key_columns() +
                              driver_predicates.size());
    for (uint32_t cid = 0; cid < _tablet_reader_params.tablet_schema->num_key_columns(); ++cid) {
        candidate_columns.push_back(cid);
    }
    for (const auto& predicate : driver_predicates) {
        if (std::find(candidate_columns.begin(), candidate_columns.end(), predicate->column_id()) ==
            candidate_columns.end()) {
            candidate_columns.push_back(predicate->column_id());
        }
    }
    candidate_params.return_columns = candidate_columns;
    candidate_params.origin_return_columns = &candidate_columns;
    candidate_params.tablet_columns_convert_to_null_set = nullptr;

    BlockReader candidate_reader;
    candidate_reader.set_batch_size(_state->batch_size());
    candidate_reader.set_preferred_block_size_bytes(candidate_workspace_bytes);
    Defer account_candidate_stats {[&]() {
        _merge_seq_map_candidate_stats(candidate_reader.stats(), _tablet_reader->mutable_stats());
    }};
    RETURN_IF_ERROR(candidate_reader.init(candidate_params));

    Block block = candidate_params.tablet_schema->create_block(candidate_columns);
    const size_t key_column_count = candidate_params.tablet_schema->num_key_columns();
    bool eof = false;
    while (!eof) {
        RETURN_IF_ERROR(candidate_reader.next_block_with_aggregation(&block, &eof));
        _tablet_reader->mutable_stats()->seq_map_candidate_rows += block.rows();
        for (size_t row = 0; row < block.rows(); ++row) {
            OlapTuple key;
            for (size_t col = 0; col < key_column_count; ++col) {
                Field field;
                block.get_by_position(col).column->get(row, field);
                key.add_field(std::move(field));
            }
            auto encoded_key = _encode_candidate_key(key);
            const int64_t reserved_bytes = thread_context()->thread_mem_tracker_mgr->reserved_mem();
            const size_t reservation_headroom =
                    reserved_bytes > cast_set<int64_t>(
                                             MIN_SEQ_MAP_CANDIDATE_RESERVATION_HEADROOM_BYTES)
                            ? cast_set<size_t>(reserved_bytes -
                                               MIN_SEQ_MAP_CANDIDATE_RESERVATION_HEADROOM_BYTES)
                            : 0;
            const auto insert_result = _try_add_seq_map_candidate_key(
                    std::move(encoded_key), std::move(key), key_column_count, max_candidate_bytes,
                    reservation_headroom, candidate_keys, candidate_bytes);
            if (insert_result == CandidateKeyInsertResult::KEY_BYTES_LIMIT) {
                *bytes_exceeded = true;
                break;
            }
            if (insert_result == CandidateKeyInsertResult::RESERVATION_LIMIT) {
                *reservation_exceeded = true;
                break;
            }
            if (candidate_keys->size() > static_cast<size_t>(max_candidate_keys)) {
                *limit_exceeded = true;
                break;
            }
        }
        block.clear_column_data();
        if (*limit_exceeded || *bytes_exceeded || *reservation_exceeded) {
            break;
        }
        const size_t candidate_key_count = price_point_lookups ? candidate_keys->size() : 0;
        if (cost_limit.exceeded(previous_candidate_scan_rows,
                                candidate_reader.stats().raw_rows_read, candidate_key_count)) {
            *cost_exceeded = true;
            break;
        }
    }
    return Status::OK();
}

Status OlapScanner::_materialize_seq_map_point_keys(CandidateKeyMap* candidate_keys,
                                                    size_t retained_bytes,
                                                    PointKeySetSPtr* point_keys) {
    DCHECK(candidate_keys != nullptr);
    DCHECK(point_keys != nullptr);

    const auto key_schema =
            RowCursor::create_shared_schema(_tablet_reader_params.tablet_schema,
                                            _tablet_reader_params.tablet_schema->num_key_columns());
    auto mutable_point_keys = std::make_shared<PointKeySet>(key_schema);
    mutable_point_keys->keys.reserve(candidate_keys->size());
    for (auto& entry : *candidate_keys) {
        RowCursor point_key;
        RETURN_IF_ERROR(point_key.init(key_schema, std::move(entry.second).release_fields()));
        mutable_point_keys->keys.emplace_back(std::move(point_key));
    }
    std::sort(mutable_point_keys->keys.begin(), mutable_point_keys->keys.end(),
              [](const RowCursor& lhs, const RowCursor& rhs) {
                  return compare_row_key(lhs, rhs) < 0;
              });
    mutable_point_keys->retained_bytes = retained_bytes;
    *point_keys = std::move(mutable_point_keys);
    return Status::OK();
}

bool OlapScanner::_is_candidate_memory_failure(const Status& status) {
    return status.is<ErrorCode::MEM_LIMIT_EXCEEDED>() || status.is<ErrorCode::MEM_ALLOC_FAILED>() ||
           status.is<ErrorCode::QUERY_MEMORY_EXCEEDED>() ||
           status.is<ErrorCode::WORKLOAD_GROUP_MEMORY_EXCEEDED>() ||
           status.is<ErrorCode::PROCESS_MEMORY_EXCEEDED>();
}

void OlapScanner::_record_seq_map_candidate_fallback_reason(RuntimeProfile* profile,
                                                            const std::string& fallback_reason) {
    DCHECK(profile != nullptr);
    DCHECK(!fallback_reason.empty());
    profile->add_info_string("SeqMapCandidateFallbackReason." + fallback_reason, fallback_reason);
}

Status OlapScanner::_build_seq_map_candidate_keys(
        const std::vector<std::shared_ptr<ColumnPredicate>>& key_predicates,
        const std::map<uint32_t, std::vector<std::shared_ptr<ColumnPredicate>>>& group_drivers,
        int64_t max_candidate_keys, const CandidateMemoryBudget& memory_budget,
        const CandidateScanCostLimit& cost_limit) {
    auto& params = _tablet_reader_params;
    auto* stats = _tablet_reader->mutable_stats();

    // Reserve key retention and reader workspace before either candidate map starts allocating.
    auto* mem_tracker_mgr = thread_context()->thread_mem_tracker_mgr.get();
    auto inherited_reservation = mem_tracker_mgr->take_reserved_memory();
    Defer restore_inherited_reservation {
            [&] { mem_tracker_mgr->adopt_reserved_memory(std::move(inherited_reservation)); }};
    auto reserve_status =
            mem_tracker_mgr->try_reserve(cast_set<int64_t>(memory_budget.reservation_bytes));
    if (!reserve_status.ok()) {
        ++stats->seq_map_candidate_fallbacks;
        _seq_map_candidate_fallback_reason = "candidate_key_memory_reservation";
        return Status::OK();
    }
    DEFER_RELEASE_RESERVED();

    CandidateKeyMap final_keys;
    size_t final_key_bytes = 0;
    bool first_group = true;
    for (const auto& [seq_col, predicates] : group_drivers) {
        CandidateKeyMap group_keys;
        size_t group_key_bytes = 0;
        bool limit_exceeded = false;
        bool bytes_exceeded = false;
        bool reservation_exceeded = false;
        bool cost_exceeded = false;
        const size_t remaining_candidate_bytes = memory_budget.key_bytes - final_key_bytes;
        RETURN_IF_ERROR(_collect_seq_map_candidate_keys(
                predicates, key_predicates, stats->seq_map_candidate_scan_rows,
                group_drivers.size() == 1, max_candidate_keys, remaining_candidate_bytes,
                memory_budget.workspace_bytes, cost_limit, &group_keys, &group_key_bytes,
                &limit_exceeded, &bytes_exceeded, &reservation_exceeded, &cost_exceeded));
        stats->seq_map_candidate_key_bytes =
                std::max(stats->seq_map_candidate_key_bytes,
                         cast_set<int64_t>(final_key_bytes + group_key_bytes));
        if (limit_exceeded) {
            ++stats->seq_map_candidate_fallbacks;
            _seq_map_candidate_fallback_reason = "candidate_key_limit";
            return Status::OK();
        }
        if (bytes_exceeded) {
            ++stats->seq_map_candidate_fallbacks;
            _seq_map_candidate_fallback_reason = "candidate_key_bytes_limit";
            return Status::OK();
        }
        if (reservation_exceeded) {
            ++stats->seq_map_candidate_fallbacks;
            _seq_map_candidate_fallback_reason = "candidate_memory_exhausted";
            return Status::OK();
        }
        if (cost_exceeded) {
            ++stats->seq_map_candidate_fallbacks;
            _seq_map_candidate_fallback_reason = "candidate_cost_limit";
            return Status::OK();
        }

        stats->seq_map_candidate_keys_before_intersect += group_keys.size();
        if (first_group) {
            final_keys = std::move(group_keys);
            final_key_bytes = group_key_bytes;
            first_group = false;
        } else {
            for (auto it = final_keys.begin(); it != final_keys.end();) {
                if (!group_keys.contains(it->first)) {
                    it = final_keys.erase(it);
                } else {
                    ++it;
                }
            }
            final_key_bytes = _estimate_candidate_map_bytes(final_keys);
        }
        if (final_keys.empty()) {
            params.seq_map_candidate_pruned = true;
            ++stats->seq_map_candidate_pruned_tablets;
            stats->seq_map_candidate_keys_after_intersect = 0;
            return Status::OK();
        }
        if (cost_limit.exceeded(0, stats->seq_map_candidate_scan_rows, 0)) {
            ++stats->seq_map_candidate_fallbacks;
            _seq_map_candidate_fallback_reason = "candidate_cost_limit";
            return Status::OK();
        }
    }

    // Use the post-intersection key count for multiple groups.
    if (cost_limit.exceeded(0, stats->seq_map_candidate_scan_rows, final_keys.size())) {
        ++stats->seq_map_candidate_fallbacks;
        _seq_map_candidate_fallback_reason = "candidate_cost_limit";
        return Status::OK();
    }

    stats->seq_map_candidate_keys_after_intersect = final_keys.size();
    stats->seq_map_candidate_key_bytes = cast_set<int64_t>(final_key_bytes);
    PointKeySetSPtr point_keys;
    RETURN_IF_ERROR(_materialize_seq_map_point_keys(&final_keys, final_key_bytes, &point_keys));
    params.point_keys = std::move(point_keys);
    return Status::OK();
}

Status OlapScanner::_prepare_seq_map_candidate_keys() {
    const auto& query_options = _state->query_options();
    auto& params = _tablet_reader_params;
    auto& schema = params.tablet_schema;
    if (!query_options.enable_seq_map_candidate_key_scan || schema == nullptr ||
        !schema->has_seq_map() || schema->keys_type() != KeysType::UNIQUE_KEYS ||
        params.tablet->enable_unique_key_merge_on_write() || params.direct_mode) {
        return Status::OK();
    }

    auto* stats = _tablet_reader->mutable_stats();
    SCOPED_RAW_TIMER(&stats->seq_map_candidate_build_ns);
    for (const auto& split : params.rs_splits) {
        if (split.segment_offsets != std::pair<int64_t, int64_t> {0, 0} ||
            !split.segment_row_ranges.empty()) {
            ++stats->seq_map_candidate_fallbacks;
            _seq_map_candidate_fallback_reason = "partial_scanner_split";
            return Status::OK();
        }
    }
    const int64_t max_candidate_keys = query_options.seq_map_candidate_key_max_count;
    if (max_candidate_keys <= 0) {
        ++stats->seq_map_candidate_fallbacks;
        _seq_map_candidate_fallback_reason = "invalid_candidate_limit";
        return Status::OK();
    }
    const auto memory_budget = _candidate_memory_budget();
    if (memory_budget.key_bytes == 0) {
        ++stats->seq_map_candidate_fallbacks;
        _seq_map_candidate_fallback_reason = "candidate_key_bytes_limit";
        return Status::OK();
    }
    if (!query_options.enable_inverted_index_query) {
        ++stats->seq_map_candidate_fallbacks;
        _seq_map_candidate_fallback_reason = "inverted_index_query_disabled";
        return Status::OK();
    }

    std::vector<std::shared_ptr<ColumnPredicate>> key_predicates;
    std::map<uint32_t, std::vector<std::shared_ptr<ColumnPredicate>>> group_drivers;
    const auto& value_to_seq = schema->value_col_idx_to_seq_col_idx();
    for (const auto& predicate : params.predicates) {
        const auto cid = predicate->column_id();
        const auto& column = schema->column(cid);
        if (column.is_key()) {
            // Key predicates constrain the candidate reader, but do not identify a value group.
            key_predicates.push_back(predicate);
            continue;
        }
        const auto seq_it = value_to_seq.find(cid);
        if (seq_it == value_to_seq.end()) {
            continue;
        }
        const auto type = predicate->type();
        const bool positive_driver = type == PredicateType::EQ || type == PredicateType::IN_LIST;
        if (!positive_driver || schema->inverted_indexs(column).empty()) {
            continue;
        }
        group_drivers[seq_it->second].push_back(predicate);
        ++stats->seq_map_candidate_driver_predicates;
    }

    if (group_drivers.empty()) {
        ++stats->seq_map_candidate_fallbacks;
        _seq_map_candidate_fallback_reason = "no_indexed_positive_driver";
        return Status::OK();
    }
    stats->seq_map_candidate_driver_groups = group_drivers.size();
    if (!params.start_key.empty() || !params.end_key.empty()) {
        ++stats->seq_map_candidate_fallbacks;
        _seq_map_candidate_fallback_reason = "key_range_present";
        return Status::OK();
    }

    CandidateScanCostLimit cost_limit;
    for (const auto& split : params.rs_splits) {
        const auto& rowset = split.rs_reader->rowset();
        _add_seq_map_candidate_cost(rowset->num_rows(), cast_set<size_t>(rowset->num_segments()),
                                    &cost_limit);
    }
    cost_limit.enabled = cost_limit.point_probe_cost_per_key > 0 && cost_limit.full_scan_rows > 0;

    Status build_status;
    try {
        ++enable_thread_catch_bad_alloc;
        Defer restore_bad_alloc_catch {[&] { --enable_thread_catch_bad_alloc; }};
        build_status = _build_seq_map_candidate_keys(key_predicates, group_drivers,
                                                     max_candidate_keys, memory_budget, cost_limit);
    } catch (const Exception& exception) {
        build_status = exception.code() == ErrorCode::MEM_ALLOC_FAILED
                               ? Status::MemoryLimitExceeded(exception.to_string())
                               : exception.to_status();
    } catch (const std::bad_alloc& exception) {
        build_status = Status::MemoryLimitExceeded("candidate-key scan allocation failed: {}",
                                                   exception.what());
    }

    if (build_status.ok()) {
        return Status::OK();
    }
    if (build_status.is<ErrorCode::CANCELLED>()) {
        return build_status;
    }
    ++stats->seq_map_candidate_fallbacks;
    _seq_map_candidate_fallback_reason = _is_candidate_memory_failure(build_status)
                                                 ? "candidate_memory_exhausted"
                                                 : "candidate_scan_error";
    LOG(WARNING) << "fallback sequence-mapping candidate scan for tablet "
                 << params.tablet->tablet_id() << ": " << build_status;
    return Status::OK();
}

io::IOContext build_score_runtime_collection_io_context(RuntimeState* state, ReaderType reader_type,
                                                        int64_t expiration_time,
                                                        io::FileCacheStatistics* file_cache_stats) {
    io::IOContext io_ctx {
            .reader_type = reader_type,
            .expiration_time = expiration_time,
            .query_id = &state->query_id(),
            .file_cache_stats = file_cache_stats,
            .is_inverted_index = true,
    };
    if (auto* query_ctx = state->get_query_ctx(); query_ctx != nullptr) {
        io_ctx.remote_scan_cache_write_limiter = query_ctx->remote_scan_cache_write_limiter();
    }
    return io_ctx;
}

Status OlapScanner::_prepare_impl() {
    auto* local_state = static_cast<OlapScanLocalState*>(_local_state);
    auto& tablet = _tablet_reader_params.tablet;
    auto& tablet_schema = _tablet_reader_params.tablet_schema;
    DBUG_EXECUTE_IF("CloudTablet.capture_rs_readers.return.e-230", {
        LOG_WARNING("CloudTablet.capture_rs_readers.return e-230 init")
                .tag("tablet_id", tablet->tablet_id());
        return Status::Error<false>(-230, "injected error");
    });

    for (auto& ctx : local_state->_common_expr_ctxs_push_down) {
        VExprContextSPtr context;
        RETURN_IF_ERROR(ctx->clone(_state, context));
        _common_expr_ctxs_push_down.emplace_back(context);
        context->prepare_ann_range_search(_vector_search_params);
    }

    for (auto pair : local_state->_slot_id_to_virtual_column_expr) {
        // Scanner will be executed in a different thread, so we need to clone the context.
        VExprContextSPtr context;
        RETURN_IF_ERROR(pair.second->clone(_state, context));
        _slot_id_to_virtual_column_expr[pair.first] = context;
    }

    _score_runtime = local_state->_score_runtime;
    // All scanners share the same ann_topn_runtime.
    _ann_topn_runtime = local_state->_ann_topn_runtime;

    // set limit to reduce end of rowset and segment mem use
    _tablet_reader = std::make_unique<BlockReader>();
    // batch size is passed down to segment iterator, use _state->batch_size()
    // instead of _parent->limit(), because if _parent->limit() is a very small
    // value (e.g. select a from t where a .. and b ... limit 1),
    // it will be very slow when reading data in segment iterator
    _tablet_reader->set_batch_size(_state->batch_size());
    // Adaptive batch size: pass byte-budget settings to the storage reader.
    // The reader still uses batch_size() as the row ceiling.
    _tablet_reader->set_preferred_block_size_bytes(_state->preferred_block_size_bytes());
    {
        TOlapScanNode& olap_scan_node = local_state->olap_scan_node();
        TabletSchemaSPtr source_tablet_schema = tablet->tablet_schema();

        tablet_schema = std::make_shared<TabletSchema>();
        tablet_schema->copy_from(*source_tablet_schema);
        if (olap_scan_node.__isset.columns_desc && !olap_scan_node.columns_desc.empty() &&
            olap_scan_node.columns_desc[0].col_unique_id >= 0) {
            tablet_schema->clear_columns();
            for (const auto& column_desc : olap_scan_node.columns_desc) {
                tablet_schema->append_column(TabletColumn(column_desc));
            }
            if (olap_scan_node.__isset.schema_version) {
                tablet_schema->set_schema_version(olap_scan_node.schema_version);
            }
        }
        if (olap_scan_node.__isset.indexes_desc) {
            tablet_schema->update_indexes_from_thrift(olap_scan_node.indexes_desc);
        }

        if (_tablet_reader_params.rs_splits.empty()) {
            // Non-pipeline mode, Tablet : Scanner = 1 : 1
            // acquire tablet rowset readers at the beginning of the scan node
            // to prevent this case: when there are lots of olap scanners to run for example 10000
            // the rowsets maybe compacted when the last olap scanner starts
            ReadSource read_source;

            if (config::is_cloud_mode()) {
                // FIXME(plat1ko): Avoid pointer cast
                ExecEnv::GetInstance()->storage_engine().to_cloud().tablet_hotspot().count(*tablet);
            }

            auto maybe_read_source = tablet->capture_read_source(
                    _tablet_reader_params.version,
                    {
                            .skip_missing_versions = _state->skip_missing_version(),
                            .enable_fetch_rowsets_from_peers =
                                    config::enable_fetch_rowsets_from_peer_replicas,
                            .enable_prefer_cached_rowset =
                                    config::is_cloud_mode() ? _state->enable_prefer_cached_rowset()
                                                            : false,
                            .query_freshness_tolerance_ms =
                                    config::is_cloud_mode() ? _state->query_freshness_tolerance_ms()
                                                            : -1,
                    });
            if (!maybe_read_source) {
                LOG(WARNING) << "fail to init reader. res=" << maybe_read_source.error();
                return maybe_read_source.error();
            }
            read_source = std::move(maybe_read_source.value());

            if (config::enable_mow_verbose_log && tablet->enable_unique_key_merge_on_write()) {
                LOG_INFO("finish capture_rs_readers for tablet={}, query_id={}",
                         tablet->tablet_id(), print_id(_state->query_id()));
            }

            if (!_state->skip_delete_predicate()) {
                read_source.fill_delete_predicates();
            }
            _tablet_reader_params.set_read_source(std::move(read_source));
        }

        // Initialize tablet_reader_params
        RETURN_IF_ERROR(_init_tablet_reader_params(
                local_state->_parent->cast<OlapScanOperatorX>()._slot_id_to_slot_desc, _key_ranges,
                local_state->_slot_id_to_predicates, local_state->_push_down_functions));
    }

    // add read columns in profile
    if (_state->enable_profile()) {
        _profile->add_info_string("ReadColumns",
                                  read_columns_to_string(tablet_schema, _return_columns));
    }

    if (_tablet_reader_params.score_runtime) {
        SCOPED_TIMER(local_state->_statistics_collect_timer);
        _tablet_reader_params.collection_statistics = std::make_shared<CollectionStatistics>();

        auto io_ctx = build_score_runtime_collection_io_context(
                _state, _tablet_reader_params.reader_type, tablet->ttl_seconds(),
                &_tablet_reader->mutable_stats()->file_cache_stats);

        RETURN_IF_ERROR(_tablet_reader_params.collection_statistics->collect(
                _state, _tablet_reader_params.rs_splits, _tablet_reader_params.tablet_schema,
                _tablet_reader_params.common_expr_ctxs_push_down, &io_ctx));
    }

    _has_prepared = true;
    return Status::OK();
}

Status OlapScanner::_open_impl(RuntimeState* state) {
    RETURN_IF_ERROR(Scanner::_open_impl(state));
    SCOPED_TIMER(_local_state->cast<OlapScanLocalState>()._reader_init_timer);

    RETURN_IF_ERROR(_prepare_seq_map_candidate_keys());
    if (_state->enable_profile() && !_seq_map_candidate_fallback_reason.empty()) {
        _record_seq_map_candidate_fallback_reason(_profile, _seq_map_candidate_fallback_reason);
    }

    auto res = _tablet_reader->init(_tablet_reader_params);
    if (!res.ok()) {
        // init() also runs the eager first-row read that evaluates pushed-down expressions,
        // so res may be a data/expression error rather than a storage failure. Keep its own
        // message and only append the tablet/backend, without a misleading storage wording.
        res.append(". tablet=" + std::to_string(_tablet_reader_params.tablet->tablet_id()) +
                   ", backend=" + BackendOptions::get_localhost());
        return res;
    }
    _tablet_reader->mutable_stats()->file_cache_stats.merge_from(_initial_file_cache_stats);

    // Do not hold rs_splits any more to release memory.
    _tablet_reader_params.rs_splits.clear();

    return Status::OK();
}

// For binlog/snapshot incremental read. Forwards the (start_tso, end_tso] range and the TSO
// column id down to BetaRowsetReader, which builds the comparison predicates directly on read
// options. This bypasses the value/key predicate split in TabletReader::_init_conditions_param,
// guaranteeing the range filter always reaches storage (a correctness requirement for MIN_DELTA).
Status OlapScanner::_init_tso_pushdown() {
    if (!_start_tso.has_value() && !_end_tso.has_value()) {
        return Status::OK();
    }

    auto& tablet_schema = _tablet_reader_params.tablet_schema;
    int32_t tso_index = _tablet_reader_params.read_row_binlog ? tablet_schema->binlog_tso_col_idx()
                                                              : tablet_schema->commit_tso_col_idx();
    const std::string& column_name =
            _tablet_reader_params.read_row_binlog ? BINLOG_TSO_COL : COMMIT_TSO_COL;
    if (tso_index < 0) {
        return Status::InternalError("Column {} not found in tablet schema after append",
                                     column_name);
    }

    // Push the TSO range down as-is; BetaRowsetReader builds the comparison predicates and
    // injects them straight into read options, so they cannot be dropped by the value/key
    // predicate split in TabletReader::_init_conditions_param.
    _tablet_reader_params.start_tso = _start_tso;
    _tablet_reader_params.end_tso = _end_tso;

    // The storage-layer statistics fast path (VStatisticsIterator, picked when
    // push_down_agg_type is COUNT/MINMAX) bypasses SegmentIterator and returns raw
    // segment row counts without applying any column predicate. The commit-tso
    // predicate injected above is row-level, so the fast path would both miscount
    // (ignoring commit_tso <= snapshot_tso) and crash on a column-count DCHECK when
    // the tso predicate column is not in return_columns. Disable it here, matching
    // the binlog DETAIL/MIN_DELTA handling.
    _tablet_reader_params.push_down_agg_type_opt = TPushAggOp::NONE;

    // Always carry the tso column id so BetaRowsetReader can build predicates on it.
    // Whether the column must be appended to read_columns (because it is not in
    // return_columns) is decided downstream in BetaRowsetReader.
    _tablet_reader_params.tso_predicate_column_id = static_cast<ColumnId>(tso_index);

    return Status::OK();
}

// it will be called under tablet read lock because capture rs readers need
Status OlapScanner::_init_tablet_reader_params(
        const phmap::flat_hash_map<int, SlotDescriptor*>& slot_id_to_slot_desc,
        const std::vector<OlapScanRange*>& key_ranges,
        const phmap::flat_hash_map<int, std::vector<std::shared_ptr<ColumnPredicate>>>&
                slot_to_predicates,
        const std::vector<FunctionFilter>& function_filters) {
    // if the table with rowset [0-x] or [0-1] [2-y], and [0-1] is empty
    const bool single_version = _tablet_reader_params.has_single_version();

    auto* olap_local_state = static_cast<OlapScanLocalState*>(_local_state);
    bool read_mor_as_dup = olap_local_state->olap_scan_node().__isset.read_mor_as_dup &&
                           olap_local_state->olap_scan_node().read_mor_as_dup;
    if (_state->skip_storage_engine_merge() || read_mor_as_dup) {
        _tablet_reader_params.direct_mode = true;
        _tablet_reader_params.aggregation = true;
    } else {
        auto push_down_agg_type = _local_state->get_push_down_agg_type();
        _tablet_reader_params.direct_mode = _tablet_reader_params.aggregation || single_version ||
                                            (push_down_agg_type != TPushAggOp::NONE &&
                                             push_down_agg_type != TPushAggOp::COUNT_ON_INDEX);
    }

    RETURN_IF_ERROR(_init_variant_columns());
    RETURN_IF_ERROR(_init_return_columns());

    _tablet_reader_params.push_down_agg_type_opt = _local_state->get_push_down_agg_type();

    // Binlog DETAIL/MIN_DELTA scans widen `return_columns` with key/tso/op/before
    // columns to drive the row-level merge in BlockReader. The storage-layer
    // statistics fast path (VStatisticsIterator, picked when push_down_agg_type
    // is COUNT/MINMAX) bypasses SegmentIterator entirely, returning raw segment
    // row counts without binlog op filtering and with a schema that does not
    // match the widened read schema. The result is both wrong (raw segment
    // count != binlog row count) and unsafe (column-count DCHECK fires inside
    // VStatisticsIterator::next_batch). Disable the fast path for these scans.
    if (_tablet_reader_params.binlog_scan_type == TBinlogScanType::DETAIL ||
        _tablet_reader_params.binlog_scan_type == TBinlogScanType::MIN_DELTA) {
        _tablet_reader_params.push_down_agg_type_opt = TPushAggOp::NONE;
    }

    _tablet_reader_params.common_expr_ctxs_push_down = _common_expr_ctxs_push_down;
    _tablet_reader_params.virtual_column_exprs = _virtual_column_exprs;
    _tablet_reader_params.score_runtime = _score_runtime;
    _tablet_reader_params.output_columns = ((OlapScanLocalState*)_local_state)->_output_column_ids;
    _tablet_reader_params.ann_topn_runtime = _ann_topn_runtime;
    for (const auto& ele : ((OlapScanLocalState*)_local_state)->_cast_types_for_variants) {
        _tablet_reader_params.target_cast_type_for_variants[ele.first] = ele.second;
    };
    auto& tablet_schema = _tablet_reader_params.tablet_schema;
    for (auto& predicates : slot_to_predicates) {
        const int sid = predicates.first;
        DCHECK(slot_id_to_slot_desc.contains(sid));
        int32_t index =
                tablet_schema->field_index(slot_id_to_slot_desc.find(sid)->second->col_name());
        if (index < 0) {
            throw Exception(
                    Status::InternalError("Column {} not found in tablet schema",
                                          slot_id_to_slot_desc.find(sid)->second->col_name()));
        }
        for (auto& predicate : predicates.second) {
            _tablet_reader_params.predicates.push_back(predicate->clone(index));
        }
    }

    std::copy(function_filters.cbegin(), function_filters.cend(),
              std::inserter(_tablet_reader_params.function_filters,
                            _tablet_reader_params.function_filters.begin()));

    // Merge the columns in delete predicate that not in latest schema in to current tablet schema
    for (auto& del_pred : _tablet_reader_params.delete_predicates) {
        tablet_schema->merge_dropped_columns(*del_pred->tablet_schema());
    }

    // Push key ranges to the tablet reader.
    // Skip the "full scan" placeholder (has_lower_bound == false) — when no key
    // predicates exist, start_key/end_key remain empty and the reader does a full scan.
    for (auto* key_range : key_ranges) {
        if (!key_range->has_lower_bound) {
            continue;
        }

        _tablet_reader_params.start_key_include = key_range->begin_include;
        _tablet_reader_params.end_key_include = key_range->end_include;

        _tablet_reader_params.start_key.push_back(key_range->begin_scan_range);
        _tablet_reader_params.end_key.push_back(key_range->end_scan_range);
    }

    _tablet_reader_params.profile = _local_state->custom_profile();
    _tablet_reader_params.runtime_state = _state;

    _tablet_reader_params.origin_return_columns = &_return_columns;
    _tablet_reader_params.tablet_columns_convert_to_null_set = &_tablet_columns_convert_to_null_set;

    auto add_return_column_if_absent = [&](uint32_t cid) {
        if (std::find(_tablet_reader_params.return_columns.begin(),
                      _tablet_reader_params.return_columns.end(),
                      cid) == _tablet_reader_params.return_columns.end()) {
            _tablet_reader_params.return_columns.push_back(cid);
        }
    };

    // MIN_DELTA / DETAIL row-binlog scans reconstruct change rows in BlockReader through a
    // key-ordered merge. They must read every key column, every requested value column, the
    // binlog meta columns (tso / op) and their __BEFORE__ mirrors. APPEND_ONLY streams rows
    // as-is and stays on the plain projection paths below.
    const bool is_binlog_merge_scan =
            _tablet_reader_params.binlog_scan_type == TBinlogScanType::MIN_DELTA ||
            _tablet_reader_params.binlog_scan_type == TBinlogScanType::DETAIL;
    if (is_binlog_merge_scan) {
        for (size_t i = 0; i < tablet_schema->num_key_columns(); ++i) {
            add_return_column_if_absent(static_cast<uint32_t>(i));
        }
        for (auto cid : _return_columns) {
            add_return_column_if_absent(cid);
        }

        if (int32_t tso_idx = tablet_schema->binlog_tso_col_idx(); tso_idx >= 0) {
            add_return_column_if_absent(static_cast<uint32_t>(tso_idx));
        }
        if (int32_t op_idx = tablet_schema->binlog_op_col_idx(); op_idx >= 0) {
            add_return_column_if_absent(static_cast<uint32_t>(op_idx));
        }

        for (auto cid : _return_columns) {
            if (cid >= tablet_schema->num_key_columns()) {
                const auto& col_name = tablet_schema->column(cid).name();
                std::string before_col_name;
                before_col_name.append("__BEFORE__");
                before_col_name.append(col_name);
                before_col_name.append("__");
                if (int32_t before_idx = tablet_schema->field_index(before_col_name);
                    before_idx >= 0) {
                    add_return_column_if_absent(static_cast<uint32_t>(before_idx));
                }
            }
        }
    } else if (_tablet_reader_params.direct_mode) {
        _tablet_reader_params.return_columns = _return_columns;
    } else {
        // we need to fetch all key columns to do the right aggregation on storage engine side.
        for (size_t i = 0; i < tablet_schema->num_key_columns(); ++i) {
            _tablet_reader_params.return_columns.push_back(i);
        }
        for (auto index : _return_columns) {
            if (tablet_schema->column(index).is_key()) {
                continue;
            }
            _tablet_reader_params.return_columns.push_back(index);
        }
        // expand the sequence column
        if (tablet_schema->has_sequence_col() || tablet_schema->has_seq_map()) {
            bool has_replace_col = false;
            for (auto col : _return_columns) {
                if (tablet_schema->column(col).aggregation() ==
                    FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE) {
                    has_replace_col = true;
                    break;
                }
            }
            if (auto sequence_col_idx = tablet_schema->sequence_col_idx();
                has_replace_col && tablet_schema->has_sequence_col() &&
                std::find(_return_columns.begin(), _return_columns.end(), sequence_col_idx) ==
                        _return_columns.end()) {
                _tablet_reader_params.return_columns.push_back(sequence_col_idx);
            }
            if (has_replace_col) {
                const auto& val_to_seq = tablet_schema->value_col_idx_to_seq_col_idx();
                std::set<uint32_t> return_seq_columns;

                for (auto col : _tablet_reader_params.return_columns) {
                    // we need to add the necessary sequence column in _return_columns, and
                    // Avoid adding the same seq column twice
                    const auto val_iter = val_to_seq.find(col);
                    if (val_iter != val_to_seq.end()) {
                        auto seq = val_iter->second;
                        if (std::find(_tablet_reader_params.return_columns.begin(),
                                      _tablet_reader_params.return_columns.end(),
                                      seq) == _tablet_reader_params.return_columns.end()) {
                            return_seq_columns.insert(seq);
                        }
                    }
                }
                _tablet_reader_params.return_columns.insert(
                        std::end(_tablet_reader_params.return_columns),
                        std::begin(return_seq_columns), std::end(return_seq_columns));
            }
        }
    }

    RETURN_IF_ERROR(_init_tso_pushdown());

    // Row-binlog scans must not be re-ordered or truncated by ORDER BY / TopN pushdowns,
    // so reset every reorder-related param for all binlog scan types.
    //
    // Only MIN_DELTA / DETAIL additionally force the storage layer to deliver rows strictly
    // in primary-key order, so the BlockReader can group consecutive same-key changes
    // (MIN_DELTA) or emit BEFORE/AFTER pairs in deterministic order (DETAIL). Their storage
    // projection is widened above with the full key prefix, which the key-ordered merge
    // comparator relies on: with read_orderby_key_num_prefix_columns == 0 the comparator
    // falls back to comparing the first num_key_columns block positions.
    //
    // APPEND_ONLY does no key grouping and keeps the raw SQL projection, which may omit
    // some or even all key columns. Forcing a key-ordered merge would make the fallback
    // comparator read key positions that do not exist in the projected blocks and crash
    // the BE (issue #66390), so it reads unordered like a plain scan.
    if (_tablet_reader_params.binlog_scan_type != TBinlogScanType::NONE) {
        _tablet_reader_params.read_orderby_key = is_binlog_merge_scan;
        _tablet_reader_params.force_key_ordered_read = is_binlog_merge_scan;
        _tablet_reader_params.read_orderby_key_reverse = false;
        _tablet_reader_params.read_orderby_key_num_prefix_columns = 0;
        _tablet_reader_params.read_orderby_key_limit = 0;
        _tablet_reader_params.topn_filter_source_node_ids.clear();
    }

    _tablet_reader_params.use_page_cache = _state->enable_page_cache();

    DBUG_EXECUTE_IF("NewOlapScanner::_init_tablet_reader_params.block", DBUG_BLOCK);

    if (!_state->skip_storage_engine_merge()) {
        auto* olap_scan_local_state = (OlapScanLocalState*)_local_state;
        TOlapScanNode& olap_scan_node = olap_scan_local_state->olap_scan_node();

        // Set MOR value predicate pushdown flag
        if (olap_scan_node.__isset.enable_mor_value_predicate_pushdown &&
            olap_scan_node.enable_mor_value_predicate_pushdown) {
            _tablet_reader_params.enable_mor_value_predicate_pushdown = true;
        }

        const bool has_key_topn =
                olap_scan_node.__isset.sort_info && !olap_scan_node.sort_info.is_asc_order.empty();
        if (has_key_topn) {
            _limit = _local_state->limit_per_scanner();
        }

        const bool no_runtime_filters = _total_rf_num == 0;
        const bool segment_limit_enabled = _state->enable_segment_limit_pushdown();
        const bool storage_no_merge = olap_scan_local_state->_storage_no_merge();

        if (_limit > 0 && no_runtime_filters && segment_limit_enabled && storage_no_merge) {
            for (const auto& conjunct : _conjuncts) {
                DORIS_CHECK(!olap_scan_local_state->_check_expr_storage_filter(
                        conjunct->root(), OlapScanLocalState::ExprStorageFilterCheckMode::
                                                  HAS_SEGMENT_EVALUABLE_EXPR));
            }
        }

        // Segment LIMIT has only two legal states: completely disabled, or enabled after every
        // row-filtering conjunct has become a storage predicate or SegmentIterator common expr.
        const bool can_push_down_segment_limit = _limit > 0 && no_runtime_filters &&
                                                 _conjuncts.empty() && segment_limit_enabled &&
                                                 storage_no_merge;
        if (can_push_down_segment_limit) {
            if (has_key_topn) {
                _tablet_reader_params.read_orderby_key = true;
                if (!olap_scan_node.sort_info.is_asc_order[0]) {
                    _tablet_reader_params.read_orderby_key_reverse = true;
                }
                _tablet_reader_params.read_orderby_key_num_prefix_columns =
                        olap_scan_node.sort_info.is_asc_order.size();
                _tablet_reader_params.read_orderby_key_limit = _limit;
            } else {
                _tablet_reader_params.general_read_limit = _limit;
            }
        }

        if (_tablet_reader_params.read_orderby_key_limit > 0 ||
            _tablet_reader_params.general_read_limit > 0) {
            DORIS_CHECK(can_push_down_segment_limit);
            DORIS_CHECK(_conjuncts.empty());
        }

        // A key TopN scan cannot share the plain LIMIT early-stop counter. If
        // storage TopN is pushed down, each scanner must produce its full local
        // candidates. If it is not pushed down for any reason, the upper TopN
        // still needs all rows from the scan.
        if (has_key_topn) {
            _shared_scan_limit = nullptr;
            if (_tablet_reader_params.read_orderby_key_limit == 0) {
                _limit = -1;
            }
        }
        // Note: _shared_scan_limit is intentionally not pushed into the
        // storage layer. SegmentIterator's _process_eof() is irreversible,
        // so a concurrently-decremented atomic could reach 0 while a segment
        // still has data needed by other scanners.

        // set push down topn filter
        _tablet_reader_params.topn_filter_source_node_ids =
                olap_scan_local_state->get_topn_filter_source_node_ids(_state, true);
        if (!_tablet_reader_params.topn_filter_source_node_ids.empty()) {
            _tablet_reader_params.topn_filter_target_node_id =
                    olap_scan_local_state->parent()->node_id();
        }
    }

    if (tablet_schema->has_global_row_id()) {
        auto& id_file_map = _state->get_id_file_map();
        for (auto rs_reader : _tablet_reader_params.rs_splits) {
            id_file_map->add_temp_rowset(rs_reader.rs_reader->rowset());
        }
    }

    return Status::OK();
}

Status OlapScanner::_init_variant_columns() {
    auto& tablet_schema = _tablet_reader_params.tablet_schema;
    if (tablet_schema->num_variant_columns() == 0) {
        return Status::OK();
    }
    // A Variant read column is identified by its parent uid and PathInData. Root and already
    // materialized paths may already exist in the copied tablet schema; missing paths are added
    // below as transient read-schema columns.
    for (auto* slot : _output_tuple_desc->slots()) {
        if (slot->type()->get_primitive_type() != PrimitiveType::TYPE_VARIANT) {
            continue;
        }
        // Materialized paths are absent from the persisted frontend schema. Build their transient
        // read-schema entries from the slot type so V1 and V2 share the same path/type mapping.
        const PathInData path(tablet_schema->column_by_uid(slot->col_unique_id()).name_lower_case(),
                              slot->column_paths());
        // Keep transient paths nullable so an absent path preserves the existing NULL result.
        TabletColumn subcol = variant_util::get_column_by_type(
                make_nullable(slot->type()), path.get_path(),
                variant_util::ExtraInfo {.parent_unique_id = slot->col_unique_id(),
                                         .path_info = path});
        const int32_t column_index = tablet_schema->field_index(path);
        if (column_index < 0) {
            tablet_schema->append_column(subcol, TabletSchema::ColumnType::VARIANT);
            continue;
        }
        if (subcol.variant_is_v2()) {
            // TODO: Remove this promotion after legacy ColumnVariant read destinations are
            // deleted. Persisted metadata describes the shared storage layout; this transient
            // marker only makes the current scan construct a ColumnVariantV2 destination.
            tablet_schema->mutable_column(column_index).set_variant_is_v2(true);
        }
    }
    variant_util::inherit_column_attributes(tablet_schema);
    return Status::OK();
}

Status OlapScanner::_init_return_columns() {
    // For OLAP scan, _output_tuple_desc is the storage-aligned scan tuple
    // descriptor. extra_key_column_slot_ids marks extra key slots that are
    // present only for scan-schema alignment. For example, on an AGG table with
    // keys (k1, k2), a query returning only k2 may still scan (k1, k2); k1 is
    // an extra column and can be removed by the projection output tuple.
    for (auto* slot : _output_tuple_desc->slots()) {
        // variant column using path to index a column
        int32_t index = 0;
        auto& tablet_schema = _tablet_reader_params.tablet_schema;
        if (slot->type()->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
            index = tablet_schema->field_index(PathInData(
                    tablet_schema->column_by_uid(slot->col_unique_id()).name_lower_case(),
                    slot->column_paths()));
        } else {
            index = slot->col_unique_id() >= 0 ? tablet_schema->field_index(slot->col_unique_id())
                                               : tablet_schema->field_index(slot->col_name());
        }

        if (index < 0) {
            return Status::InternalError(
                    "field name is invalid. field={}, field_name_to_index={}, col_unique_id={}",
                    slot->col_name(), tablet_schema->get_all_field_names(), slot->col_unique_id());
        }

        if (slot->get_virtual_column_expr()) {
            _virtual_column_exprs[index] = _slot_id_to_virtual_column_expr[slot->id()];

            VLOG_DEBUG << fmt::format("Virtual column, slot id: {}, cid {}, type: {}", slot->id(),
                                      index, slot->get_data_type_ptr()->get_name());
        }

        const auto& column = tablet_schema->column(index);
        auto* olap_local_state = static_cast<OlapScanLocalState*>(_local_state);
        const auto& olap_scan_node = olap_local_state->olap_scan_node();
        if (olap_scan_node.__isset.extra_key_column_slot_ids &&
            olap_scan_node.extra_key_column_slot_ids.contains(slot->id())) {
            DORIS_CHECK(column.is_key());
            if (_tablet_reader_params.direct_mode) {
                // Direct readers can synthesize extra storage keys because they are only
                // placeholders before the scan projection removes them. Merge/aggregation
                // readers must still read real key values to preserve storage semantics.
                _tablet_reader_params.extra_columns.insert(index);
            }
        }
        int32_t unique_id =
                column.unique_id() >= 0 ? column.unique_id() : column.parent_unique_id();
        if (!slot->all_access_paths().empty()) {
            _tablet_reader_params.all_access_paths.insert({unique_id, slot->all_access_paths()});
        }

        if (!slot->predicate_access_paths().empty()) {
            _tablet_reader_params.predicate_access_paths.insert(
                    {unique_id, slot->predicate_access_paths()});
        }

        if ((slot->type()->get_primitive_type() == PrimitiveType::TYPE_STRUCT ||
             slot->type()->get_primitive_type() == PrimitiveType::TYPE_MAP ||
             slot->type()->get_primitive_type() == PrimitiveType::TYPE_ARRAY) &&
            !slot->all_access_paths().empty()) {
            tablet_schema->add_pruned_columns_data_type(column.unique_id(), slot->type());
        }

        _return_columns.push_back(index);
        if (slot->is_nullable() && !tablet_schema->column(index).is_nullable()) {
            _tablet_columns_convert_to_null_set.emplace(index);
        } else if (!slot->is_nullable() && tablet_schema->column(index).is_nullable()) {
            return Status::Error<ErrorCode::INVALID_SCHEMA>(
                    "slot(id: {}, name: {})'s nullable does not match "
                    "column(tablet id: {}, index: {}, name: {}) ",
                    slot->id(), slot->col_name(), tablet_schema->table_id(), index,
                    tablet_schema->column(index).name());
        }
    }

    if (_return_columns.empty()) {
        return Status::InternalError("failed to build storage scanner, no materialized slot!");
    }

    return Status::OK();
}

bool OlapScanner::check_partition_pruned() const {
    if (!_local_state) {
        return false;
    }
    return _local_state->is_partition_pruned(_tablet_reader_params.tablet->partition_id());
}

doris::TabletStorageType OlapScanner::get_storage_type() {
    if (config::is_cloud_mode()) {
        // we don't have cold storage in cloud mode, all storage is treated as local
        return doris::TabletStorageType::STORAGE_TYPE_LOCAL;
    }
    int local_reader = 0;
    for (const auto& reader : _tablet_reader_params.rs_splits) {
        local_reader += reader.rs_reader->rowset()->is_local();
    }
    int total_reader = _tablet_reader_params.rs_splits.size();

    if (local_reader == total_reader) {
        return doris::TabletStorageType::STORAGE_TYPE_LOCAL;
    } else if (local_reader == 0) {
        return doris::TabletStorageType::STORAGE_TYPE_REMOTE;
    }
    return doris::TabletStorageType::STORAGE_TYPE_REMOTE_AND_LOCAL;
}

Status OlapScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    // Read one block from block reader
    // ATTN: Here we need to let the _get_block_impl method guarantee the semantics of the interface,
    // that is, eof can be set to true only when the returned block is empty.
    RETURN_IF_ERROR(_tablet_reader->next_block_with_aggregation(block, eof));
    if (block->rows() > 0) {
        _tablet_reader_params.tablet->read_block_count.fetch_add(1, std::memory_order_relaxed);
        *eof = false;
    }
#ifndef NDEBUG
    RETURN_IF_ERROR(_check_ann_cache_hit_debug_points(_tablet_reader->stats()));
#endif
    return Status::OK();
}

Status OlapScanner::close(RuntimeState* state) {
    if (!_try_close()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(Scanner::close(state));
    return Status::OK();
}

void OlapScanner::update_realtime_counters() {
    if (!_has_prepared) {
        // Counter update need prepare successfully, or it maybe core. For example, olap scanner
        // will open tablet reader during prepare, if not prepare successfully, tablet reader == nullptr.
        return;
    }
    OlapScanLocalState* local_state = static_cast<OlapScanLocalState*>(_local_state);
    const OlapReaderStatistics& stats = _tablet_reader->stats();
    COUNTER_UPDATE(local_state->_read_compressed_counter, stats.compressed_bytes_read);
    COUNTER_UPDATE(local_state->_read_uncompressed_counter, stats.uncompressed_bytes_read);
    COUNTER_UPDATE(local_state->_scan_bytes, stats.uncompressed_bytes_read);
    COUNTER_UPDATE(local_state->_scan_rows, stats.raw_rows_read);

    // Make sure the scan bytes and scan rows counter in audit log is the same as the counter in
    // doris metrics.
    // ScanBytes is the uncompressed bytes read from local + remote
    // bytes_read_from_local is the compressed bytes read from local
    // bytes_read_from_remote is the compressed bytes read from remote
    // scan bytes > bytes_read_from_local + bytes_read_from_remote
    _state->get_query_ctx()->resource_ctx()->io_context()->update_scan_rows(stats.raw_rows_read);
    _state->get_query_ctx()->resource_ctx()->io_context()->update_scan_bytes(
            stats.uncompressed_bytes_read);

    // In case of no cache, we still need to update the IO stats. uncompressed bytes read == local + remote
    if (stats.file_cache_stats.bytes_read_from_local == 0 &&
        stats.file_cache_stats.bytes_read_from_remote == 0) {
        _state->get_query_ctx()->resource_ctx()->io_context()->update_scan_bytes_from_local_storage(
                stats.compressed_bytes_read);
        DorisMetrics::instance()->query_scan_bytes_from_local->increment(
                stats.compressed_bytes_read);
    } else {
        _state->get_query_ctx()->resource_ctx()->io_context()->update_scan_bytes_from_local_storage(
                stats.file_cache_stats.bytes_read_from_local);
        _state->get_query_ctx()
                ->resource_ctx()
                ->io_context()
                ->update_scan_bytes_from_remote_storage(
                        stats.file_cache_stats.bytes_read_from_remote);

        DorisMetrics::instance()->query_scan_bytes_from_local->increment(
                stats.file_cache_stats.bytes_read_from_local);
        DorisMetrics::instance()->query_scan_bytes_from_remote->increment(
                stats.file_cache_stats.bytes_read_from_remote);
    }

    if (has_file_cache_statistics(stats.file_cache_stats)) {
        io::FileCacheProfileReporter cache_profile(local_state->_segment_profile.get());
        cache_profile.update(&stats.file_cache_stats);
        _state->get_query_ctx()->resource_ctx()->io_context()->update_bytes_write_into_cache(
                stats.file_cache_stats.bytes_write_into_cache);
    }

    _tablet_reader->mutable_stats()->compressed_bytes_read = 0;
    _tablet_reader->mutable_stats()->uncompressed_bytes_read = 0;
    _tablet_reader->mutable_stats()->raw_rows_read = 0;
    _tablet_reader->mutable_stats()->file_cache_stats = {};
}

void OlapScanner::_collect_profile_before_close() {
    //  Please don't directly enable the profile here, we need to set QueryStatistics using the counter inside.
    if (_has_updated_counter) {
        return;
    }
    _has_updated_counter = true;
    _tablet_reader->update_profile(_profile);

    Scanner::_collect_profile_before_close();

    // Update counters for OlapScanner
    // Update counters from tablet reader's stats
    auto& stats = _tablet_reader->stats();
    auto* local_state = (OlapScanLocalState*)_local_state;
    COUNTER_UPDATE(local_state->_io_timer, stats.io_ns);
    COUNTER_UPDATE(local_state->_read_compressed_counter, stats.compressed_bytes_read);
    COUNTER_UPDATE(local_state->_scan_bytes, stats.uncompressed_bytes_read);
    COUNTER_UPDATE(local_state->_decompressor_timer, stats.decompress_ns);
    COUNTER_UPDATE(local_state->_read_uncompressed_counter, stats.uncompressed_bytes_read);
    COUNTER_UPDATE(local_state->_block_load_timer, stats.block_load_ns);
    COUNTER_UPDATE(local_state->_block_load_counter, stats.blocks_load);
    COUNTER_UPDATE(local_state->_block_fetch_timer, stats.block_fetch_ns);
    COUNTER_UPDATE(local_state->_delete_bitmap_get_agg_timer, stats.delete_bitmap_get_agg_ns);
    COUNTER_UPDATE(local_state->_scan_rows, stats.raw_rows_read);
    COUNTER_UPDATE(local_state->_vec_cond_timer, stats.vec_cond_ns);
    COUNTER_UPDATE(local_state->_short_cond_timer, stats.short_cond_ns);
    COUNTER_UPDATE(local_state->_expr_filter_timer, stats.expr_filter_ns);
    COUNTER_UPDATE(local_state->_block_init_timer, stats.block_init_ns);
    COUNTER_UPDATE(local_state->_block_init_seek_timer, stats.block_init_seek_ns);
    COUNTER_UPDATE(local_state->_block_init_seek_counter, stats.block_init_seek_num);
    COUNTER_UPDATE(local_state->_segment_generate_row_range_by_keys_timer,
                   stats.generate_row_ranges_by_keys_ns);
    COUNTER_UPDATE(local_state->_segment_generate_row_range_by_column_conditions_timer,
                   stats.generate_row_ranges_by_column_conditions_ns);
    COUNTER_UPDATE(local_state->_segment_generate_row_range_by_bf_timer,
                   stats.generate_row_ranges_by_bf_ns);
    COUNTER_UPDATE(local_state->_collect_iterator_merge_next_timer,
                   stats.collect_iterator_merge_next_timer);
    COUNTER_UPDATE(local_state->_segment_generate_row_range_by_zonemap_timer,
                   stats.generate_row_ranges_by_zonemap_ns);
    COUNTER_UPDATE(local_state->_segment_generate_row_range_by_dict_timer,
                   stats.generate_row_ranges_by_dict_ns);
    COUNTER_UPDATE(local_state->_predicate_column_read_timer, stats.predicate_column_read_ns);
    COUNTER_UPDATE(local_state->_non_predicate_column_read_timer, stats.non_predicate_read_ns);
    COUNTER_UPDATE(local_state->_predicate_column_read_seek_timer,
                   stats.predicate_column_read_seek_ns);
    COUNTER_UPDATE(local_state->_predicate_column_read_seek_counter,
                   stats.predicate_column_read_seek_num);
    COUNTER_UPDATE(local_state->_lazy_read_timer, stats.lazy_read_ns);
    COUNTER_UPDATE(local_state->_lazy_read_pruned_timer, stats.lazy_read_pruned_ns);
    COUNTER_UPDATE(local_state->_lazy_read_seek_timer, stats.block_lazy_read_seek_ns);
    COUNTER_UPDATE(local_state->_lazy_read_seek_counter, stats.block_lazy_read_seek_num);
    COUNTER_UPDATE(local_state->_output_col_timer, stats.output_col_ns);
    COUNTER_UPDATE(local_state->_rows_vec_cond_filtered_counter, stats.rows_vec_cond_filtered);
    COUNTER_UPDATE(local_state->_rows_short_circuit_cond_filtered_counter,
                   stats.rows_short_circuit_cond_filtered);
    COUNTER_UPDATE(local_state->_rows_expr_cond_filtered_counter, stats.rows_expr_cond_filtered);
    COUNTER_UPDATE(local_state->_rows_vec_cond_input_counter, stats.vec_cond_input_rows);
    COUNTER_UPDATE(local_state->_rows_short_circuit_cond_input_counter,
                   stats.short_circuit_cond_input_rows);
    COUNTER_UPDATE(local_state->_rows_expr_cond_input_counter, stats.expr_cond_input_rows);
    COUNTER_UPDATE(local_state->_stats_filtered_counter, stats.rows_stats_filtered);
    COUNTER_UPDATE(local_state->_stats_rp_filtered_counter, stats.rows_stats_rp_filtered);
    COUNTER_UPDATE(local_state->_expr_zonemap_filtered_segment_counter,
                   stats.expr_zonemap_filtered_segments);
    COUNTER_UPDATE(local_state->_expr_zonemap_filtered_page_counter,
                   stats.expr_zonemap_filtered_pages);
    COUNTER_UPDATE(local_state->_expr_zonemap_unusable_counter, stats.expr_zonemap_unusable_evals);
    COUNTER_UPDATE(local_state->_in_zonemap_point_check_counter,
                   stats.in_zonemap_point_check_count);
    COUNTER_UPDATE(local_state->_in_zonemap_range_only_counter, stats.in_zonemap_range_only_count);
    COUNTER_UPDATE(local_state->_dict_filtered_counter, stats.segment_dict_filtered);
    COUNTER_UPDATE(local_state->_bf_filtered_counter, stats.rows_bf_filtered);
    COUNTER_UPDATE(local_state->_del_filtered_counter, stats.rows_del_filtered);
    COUNTER_UPDATE(local_state->_del_filtered_counter, stats.rows_del_by_bitmap);
    COUNTER_UPDATE(local_state->_del_filtered_counter, stats.rows_vec_del_cond_filtered);
    COUNTER_UPDATE(local_state->_conditions_filtered_counter, stats.rows_conditions_filtered);
    COUNTER_UPDATE(local_state->_key_range_filtered_counter, stats.rows_key_range_filtered);
    COUNTER_UPDATE(local_state->_total_pages_num_counter, stats.total_pages_num);
    COUNTER_UPDATE(local_state->_cached_pages_num_counter, stats.cached_pages_num);
    COUNTER_UPDATE(local_state->_inverted_index_filter_counter, stats.rows_inverted_index_filtered);
    COUNTER_UPDATE(local_state->_inverted_index_filter_timer, stats.inverted_index_filter_timer);
    COUNTER_UPDATE(local_state->_inverted_index_query_cache_hit_counter,
                   stats.inverted_index_query_cache_hit);
    COUNTER_UPDATE(local_state->_inverted_index_query_cache_miss_counter,
                   stats.inverted_index_query_cache_miss);
    COUNTER_UPDATE(local_state->_inverted_index_query_cache_lookup_counter,
                   stats.inverted_index_query_cache_lookup);
    COUNTER_UPDATE(local_state->_inverted_index_query_cache_insert_counter,
                   stats.inverted_index_query_cache_insert);
    COUNTER_UPDATE(local_state->_inverted_index_query_timer, stats.inverted_index_query_timer);
    COUNTER_UPDATE(local_state->_inverted_index_query_null_bitmap_timer,
                   stats.inverted_index_query_null_bitmap_timer);
    COUNTER_UPDATE(local_state->_inverted_index_query_bitmap_copy_timer,
                   stats.inverted_index_query_bitmap_copy_timer);
    COUNTER_UPDATE(local_state->_inverted_index_searcher_open_timer,
                   stats.inverted_index_searcher_open_timer);
    COUNTER_UPDATE(local_state->_inverted_index_searcher_search_timer,
                   stats.inverted_index_searcher_search_timer);
    COUNTER_UPDATE(local_state->_inverted_index_searcher_search_init_timer,
                   stats.inverted_index_searcher_search_init_timer);
    COUNTER_UPDATE(local_state->_inverted_index_searcher_search_exec_timer,
                   stats.inverted_index_searcher_search_exec_timer);
    COUNTER_UPDATE(local_state->_inverted_index_searcher_cache_hit_counter,
                   stats.inverted_index_searcher_cache_hit);
    COUNTER_UPDATE(local_state->_inverted_index_searcher_cache_miss_counter,
                   stats.inverted_index_searcher_cache_miss);
    COUNTER_UPDATE(local_state->_inverted_index_downgrade_count_counter,
                   stats.inverted_index_downgrade_count);
    COUNTER_UPDATE(local_state->_inverted_index_analyzer_timer,
                   stats.inverted_index_analyzer_timer);
    COUNTER_UPDATE(local_state->_inverted_index_lookup_timer, stats.inverted_index_lookup_timer);
    COUNTER_UPDATE(local_state->_seq_map_candidate_driver_groups_counter,
                   stats.seq_map_candidate_driver_groups);
    COUNTER_UPDATE(local_state->_seq_map_candidate_driver_predicates_counter,
                   stats.seq_map_candidate_driver_predicates);
    COUNTER_UPDATE(local_state->_seq_map_candidate_rows_counter, stats.seq_map_candidate_rows);
    COUNTER_UPDATE(local_state->_seq_map_candidate_scan_rows_counter,
                   stats.seq_map_candidate_scan_rows);
    COUNTER_UPDATE(local_state->_seq_map_candidate_scan_bytes_counter,
                   stats.seq_map_candidate_scan_bytes);
    COUNTER_UPDATE(local_state->_seq_map_candidate_index_filtered_rows_counter,
                   stats.seq_map_candidate_index_filtered_rows);
    COUNTER_UPDATE(local_state->_seq_map_candidate_index_downgrades_counter,
                   stats.seq_map_candidate_index_downgrades);
    COUNTER_UPDATE(local_state->_seq_map_candidate_index_lookup_timer,
                   stats.seq_map_candidate_index_lookup_ns);
    COUNTER_UPDATE(local_state->_seq_map_candidate_cache_local_bytes_counter,
                   stats.seq_map_candidate_cache_local_bytes);
    COUNTER_UPDATE(local_state->_seq_map_candidate_cache_remote_bytes_counter,
                   stats.seq_map_candidate_cache_remote_bytes);
    COUNTER_UPDATE(local_state->_seq_map_candidate_keys_before_intersect_counter,
                   stats.seq_map_candidate_keys_before_intersect);
    COUNTER_UPDATE(local_state->_seq_map_candidate_keys_after_intersect_counter,
                   stats.seq_map_candidate_keys_after_intersect);
    COUNTER_UPDATE(local_state->_seq_map_candidate_key_bytes_counter,
                   stats.seq_map_candidate_key_bytes);
    COUNTER_UPDATE(local_state->_seq_map_candidate_build_timer, stats.seq_map_candidate_build_ns);
    COUNTER_UPDATE(local_state->_seq_map_point_range_build_timer,
                   stats.seq_map_point_range_build_ns);
    COUNTER_UPDATE(local_state->_seq_map_candidate_fallbacks_counter,
                   stats.seq_map_candidate_fallbacks);
    COUNTER_UPDATE(local_state->_seq_map_candidate_pruned_tablets_counter,
                   stats.seq_map_candidate_pruned_tablets);
    local_state->_snii_prx_profile_counters.update(stats);
    local_state->_snii_phrase_profile_counters.update(stats);
    COUNTER_UPDATE(local_state->_variant_scan_sparse_column_timer,
                   stats.variant_scan_sparse_column_timer_ns);
    COUNTER_UPDATE(local_state->_variant_scan_sparse_column_bytes,
                   stats.variant_scan_sparse_column_bytes);
    COUNTER_UPDATE(local_state->_variant_fill_path_from_sparse_column_timer,
                   stats.variant_fill_path_from_sparse_column_timer_ns);
    COUNTER_UPDATE(local_state->_variant_subtree_default_iter_count,
                   stats.variant_subtree_default_iter_count);
    COUNTER_UPDATE(local_state->_variant_subtree_leaf_iter_count,
                   stats.variant_subtree_leaf_iter_count);
    COUNTER_UPDATE(local_state->_variant_subtree_hierarchical_iter_count,
                   stats.variant_subtree_hierarchical_iter_count);
    COUNTER_UPDATE(local_state->_variant_subtree_sparse_iter_count,
                   stats.variant_subtree_sparse_iter_count);
    COUNTER_UPDATE(local_state->_variant_doc_value_column_iter_count,
                   stats.variant_doc_value_column_iter_count);

    if (stats.adaptive_batch_size_predict_max_rows > 0) {
        local_state->_adaptive_batch_predict_min_rows_counter->set(
                stats.adaptive_batch_size_predict_min_rows);
        local_state->_adaptive_batch_predict_max_rows_counter->set(
                stats.adaptive_batch_size_predict_max_rows);
    }

    InvertedIndexProfileReporter inverted_index_profile;
    inverted_index_profile.update(local_state->_index_filter_profile.get(),
                                  &stats.inverted_index_stats);

    if (has_file_cache_statistics(stats.file_cache_stats)) {
        io::FileCacheProfileReporter cache_profile(local_state->_segment_profile.get());
        cache_profile.update(&stats.file_cache_stats);
        _state->get_query_ctx()->resource_ctx()->io_context()->update_bytes_write_into_cache(
                stats.file_cache_stats.bytes_write_into_cache);
    }
    COUNTER_UPDATE(local_state->_output_index_result_column_timer,
                   stats.output_index_result_column_timer);
    COUNTER_UPDATE(local_state->_filtered_segment_counter, stats.filtered_segment_number);
    COUNTER_UPDATE(local_state->_total_segment_counter, stats.total_segment_number);
    COUNTER_UPDATE(local_state->_condition_cache_hit_counter, stats.condition_cache_hit_seg_nums);
    COUNTER_UPDATE(local_state->_condition_cache_filtered_rows_counter,
                   stats.condition_cache_filtered_rows);

    COUNTER_UPDATE(local_state->_tablet_reader_init_timer, stats.tablet_reader_init_timer_ns);
    COUNTER_UPDATE(local_state->_tablet_reader_capture_rs_readers_timer,
                   stats.tablet_reader_capture_rs_readers_timer_ns);
    COUNTER_UPDATE(local_state->_tablet_reader_init_return_columns_timer,
                   stats.tablet_reader_init_return_columns_timer_ns);
    COUNTER_UPDATE(local_state->_tablet_reader_init_keys_param_timer,
                   stats.tablet_reader_init_keys_param_timer_ns);
    COUNTER_UPDATE(local_state->_tablet_reader_init_orderby_keys_param_timer,
                   stats.tablet_reader_init_orderby_keys_param_timer_ns);
    COUNTER_UPDATE(local_state->_tablet_reader_init_conditions_param_timer,
                   stats.tablet_reader_init_conditions_param_timer_ns);
    COUNTER_UPDATE(local_state->_tablet_reader_init_delete_condition_param_timer,
                   stats.tablet_reader_init_delete_condition_param_timer_ns);
    COUNTER_UPDATE(local_state->_block_reader_vcollect_iter_init_timer,
                   stats.block_reader_vcollect_iter_init_timer_ns);
    COUNTER_UPDATE(local_state->_block_reader_rs_readers_init_timer,
                   stats.block_reader_rs_readers_init_timer_ns);
    COUNTER_UPDATE(local_state->_block_reader_build_heap_init_timer,
                   stats.block_reader_build_heap_init_timer_ns);

    COUNTER_UPDATE(local_state->_rowset_reader_get_segment_iterators_timer,
                   stats.rowset_reader_get_segment_iterators_timer_ns);
    COUNTER_UPDATE(local_state->_rowset_reader_create_iterators_timer,
                   stats.rowset_reader_create_iterators_timer_ns);
    COUNTER_UPDATE(local_state->_rowset_reader_init_iterators_timer,
                   stats.rowset_reader_init_iterators_timer_ns);
    COUNTER_UPDATE(local_state->_rowset_reader_load_segments_timer,
                   stats.rowset_reader_load_segments_timer_ns);

    COUNTER_UPDATE(local_state->_segment_iterator_init_timer, stats.segment_iterator_init_timer_ns);
    COUNTER_UPDATE(local_state->_segment_iterator_init_return_column_iterators_timer,
                   stats.segment_iterator_init_return_column_iterators_timer_ns);
    COUNTER_UPDATE(local_state->_segment_iterator_init_index_iterators_timer,
                   stats.segment_iterator_init_index_iterators_timer_ns);
    COUNTER_UPDATE(local_state->_segment_iterator_init_segment_prefetchers_timer,
                   stats.segment_iterator_init_segment_prefetchers_timer_ns);

    COUNTER_UPDATE(local_state->_segment_create_column_readers_timer,
                   stats.segment_create_column_readers_timer_ns);
    COUNTER_UPDATE(local_state->_segment_load_index_timer, stats.segment_load_index_timer_ns);

    // Update metrics
    DorisMetrics::instance()->query_scan_bytes->increment(
            local_state->_read_uncompressed_counter->value());
    DorisMetrics::instance()->query_scan_rows->increment(local_state->_scan_rows->value());
    auto& tablet = _tablet_reader_params.tablet;
    tablet->query_scan_bytes->increment(local_state->_read_uncompressed_counter->value());
    tablet->query_scan_rows->increment(local_state->_scan_rows->value());
    tablet->query_scan_count->increment(1);

    COUNTER_UPDATE(local_state->_ann_range_search_filter_counter,
                   stats.rows_ann_index_range_filtered);
    COUNTER_UPDATE(local_state->_ann_topn_filter_counter, stats.rows_ann_index_topn_filtered);
    COUNTER_UPDATE(local_state->_ann_index_load_costs, stats.ann_index_load_ns);
    COUNTER_UPDATE(local_state->_ann_ivf_on_disk_load_costs, stats.ann_ivf_on_disk_load_ns);
    COUNTER_UPDATE(local_state->_ann_ivf_on_disk_cache_hit_cnt,
                   stats.ann_ivf_on_disk_cache_hit_cnt);
    COUNTER_UPDATE(local_state->_ann_ivf_on_disk_cache_miss_cnt,
                   stats.ann_ivf_on_disk_cache_miss_cnt);
    COUNTER_UPDATE(local_state->_ann_range_search_costs, stats.ann_index_range_search_ns);
    COUNTER_UPDATE(local_state->_ann_range_search_cnt, stats.ann_index_range_search_cnt);
    COUNTER_UPDATE(local_state->_ann_range_engine_search_costs, stats.ann_range_engine_search_ns);
    // Engine prepare before search
    COUNTER_UPDATE(local_state->_ann_range_pre_process_costs, stats.ann_range_pre_process_ns);
    // Post process parent: Doris result process + engine convert
    COUNTER_UPDATE(local_state->_ann_range_post_process_costs,
                   stats.ann_range_result_convert_ns + stats.ann_range_engine_convert_ns);
    // Engine convert (child under post-process)
    COUNTER_UPDATE(local_state->_ann_range_engine_convert_costs, stats.ann_range_engine_convert_ns);
    // Doris-side result convert (child under post-process)
    COUNTER_UPDATE(local_state->_ann_range_result_convert_costs, stats.ann_range_result_convert_ns);

    COUNTER_UPDATE(local_state->_ann_topn_search_costs, stats.ann_topn_search_ns);
    COUNTER_UPDATE(local_state->_ann_topn_search_cnt, stats.ann_index_topn_search_cnt);
    COUNTER_UPDATE(local_state->_ann_cache_hit_cnt, stats.ann_index_cache_hits);
    COUNTER_UPDATE(local_state->_ann_range_cache_hit_cnt, stats.ann_index_range_cache_hits);

    // Detailed ANN timers
    // ANN TopN timers with hierarchy
    // Engine search time (FAISS)
    COUNTER_UPDATE(local_state->_ann_topn_engine_search_costs,
                   stats.ann_index_topn_engine_search_ns);
    // Engine prepare time (allocations/buffer setup before search)
    COUNTER_UPDATE(local_state->_ann_topn_pre_process_costs,
                   stats.ann_index_topn_engine_prepare_ns);
    // Post process parent includes Doris result processing + engine convert
    COUNTER_UPDATE(local_state->_ann_topn_post_process_costs,
                   stats.ann_index_topn_result_process_ns + stats.ann_index_topn_engine_convert_ns);
    // Engine-side conversion time inside FAISS wrappers (child under post-process)
    COUNTER_UPDATE(local_state->_ann_topn_engine_convert_costs,
                   stats.ann_index_topn_engine_convert_ns);

    // Doris-side result convert costs (show separately as another child counter); use pure process time
    COUNTER_UPDATE(local_state->_ann_topn_result_convert_costs,
                   stats.ann_index_topn_result_process_ns);

    COUNTER_UPDATE(local_state->_ann_fallback_brute_force_cnt, stats.ann_fall_back_brute_force_cnt);
    COUNTER_UPDATE(local_state->_ann_topn_fallback_by_small_candidate_cnt,
                   stats.ann_topn_fallback_by_small_candidate_cnt);
    COUNTER_UPDATE(local_state->_ann_topn_fallback_small_candidate_rows,
                   stats.ann_topn_fallback_small_candidate_rows);
    COUNTER_UPDATE(local_state->_ann_range_fallback_by_small_candidate_cnt,
                   stats.ann_range_fallback_by_small_candidate_cnt);
    COUNTER_UPDATE(local_state->_ann_range_fallback_small_candidate_rows,
                   stats.ann_range_fallback_small_candidate_rows);

    // Overhead counter removed; precise instrumentation is reported via engine_prepare above.
}

#ifndef NDEBUG
Status OlapScanner::_check_ann_cache_hit_debug_points(const OlapReaderStatistics& stats) {
    DBUG_EXECUTE_IF("olap_scanner.ann_topn_cache_hits", {
        auto expected_hits = dp->param<int32_t>("expected_hits", -1);
        auto min_hits = dp->param<int32_t>("min_hits", -1);
        if (expected_hits >= 0 && stats.ann_index_cache_hits != expected_hits) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "ann_index_cache_hits: {} not equal to expected: {}",
                    stats.ann_index_cache_hits, expected_hits);
        }
        if (min_hits >= 0 && stats.ann_index_cache_hits < min_hits) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "ann_index_cache_hits: {} less than expected min: {}",
                    stats.ann_index_cache_hits, min_hits);
        }
    })
    DBUG_EXECUTE_IF("olap_scanner.ann_range_cache_hits", {
        auto expected_hits = dp->param<int32_t>("expected_hits", -1);
        auto min_hits = dp->param<int32_t>("min_hits", -1);
        if (expected_hits >= 0 && stats.ann_index_range_cache_hits != expected_hits) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "ann_index_range_cache_hits: {} not equal to expected: {}",
                    stats.ann_index_range_cache_hits, expected_hits);
        }
        if (min_hits >= 0 && stats.ann_index_range_cache_hits < min_hits) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "ann_index_range_cache_hits: {} less than expected min: {}",
                    stats.ann_index_range_cache_hits, min_hits);
        }
    })
    return Status::OK();
}
#endif

#include "common/compile_check_avoid_end.h"
} // namespace doris
