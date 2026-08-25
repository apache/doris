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

#include "exec/rowid_fetcher.h"

#include <fmt/format.h>
#include <gen_cpp/data.pb.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/signal_handler.h"
#include "core/block/block.h" // Block
#include "core/column/column.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type_serde/data_type_serde.h"
#include "exec/scan/file_scanner.h"
#include "format/orc/vorc_reader.h"
#include "format/parquet/vparquet_reader.h"
#include "io/io_common.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"      // ExecEnv
#include "runtime/fragment_mgr.h"  // FragmentMgr
#include "runtime/runtime_state.h" // RuntimeState
#include "runtime/workload_group/workload_group_manager.h"
#include "semaphore"
#include "storage/olap_common.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/segment/column_reader.h"
#include "storage/tablet/tablet_fwd.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"
#include "util/jsonb/serialize.h"

namespace doris {

namespace {

void set_topn_lazy_materialization_file_cache_stats(
        const io::FileCacheStatistics& stats, PTopNLazyMaterializationFileCacheStats* pstats) {
    pstats->set_local_io_count(stats.num_local_io_total);
    pstats->set_local_io_bytes(stats.bytes_read_from_local);
    pstats->set_remote_io_count(stats.num_remote_io_total);
    pstats->set_remote_io_bytes(stats.bytes_read_from_remote);
    pstats->set_skip_cache_io_count(stats.num_skip_cache_io_total);
    pstats->set_write_cache_bytes(stats.bytes_write_into_cache);
    pstats->set_local_io_time(stats.local_io_timer);
    pstats->set_remote_io_time(stats.remote_io_timer);
    pstats->set_write_cache_io_time(stats.write_cache_io_timer);
}

} // namespace

struct IteratorKey {
    int64_t tablet_id;
    RowsetId rowset_id;
    uint64_t segment_id;
    int slot_id;

    // unordered map std::equal_to
    bool operator==(const IteratorKey& rhs) const {
        return tablet_id == rhs.tablet_id && rowset_id == rhs.rowset_id &&
               segment_id == rhs.segment_id && slot_id == rhs.slot_id;
    }
};

struct SegKey {
    int64_t tablet_id;
    RowsetId rowset_id;
    uint64_t segment_id;

    // unordered map std::equal_to
    bool operator==(const SegKey& rhs) const {
        return tablet_id == rhs.tablet_id && rowset_id == rhs.rowset_id &&
               segment_id == rhs.segment_id;
    }
};

struct HashOfSegKey {
    size_t operator()(const SegKey& key) const {
        size_t seed = 0;
        seed = HashUtil::hash64(&key.tablet_id, sizeof(key.tablet_id), seed);
        seed = HashUtil::hash64(&key.rowset_id.hi, sizeof(key.rowset_id.hi), seed);
        seed = HashUtil::hash64(&key.rowset_id.mi, sizeof(key.rowset_id.mi), seed);
        seed = HashUtil::hash64(&key.rowset_id.lo, sizeof(key.rowset_id.lo), seed);
        seed = HashUtil::hash64(&key.segment_id, sizeof(key.segment_id), seed);
        return seed;
    }
};

struct HashOfIteratorKey {
    size_t operator()(const IteratorKey& key) const {
        size_t seed = 0;
        seed = HashUtil::hash64(&key.tablet_id, sizeof(key.tablet_id), seed);
        seed = HashUtil::hash64(&key.rowset_id.hi, sizeof(key.rowset_id.hi), seed);
        seed = HashUtil::hash64(&key.rowset_id.mi, sizeof(key.rowset_id.mi), seed);
        seed = HashUtil::hash64(&key.rowset_id.lo, sizeof(key.rowset_id.lo), seed);
        seed = HashUtil::hash64(&key.segment_id, sizeof(key.segment_id), seed);
        seed = HashUtil::hash64(&key.slot_id, sizeof(key.slot_id), seed);
        return seed;
    }
};

struct IteratorItem {
    std::unique_ptr<ColumnIterator> iterator;
    SegmentSharedPtr segment;
    // for holding the reference of storage read options to avoid use after release
    StorageReadOptions storage_read_options;
};

static void set_slot_access_paths(const SlotDescriptor& slot, const TabletSchema& schema,
                                  StorageReadOptions& storage_read_options) {
    int32_t unique_id = slot.col_unique_id();
    const int field_index =
            unique_id >= 0 ? schema.field_index(unique_id) : schema.field_index(slot.col_name());
    if (field_index >= 0) {
        const auto& column = schema.column(field_index);
        unique_id = column.unique_id() >= 0 ? column.unique_id() : column.parent_unique_id();
    }
    if (unique_id < 0) {
        return;
    }

    if (!slot.all_access_paths().empty()) {
        storage_read_options.all_access_paths[unique_id] = slot.all_access_paths();
    }

    if (!slot.predicate_access_paths().empty()) {
        storage_read_options.predicate_access_paths[unique_id] = slot.predicate_access_paths();
    }
}

struct SegItem {
    BaseTabletSPtr tablet;
    BetaRowsetSharedPtr rowset;
    // for holding the reference of segment to avoid use after release
    SegmentSharedPtr segment;
};

// Groups all row_ids belonging to the same segment for batched reading.
// Position index tracks where each row_id originated in the original request,
// so results can be scattered back to the correct output positions.
struct DorisFormatReadBatch {
    std::shared_ptr<FileMapping> file_mapping;
    // (row_id, index_in_request) pairs for all rows in this segment.
    std::vector<std::pair<segment_v2::rowid_t, size_t>> row_ids_with_positions;
};

static void scatter_scan_blocks_to_result_block(
        const std::vector<std::pair<size_t, size_t>>& row_id_block_idx,
        const std::vector<Block>& scan_blocks, Block& result_block) {
    for (size_t column_id = 0; column_id < result_block.columns(); ++column_id) {
        auto dst_col_guard = result_block.mutate_column_scoped(column_id);
        MutableColumnPtr& dst_col = dst_col_guard.mutable_column();

        std::vector<const IColumn*> scan_src_columns;
        scan_src_columns.reserve(row_id_block_idx.size());
        std::vector<size_t> scan_positions;
        scan_positions.reserve(row_id_block_idx.size());
        for (const auto& [pos_block, block_idx] : row_id_block_idx) {
            DCHECK(scan_blocks.size() > pos_block);
            DCHECK(scan_blocks[pos_block].columns() > column_id);
            scan_src_columns.emplace_back(
                    scan_blocks[pos_block].get_by_position(column_id).column.get());
            scan_positions.emplace_back(block_idx);
        }
        dst_col->insert_from_multi_column(scan_src_columns, scan_positions);
    }
}

Status RowIdStorageReader::read_by_rowids(const PMultiGetRequestV2& request,
                                          PMultiGetResponseV2* response) {
    if (request.request_block_descs_size()) {
        auto tquery_id = ((UniqueId)request.query_id()).to_thrift();
        // todo: use mutableBlock instead of block
        std::vector<Block> result_blocks(request.request_block_descs_size());

        OlapReaderStatistics stats;
        int64_t acquire_tablet_ms = 0;
        int64_t acquire_rowsets_ms = 0;
        int64_t acquire_segments_ms = 0;
        int64_t lookup_row_data_ms = 0;

        int64_t external_init_reader_avg_ms = 0;
        int64_t external_get_block_avg_ms = 0;
        size_t external_scan_range_cnt = 0;

        const auto file_cache_miss_policy =
                request.file_cache_remote_only_on_miss()
                        ? io::FileCacheMissPolicy::REMOTE_ONLY_ON_MISS
                        : io::FileCacheMissPolicy::READ_THROUGH_AND_WRITE_BACK;

        // Add counters for different file mapping types
        std::unordered_map<FileMappingType, int64_t> file_type_counts;

        auto id_file_map =
                ExecEnv::GetInstance()->get_id_manager()->get_id_file_map(request.query_id());
        // if id_file_map is null, means the BE not have scan range, just return ok
        if (!id_file_map) {
            // padding empty block to response
            LOG(INFO) << "id_file_map not found for query_id: " << print_id(request.query_id());
            for (int i = 0; i < request.request_block_descs_size(); ++i) {
                response->add_blocks();
            }
            return Status::OK();
        }

        for (int i = 0; i < request.request_block_descs_size(); ++i) {
            const auto& request_block_desc = request.request_block_descs(i);
            PMultiGetBlockV2* pblock = response->add_blocks();
            if (request_block_desc.row_id_size() >= 1) {
                // Since this block belongs to the same table, we only need to take the first type for judgment.
                auto first_file_id = request_block_desc.file_id(0);
                auto first_file_mapping = id_file_map->get_file_mapping(first_file_id);
                if (!first_file_mapping) {
                    return Status::InternalError(
                            "Backend:{} file_mapping not found, query_id: {}, file_id: {}",
                            BackendOptions::get_localhost(), print_id(request.query_id()),
                            first_file_id);
                }
                file_type_counts[first_file_mapping->type] += request_block_desc.row_id_size();

                // prepare slots to build block
                std::vector<SlotDescriptor> slots;
                slots.reserve(request_block_desc.slots_size());
                for (const auto& pslot : request_block_desc.slots()) {
                    slots.push_back(SlotDescriptor(pslot));
                }
                try {
                    if (first_file_mapping->type == FileMappingType::INTERNAL) {
                        RETURN_IF_ERROR(read_batch_doris_format_row(
                                request_block_desc, id_file_map, slots, tquery_id, result_blocks[i],
                                stats, &acquire_tablet_ms, &acquire_rowsets_ms,
                                &acquire_segments_ms, &lookup_row_data_ms, file_cache_miss_policy));
                    } else {
                        RETURN_IF_ERROR(read_batch_external_row(
                                request.wg_id(), request_block_desc, id_file_map, slots,
                                first_file_mapping, tquery_id, result_blocks[i],
                                pblock->mutable_profile(), &external_init_reader_avg_ms,
                                &external_get_block_avg_ms, &external_scan_range_cnt));
                    }
                } catch (const Exception& e) {
                    return Status::Error<false>(e.code(), "Row id fetch failed because {}",
                                                e.what());
                }
            }

            [[maybe_unused]] size_t compressed_size = 0;
            [[maybe_unused]] size_t uncompressed_size = 0;
            [[maybe_unused]] int64_t compress_time = 0;
            int be_exec_version = request.has_be_exec_version() ? request.be_exec_version() : 0;
            RETURN_IF_ERROR(result_blocks[i].serialize(
                    be_exec_version, pblock->mutable_block(), &uncompressed_size, &compressed_size,
                    &compress_time, segment_v2::CompressionTypePB::LZ4));
        }

        // Build file type statistics string
        std::string file_type_stats;
        for (const auto& [type, count] : file_type_counts) {
            if (!file_type_stats.empty()) {
                file_type_stats += ", ";
            }
            file_type_stats += fmt::format("{}:{}", type, count);
        }

        LOG(INFO) << "Query stats: "
                  << fmt::format(
                             "query_id:{}, "
                             "Internal table:"
                             "hit_cached_pages:{}, total_pages_read:{}, compressed_bytes_read:{}, "
                             "io_latency:{}ns, uncompressed_bytes_read:{}, bytes_read:{}, "
                             "acquire_tablet_ms:{}, acquire_rowsets_ms:{}, acquire_segments_ms:{}, "
                             "lookup_row_data_ms:{}, file_types:[{}]; "
                             "External table : init_reader_ms:{}, get_block_ms:{}, "
                             "external_scan_range_cnt:{}",
                             print_id(request.query_id()), stats.cached_pages_num,
                             stats.total_pages_num, stats.compressed_bytes_read, stats.io_ns,
                             stats.uncompressed_bytes_read, stats.bytes_read, acquire_tablet_ms,
                             acquire_rowsets_ms, acquire_segments_ms, lookup_row_data_ms,
                             file_type_stats, external_init_reader_avg_ms,
                             external_get_block_avg_ms, external_scan_range_cnt);
        set_topn_lazy_materialization_file_cache_stats(
                stats.file_cache_stats,
                response->mutable_topn_lazy_materialization_file_cache_stats());
    }

    return Status::OK();
}

Status RowIdStorageReader::read_batch_doris_format_row(
        const PRequestBlockDesc& request_block_desc, std::shared_ptr<IdFileMap> id_file_map,
        std::vector<SlotDescriptor>& slots, const TUniqueId& query_id, Block& result_block,
        OlapReaderStatistics& stats, int64_t* acquire_tablet_ms, int64_t* acquire_rowsets_ms,
        int64_t* acquire_segments_ms, int64_t* lookup_row_data_ms,
        io::FileCacheMissPolicy file_cache_miss_policy) {
    if (result_block.is_empty_column()) [[likely]] {
        result_block = Block(slots, request_block_desc.row_id_size());
    }
    TabletSchema full_read_schema;
    for (const ColumnPB& column_pb : request_block_desc.column_descs()) {
        full_read_schema.append_column(TabletColumn(column_pb));
    }

    std::unordered_map<IteratorKey, IteratorItem, HashOfIteratorKey> iterator_map;
    std::unordered_map<SegKey, SegItem, HashOfSegKey> seg_map;
    std::string row_store_buffer;
    RowStoreReadStruct row_store_read_struct(row_store_buffer);
    if (request_block_desc.fetch_row_store()) {
        for (int i = 0; i < request_block_desc.slots_size(); ++i) {
            row_store_read_struct.serdes.emplace_back(slots[i].get_data_type_ptr()->get_serde());
            row_store_read_struct.col_uid_to_idx[slots[i].col_unique_id()] = i;
            row_store_read_struct.default_values.emplace_back(slots[i].col_default_value());
        }
    }

    // Phase 1: Group all row_ids by their (tablet_id, rowset_id, segment_id) key.
    // Unlike the old code which only batched adjacent rows with the same file_id,
    // this merges non-contiguous same-segment requests into a single batch,
    // maximizing the number of rows read per seek_and_read_by_rowid call.
    std::vector<DorisFormatReadBatch> scan_batches;
    std::unordered_map<SegKey, size_t, HashOfSegKey> batch_idx_by_seg;
    // (batch_idx, position_in_batch) for each row in the original request.
    std::vector<std::pair<size_t, size_t>> row_id_block_idx(request_block_desc.row_id_size());
    for (int j = 0; j < request_block_desc.row_id_size(); ++j) {
        auto file_id = request_block_desc.file_id(j);
        auto file_mapping = id_file_map->get_file_mapping(file_id);
        if (!file_mapping) {
            return Status::InternalError(
                    "Backend:{} file_mapping not found, query_id: {}, file_id: {}",
                    BackendOptions::get_localhost(), print_id(query_id), file_id);
        }

        // Derive segment key and group by it — rows from the same segment are batched together
        // even if they are interleaved with rows from other segments in the request.
        auto [tablet_id, rowset_id, segment_id] = file_mapping->get_doris_format_info();
        SegKey seg_key {.tablet_id = tablet_id, .rowset_id = rowset_id, .segment_id = segment_id};
        auto [it, inserted] = batch_idx_by_seg.emplace(seg_key, scan_batches.size());
        if (inserted) {
            // First time seeing this segment, create a new batch for it.
            scan_batches.emplace_back();
            scan_batches.back().file_mapping = file_mapping;
        }
        // Record (row_id, original_request_index) for later sorting and scattering.
        scan_batches[it->second].row_ids_with_positions.emplace_back(request_block_desc.row_id(j),
                                                                     j);
    }

    // Phase 2: For each segment, sort row_ids ascending (required by ColumnIterator),
    // deduplicate, then read all rows in a single batch call.
    std::vector<Block> scan_blocks(scan_batches.size());
    for (size_t batch_idx = 0; batch_idx < scan_batches.size(); ++batch_idx) {
        auto& scan_batch = scan_batches[batch_idx];
        auto& row_ids_with_positions = scan_batch.row_ids_with_positions;
        std::sort(row_ids_with_positions.begin(), row_ids_with_positions.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

        // Column iterators read rowids monotonically. Deduplicate consecutive identical row_ids
        // (different file_ids may map to the same row), then scatter rows back to their original
        // request positions.
        std::vector<uint32_t> row_ids;
        row_ids.reserve(row_ids_with_positions.size());

        // Also builds the scatter map: row_id_block_idx[original_request_idx] ->
        // (batch_idx, deduplicated_position_in_batch).
        for (const auto& [row_id, result_idx] : row_ids_with_positions) {
            if (row_ids.empty() || row_ids.back() != row_id) {
                row_ids.emplace_back(row_id);
            }
            row_id_block_idx[result_idx] = std::make_pair(batch_idx, row_ids.size() - 1);
        }

        scan_blocks[batch_idx] = Block(slots, row_ids.size());
        RETURN_IF_ERROR(read_doris_format_row(
                id_file_map, scan_batch.file_mapping, row_ids, slots, full_read_schema,
                row_store_read_struct, stats, acquire_tablet_ms, acquire_rowsets_ms,
                acquire_segments_ms, lookup_row_data_ms, seg_map, iterator_map,
                file_cache_miss_policy, scan_blocks[batch_idx]));
    }

    scatter_scan_blocks_to_result_block(row_id_block_idx, scan_blocks, result_block);

    return Status::OK();
}

const std::string RowIdStorageReader::ScannersRunningTimeProfile = "ScannersRunningTime";
const std::string RowIdStorageReader::InitReaderAvgTimeProfile = "InitReaderAvgTime";
const std::string RowIdStorageReader::GetBlockAvgTimeProfile = "GetBlockAvgTime";
const std::string RowIdStorageReader::FileReadLinesProfile = "FileReadLines";
const std::string RowIdStorageReader::TopNLazyMaterializationSecondPhaseLocalIOCount =
        "TopNLazyMaterializationSecondPhaseLocalIOCount";
const std::string RowIdStorageReader::TopNLazyMaterializationSecondPhaseLocalIOBytes =
        "TopNLazyMaterializationSecondPhaseLocalIOBytes";
const std::string RowIdStorageReader::TopNLazyMaterializationSecondPhaseRemoteIOCount =
        "TopNLazyMaterializationSecondPhaseRemoteIOCount";
const std::string RowIdStorageReader::TopNLazyMaterializationSecondPhaseRemoteIOBytes =
        "TopNLazyMaterializationSecondPhaseRemoteIOBytes";
const std::string RowIdStorageReader::TopNLazyMaterializationSecondPhaseSkipCacheIOCount =
        "TopNLazyMaterializationSecondPhaseSkipCacheIOCount";
const std::string RowIdStorageReader::TopNLazyMaterializationSecondPhaseWriteCacheBytes =
        "TopNLazyMaterializationSecondPhaseWriteCacheBytes";
const std::string RowIdStorageReader::TopNLazyMaterializationSecondPhaseLocalIOTime =
        "TopNLazyMaterializationSecondPhaseLocalIOTime";
const std::string RowIdStorageReader::TopNLazyMaterializationSecondPhaseRemoteIOTime =
        "TopNLazyMaterializationSecondPhaseRemoteIOTime";
const std::string RowIdStorageReader::TopNLazyMaterializationSecondPhaseWriteCacheIOTime =
        "TopNLazyMaterializationSecondPhaseWriteCacheIOTime";
const std::string RowIdStorageReader::TopNLazyMaterializationSecondPhaseRowsRead =
        "TopNLazyMaterializationSecondPhaseRowsRead";
const std::string RowIdStorageReader::TopNLazyMaterializationSecondPhaseSegmentsRead =
        "TopNLazyMaterializationSecondPhaseSegmentsRead";

Status RowIdStorageReader::read_external_row_from_file_mapping(
        size_t idx, const std::multimap<segment_v2::rowid_t, size_t>& row_ids,
        const std::shared_ptr<FileMapping>& file_mapping,
        const std::vector<SlotDescriptor>& scan_slots, const TUniqueId& query_id,
        const std::shared_ptr<RuntimeState>& runtime_state, std::vector<Block>& scan_blocks,
        std::vector<std::pair<size_t, size_t>>& row_id_block_idx,
        std::vector<RowIdStorageReader::ExternalFetchStatistics>& fetch_statistics,
        const TFileScanRangeParams& rpc_scan_params,
        const std::unordered_map<std::string, int>& colname_to_slot_id,
        std::counting_semaphore<>& semaphore, TupleDescriptor& tuple_desc) {
    SCOPED_ATTACH_TASK(ExecEnv::GetInstance()->rowid_storage_reader_tracker());
    signal::set_signal_task_id(query_id);

    // Release the concurrency permit on every exit path (including error returns
    // and exceptions). Completion accounting and status publishing are owned by
    // the caller, so the status is always published before the waiter is woken.
    Defer defer([&] { semaphore.release(); });

    std::list<int64_t> read_ids;
    //Generate an ordered list with the help of the orderliness of the map.
    for (const auto& [row_id, result_block_idx] : row_ids) {
        if (read_ids.empty() || read_ids.back() != row_id) {
            read_ids.emplace_back(row_id);
        }
        row_id_block_idx[result_block_idx] = std::make_pair(idx, read_ids.size() - 1);
    }

    scan_blocks[idx] = Block(scan_slots, read_ids.size());

    auto& external_info = file_mapping->get_external_file_info();
    auto& scan_range_desc = external_info.scan_range_desc;

    // Clear to avoid reading iceberg position delete file...
    scan_range_desc.table_format_params.iceberg_params = TIcebergFileDesc {};

    // Clear to avoid reading hive transactional delete delta file...
    scan_range_desc.table_format_params.transactional_hive_params = TTransactionalHiveDesc {};

    std::unique_ptr<RuntimeProfile> sub_runtime_profile =
            std::make_unique<RuntimeProfile>("ExternalRowIDFetcher");
    {
        std::unique_ptr<FileScanner> vfile_scanner_ptr =
                FileScanner::create_unique(runtime_state.get(), sub_runtime_profile.get(),
                                           &rpc_scan_params, &colname_to_slot_id, &tuple_desc);

        RETURN_IF_ERROR(vfile_scanner_ptr->prepare_for_read_lines(scan_range_desc));
        RETURN_IF_ERROR(vfile_scanner_ptr->read_lines_from_range(
                scan_range_desc, read_ids, &scan_blocks[idx], external_info,
                &fetch_statistics[idx].init_reader_ms, &fetch_statistics[idx].get_block_ms));
    }

    if (scan_blocks[idx].rows() != read_ids.size()) {
        return Status::InternalError(
                "Row id fetch scan row count mismatch, "
                "query_id={}, path={}, expected_rows={}, actual_rows={}",
                print_id(query_id), scan_range_desc.path, read_ids.size(), scan_blocks[idx].rows());
    }
    for (size_t column_id = 0; column_id < scan_blocks[idx].columns(); ++column_id) {
        const auto& column = scan_blocks[idx].get_by_position(column_id);
        if (column.column->size() != read_ids.size()) {
            return Status::InternalError(
                    "Row id fetch scan column row count mismatch, "
                    "query_id={}, path={}, column={}, expected_rows={}, actual_rows={}",
                    print_id(query_id), scan_range_desc.path, column.name, read_ids.size(),
                    column.column->size());
        }
    }

    auto file_read_bytes_counter =
            sub_runtime_profile->get_counter(FileScanner::FileReadBytesProfile);

    if (file_read_bytes_counter != nullptr) {
        fetch_statistics[idx].file_read_bytes = PrettyPrinter::print(
                file_read_bytes_counter->value(), file_read_bytes_counter->type());
    }

    auto file_read_times_counter =
            sub_runtime_profile->get_counter(FileScanner::FileReadTimeProfile);
    if (file_read_times_counter != nullptr) {
        fetch_statistics[idx].file_read_times = PrettyPrinter::print(
                file_read_times_counter->value(), file_read_times_counter->type());
    }

    return Status::OK();
}

std::string RowIdStorageReader::source_column_key(const SlotDescriptor& slot, uint32_t column_idx) {
    fmt::memory_buffer key;
    // Length-prefix each component so distinct sequences cannot alias, e.g.
    // paths ["a", "b"] -> "1:a1:b" while ["a:b"] -> "3:a:b".
    auto append = [&key](std::string_view component) {
        fmt::format_to(key, "{}:", component.size());
        key.append(component.data(), component.data() + component.size());
    };
    append(slot.col_name());
    append(std::to_string(column_idx));
    append(std::to_string(slot.col_unique_id()));
    append(std::to_string(slot.column_paths().size()));
    for (const auto& path : slot.column_paths()) {
        append(path);
    }
    append(std::to_string(slot.all_access_paths().size()));
    // Encode each optional sub-path's presence bit separately from its element
    // count so an absent path ("0") never aliases a present-but-empty path
    // ("1" + size "0").
    auto append_optional_path = [&append](bool is_set, const std::vector<std::string>& items) {
        append(is_set ? "1" : "0");
        if (is_set) {
            append(std::to_string(items.size()));
            for (const auto& item : items) {
                append(item);
            }
        }
    };
    for (const auto& path : slot.all_access_paths()) {
        append(fmt::format("{}", path.type));
        append_optional_path(path.__isset.data_access_path, path.data_access_path.path);
        append_optional_path(path.__isset.meta_access_path, path.meta_access_path.path);
    }
    return fmt::to_string(key);
}

Status RowIdStorageReader::submit_external_scan_tasks(
        ScannerScheduler* scheduler, std::counting_semaphore<>& semaphore, size_t task_count,
        const std::function<std::string(size_t)>& make_task_id,
        const std::function<Status(size_t)>& run_task) {
    // `completed_count` is a plain counter guarded by `mtx`; the same mutex guards
    // the wait predicate below, so a worker can never notify between the waiter's
    // predicate check and its wait.
    AtomicStatus scan_status;
    std::condition_variable cv;
    std::mutex mtx;
    size_t completed_count = 0;

    // Only tasks the scheduler actually accepted are waited for. If a submission
    // fails we stop submitting, but still wait for the already-accepted tasks so
    // their workers cannot outlive the locals they capture by reference.
    size_t submitted_count = 0;
    for (size_t idx = 0; idx < task_count; ++idx) {
        semaphore.acquire();
        auto run_one_task = [&, idx]() -> bool {
            Status task_status = Status::OK();
            // Publish the status before the completion signal wakes the waiter, on every
            // path. A scanner that throws would otherwise leave scan_status OK while this
            // Defer still counts the task as finished, and the caller would report success
            // over a half-filled result block.
            Defer complete([&] {
                scan_status.update(task_status);
                std::lock_guard<std::mutex> lock(mtx);
                ++completed_count;
                cv.notify_one();
            });
            ASSIGN_STATUS_IF_CATCH_EXCEPTION(task_status = run_task(idx), task_status);
            return true;
        };
        Status submit_st = scheduler->submit_scan_task(
                SimplifiedScanTask(run_one_task, nullptr, nullptr), make_task_id(idx));
        if (!submit_st.ok()) {
            scan_status.update(submit_st);
            semaphore.release();
            break;
        }
        ++submitted_count;
    }

    {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [&] { return completed_count == submitted_count; });
    }
    return scan_status.ok() ? Status::OK() : scan_status.status();
}

Status RowIdStorageReader::read_batch_external_row(
        const uint64_t workload_group_id, const PRequestBlockDesc& request_block_desc,
        std::shared_ptr<IdFileMap> id_file_map, std::vector<SlotDescriptor>& slots,
        std::shared_ptr<FileMapping> first_file_mapping, const TUniqueId& query_id,
        Block& result_block, PRuntimeProfileTree* pprofile, int64_t* init_reader_avg_ms,
        int64_t* get_block_avg_ms, size_t* scan_range_cnt) {
    TFileScanRangeParams rpc_scan_params;
    TupleDescriptor tuple_desc(request_block_desc.desc(), false);
    std::unordered_map<std::string, int> colname_to_slot_id;
    std::shared_ptr<RuntimeState> runtime_state = nullptr;
    std::vector<SlotDescriptor> scan_slots;
    std::vector<size_t> result_column_to_scan_column;
    std::vector<uint32_t> scan_column_idxs;

    int max_file_scanners = 0;
    {
        if (result_block.is_empty_column()) [[likely]] {
            result_block = Block(slots, request_block_desc.row_id_size());
        }
        if (request_block_desc.column_idxs_size() != slots.size()) {
            return Status::InternalError(
                    "Row id fetch request has mismatched slots and column indexes, "
                    "query_id={}, slots={}, column_idxs={}",
                    print_id(query_id), slots.size(), request_block_desc.column_idxs_size());
        }

        auto& external_info = first_file_mapping->get_external_file_info();
        int plan_node_id = external_info.plan_node_id;
        const auto& first_scan_range_desc = external_info.scan_range_desc;

        DCHECK(id_file_map->get_external_scan_params().contains(plan_node_id));
        const auto* old_scan_params = &(id_file_map->get_external_scan_params().at(plan_node_id));
        rpc_scan_params = *old_scan_params;

        rpc_scan_params.required_slots.clear();
        rpc_scan_params.column_idxs.clear();
        rpc_scan_params.slot_name_to_schema_pos.clear();

        std::set partition_name_set(first_scan_range_desc.columns_from_path_keys.begin(),
                                    first_scan_range_desc.columns_from_path_keys.end());

        std::unordered_map<std::string, size_t> source_column_to_scan_idx;

        result_column_to_scan_column.reserve(slots.size());
        scan_slots.reserve(slots.size());
        scan_column_idxs.reserve(slots.size());
        for (auto slot_idx = 0; slot_idx < slots.size(); ++slot_idx) {
            const auto& slot = slots[slot_idx];
            const auto column_idx = request_block_desc.column_idxs(slot_idx);
            const auto key = source_column_key(slot, column_idx);
            auto [it, inserted] =
                    source_column_to_scan_idx.emplace(key, source_column_to_scan_idx.size());
            result_column_to_scan_column.emplace_back(it->second);
            if (inserted) {
                scan_slots.emplace_back(slot);
                scan_column_idxs.emplace_back(column_idx);
            }
        }

        for (auto slot_idx = 0; slot_idx < scan_slots.size(); ++slot_idx) {
            auto& slot = scan_slots[slot_idx];
            tuple_desc.add_slot(&slot);
            colname_to_slot_id[slot.col_name()] = slot.id();
            TFileScanSlotInfo slot_info;
            slot_info.slot_id = slot.id();
            auto column_idx = scan_column_idxs[slot_idx];
            if (partition_name_set.contains(slot.col_name())) {
                //This is partition column.
                slot_info.is_file_slot = false;
            } else {
                rpc_scan_params.column_idxs.emplace_back(column_idx);
                slot_info.is_file_slot = true;
            }
            rpc_scan_params.default_value_of_src_slot.emplace(slot.id(), TExpr {});
            rpc_scan_params.required_slots.emplace_back(slot_info);
            rpc_scan_params.slot_name_to_schema_pos.emplace(slot.col_name(), column_idx);
        }

        const auto& query_options = id_file_map->get_query_options();
        const auto& query_globals = id_file_map->get_query_globals();
        /*
         * The scan stage needs the information in query_options to generate different behaviors according to the specific variables:
         *  query_options.hive_parquet_use_column_names, query_options.truncate_char_or_varchar_columns,query_globals.time_zone ...
         *
         * To ensure the same behavior as the scan stage, I get query_options query_globals from id_file_map, then create runtime_state
         * and pass it to vfile_scanner so that the runtime_state information is the same as the scan stage and the behavior is also consistent.
         */
        runtime_state = RuntimeState::create_shared(
                query_id, -1, query_options, query_globals, ExecEnv::GetInstance(),
                ExecEnv::GetInstance()->rowid_storage_reader_tracker());

        max_file_scanners = id_file_map->get_max_file_scanners();
    }

    // Hash(TFileRangeDesc) => { all the rows that need to be read and their positions in the result block. } +  file mapping
    // std::multimap<segment_v2::rowid_t, size_t> : The reason for using multimap is: may need the same row of data multiple times.
    std::map<std::string,
             std::pair<std::multimap<segment_v2::rowid_t, size_t>, std::shared_ptr<FileMapping>>>
            scan_rows;

    // Block corresponding to the order of `scan_rows` map.
    std::vector<Block> scan_blocks;

    // row_id (Indexing of vectors) => < In which block, which line in the block >
    std::vector<std::pair<size_t, size_t>> row_id_block_idx;

    // Count the time/bytes it takes to read each TFileRangeDesc. (for profile)
    std::vector<ExternalFetchStatistics> fetch_statistics;

    auto hash_file_range = [](const TFileRangeDesc& file_range_desc) {
        std::string value;
        value.resize(file_range_desc.path.size() + sizeof(file_range_desc.start_offset));
        auto* ptr = value.data();

        memcpy(ptr, &file_range_desc.start_offset, sizeof(file_range_desc.start_offset));
        ptr += sizeof(file_range_desc.start_offset);
        memcpy(ptr, file_range_desc.path.data(), file_range_desc.path.size());
        return value;
    };

    for (int j = 0; j < request_block_desc.row_id_size(); ++j) {
        auto file_id = request_block_desc.file_id(j);
        auto file_mapping = id_file_map->get_file_mapping(file_id);
        if (!file_mapping) {
            return Status::InternalError(
                    "Backend:{} file_mapping not found, query_id: {}, file_id: {}",
                    BackendOptions::get_localhost(), print_id(query_id), file_id);
        }

        const auto& external_info = file_mapping->get_external_file_info();
        const auto& scan_range_desc = external_info.scan_range_desc;

        auto scan_range_hash = hash_file_range(scan_range_desc);
        if (scan_rows.contains(scan_range_hash)) {
            scan_rows.at(scan_range_hash).first.emplace(request_block_desc.row_id(j), j);
        } else {
            std::multimap<segment_v2::rowid_t, size_t> tmp {{request_block_desc.row_id(j), j}};
            scan_rows.emplace(scan_range_hash, std::make_pair(tmp, file_mapping));
        }
    }

    scan_blocks.resize(scan_rows.size());
    row_id_block_idx.resize(request_block_desc.row_id_size());
    fetch_statistics.resize(scan_rows.size());

    // Get the workload group for subsequent scan task submission.
    std::vector<uint64_t> workload_group_ids;
    workload_group_ids.emplace_back(workload_group_id);
    auto wg = ExecEnv::GetInstance()->workload_group_mgr()->get_group(workload_group_ids);
    doris::TaskScheduler* exec_sched = nullptr;
    ScannerScheduler* scan_sched = nullptr;
    ScannerScheduler* remote_scan_sched = nullptr;
    wg->get_query_scheduler(&exec_sched, &scan_sched, &remote_scan_sched);
    DCHECK(remote_scan_sched);

    int64_t scan_running_time = 0;
    RETURN_IF_ERROR(scope_timer_run(
            [&]() -> Status {
                //semaphore: Limit the number of scan tasks submitted at one time
                std::counting_semaphore semaphore {max_file_scanners};

                std::vector<std::pair<std::multimap<segment_v2::rowid_t, size_t>,
                                      std::shared_ptr<FileMapping>>>
                        scan_info_list;
                scan_info_list.reserve(scan_rows.size());
                for (const auto& [_, scan_info] : scan_rows) {
                    scan_info_list.emplace_back(scan_info);
                }

                return submit_external_scan_tasks(
                        remote_scan_sched, semaphore, scan_rows.size(),
                        [&](size_t idx) {
                            return fmt::format("{}-read_batch_external_row-{}", print_id(query_id),
                                               idx);
                        },
                        [&](size_t idx) -> Status {
                            const auto& [row_ids, file_mapping] = scan_info_list[idx];
                            return read_external_row_from_file_mapping(
                                    idx, row_ids, file_mapping, scan_slots, query_id, runtime_state,
                                    scan_blocks, row_id_block_idx, fetch_statistics,
                                    rpc_scan_params, colname_to_slot_id, semaphore, tuple_desc);
                        });
            },
            &scan_running_time));

    // Insert the read data into result_block. Use insert_indices_from() instead of
    // scatter_scan_blocks_to_result_block()/insert_from_multi_column(), because
    // scan_blocks may have fewer columns than result_block when duplicate physical columns
    // are deduplicated, and insert_from_multi_column() cannot handle ColumnString
    // cross-type (32/64) copies safely.
    const size_t result_column_count = result_block.columns();
    for (size_t column_id = 0; column_id < result_column_count; column_id++) {
        auto dst_col_guard = result_block.mutate_column_scoped(column_id);
        MutableColumnPtr& dst_col = dst_col_guard.mutable_column();

        bool dst_is_nullable = dst_col->is_nullable();
        std::vector<ColumnPtr> nullable_src_columns(scan_blocks.size());
        auto scan_column_id = result_column_to_scan_column[column_id];
        for (const auto& [pos_block, block_idx] : row_id_block_idx) {
            DCHECK_GT(scan_blocks.size(), pos_block);
            DCHECK_GT(scan_blocks[pos_block].columns(), scan_column_id);
            const auto& src_column_ptr =
                    scan_blocks[pos_block].get_by_position(scan_column_id).column;
            const auto* src_col = src_column_ptr.get();
            if (dst_is_nullable && !src_col->is_nullable()) {
                if (!nullable_src_columns[pos_block]) {
                    nullable_src_columns[pos_block] = make_nullable(src_column_ptr);
                }
                src_col = nullable_src_columns[pos_block].get();
            }
            if (block_idx >= src_col->size()) {
                return Status::InternalError(
                        "Row id fetch source index out of range, query_id={}, column={}, "
                        "source_block={}, source_rows={}, row_index={}",
                        print_id(query_id), result_block.get_by_position(column_id).name, pos_block,
                        src_col->size(), block_idx);
            }
            uint32_t scan_position = cast_set<uint32_t>(block_idx);
            dst_col->insert_indices_from(*src_col, &scan_position, &scan_position + 1);
        }
    }

    // Statistical runtime profile information.
    std::unique_ptr<RuntimeProfile> runtime_profile =
            std::make_unique<RuntimeProfile>("ExternalRowIDFetcher");
    {
        runtime_profile->add_info_string(ScannersRunningTimeProfile,
                                         std::to_string(scan_running_time) + "ms");
        fmt::memory_buffer file_read_lines_buffer;
        format_to(file_read_lines_buffer, "[");
        fmt::memory_buffer file_read_bytes_buffer;
        format_to(file_read_bytes_buffer, "[");
        fmt::memory_buffer file_read_times_buffer;
        format_to(file_read_times_buffer, "[");

        size_t idx = 0;
        for (const auto& [_, scan_info] : scan_rows) {
            format_to(file_read_lines_buffer, "{}, ", scan_info.first.size());
            *init_reader_avg_ms = fetch_statistics[idx].init_reader_ms;
            *get_block_avg_ms += fetch_statistics[idx].get_block_ms;
            format_to(file_read_bytes_buffer, "{}, ", fetch_statistics[idx].file_read_bytes);
            format_to(file_read_times_buffer, "{}, ", fetch_statistics[idx].file_read_times);
            idx++;
        }

        format_to(file_read_lines_buffer, "]");
        format_to(file_read_bytes_buffer, "]");
        format_to(file_read_times_buffer, "]");

        *init_reader_avg_ms /= fetch_statistics.size();
        *get_block_avg_ms /= fetch_statistics.size();
        runtime_profile->add_info_string(InitReaderAvgTimeProfile,
                                         std::to_string(*init_reader_avg_ms) + "ms");
        runtime_profile->add_info_string(GetBlockAvgTimeProfile,
                                         std::to_string(*init_reader_avg_ms) + "ms");
        runtime_profile->add_info_string(FileReadLinesProfile,
                                         fmt::to_string(file_read_lines_buffer));
        runtime_profile->add_info_string(FileScanner::FileReadBytesProfile,
                                         fmt::to_string(file_read_bytes_buffer));
        runtime_profile->add_info_string(FileScanner::FileReadTimeProfile,
                                         fmt::to_string(file_read_times_buffer));
    }

    runtime_profile->to_proto(pprofile, 2);

    *scan_range_cnt = scan_rows.size();

    return Status::OK();
}

Status RowIdStorageReader::read_doris_format_row(
        const std::shared_ptr<IdFileMap>& id_file_map,
        const std::shared_ptr<FileMapping>& file_mapping, const std::vector<uint32_t>& row_ids,
        std::vector<SlotDescriptor>& slots, const TabletSchema& full_read_schema,
        RowStoreReadStruct& row_store_read_struct, OlapReaderStatistics& stats,
        int64_t* acquire_tablet_ms, int64_t* acquire_rowsets_ms, int64_t* acquire_segments_ms,
        int64_t* lookup_row_data_ms, std::unordered_map<SegKey, SegItem, HashOfSegKey>& seg_map,
        std::unordered_map<IteratorKey, IteratorItem, HashOfIteratorKey>& iterator_map,
        io::FileCacheMissPolicy file_cache_miss_policy, Block& result_block) {
    auto [tablet_id, rowset_id, segment_id] = file_mapping->get_doris_format_info();
    SegKey seg_key {.tablet_id = tablet_id, .rowset_id = rowset_id, .segment_id = segment_id};

    BaseTabletSPtr tablet;
    BetaRowsetSharedPtr rowset;
    SegmentSharedPtr segment;
    if (seg_map.find(seg_key) == seg_map.end()) {
        tablet = scope_timer_run(
                [&]() {
                    auto res = ExecEnv::get_tablet(tablet_id);
                    return !res.has_value() ? nullptr
                                            : std::dynamic_pointer_cast<BaseTablet>(res.value());
                },
                acquire_tablet_ms);
        if (!tablet) {
            return Status::InternalError(
                    "Backend:{} tablet not found, tablet_id: {}, rowset_id: {}, segment_id: {}, "
                    "row_id: {}",
                    BackendOptions::get_localhost(), tablet_id, rowset_id.to_string(), segment_id,
                    row_ids[0]);
        }

        rowset = std::static_pointer_cast<BetaRowset>(scope_timer_run(
                [&]() { return id_file_map->get_temp_rowset(tablet_id, rowset_id); },
                acquire_rowsets_ms));
        if (!rowset) {
            return Status::InternalError(
                    "Backend:{} rowset_id not found, tablet_id: {}, rowset_id: {}, segment_id: {}, "
                    "row_id: {}",
                    BackendOptions::get_localhost(), tablet_id, rowset_id.to_string(), segment_id,
                    row_ids[0]);
        }

        SegmentCacheHandle segment_cache;
        RETURN_IF_ERROR(scope_timer_run(
                [&]() {
                    return SegmentLoader::instance()->load_segments(rowset, &segment_cache, true);
                },
                acquire_segments_ms));

        auto it = std::find_if(segment_cache.get_segments().cbegin(),
                               segment_cache.get_segments().cend(),
                               [segment_id](const segment_v2::SegmentSharedPtr& seg) {
                                   return seg->id() == segment_id;
                               });
        if (it == segment_cache.get_segments().end()) {
            return Status::InternalError(
                    "Backend:{} segment not found, tablet_id: {}, rowset_id: {}, segment_id: {}, "
                    "row_id: {}",
                    BackendOptions::get_localhost(), tablet_id, rowset_id.to_string(), segment_id,
                    row_ids[0]);
        }
        segment = *it;
        seg_map[seg_key] = SegItem {.tablet = tablet, .rowset = rowset, .segment = segment};
    } else {
        auto& seg_item = seg_map[seg_key];
        tablet = seg_item.tablet;
        rowset = seg_item.rowset;
        segment = seg_item.segment;
    }

    // if row_store_read_struct not empty, means the line we should read from row_store
    if (!row_store_read_struct.default_values.empty()) {
        if (!tablet->tablet_schema()->has_row_store_for_all_columns()) {
            return Status::InternalError("Tablet {} does not have row store for all columns",
                                         tablet->tablet_id());
        }
        auto result_columns_guard = result_block.mutate_columns_scoped();
        MutableColumns& result_columns = result_columns_guard.mutable_columns();
        io::IOContext io_ctx;
        io_ctx.reader_type = ReaderType::READER_QUERY;
        io_ctx.file_cache_stats = &stats.file_cache_stats;
        io_ctx.file_cache_miss_policy = file_cache_miss_policy;
        for (auto row_id : row_ids) {
            RowLocation loc(rowset_id, segment->id(), cast_set<uint32_t>(row_id));
            row_store_read_struct.row_store_buffer.clear();
            RETURN_IF_ERROR(scope_timer_run(
                    [&]() {
                        return tablet->lookup_row_data({}, loc, rowset, stats,
                                                       row_store_read_struct.row_store_buffer,
                                                       false, &io_ctx);
                    },
                    lookup_row_data_ms));

            RETURN_IF_ERROR(JsonbSerializeUtil::jsonb_to_columns(
                    row_store_read_struct.serdes, row_store_read_struct.row_store_buffer.data(),
                    row_store_read_struct.row_store_buffer.size(),
                    row_store_read_struct.col_uid_to_idx, result_columns,
                    row_store_read_struct.default_values, {}));
        }
    } else {
        for (int x = 0; x < slots.size(); ++x) {
            auto column_guard = result_block.mutate_column_scoped(x);
            MutableColumnPtr& column = column_guard.mutable_column();
            IteratorKey iterator_key {.tablet_id = tablet_id,
                                      .rowset_id = rowset_id,
                                      .segment_id = segment_id,
                                      .slot_id = slots[x].id()};
            IteratorItem& iterator_item = iterator_map[iterator_key];
            if (iterator_item.segment == nullptr) {
                iterator_map[iterator_key].segment = segment;
                iterator_item.storage_read_options.stats = &stats;
                iterator_item.storage_read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
                iterator_item.storage_read_options.io_ctx.file_cache_miss_policy =
                        file_cache_miss_policy;
            }
            set_slot_access_paths(slots[x], full_read_schema, iterator_item.storage_read_options);
            RETURN_IF_ERROR(segment->seek_and_read_by_rowid(
                    full_read_schema, &slots[x], row_ids, column,
                    iterator_item.storage_read_options, iterator_item.iterator));
        }
    }
    return Status::OK();
}

} // namespace doris
