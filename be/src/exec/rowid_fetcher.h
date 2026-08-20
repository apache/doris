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

#include <gen_cpp/internal_service.pb.h>

#include <memory>
#include <semaphore>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/data_type/data_type.h"
#include "storage/id_manager.h"

namespace doris {

class DorisNodesInfo;
class RuntimeProfile;
class RuntimeState;
class TupleDescriptor;
namespace io {
enum class FileCacheMissPolicy : uint8_t;
}

struct FileMapping;
struct SegKey;
struct SegItem;
struct HashOfSegKey;
struct IteratorKey;
struct IteratorItem;
struct HashOfIteratorKey;

class MutableBlock;

struct RowStoreReadStruct {
    RowStoreReadStruct(std::string& buffer) : row_store_buffer(buffer) {};
    std::string& row_store_buffer;
    DataTypeSerDeSPtrs serdes;
    std::unordered_map<uint32_t, uint32_t> col_uid_to_idx;
    std::vector<std::string> default_values;
};

class RowIdStorageReader {
public:
    //external profile info key.
    static const std::string ScannersRunningTimeProfile;
    static const std::string InitReaderAvgTimeProfile;
    static const std::string GetBlockAvgTimeProfile;
    static const std::string FileReadLinesProfile;
    static const std::string TopNLazyMaterializationSecondPhaseLocalIOCount;
    static const std::string TopNLazyMaterializationSecondPhaseLocalIOBytes;
    static const std::string TopNLazyMaterializationSecondPhaseRemoteIOCount;
    static const std::string TopNLazyMaterializationSecondPhaseRemoteIOBytes;
    static const std::string TopNLazyMaterializationSecondPhaseSkipCacheIOCount;
    static const std::string TopNLazyMaterializationSecondPhaseWriteCacheBytes;
    static const std::string TopNLazyMaterializationSecondPhaseLocalIOTime;
    static const std::string TopNLazyMaterializationSecondPhaseRemoteIOTime;
    static const std::string TopNLazyMaterializationSecondPhaseWriteCacheIOTime;
    static const std::string TopNLazyMaterializationSecondPhaseRowsRead;
    static const std::string TopNLazyMaterializationSecondPhaseSegmentsRead;

    static Status read_by_rowids(const PMultiGetRequestV2& request, PMultiGetResponseV2* response);

private:
    struct ExternalFetchStatistics;

    static Status read_doris_format_row(
            const std::shared_ptr<IdFileMap>& id_file_map,
            const std::shared_ptr<FileMapping>& file_mapping, const std::vector<uint32_t>& row_id,
            std::vector<SlotDescriptor>& slots, const TabletSchema& full_read_schema,
            RowStoreReadStruct& row_store_read_struct, OlapReaderStatistics& stats,
            int64_t* acquire_tablet_ms, int64_t* acquire_rowsets_ms, int64_t* acquire_segments_ms,
            int64_t* lookup_row_data_ms, std::unordered_map<SegKey, SegItem, HashOfSegKey>& seg_map,
            std::unordered_map<IteratorKey, IteratorItem, HashOfIteratorKey>& iterator_map,
            io::FileCacheMissPolicy file_cache_miss_policy, Block& result_block);

    static Status read_batch_doris_format_row(
            const PRequestBlockDesc& request_block_desc, std::shared_ptr<IdFileMap> id_file_map,
            std::vector<SlotDescriptor>& slots, const TUniqueId& query_id, Block& result_block,
            OlapReaderStatistics& stats, int64_t* acquire_tablet_ms, int64_t* acquire_rowsets_ms,
            int64_t* acquire_segments_ms, int64_t* lookup_row_data_ms,
            io::FileCacheMissPolicy file_cache_miss_policy);

    static Status read_batch_external_row(
            const uint64_t workload_group_id, const PRequestBlockDesc& request_block_desc,
            std::shared_ptr<IdFileMap> id_file_map, std::vector<SlotDescriptor>& slots,
            std::shared_ptr<FileMapping> first_file_mapping, const TUniqueId& query_id,
            Block& result_block, PRuntimeProfileTree* pprofile, int64_t* init_reader_avg_ms,
            int64_t* get_block_avg_ms, size_t* scan_range_cnt);

    static Status read_lance_rows_by_row_ids(const TFileRangeDesc& scan_range_desc,
                                             const std::vector<uint64_t>& row_ids,
                                             const std::vector<SlotDescriptor>& slots,
                                             RuntimeState* runtime_state,
                                             RuntimeProfile* runtime_profile,
                                             const TFileScanRangeParams& scan_params, Block* block,
                                             ExternalFetchStatistics* fetch_statistics);

    static Status read_external_row_from_file_mapping(
            size_t idx, const std::multimap<uint64_t, size_t>& row_ids,
            const std::shared_ptr<FileMapping>& file_mapping,
            const std::vector<SlotDescriptor>& slots, const TUniqueId& query_id,
            const std::shared_ptr<RuntimeState>& runtime_state, std::vector<Block>& scan_blocks,
            std::vector<std::pair<size_t, size_t>>& row_id_block_idx,
            std::vector<ExternalFetchStatistics>& fetch_statistics,
            const TFileScanRangeParams& rpc_scan_params,
            const std::unordered_map<std::string, int>& colname_to_slot_id,
            TupleDescriptor& tuple_desc);

    struct ExternalFetchStatistics {
        int64_t init_reader_ms = 0;
        int64_t get_block_ms = 0;
        std::string file_read_bytes;
        std::string file_read_times;
    };
};

template <typename Func>
auto scope_timer_run(Func fn, int64_t* cost) -> decltype(fn()) {
    MonotonicStopWatch watch;
    watch.start();
    auto res = fn();
    *cost += watch.elapsed_time() / 1000 / 1000;
    return res;
}
} // namespace doris
