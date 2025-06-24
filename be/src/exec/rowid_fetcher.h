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

#include <brpc/controller.h>
#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/internal_service.pb.h>

#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exec/tablet_info.h" // DorisNodesInfo
#include "olap/id_manager.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"

namespace doris {

class DorisNodesInfo;
class RuntimeState;
class TupleDescriptor;

struct FileMapping;
struct IteratorKey;
struct IteratorItem;
struct HashOfIteratorKey;

namespace vectorized {
template <typename T>
class ColumnStr;
using ColumnString = ColumnStr<UInt32>;
class MutableBlock;
} // namespace vectorized

// fetch rows by global rowid
// tablet_id/rowset_name/segment_id/ordinal_id

struct FetchOption {
    TupleDescriptor* desc = nullptr;
    RuntimeState* runtime_state = nullptr;
    TFetchOption t_fetch_opt;
};

class RowIDFetcher {
public:
    RowIDFetcher(FetchOption fetch_opt) : _fetch_option(std::move(fetch_opt)) {}
    Status init();
    Status fetch(const vectorized::ColumnPtr& row_ids, vectorized::Block* block);

private:
    PMultiGetRequest _init_fetch_request(const vectorized::ColumnString& row_ids) const;
    Status _merge_rpc_results(const PMultiGetRequest& request,
                              const std::vector<PMultiGetResponse>& rsps,
                              const std::vector<brpc::Controller>& cntls,
                              vectorized::Block* output_block,
                              std::vector<PRowLocation>* rows_id) const;

    std::vector<std::shared_ptr<PBackendService_Stub>> _stubs;
    FetchOption _fetch_option;
};

struct RowStoreReadStruct {
    RowStoreReadStruct(std::string& buffer) : row_store_buffer(buffer) {};
    std::string& row_store_buffer;
    vectorized::DataTypeSerDeSPtrs serdes;
    std::unordered_map<uint32_t, uint32_t> col_uid_to_idx;
    std::vector<std::string> default_values;
};

class RowIdStorageReader {
public:
    static Status read_by_rowids(const PMultiGetRequest& request, PMultiGetResponse* response);
    static Status read_by_rowids(const PMultiGetRequestV2& request, PMultiGetResponseV2* response);

private:
    static Status read_doris_format_row(
            const std::shared_ptr<IdFileMap>& id_file_map,
            const std::shared_ptr<FileMapping>& file_mapping, int64_t row_id,
            std::vector<SlotDescriptor>& slots, const TabletSchema& full_read_schema,
            RowStoreReadStruct& row_store_read_struct, OlapReaderStatistics& stats,
            int64_t* acquire_tablet_ms, int64_t* acquire_rowsets_ms, int64_t* acquire_segments_ms,
            int64_t* lookup_row_data_ms,
            std::unordered_map<IteratorKey, IteratorItem, HashOfIteratorKey>& iterator_map,
            vectorized::Block& result_block);

    static Status read_batch_doris_format_row(
            const PRequestBlockDesc& request_block_desc, std::shared_ptr<IdFileMap> id_file_map,
            std::vector<SlotDescriptor>& slots, const TUniqueId& query_id,
            vectorized::Block& result_block, OlapReaderStatistics& stats,
            int64_t* acquire_tablet_ms, int64_t* acquire_rowsets_ms, int64_t* acquire_segments_ms,
            int64_t* lookup_row_data_ms);

    static Status read_batch_external_row(const PRequestBlockDesc& request_block_desc,
                                          std::shared_ptr<IdFileMap> id_file_map,
                                          std::vector<SlotDescriptor>& slots,
                                          std::shared_ptr<FileMapping> first_file_mapping,
                                          const TUniqueId& query_id,
                                          vectorized::Block& result_block, int64_t* init_reader_ms,
                                          int64_t* get_block_ms);
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
