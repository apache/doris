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

#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"

namespace butil {
class IOBufAsZeroCopyInputStream;
}

namespace doris {
class PPublishFilterRequestV2;
class PMergeFilterRequest;
class RuntimeFilterMerger;
class RuntimeFilterProducer;
class RuntimeFilterConsumer;
class MemTracker;
class MemTrackerLimiter;
class RuntimeState;
class RuntimeFilterWrapper;
class QueryContext;
class RuntimeFilterParamsContext;
class ExecEnv;
class RuntimeProfile;

struct LocalMergeContext {
    std::mutex mtx;
    std::shared_ptr<RuntimeFilterMerger> merger;
    std::vector<std::shared_ptr<RuntimeFilterProducer>> producers;
};

struct GlobalMergeContext {
    std::mutex mtx;
    std::shared_ptr<RuntimeFilterMerger> merger;
    TRuntimeFilterDesc runtime_filter_desc;
    std::vector<TRuntimeFilterTargetParamsV2> targetv2_info;
    std::unordered_set<UniqueId> arrive_id;
    std::vector<PNetworkAddress> source_addrs;
};

// owned by RuntimeState
// RuntimeFilterMgr will be destroyed when RuntimeState is destroyed
class RuntimeFilterMgr {
public:
    RuntimeFilterMgr(const UniqueId& query_id, RuntimeFilterParamsContext* state,
                     const std::shared_ptr<MemTrackerLimiter>& query_mem_tracker,
                     const bool is_global);

    ~RuntimeFilterMgr();

    // get/set consumer
    std::vector<std::shared_ptr<RuntimeFilterConsumer>> get_consume_filters(int filter_id);
    Status register_consumer_filter(const TRuntimeFilterDesc& desc, const TQueryOptions& options,
                                    int node_id,
                                    std::shared_ptr<RuntimeFilterConsumer>* consumer_filter,
                                    bool need_local_merge, RuntimeProfile* parent_profile);

    Status register_local_merger_producer_filter(
            const TRuntimeFilterDesc& desc, const TQueryOptions& options,
            std::shared_ptr<RuntimeFilterProducer> producer_filter, RuntimeProfile* parent_profile);

    Status get_local_merge_producer_filters(int filter_id, LocalMergeContext** local_merge_filters);

    // Create local producer. This producer is hold by RuntimeFilterProducerHelper.
    Status register_producer_filter(const TRuntimeFilterDesc& desc, const TQueryOptions& options,
                                    std::shared_ptr<RuntimeFilterProducer>* producer_filter,
                                    RuntimeProfile* parent_profile);

    // update filter by remote
    void set_runtime_filter_params(const TRuntimeFilterParams& runtime_filter_params);
    Status get_merge_addr(TNetworkAddress* addr);
    Status sync_filter_size(const PSyncFilterSizeRequest* request);

private:
    /**
     * `_is_global = true` means this runtime filter manager menages query-level runtime filters.
     * If so, all consumers in this query shared the same RF with the same ID. For producers, all
     * RFs produced should be merged.
     *
     * If `_is_global` is false, a RF will be produced and consumed by a single-producer-single-consumer mode.
     * This is usually happened in a co-located join and scan operators are not serial.
     *
     * `_local_merge_map` is used only if `_is_global` is true. It is said, RFs produced by
     * different producers need to be merged only if it is a global RF.
     */
    const bool _is_global;
    // RuntimeFilterMgr is owned by RuntimeState, so we only
    // use filter_id as key
    // key: "filter-id"
    std::map<int32_t, std::vector<std::shared_ptr<RuntimeFilterConsumer>>> _consumer_map;
    std::set<int32_t> _producer_id_set;
    std::map<int32_t, LocalMergeContext> _local_merge_map;

    RuntimeFilterParamsContext* _state = nullptr;
    std::unique_ptr<MemTracker> _tracker;
    std::shared_ptr<MemTrackerLimiter> _query_mem_tracker;
    ObjectPool _pool;

    TNetworkAddress _merge_addr;

    bool _has_merge_addr = false;
    std::mutex _lock;
};

// controller -> <query-id, entity>
// RuntimeFilterMergeControllerEntity is the context used by runtimefilter for merging
// During a query, only the last sink node owns this class, with the end of the query,
// the class is destroyed with the last fragment_exec.
class RuntimeFilterMergeControllerEntity {
public:
    RuntimeFilterMergeControllerEntity(RuntimeFilterParamsContext* state)
            : _query_id(0, 0), _state(state) {}
    ~RuntimeFilterMergeControllerEntity() = default;

    Status init(UniqueId query_id, const TRuntimeFilterParams& runtime_filter_params,
                const TQueryOptions& query_options);

    // handle merge rpc
    Status merge(std::weak_ptr<QueryContext> query_ctx, const PMergeFilterRequest* request,
                 butil::IOBufAsZeroCopyInputStream* attach_data);

    Status send_filter_size(std::weak_ptr<QueryContext> query_ctx,
                            const PSendFilterSizeRequest* request);

    UniqueId query_id() const { return _query_id; }

private:
    Status _init_with_desc(const TRuntimeFilterDesc* runtime_filter_desc,
                           const std::vector<TRuntimeFilterTargetParamsV2>&& target_info,
                           const int producer_size);

    UniqueId _query_id;
    // protect _filter_map
    std::shared_mutex _filter_map_mutex;
    std::shared_ptr<MemTracker> _mem_tracker;

    std::map<int, GlobalMergeContext> _filter_map;
    RuntimeFilterParamsContext* _state = nullptr;
};

} // namespace doris
