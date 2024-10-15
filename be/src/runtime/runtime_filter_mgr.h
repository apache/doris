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
#include "util/uid_util.h"

namespace butil {
class IOBufAsZeroCopyInputStream;
}

namespace doris {
class PPublishFilterRequest;
class PPublishFilterRequestV2;
class PMergeFilterRequest;
class IRuntimeFilter;
class MemTracker;
class MemTrackerLimiter;
class RuntimeState;
enum class RuntimeFilterRole;
class RuntimePredicateWrapper;
class QueryContext;
struct RuntimeFilterParamsContext;
class ExecEnv;

struct LocalMergeFilters {
    std::unique_ptr<std::mutex> lock = std::make_unique<std::mutex>();
    int merge_time = 0;
    int merge_size_times = 0;
    uint64_t local_merged_size = 0;
    std::vector<std::shared_ptr<IRuntimeFilter>> filters;
};

/// producer:
/// Filter filter;
/// get_filter(filter_id, &filter);
/// filter->merge(origin_filter)

/// consumer:
/// get_filter(filter_id, &filter)
/// filter->wait
/// if filter->ready().ok(), use filter

// owned by RuntimeState
// RuntimeFilterMgr will be destroyed when RuntimeState is destroyed
class RuntimeFilterMgr {
public:
    RuntimeFilterMgr(const UniqueId& query_id, RuntimeFilterParamsContext* state,
                     const std::shared_ptr<MemTrackerLimiter>& query_mem_tracker);

    ~RuntimeFilterMgr();

    Status get_consume_filters(const int filter_id,
                               std::vector<std::shared_ptr<IRuntimeFilter>>& consumer_filters);

    std::shared_ptr<IRuntimeFilter> try_get_product_filter(const int filter_id) {
        std::lock_guard<std::mutex> l(_lock);
        auto iter = _producer_map.find(filter_id);
        if (iter == _producer_map.end()) {
            return nullptr;
        }
        return iter->second;
    }

    // register filter
    Status register_consumer_filter(const TRuntimeFilterDesc& desc, const TQueryOptions& options,
                                    int node_id, std::shared_ptr<IRuntimeFilter>* consumer_filter,
                                    bool build_bf_exactly = false, bool need_local_merge = false);

    Status register_local_merge_producer_filter(const TRuntimeFilterDesc& desc,
                                                const TQueryOptions& options,
                                                std::shared_ptr<IRuntimeFilter>* producer_filter,
                                                bool build_bf_exactly = false);

    Status get_local_merge_producer_filters(int filter_id, LocalMergeFilters** local_merge_filters);

    Status register_producer_filter(const TRuntimeFilterDesc& desc, const TQueryOptions& options,
                                    std::shared_ptr<IRuntimeFilter>* producer_filter,
                                    bool build_bf_exactly = false);

    // update filter by remote
    Status update_filter(const PPublishFilterRequest* request,
                         butil::IOBufAsZeroCopyInputStream* data);

    void set_runtime_filter_params(const TRuntimeFilterParams& runtime_filter_params);

    Status get_merge_addr(TNetworkAddress* addr);

    Status sync_filter_size(const PSyncFilterSizeRequest* request);

private:
    struct ConsumerFilterHolder {
        int node_id;
        std::shared_ptr<IRuntimeFilter> filter;
    };
    // RuntimeFilterMgr is owned by RuntimeState, so we only
    // use filter_id as key
    // key: "filter-id"
    std::map<int32_t, std::vector<ConsumerFilterHolder>> _consumer_map;
    std::map<int32_t, std::shared_ptr<IRuntimeFilter>> _producer_map;
    std::map<int32_t, LocalMergeFilters> _local_merge_producer_map;

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
    Status merge(const PMergeFilterRequest* request,
                 butil::IOBufAsZeroCopyInputStream* attach_data);

    Status send_filter_size(const PSendFilterSizeRequest* request);

    UniqueId query_id() const { return _query_id; }

    struct RuntimeFilterCntlVal {
        int64_t merge_time;
        int producer_size;
        uint64_t global_size;
        TRuntimeFilterDesc runtime_filter_desc;
        std::vector<doris::TRuntimeFilterTargetParams> target_info;
        std::vector<doris::TRuntimeFilterTargetParamsV2> targetv2_info;
        IRuntimeFilter* filter = nullptr;
        std::unordered_set<UniqueId> arrive_id;
        std::vector<PNetworkAddress> source_addrs;
        std::shared_ptr<ObjectPool> pool;
    };

private:
    Status _init_with_desc(const TRuntimeFilterDesc* runtime_filter_desc,
                           const TQueryOptions* query_options,
                           const std::vector<doris::TRuntimeFilterTargetParams>* target_info,
                           const int producer_size);

    Status _init_with_desc(const TRuntimeFilterDesc* runtime_filter_desc,
                           const TQueryOptions* query_options,
                           const std::vector<doris::TRuntimeFilterTargetParamsV2>* target_info,
                           const int producer_size);

    UniqueId _query_id;
    // protect _filter_map
    std::shared_mutex _filter_map_mutex;
    std::shared_ptr<MemTracker> _mem_tracker;

    struct CntlValwithLock {
        std::shared_ptr<RuntimeFilterCntlVal> cnt_val;
        std::unique_ptr<std::mutex> mutex;
        CntlValwithLock(std::shared_ptr<RuntimeFilterCntlVal> input_cnt_val)
                : cnt_val(std::move(input_cnt_val)), mutex(std::make_unique<std::mutex>()) {}
    };

    std::map<int, CntlValwithLock> _filter_map;
    RuntimeFilterParamsContext* _state = nullptr;
};

// RuntimeFilterMergeController has a map query-id -> entity
class RuntimeFilterMergeController {
public:
    RuntimeFilterMergeController() = default;
    ~RuntimeFilterMergeController() = default;

    // thread safe
    // add a query-id -> entity
    // If a query-id -> entity already exists
    // add_entity will return a exists entity
    Status add_entity(const auto& params, UniqueId query_id, const TQueryOptions& query_options,
                      std::shared_ptr<RuntimeFilterMergeControllerEntity>* handle,
                      RuntimeFilterParamsContext* state) {
        if (!params.__isset.runtime_filter_params ||
            params.runtime_filter_params.rid_to_runtime_filter.size() == 0) {
            return Status::OK();
        }

        // TODO: why we need string, direct use UniqueId
        uint32_t shard = _get_controller_shard_idx(query_id);
        std::lock_guard<std::mutex> guard(_controller_mutex[shard]);
        auto iter = _filter_controller_map[shard].find(query_id);
        if (iter == _filter_controller_map[shard].end()) {
            *handle = std::shared_ptr<RuntimeFilterMergeControllerEntity>(
                    new RuntimeFilterMergeControllerEntity(state),
                    [this](RuntimeFilterMergeControllerEntity* entity) {
                        remove_entity(entity->query_id());
                        delete entity;
                    });
            _filter_controller_map[shard][query_id] = *handle;
            const TRuntimeFilterParams& filter_params = params.runtime_filter_params;
            RETURN_IF_ERROR(handle->get()->init(query_id, filter_params, query_options));
        } else {
            *handle = _filter_controller_map[shard][query_id].lock();
        }
        return Status::OK();
    }

    // thread safe
    // increase a reference count
    // if a query-id is not exist
    // Status.not_ok will be returned and a empty ptr will returned by *handle
    Status acquire(UniqueId query_id, std::shared_ptr<RuntimeFilterMergeControllerEntity>* handle);

    // thread safe
    // remove a entity by query-id
    // remove_entity will be called automatically by entity when entity is destroyed
    void remove_entity(UniqueId query_id);

    static const int kShardNum = 128;

private:
    uint32_t _get_controller_shard_idx(UniqueId& query_id) {
        return (uint32_t)query_id.hi % kShardNum;
    }

    std::mutex _controller_mutex[kShardNum];
    // We store the weak pointer here.
    // When the external object is destroyed, we need to clear this record
    using FilterControllerMap =
            std::unordered_map<UniqueId, std::weak_ptr<RuntimeFilterMergeControllerEntity>>;
    // str(query-id) -> entity
    FilterControllerMap _filter_controller_map[kShardNum];
};

//There are two types of runtime filters:
// one is global, originating from QueryContext,
// and the other is local, originating from RuntimeState.
// In practice, we have already distinguished between them through UpdateRuntimeFilterParamsV2/V1.
// RuntimeState/QueryContext is only used to store runtime_filter_wait_time_ms...
struct RuntimeFilterParamsContext {
    RuntimeFilterParamsContext() = default;
    static RuntimeFilterParamsContext* create(RuntimeState* state);
    static RuntimeFilterParamsContext* create(QueryContext* query_ctx);

    bool runtime_filter_wait_infinitely;
    int32_t runtime_filter_wait_time_ms;
    int32_t execution_timeout;
    RuntimeFilterMgr* runtime_filter_mgr;
    ExecEnv* exec_env;
    PUniqueId query_id;
    int be_exec_version;
    QueryContext* query_ctx;
    QueryContext* get_query_ctx() const { return query_ctx; }
};
} // namespace doris
