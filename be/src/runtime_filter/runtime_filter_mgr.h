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
struct RuntimeFilterParamsContext;
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
    std::vector<doris::TRuntimeFilterTargetParamsV2> targetv2_info;
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

    std::vector<std::shared_ptr<RuntimeFilterConsumer>> get_consume_filters(int filter_id);

    std::shared_ptr<RuntimeFilterProducer> try_get_product_filter(const int filter_id) {
        std::lock_guard<std::mutex> l(_lock);
        auto iter = _producer_map.find(filter_id);
        if (iter == _producer_map.end()) {
            return nullptr;
        }
        return iter->second;
    }

    // register filter
    Status register_consumer_filter(const TRuntimeFilterDesc& desc, const TQueryOptions& options,
                                    int node_id,
                                    std::shared_ptr<RuntimeFilterConsumer>* consumer_filter,
                                    bool need_local_merge, RuntimeProfile* parent_profile);

    Status register_local_merger_filter(const TRuntimeFilterDesc& desc,
                                        const TQueryOptions& options,
                                        std::shared_ptr<RuntimeFilterProducer> producer_filter);

    Status get_local_merge_producer_filters(int filter_id, LocalMergeContext** local_merge_filters);

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
    std::map<int32_t, std::shared_ptr<RuntimeFilterProducer>> _producer_map;
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
                           const std::vector<doris::TRuntimeFilterTargetParamsV2>&& target_info,
                           const int producer_size);

    UniqueId _query_id;
    // protect _filter_map
    std::shared_mutex _filter_map_mutex;
    std::shared_ptr<MemTracker> _mem_tracker;

    std::map<int, GlobalMergeContext> _filter_map;
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
} // namespace doris
