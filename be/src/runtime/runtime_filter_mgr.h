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

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <thread>

#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/runtime_filter.h"
#include "util/time.h"
#include "util/uid_util.h"
// defination for TRuntimeFilterDesc
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {
class TUniqueId;
class RuntimeFilter;
class FragmentExecState;
class PlanFragmentExecutor;
class PPublishFilterRequest;
class PMergeFilterRequest;

/// producer:
/// Filter filter;
/// get_filter(filter_id, &filter);
/// filter->merge(origin_filter)

/// comsumer:
/// get_filter(filter_id, &filter)
/// filter->wait
/// if filter->ready().ok(), use filter

// owned by RuntimeState
// RuntimeFilterMgr will be destoryed when RuntimeState is destoryed
class RuntimeFilterMgr {
public:
    RuntimeFilterMgr(const UniqueId& query_id, RuntimeState* state);

    ~RuntimeFilterMgr();

    Status init();

    // get a consumer filter by filter-id
    Status get_consume_filter(const int filter_id, IRuntimeFilter** consumer_filter);

    Status get_producer_filter(const int filter_id, IRuntimeFilter** producer_filter);
    // regist filter
    Status regist_filter(const RuntimeFilterRole role, const TRuntimeFilterDesc& desc,
                         const TQueryOptions& options, int node_id = -1);

    // update filter by remote
    Status update_filter(const PPublishFilterRequest* request, const char* data);

    void set_runtime_filter_params(const TRuntimeFilterParams& runtime_filter_params);

    Status get_merge_addr(TNetworkAddress* addr);

private:
    Status get_filter_by_role(const int filter_id, const RuntimeFilterRole role,
                              IRuntimeFilter** target);

    struct RuntimeFilterMgrVal {
        RuntimeFilterRole role; // consumer or producer
        IRuntimeFilter* filter;
    };
    // RuntimeFilterMgr is owned by RuntimeState, so we only
    // use filter_id as key
    // key: "filter-id"
    /// TODO: should it need protected by a mutex?
    std::map<int32_t, RuntimeFilterMgrVal> _consumer_map;
    std::map<int32_t, RuntimeFilterMgrVal> _producer_map;

    RuntimeState* _state;
    std::shared_ptr<MemTracker> _tracker;
    ObjectPool _pool;

    TNetworkAddress _merge_addr;

    bool _has_merge_addr;
};

// controller -> <query-id, entity>
// RuntimeFilterMergeControllerEntity is the context used by runtimefilter for merging
// During a query, only the last sink node owns this class, with the end of the query,
// the class is destroyed with the last fragment_exec.
class RuntimeFilterMergeControllerEntity {
public:
    RuntimeFilterMergeControllerEntity() : _query_id(0, 0), _fragment_instance_id(0, 0) {}
    ~RuntimeFilterMergeControllerEntity() = default;

    Status init(UniqueId query_id, UniqueId fragment_instance_id,
                const TRuntimeFilterParams& runtime_filter_params,
                const TQueryOptions& query_options);

    // handle merge rpc
    Status merge(const PMergeFilterRequest* request, const char* data);

    UniqueId query_id() { return _query_id; }

private:
    Status _init_with_desc(const TRuntimeFilterDesc* runtime_filter_desc,
                           const TQueryOptions* query_options,
                           const std::vector<doris::TRuntimeFilterTargetParams>* target_info,
                           const int producer_size);

    struct RuntimeFilterCntlVal {
        int64_t create_time;
        int producer_size;
        TRuntimeFilterDesc runtime_filter_desc;
        std::vector<doris::TRuntimeFilterTargetParams> target_info;
        IRuntimeFilter* filter;
        std::unordered_set<std::string> arrive_id; // fragment_instance_id ?
        std::shared_ptr<MemTracker> tracker;
        std::shared_ptr<ObjectPool> pool;
    };
    UniqueId _query_id;
    UniqueId _fragment_instance_id;
    // protect _filter_map
    std::mutex _filter_map_mutex;
    std::shared_ptr<MemTracker> _mem_tracker;
    // TODO: convert filter id to i32
    // filter-id -> val
    std::map<std::string, std::shared_ptr<RuntimeFilterCntlVal>> _filter_map;
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
    Status add_entity(const TExecPlanFragmentParams& params,
                      std::shared_ptr<RuntimeFilterMergeControllerEntity>* handle);
    // thread safe
    // increate a reference count
    // if a query-id is not exist
    // Status.not_ok will be returned and a empty ptr will returned by *handle
    Status acquire(UniqueId query_id, std::shared_ptr<RuntimeFilterMergeControllerEntity>* handle);

    // thread safe
    // remove a entity by query-id
    // remove_entity will be called automatically by entity when entity is destroyed
    Status remove_entity(UniqueId queryId);

private:
    std::mutex _controller_mutex;
    // We store the weak pointer here.
    // When the external object is destroyed, we need to clear this record
    using FilterControllerMap =
            std::map<std::string, std::weak_ptr<RuntimeFilterMergeControllerEntity>>;
    // str(query-id) -> entity
    FilterControllerMap _filter_controller_map;
};

using runtime_filter_merge_entity_closer = std::function<void(RuntimeFilterMergeControllerEntity*)>;

void runtime_filter_merge_entity_close(RuntimeFilterMergeController* controller,
                                       RuntimeFilterMergeControllerEntity* entity);

} // namespace doris
