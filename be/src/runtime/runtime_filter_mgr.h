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
#include <stdint.h>

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
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
class RuntimeState;
enum class RuntimeFilterRole;
class RuntimePredicateWrapper;
class QueryContext;
struct RuntimeFilterParamsContext;
class ExecEnv;

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
    RuntimeFilterMgr(const UniqueId& query_id, RuntimeFilterParamsContext* state);

    ~RuntimeFilterMgr() = default;

    Status init();

    Status get_consume_filter(const int filter_id, const int node_id,
                              IRuntimeFilter** consumer_filter);

    Status get_consume_filters(const int filter_id, std::vector<IRuntimeFilter*>& consumer_filters);

    Status get_producer_filter(const int filter_id, IRuntimeFilter** producer_filter);

    // register filter
    Status register_consumer_filter(const TRuntimeFilterDesc& desc, const TQueryOptions& options,
                                    int node_id, bool build_bf_exactly = false,
                                    bool is_global = false);
    Status register_producer_filter(const TRuntimeFilterDesc& desc, const TQueryOptions& options,
                                    bool build_bf_exactly = false, bool is_global = false,
                                    int parallel_tasks = 0);

    // update filter by remote
    Status update_filter(const PPublishFilterRequest* request,
                         butil::IOBufAsZeroCopyInputStream* data);

    void set_runtime_filter_params(const TRuntimeFilterParams& runtime_filter_params);

    Status get_merge_addr(TNetworkAddress* addr);

private:
    struct ConsumerFilterHolder {
        int node_id;
        IRuntimeFilter* filter = nullptr;
    };
    // RuntimeFilterMgr is owned by RuntimeState, so we only
    // use filter_id as key
    // key: "filter-id"
    /// TODO: should it need protected by a mutex?
    std::map<int32_t, std::vector<ConsumerFilterHolder>> _consumer_map;
    std::map<int32_t, IRuntimeFilter*> _producer_map;

    RuntimeFilterParamsContext* _state = nullptr;
    std::unique_ptr<MemTracker> _tracker;
    ObjectPool _pool;

    TNetworkAddress _merge_addr;

    bool _has_merge_addr;
    std::mutex _lock;
};

// controller -> <query-id, entity>
// RuntimeFilterMergeControllerEntity is the context used by runtimefilter for merging
// During a query, only the last sink node owns this class, with the end of the query,
// the class is destroyed with the last fragment_exec.
class RuntimeFilterMergeControllerEntity {
public:
    RuntimeFilterMergeControllerEntity(RuntimeFilterParamsContext* state)
            : _query_id(0, 0), _fragment_instance_id(0, 0), _state(state) {}
    ~RuntimeFilterMergeControllerEntity() = default;

    Status init(UniqueId query_id, UniqueId fragment_instance_id,
                const TRuntimeFilterParams& runtime_filter_params,
                const TQueryOptions& query_options);

    // handle merge rpc
    Status merge(const PMergeFilterRequest* request, butil::IOBufAsZeroCopyInputStream* attach_data,
                 bool opt_remote_rf);

    UniqueId query_id() const { return _query_id; }

    UniqueId instance_id() const { return _fragment_instance_id; }

    struct RuntimeFilterCntlVal {
        int64_t merge_time;
        int producer_size;
        TRuntimeFilterDesc runtime_filter_desc;
        std::vector<doris::TRuntimeFilterTargetParams> target_info;
        std::vector<doris::TRuntimeFilterTargetParamsV2> targetv2_info;
        IRuntimeFilter* filter = nullptr;
        std::unordered_set<UniqueId> arrive_id; // fragment_instance_id ?
        std::shared_ptr<ObjectPool> pool;
    };

public:
    RuntimeFilterCntlVal* get_filter(int id) { return _filter_map[id].first.get(); }

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
    UniqueId _fragment_instance_id;
    // protect _filter_map
    std::shared_mutex _filter_map_mutex;
    std::shared_ptr<MemTracker> _mem_tracker;
    using CntlValwithLock =
            std::pair<std::shared_ptr<RuntimeFilterCntlVal>, std::unique_ptr<std::mutex>>;
    std::map<int, CntlValwithLock> _filter_map;
    RuntimeFilterParamsContext* _state = nullptr;
    bool _opt_remote_rf = true;
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
                      std::shared_ptr<RuntimeFilterMergeControllerEntity>* handle,
                      RuntimeFilterParamsContext* state);
    Status add_entity(const TPipelineFragmentParams& params,
                      const TPipelineInstanceParams& local_params,
                      std::shared_ptr<RuntimeFilterMergeControllerEntity>* handle,
                      RuntimeFilterParamsContext* state);
    // thread safe
    // increase a reference count
    // if a query-id is not exist
    // Status.not_ok will be returned and a empty ptr will returned by *handle
    Status acquire(UniqueId query_id, std::shared_ptr<RuntimeFilterMergeControllerEntity>* handle);

    // thread safe
    // remove a entity by query-id
    // remove_entity will be called automatically by entity when entity is destroyed
    Status remove_entity(UniqueId query_id);

    static const int kShardNum = 128;

private:
    uint32_t _get_controller_shard_idx(UniqueId& query_id) {
        return (uint32_t)query_id.hi % kShardNum;
    }

    std::mutex _controller_mutex[kShardNum];
    // We store the weak pointer here.
    // When the external object is destroyed, we need to clear this record
    using FilterControllerMap =
            std::unordered_map<std::string, std::weak_ptr<RuntimeFilterMergeControllerEntity>>;
    // str(query-id) -> entity
    FilterControllerMap _filter_controller_map[kShardNum];
};

using runtime_filter_merge_entity_closer = std::function<void(RuntimeFilterMergeControllerEntity*)>;

void runtime_filter_merge_entity_close(RuntimeFilterMergeController* controller,
                                       RuntimeFilterMergeControllerEntity* entity);

//There are two types of runtime filters:
// one is global, originating from QueryContext,
// and the other is local, originating from RuntimeState.
// In practice, we have already distinguished between them through UpdateRuntimeFilterParamsV2/V1.
// RuntimeState/QueryContext is only used to store runtime_filter_wait_time_ms and enable_pipeline_exec...

/// TODO: Consider adding checks for global/local.
struct RuntimeFilterParamsContext {
    RuntimeFilterParamsContext() = default;
    static RuntimeFilterParamsContext* create(RuntimeState* state);
    static RuntimeFilterParamsContext* create(QueryContext* query_ctx);

    bool runtime_filter_wait_infinitely;
    int32_t runtime_filter_wait_time_ms;
    bool enable_pipeline_exec;
    int32_t execution_timeout;
    RuntimeFilterMgr* runtime_filter_mgr;
    ExecEnv* exec_env;
    PUniqueId query_id;
    PUniqueId _fragment_instance_id;
    int be_exec_version;
    QueryContext* query_ctx;
    QueryContext* get_query_ctx() const { return query_ctx; }
    ObjectPool* _obj_pool;
    bool _is_global = false;
    PUniqueId fragment_instance_id() const {
        DCHECK(!_is_global);
        return _fragment_instance_id;
    }
    ObjectPool* obj_pool() const {
        DCHECK(_is_global);
        return _obj_pool;
    }
};
} // namespace doris
