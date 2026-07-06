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
#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "common/thread_safety_annotations.h"
#include "util/uid_util.h"

namespace butil {
class IOBuf;
class IOBufAsZeroCopyInputStream;
} // namespace butil

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
class ExecEnv;
class RuntimeProfile;
template <typename Response>
class HandleErrorBrpcCallback;
class SyncSizeCallback;

struct RuntimeFilterPublishTarget {
    PNetworkAddress addr;
    std::vector<int32_t> fragment_ids;
};

struct RuntimeFilterPublishTask {
    RuntimeFilterPublishTarget receiver;
    PPublishFilterRequestV2 request;
};

std::vector<std::vector<RuntimeFilterPublishTarget>> split_runtime_filter_publish_targets(
        const std::vector<RuntimeFilterPublishTarget>& targets, int fanout);

std::vector<RuntimeFilterPublishTask> build_runtime_filter_publish_tasks(
        const PPublishFilterRequestV2& base_request,
        const std::vector<RuntimeFilterPublishTarget>& targets, int fanout);

int calculate_tree_publish_fanout(int64_t serialized_filter_size, size_t target_count,
                                  int64_t max_send_bytes);

Status forward_runtime_filter(const PPublishFilterRequestV2& request,
                              const butil::IOBuf& request_attachment,
                              std::weak_ptr<QueryContext> query_ctx);

struct LocalMergeContext {
    std::shared_ptr<RuntimeFilterMerger> merger;
    std::vector<std::shared_ptr<RuntimeFilterProducer>> producers;
    // Tracks the recursive CTE round.  When a producer from a newer round
    // registers, RuntimeFilterMgr replaces the whole context and old in-flight
    // users keep the previous context alive through shared_ptr.
    uint32_t stage = 0;
};

struct GlobalMergeContext {
    std::mutex mtx;
    std::shared_ptr<RuntimeFilterMerger> merger;
    TRuntimeFilterDesc runtime_filter_desc;
    std::vector<TRuntimeFilterTargetParamsV2> targetv2_info;
    std::unordered_set<UniqueId> arrive_id;
    std::vector<PNetworkAddress> source_addrs;
    std::vector<std::shared_ptr<HandleErrorBrpcCallback<PSyncFilterSizeResponse>>>
            sync_size_callbacks;
    std::vector<std::shared_ptr<HandleErrorBrpcCallback<PPublishFilterResponse>>> publish_callbacks;
    std::atomic<bool> done = false;

    // for represent the round number of recursive cte
    // if lower stage rf input to higher stage, we just discard the rf
    uint32_t stage = 0;

    Status reset(QueryContext* query_ctx);
};

// owned by RuntimeState
// RuntimeFilterMgr will be destroyed when RuntimeState is destroyed
class RuntimeFilterMgr {
public:
    RuntimeFilterMgr(const bool is_global);

    // get/set consumer
    std::vector<std::shared_ptr<RuntimeFilterConsumer>> get_consume_filters(int filter_id);
    Status register_consumer_filter(const RuntimeState* state, const TRuntimeFilterDesc& desc,
                                    int node_id,
                                    std::shared_ptr<RuntimeFilterConsumer>* consumer_filter);

    Status register_local_merge_producer_filter(const QueryContext* query_ctx,
                                                const TRuntimeFilterDesc& desc,
                                                std::shared_ptr<RuntimeFilterProducer> producer);

    Status get_local_merge_context(int filter_id, uint32_t expected_stage,
                                   std::shared_ptr<LocalMergeContext>* context);

    // Create local producer. This producer is hold by RuntimeFilterProducerHelper.
    Status register_producer_filter(const QueryContext* query_ctx, const TRuntimeFilterDesc& desc,
                                    std::shared_ptr<RuntimeFilterProducer>* producer);

    // update filter by remote
    bool set_runtime_filter_params(const TRuntimeFilterParams& runtime_filter_params);
    Status get_merge_addr(TNetworkAddress* addr);
    Status sync_filter_size(const PSyncFilterSizeRequest* request);

    std::string debug_string();

    void remove_filter(int32_t filter_id) {
        LockGuard l(_lock);
        _consumer_map.erase(filter_id);
        // NOTE: _local_merge_map is NOT erased here.  It is replaced lazily in
        // register_local_merge_producer_filter when a producer from a newer
        // recursive CTE round registers.  Erasing eagerly here would race with
        // multi-fragment REBUILD: a consumer-only fragment's remove_filter could
        // delete the entry that the producer fragment just re-registered.
        _producer_id_set.erase(filter_id);
    }

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
    // Protects fields marked GUARDED_BY(_lock). While holding this lock, only
    // access RuntimeFilterMgr-owned state or copy shared_ptr snapshots; do not
    // call methods on existing RuntimeFilter objects, because RF objects have
    // their own locks and may call back into RuntimeFilterMgr.
    AnnotatedMutex _lock;
    std::map<int32_t, std::vector<std::shared_ptr<RuntimeFilterConsumer>>> _consumer_map
            GUARDED_BY(_lock);
    std::set<int32_t> _producer_id_set GUARDED_BY(_lock);
    std::map<int32_t, std::shared_ptr<LocalMergeContext>> _local_merge_map GUARDED_BY(_lock);

    std::unique_ptr<MemTracker> _tracker;

    TNetworkAddress _merge_addr;

    bool _has_merge_addr = false;
};

// controller -> <query-id, entity>
// RuntimeFilterMergeControllerEntity is the context used by runtimefilter for merging
// During a query, only the last sink node owns this class, with the end of the query,
// the class is destroyed with the last fragment_exec.
class RuntimeFilterMergeControllerEntity {
public:
    Status init(std::shared_ptr<QueryContext> query_ctx,
                const TRuntimeFilterParams& runtime_filter_params);

    // handle merge rpc
    Status merge(std::shared_ptr<QueryContext> query_ctx, const PMergeFilterRequest* request,
                 butil::IOBufAsZeroCopyInputStream* attach_data);

    Status send_filter_size(std::shared_ptr<QueryContext> query_ctx,
                            const PSendFilterSizeRequest* request);

    std::string debug_string();

    bool empty() {
        SharedLockGuard read_lock(_filter_map_mutex);
        return _filter_map.empty();
    }

    Status reset_global_rf(QueryContext* query_ctx,
                           const google::protobuf::RepeatedField<int32_t>& filter_ids);

private:
    Status _init_with_desc(std::shared_ptr<QueryContext> query_ctx,
                           const TRuntimeFilterDesc* runtime_filter_desc,
                           const std::vector<TRuntimeFilterTargetParamsV2>&& target_info,
                           const int producer_size);

    Status _send_rf_to_target(GlobalMergeContext& cnt_val, std::weak_ptr<QueryContext> ctx,
                              int64_t merge_time, PUniqueId query_id, int execution_timeout,
                              const TQueryOptions& query_options);

    // protect _filter_map
    AnnotatedSharedMutex _filter_map_mutex;
    std::shared_ptr<MemTracker> _mem_tracker;

    std::map<int, GlobalMergeContext> _filter_map GUARDED_BY(_filter_map_mutex);
};
} // namespace doris
