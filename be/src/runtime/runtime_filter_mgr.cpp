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

#include "runtime/runtime_filter_mgr.h"

#include <brpc/controller.h>
#include <butil/iobuf.h>
#include <butil/iobuf_inl.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <stddef.h>

#include <ostream>
#include <string>
#include <utility>

#include "common/logging.h"
#include "common/status.h"
#include "exprs/bloom_filter_func.h"
#include "exprs/runtime_filter.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/brpc_client_cache.h"
#include "util/ref_count_closure.h"

namespace doris {

template <class RPCRequest, class RPCResponse>
struct AsyncRPCContext {
    RPCRequest request;
    RPCResponse response;
    brpc::Controller cntl;
    brpc::CallId cid;
};

RuntimeFilterMgr::RuntimeFilterMgr(const UniqueId& query_id, RuntimeFilterParamsContext* state,
                                   const std::shared_ptr<MemTrackerLimiter>& query_mem_tracker) {
    _state = state;
    _state->runtime_filter_mgr = this;
    _query_mem_tracker = query_mem_tracker;
    _tracker = std::make_unique<MemTracker>("RuntimeFilterMgr(experimental)",
                                            _query_mem_tracker.get());
}

RuntimeFilterMgr::~RuntimeFilterMgr() {
    CHECK(_query_mem_tracker != nullptr);
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_query_mem_tracker);
    _pool.clear();
}

Status RuntimeFilterMgr::get_consume_filters(const int filter_id,
                                             std::vector<IRuntimeFilter*>& consumer_filters) {
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _consumer_map.find(filter_id);
    if (iter == _consumer_map.end()) {
        return Status::InvalidArgument("unknown filter: {}, role: CONSUMER.", filter_id);
    }
    for (auto& holder : iter->second) {
        consumer_filters.emplace_back(holder.filter);
    }
    return Status::OK();
}

Status RuntimeFilterMgr::register_consumer_filter(const TRuntimeFilterDesc& desc,
                                                  const TQueryOptions& options, int node_id,
                                                  IRuntimeFilter** consumer_filter,
                                                  bool build_bf_exactly, bool need_local_merge) {
    SCOPED_CONSUME_MEM_TRACKER(_tracker.get());
    int32_t key = desc.filter_id;
    bool has_exist = false;

    std::lock_guard<std::mutex> l(_lock);
    if (auto iter = _consumer_map.find(key); iter != _consumer_map.end()) {
        for (auto holder : iter->second) {
            if (holder.node_id == node_id) {
                *consumer_filter = holder.filter;
                has_exist = true;
            }
        }
    }

    if (!has_exist) {
        IRuntimeFilter* filter;
        RETURN_IF_ERROR(IRuntimeFilter::create(_state, &_pool, &desc, &options,
                                               RuntimeFilterRole::CONSUMER, node_id, &filter,
                                               build_bf_exactly, need_local_merge));
        _consumer_map[key].emplace_back(node_id, filter);
        *consumer_filter = filter;
    } else if (!need_local_merge) {
        return Status::InvalidArgument("filter has registered");
    }

    return Status::OK();
}

Status RuntimeFilterMgr::register_local_merge_producer_filter(
        const doris::TRuntimeFilterDesc& desc, const doris::TQueryOptions& options,
        doris::IRuntimeFilter** producer_filter, bool build_bf_exactly) {
    SCOPED_CONSUME_MEM_TRACKER(_tracker.get());
    int32_t key = desc.filter_id;

    decltype(_local_merge_producer_map.end()) iter;
    {
        std::lock_guard<std::mutex> l(_lock);
        iter = _local_merge_producer_map.find(key);
        if (iter == _local_merge_producer_map.end()) {
            auto [new_iter, _] = _local_merge_producer_map.emplace(key, LocalMergeFilters {});
            iter = new_iter;
        }
    }

    DCHECK(_state != nullptr);
    RETURN_IF_ERROR(IRuntimeFilter::create(_state, &_pool, &desc, &options,
                                           RuntimeFilterRole::PRODUCER, -1, producer_filter,
                                           build_bf_exactly, true));
    {
        std::lock_guard<std::mutex> l(*iter->second.lock);
        if (iter->second.filters.empty()) {
            IRuntimeFilter* merge_filter = nullptr;
            RETURN_IF_ERROR(IRuntimeFilter::create(_state, &_pool, &desc, &options,
                                                   RuntimeFilterRole::PRODUCER, -1, &merge_filter,
                                                   build_bf_exactly, true));
            iter->second.filters.emplace_back(merge_filter);
        }
        iter->second.merge_time++;
        iter->second.merge_size_times++;
        iter->second.filters.emplace_back(*producer_filter);
    }
    return Status::OK();
}

Status RuntimeFilterMgr::get_local_merge_producer_filters(
        int filter_id, doris::LocalMergeFilters** local_merge_filters) {
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _local_merge_producer_map.find(filter_id);
    if (iter == _local_merge_producer_map.end()) {
        return Status::InvalidArgument("unknown filter: {}, role: LOCAL_MERGE_PRODUCER.",
                                       filter_id);
    }
    *local_merge_filters = &iter->second;
    DCHECK(!iter->second.filters.empty());
    DCHECK_GT(iter->second.merge_time, 0);
    return Status::OK();
}

Status RuntimeFilterMgr::register_producer_filter(const TRuntimeFilterDesc& desc,
                                                  const TQueryOptions& options,
                                                  IRuntimeFilter** producer_filter,
                                                  bool build_bf_exactly) {
    SCOPED_CONSUME_MEM_TRACKER(_tracker.get());
    int32_t key = desc.filter_id;
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _producer_map.find(key);

    DCHECK(_state != nullptr);
    if (iter != _producer_map.end()) {
        return Status::InvalidArgument("filter has registed");
    }
    RETURN_IF_ERROR(IRuntimeFilter::create(_state, &_pool, &desc, &options,
                                           RuntimeFilterRole::PRODUCER, -1, producer_filter,
                                           build_bf_exactly));
    _producer_map.emplace(key, *producer_filter);
    return Status::OK();
}

Status RuntimeFilterMgr::update_filter(const PPublishFilterRequest* request,
                                       butil::IOBufAsZeroCopyInputStream* data) {
    SCOPED_CONSUME_MEM_TRACKER(_tracker.get());
    UpdateRuntimeFilterParams params(request, data, &_pool);
    int filter_id = request->filter_id();
    std::vector<IRuntimeFilter*> filters;
    // The code is organized for upgrade compatibility to prevent infinite waiting
    // old way update filter the code should be deleted after the upgrade is complete.
    {
        std::lock_guard<std::mutex> l(_lock);
        auto iter = _consumer_map.find(filter_id);
        if (iter == _consumer_map.end()) {
            return Status::InvalidArgument("unknown filter: {}, role: CONSUMER.", filter_id);
        }
        for (auto& holder : iter->second) {
            filters.emplace_back(holder.filter);
        }
        iter->second.clear();
    }
    for (auto filter : filters) {
        RETURN_IF_ERROR(filter->update_filter(&params));
    }

    return Status::OK();
}

void RuntimeFilterMgr::set_runtime_filter_params(
        const TRuntimeFilterParams& runtime_filter_params) {
    std::lock_guard l(_lock);
    if (!_has_merge_addr) {
        _merge_addr = runtime_filter_params.runtime_filter_merge_addr;
        _has_merge_addr = true;
    }
}

Status RuntimeFilterMgr::get_merge_addr(TNetworkAddress* addr) {
    DCHECK(_has_merge_addr);
    if (_has_merge_addr) {
        *addr = this->_merge_addr;
        return Status::OK();
    }
    return Status::InternalError("not found merge addr");
}

Status RuntimeFilterMergeControllerEntity::_init_with_desc(
        const TRuntimeFilterDesc* runtime_filter_desc, const TQueryOptions* query_options,
        const std::vector<doris::TRuntimeFilterTargetParams>* target_info,
        const int producer_size) {
    std::unique_lock<std::shared_mutex> guard(_filter_map_mutex);
    std::shared_ptr<RuntimeFilterCntlVal> cnt_val = std::make_shared<RuntimeFilterCntlVal>();
    // runtime_filter_desc and target will be released,
    // so we need to copy to cnt_val
    cnt_val->producer_size = producer_size;
    cnt_val->runtime_filter_desc = *runtime_filter_desc;
    cnt_val->target_info = *target_info;
    cnt_val->pool.reset(new ObjectPool());
    cnt_val->filter = cnt_val->pool->add(
            new IRuntimeFilter(_state, &_state->get_query_ctx()->obj_pool, runtime_filter_desc));

    auto filter_id = runtime_filter_desc->filter_id;
    RETURN_IF_ERROR(cnt_val->filter->init_with_desc(&cnt_val->runtime_filter_desc, query_options,
                                                    -1, false));
    _filter_map.emplace(filter_id, cnt_val);
    return Status::OK();
}

Status RuntimeFilterMergeControllerEntity::_init_with_desc(
        const TRuntimeFilterDesc* runtime_filter_desc, const TQueryOptions* query_options,
        const std::vector<doris::TRuntimeFilterTargetParamsV2>* targetv2_info,
        const int producer_size) {
    std::shared_ptr<RuntimeFilterCntlVal> cnt_val = std::make_shared<RuntimeFilterCntlVal>();
    // runtime_filter_desc and target will be released,
    // so we need to copy to cnt_val
    cnt_val->producer_size = producer_size;
    cnt_val->runtime_filter_desc = *runtime_filter_desc;
    cnt_val->targetv2_info = *targetv2_info;
    cnt_val->pool.reset(new ObjectPool());
    cnt_val->filter = cnt_val->pool->add(
            new IRuntimeFilter(_state, &_state->get_query_ctx()->obj_pool, runtime_filter_desc));
    auto filter_id = runtime_filter_desc->filter_id;
    RETURN_IF_ERROR(cnt_val->filter->init_with_desc(&cnt_val->runtime_filter_desc, query_options));

    std::unique_lock<std::shared_mutex> guard(_filter_map_mutex);
    _filter_map.emplace(filter_id, cnt_val);
    return Status::OK();
}

Status RuntimeFilterMergeControllerEntity::init(UniqueId query_id,
                                                const TRuntimeFilterParams& runtime_filter_params,
                                                const TQueryOptions& query_options) {
    _query_id = query_id;
    _mem_tracker = std::make_shared<MemTracker>("RuntimeFilterMergeControllerEntity(experimental)",
                                                ExecEnv::GetInstance()->details_mem_tracker_set());
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    if (runtime_filter_params.__isset.rid_to_runtime_filter) {
        for (auto& filterid_to_desc : runtime_filter_params.rid_to_runtime_filter) {
            int filter_id = filterid_to_desc.first;
            const auto& target_iter = runtime_filter_params.rid_to_target_param.find(filter_id);
            if (target_iter == runtime_filter_params.rid_to_target_param.end() &&
                !runtime_filter_params.__isset.rid_to_target_paramv2) {
                // This runtime filter has to target info
                return Status::InternalError("runtime filter params meet error");
            } else if (target_iter == runtime_filter_params.rid_to_target_param.end()) {
                const auto& targetv2_iter =
                        runtime_filter_params.rid_to_target_paramv2.find(filter_id);
                if (targetv2_iter == runtime_filter_params.rid_to_target_paramv2.end()) {
                    // This runtime filter has to target info
                    return Status::InternalError("runtime filter params meet error");
                }
                const auto& build_iter =
                        runtime_filter_params.runtime_filter_builder_num.find(filter_id);
                if (build_iter == runtime_filter_params.runtime_filter_builder_num.end()) {
                    // This runtime filter has to builder info
                    return Status::InternalError("runtime filter params meet error");
                }

                RETURN_IF_ERROR(_init_with_desc(&filterid_to_desc.second, &query_options,
                                                &targetv2_iter->second, build_iter->second));
            } else {
                const auto& build_iter =
                        runtime_filter_params.runtime_filter_builder_num.find(filter_id);
                if (build_iter == runtime_filter_params.runtime_filter_builder_num.end()) {
                    return Status::InternalError("runtime filter params meet error");
                }
                RETURN_IF_ERROR(_init_with_desc(&filterid_to_desc.second, &query_options,
                                                &target_iter->second, build_iter->second));
            }
        }
    }
    return Status::OK();
}

Status RuntimeFilterMergeControllerEntity::send_filter_size(const PSendFilterSizeRequest* request) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    std::shared_ptr<RuntimeFilterCntlVal> cnt_val;

    auto filter_id = request->filter_id();
    std::map<int, CntlValwithLock>::iterator iter;
    {
        std::shared_lock<std::shared_mutex> guard(_filter_map_mutex);
        iter = _filter_map.find(filter_id);
        if (iter == _filter_map.end()) {
            return Status::InvalidArgument("unknown filter id {}",
                                           std::to_string(request->filter_id()));
        }
    }
    cnt_val = iter->second.cnt_val;
    std::unique_lock<std::mutex> l(*iter->second.mutex);
    cnt_val->global_size += request->filter_size();
    cnt_val->source_addrs.push_back(request->source_addr());

    if (cnt_val->source_addrs.size() == cnt_val->producer_size) {
        for (auto addr : cnt_val->source_addrs) {
            std::shared_ptr<PBackendService_Stub> stub(
                    ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(addr));
            AsyncRPCContext<PSyncFilterSizeRequest, PSyncFilterSizeResponse> ctx;
            auto* pquery_id = ctx.request.mutable_query_id();
            pquery_id->set_hi(_state->query_id.hi());
            pquery_id->set_lo(_state->query_id.lo());

            ctx.request.set_filter_id(filter_id);
            ctx.request.set_filter_size(cnt_val->global_size);

            stub->sync_filter_size(&ctx.cntl, &ctx.request, &ctx.response, brpc::DoNothing());
            brpc::Join(ctx.cntl.call_id());
            if (auto status = Status::create(ctx.response.status()); !status) {
                return status;
            }
            if (ctx.cntl.Failed()) {
                ExecEnv::GetInstance()->brpc_internal_client_cache()->erase(ctx.cntl.remote_side());
                return Status::InternalError(ctx.cntl.ErrorText());
            }
        }
    }
    return Status::OK();
}

Status RuntimeFilterMgr::sync_filter_size(const PSyncFilterSizeRequest* request) {
    auto* filter = try_get_product_filter(request->filter_id());
    if (filter) {
        filter->set_synced_size(request->filter_size());
        return Status::OK();
    }

    LocalMergeFilters* local_merge_filters = nullptr;
    RETURN_IF_ERROR(get_local_merge_producer_filters(request->filter_id(), &local_merge_filters));
    // first filter size merged filter
    for (size_t i = 1; i < local_merge_filters->filters.size(); i++) {
        local_merge_filters->filters[i]->set_synced_size(request->filter_size());
    }
    return Status::OK();
}

// merge data
Status RuntimeFilterMergeControllerEntity::merge(const PMergeFilterRequest* request,
                                                 butil::IOBufAsZeroCopyInputStream* attach_data,
                                                 bool opt_remote_rf) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    std::shared_ptr<RuntimeFilterCntlVal> cnt_val;
    int merged_size = 0;
    int64_t merge_time = 0;
    int64_t start_merge = MonotonicMillis();
    auto filter_id = request->filter_id();
    std::map<int, CntlValwithLock>::iterator iter;
    {
        std::shared_lock<std::shared_mutex> guard(_filter_map_mutex);
        iter = _filter_map.find(filter_id);
        VLOG_ROW << "recv filter id:" << request->filter_id() << " " << request->ShortDebugString();
        if (iter == _filter_map.end()) {
            return Status::InvalidArgument("unknown filter id {}",
                                           std::to_string(request->filter_id()));
        }
    }
    cnt_val = iter->second.cnt_val;
    {
        std::lock_guard<std::mutex> l(*iter->second.mutex);
        // Skip the other broadcast join runtime filter
        if (cnt_val->arrive_id.size() == 1 && cnt_val->runtime_filter_desc.is_broadcast_join) {
            return Status::OK();
        }
        MergeRuntimeFilterParams params(request, attach_data);
        ObjectPool* pool = cnt_val->pool.get();
        RuntimeFilterWrapperHolder holder;
        RETURN_IF_ERROR(IRuntimeFilter::create_wrapper(&params, pool, holder.getHandle()));

        RETURN_IF_ERROR(cnt_val->filter->merge_from(holder.getHandle()->get()));
        cnt_val->arrive_id.insert(UniqueId(request->fragment_instance_id()));
        merged_size = cnt_val->arrive_id.size();
        // TODO: avoid log when we had acquired a lock
        VLOG_ROW << "merge size:" << merged_size << ":" << cnt_val->producer_size;
        DCHECK_LE(merged_size, cnt_val->producer_size);
        cnt_val->merge_time += (MonotonicMillis() - start_merge);
        merge_time = cnt_val->merge_time;
    }

    if (merged_size == cnt_val->producer_size) {
        if (opt_remote_rf) {
            DCHECK_GT(cnt_val->targetv2_info.size(), 0);
            // Optimize merging phase iff:
            // 1. All BE has been upgraded (e.g. _opt_remote_rf)
            // 2. FE has been upgraded (e.g. cnt_val->targetv2_info.size() > 0)
            // 3. This filter is bloom filter (only bloom filter should be used for merging)
            using PPublishFilterRpcContext =
                    AsyncRPCContext<PPublishFilterRequestV2, PPublishFilterResponse>;
            std::vector<std::unique_ptr<PPublishFilterRpcContext>> rpc_contexts;
            rpc_contexts.reserve(cnt_val->targetv2_info.size());

            butil::IOBuf request_attachment;

            PPublishFilterRequestV2 apply_request;
            // serialize filter
            void* data = nullptr;
            int len = 0;
            bool has_attachment = false;
            if (!cnt_val->filter->get_ignored()) {
                RETURN_IF_ERROR(cnt_val->filter->serialize(&apply_request, &data, &len));
            } else {
                apply_request.set_ignored(true);
                apply_request.set_filter_type(PFilterType::UNKNOW_FILTER);
            }

            if (data != nullptr && len > 0) {
                request_attachment.append(data, len);
                has_attachment = true;
            }

            std::vector<TRuntimeFilterTargetParamsV2>& targets = cnt_val->targetv2_info;
            for (size_t i = 0; i < targets.size(); i++) {
                rpc_contexts.emplace_back(new PPublishFilterRpcContext);
                size_t cur = rpc_contexts.size() - 1;
                rpc_contexts[cur]->request = apply_request;
                rpc_contexts[cur]->request.set_filter_id(request->filter_id());
                rpc_contexts[cur]->request.set_is_pipeline(request->has_is_pipeline() &&
                                                           request->is_pipeline());
                rpc_contexts[cur]->request.set_merge_time(merge_time);
                *rpc_contexts[cur]->request.mutable_query_id() = request->query_id();
                if (has_attachment) {
                    rpc_contexts[cur]->cntl.request_attachment().append(request_attachment);
                }
                rpc_contexts[cur]->cid = rpc_contexts[cur]->cntl.call_id();
                // set fragment-id
                for (size_t fid = 0; fid < targets[cur].target_fragment_instance_ids.size();
                     fid++) {
                    PUniqueId* cur_id = rpc_contexts[cur]->request.add_fragment_instance_ids();
                    cur_id->set_hi(targets[cur].target_fragment_instance_ids[fid].hi);
                    cur_id->set_lo(targets[cur].target_fragment_instance_ids[fid].lo);
                }

                std::shared_ptr<PBackendService_Stub> stub(
                        ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(
                                targets[i].target_fragment_instance_addr));
                VLOG_NOTICE << "send filter " << rpc_contexts[cur]->request.filter_id()
                            << " to:" << targets[i].target_fragment_instance_addr.hostname << ":"
                            << targets[i].target_fragment_instance_addr.port
                            << rpc_contexts[cur]->request.ShortDebugString();
                if (stub == nullptr) {
                    rpc_contexts.pop_back();
                    continue;
                }
                stub->apply_filterv2(&rpc_contexts[cur]->cntl, &rpc_contexts[cur]->request,
                                     &rpc_contexts[cur]->response, brpc::DoNothing());
            }
            for (auto& rpc_context : rpc_contexts) {
                brpc::Join(rpc_context->cid);
                if (auto status = Status::create(rpc_context->response.status()); !status) {
                    return status;
                }
                if (rpc_context->cntl.Failed()) {
                    LOG(WARNING) << "runtimefilter rpc err:" << rpc_context->cntl.ErrorText();
                    ExecEnv::GetInstance()->brpc_internal_client_cache()->erase(
                            rpc_context->cntl.remote_side());
                }
            }
        } else {
            // prepare rpc context
            using PPublishFilterRpcContext =
                    AsyncRPCContext<PPublishFilterRequest, PPublishFilterResponse>;
            std::vector<std::unique_ptr<PPublishFilterRpcContext>> rpc_contexts;
            rpc_contexts.reserve(cnt_val->target_info.size());

            butil::IOBuf request_attachment;

            PPublishFilterRequest apply_request;
            // serialize filter
            void* data = nullptr;
            int len = 0;
            bool has_attachment = false;
            if (!cnt_val->filter->get_ignored()) {
                RETURN_IF_ERROR(cnt_val->filter->serialize(&apply_request, &data, &len));
            } else {
                apply_request.set_ignored(true);
                apply_request.set_filter_type(PFilterType::UNKNOW_FILTER);
            }

            if (data != nullptr && len > 0) {
                request_attachment.append(data, len);
                has_attachment = true;
            }

            std::vector<TRuntimeFilterTargetParams>& targets = cnt_val->target_info;
            for (size_t i = 0; i < targets.size(); i++) {
                rpc_contexts.emplace_back(new PPublishFilterRpcContext);
                size_t cur = rpc_contexts.size() - 1;
                rpc_contexts[cur]->request = apply_request;
                rpc_contexts[cur]->request.set_filter_id(request->filter_id());
                rpc_contexts[cur]->request.set_is_pipeline(request->has_is_pipeline() &&
                                                           request->is_pipeline());
                rpc_contexts[cur]->request.set_merge_time(merge_time);
                *rpc_contexts[cur]->request.mutable_query_id() = request->query_id();
                if (has_attachment) {
                    rpc_contexts[cur]->cntl.request_attachment().append(request_attachment);
                }
                rpc_contexts[cur]->cid = rpc_contexts[cur]->cntl.call_id();
                // set fragment_instance_id
                auto request_fragment_instance_id =
                        rpc_contexts[cur]->request.mutable_fragment_instance_id();
                request_fragment_instance_id->set_hi(targets[cur].target_fragment_instance_id.hi);
                request_fragment_instance_id->set_lo(targets[cur].target_fragment_instance_id.lo);

                std::shared_ptr<PBackendService_Stub> stub(
                        ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(
                                targets[i].target_fragment_instance_addr));
                VLOG_NOTICE << "send filter " << rpc_contexts[cur]->request.filter_id()
                            << " to:" << targets[i].target_fragment_instance_addr.hostname << ":"
                            << targets[i].target_fragment_instance_addr.port
                            << rpc_contexts[cur]->request.ShortDebugString();
                if (stub == nullptr) {
                    rpc_contexts.pop_back();
                    continue;
                }
                stub->apply_filter(&rpc_contexts[cur]->cntl, &rpc_contexts[cur]->request,
                                   &rpc_contexts[cur]->response, brpc::DoNothing());
            }
            for (auto& rpc_context : rpc_contexts) {
                brpc::Join(rpc_context->cid);
                if (rpc_context->cntl.Failed()) {
                    LOG(WARNING) << "runtimefilter rpc err:" << rpc_context->cntl.ErrorText();
                    ExecEnv::GetInstance()->brpc_internal_client_cache()->erase(
                            rpc_context->cntl.remote_side());
                }
            }
        }
    }
    return Status::OK();
}

Status RuntimeFilterMergeController::acquire(
        UniqueId query_id, std::shared_ptr<RuntimeFilterMergeControllerEntity>* handle) {
    uint32_t shard = _get_controller_shard_idx(query_id);
    std::lock_guard<std::mutex> guard(_controller_mutex[shard]);
    auto iter = _filter_controller_map[shard].find(query_id);
    if (iter == _filter_controller_map[shard].end()) {
        LOG(WARNING) << "not found entity, query-id:" << query_id.to_string();
        return Status::InvalidArgument("not found entity");
    }
    *handle = _filter_controller_map[shard][query_id].lock();
    if (*handle == nullptr) {
        return Status::InvalidArgument("entity is closed");
    }
    return Status::OK();
}

void RuntimeFilterMergeController::remove_entity(UniqueId query_id) {
    uint32_t shard = _get_controller_shard_idx(query_id);
    std::lock_guard<std::mutex> guard(_controller_mutex[shard]);
    _filter_controller_map[shard].erase(query_id);
}

RuntimeFilterParamsContext* RuntimeFilterParamsContext::create(RuntimeState* state) {
    RuntimeFilterParamsContext* params = state->obj_pool()->add(new RuntimeFilterParamsContext());
    params->runtime_filter_wait_infinitely = state->runtime_filter_wait_infinitely();
    params->runtime_filter_wait_time_ms = state->runtime_filter_wait_time_ms();
    params->enable_pipeline_exec = state->enable_pipeline_exec();
    params->execution_timeout = state->execution_timeout();
    params->runtime_filter_mgr = state->local_runtime_filter_mgr();
    params->exec_env = state->exec_env();
    params->query_id.set_hi(state->query_id().hi);
    params->query_id.set_lo(state->query_id().lo);

    params->be_exec_version = state->be_exec_version();
    params->query_ctx = state->get_query_ctx();
    return params;
}

RuntimeFilterParamsContext* RuntimeFilterParamsContext::create(QueryContext* query_ctx) {
    RuntimeFilterParamsContext* params = query_ctx->obj_pool.add(new RuntimeFilterParamsContext());
    params->runtime_filter_wait_infinitely = query_ctx->runtime_filter_wait_infinitely();
    params->runtime_filter_wait_time_ms = query_ctx->runtime_filter_wait_time_ms();
    params->enable_pipeline_exec = query_ctx->enable_pipeline_exec();
    params->execution_timeout = query_ctx->execution_timeout();
    params->runtime_filter_mgr = query_ctx->runtime_filter_mgr();
    params->exec_env = query_ctx->exec_env();
    params->query_id.set_hi(query_ctx->query_id().hi);
    params->query_id.set_lo(query_ctx->query_id().lo);

    params->be_exec_version = query_ctx->be_exec_version();
    params->query_ctx = query_ctx;
    return params;
}

} // namespace doris
