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
#include "util/spinlock.h"

namespace doris {

template <class RPCRequest, class RPCResponse>
struct AsyncRPCContext {
    RPCRequest request;
    RPCResponse response;
    brpc::Controller cntl;
    brpc::CallId cid;
};

RuntimeFilterMgr::RuntimeFilterMgr(const UniqueId& query_id, RuntimeState* state) : _state(state) {}

RuntimeFilterMgr::RuntimeFilterMgr(const UniqueId& query_id, QueryContext* query_ctx)
        : _query_ctx(query_ctx) {}

Status RuntimeFilterMgr::init() {
    _tracker = std::make_unique<MemTracker>("RuntimeFilterMgr",
                                            ExecEnv::GetInstance()->experimental_mem_tracker());
    return Status::OK();
}

Status RuntimeFilterMgr::get_producer_filter(const int filter_id, IRuntimeFilter** target) {
    int32_t key = filter_id;

    std::lock_guard<std::mutex> l(_lock);
    auto iter = _producer_map.find(key);
    if (iter == _producer_map.end()) {
        return Status::InvalidArgument("unknown filter: {}, role: PRODUCER", key);
    }

    *target = iter->second;
    return Status::OK();
}

Status RuntimeFilterMgr::get_consume_filter(const int filter_id, const int node_id,
                                            IRuntimeFilter** consumer_filter) {
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _consumer_map.find(filter_id);
    if (iter != _consumer_map.cend()) {
        for (auto& item : iter->second) {
            if (item.node_id == node_id) {
                *consumer_filter = item.filter;
                return Status::OK();
            }
        }
    }

    return Status::InvalidArgument("unknown filter, filter_id: {}, node_id: {}, role: CONSUMER",
                                   filter_id, node_id);
}

Status RuntimeFilterMgr::get_consume_filters(const int filter_id,
                                             std::vector<IRuntimeFilter*>& consumer_filters) {
    int32_t key = filter_id;
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _consumer_map.find(key);
    if (iter == _consumer_map.end()) {
        return Status::InvalidArgument("unknown filter: {}, role: CONSUMER", key);
    }
    for (auto& holder : iter->second) {
        consumer_filters.emplace_back(holder.filter);
    }
    return Status::OK();
}

Status RuntimeFilterMgr::register_consumer_filter(const TRuntimeFilterDesc& desc,
                                                  const TQueryOptions& options, int node_id,
                                                  bool build_bf_exactly) {
    SCOPED_CONSUME_MEM_TRACKER(_tracker.get());
    int32_t key = desc.filter_id;

    std::lock_guard<std::mutex> l(_lock);
    auto iter = _consumer_map.find(key);
    if (desc.__isset.opt_remote_rf && desc.opt_remote_rf && desc.has_remote_targets &&
        desc.type == TRuntimeFilterType::BLOOM) {
        // if this runtime filter has remote target (e.g. need merge), we reuse the runtime filter between all instances
        DCHECK(_query_ctx != nullptr);

        iter = _consumer_map.find(key);
        if (iter != _consumer_map.end()) {
            for (auto holder : iter->second) {
                if (holder.node_id == node_id) {
                    return Status::OK();
                }
            }
        }
        IRuntimeFilter* filter;
        RETURN_IF_ERROR(IRuntimeFilter::create(_query_ctx, &_query_ctx->obj_pool, &desc, &options,
                                               RuntimeFilterRole::CONSUMER, node_id, &filter,
                                               build_bf_exactly));
        _consumer_map[key].emplace_back(node_id, filter);
    } else {
        DCHECK(_state != nullptr);

        if (iter != _consumer_map.end()) {
            for (auto holder : iter->second) {
                if (holder.node_id == node_id) {
                    return Status::InvalidArgument("filter has registered");
                }
            }
        }

        IRuntimeFilter* filter;
        RETURN_IF_ERROR(IRuntimeFilter::create(_state, &_pool, &desc, &options,
                                               RuntimeFilterRole::CONSUMER, node_id, &filter,
                                               build_bf_exactly));
        _consumer_map[key].emplace_back(node_id, filter);
    }
    return Status::OK();
}

Status RuntimeFilterMgr::register_producer_filter(const TRuntimeFilterDesc& desc,
                                                  const TQueryOptions& options,
                                                  bool build_bf_exactly) {
    SCOPED_CONSUME_MEM_TRACKER(_tracker.get());
    int32_t key = desc.filter_id;
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _producer_map.find(key);

    DCHECK(_state != nullptr);
    if (iter != _producer_map.end()) {
        return Status::InvalidArgument("filter has registed");
    }
    IRuntimeFilter* filter;
    RETURN_IF_ERROR(IRuntimeFilter::create(_state, &_pool, &desc, &options,
                                           RuntimeFilterRole::PRODUCER, -1, &filter,
                                           build_bf_exactly));
    _producer_map.emplace(key, filter);
    return Status::OK();
}

Status RuntimeFilterMgr::update_filter(const PPublishFilterRequest* request,
                                       butil::IOBufAsZeroCopyInputStream* data) {
    SCOPED_CONSUME_MEM_TRACKER(_tracker.get());
    UpdateRuntimeFilterParams params(request, data, &_pool);
    int filter_id = request->filter_id();
    std::vector<IRuntimeFilter*> filters;
    RETURN_IF_ERROR(get_consume_filters(filter_id, filters));
    for (auto filter : filters) {
        RETURN_IF_ERROR(filter->update_filter(&params));
    }

    return Status::OK();
}

void RuntimeFilterMgr::set_runtime_filter_params(
        const TRuntimeFilterParams& runtime_filter_params) {
    this->_merge_addr = runtime_filter_params.runtime_filter_merge_addr;
    this->_has_merge_addr = true;
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
    std::shared_ptr<RuntimeFilterCntlVal> cntVal = std::make_shared<RuntimeFilterCntlVal>();
    // runtime_filter_desc and target will be released,
    // so we need to copy to cntVal
    cntVal->producer_size = producer_size;
    cntVal->runtime_filter_desc = *runtime_filter_desc;
    cntVal->target_info = *target_info;
    cntVal->pool.reset(new ObjectPool());
    cntVal->filter = cntVal->pool->add(
            new IRuntimeFilter(_state, &_state->get_query_ctx()->obj_pool, runtime_filter_desc));

    auto filter_id = runtime_filter_desc->filter_id;
    // LOG(INFO) << "entity filter id:" << filter_id;
    static_cast<void>(
            cntVal->filter->init_with_desc(&cntVal->runtime_filter_desc, query_options, -1, false));
    _filter_map.emplace(filter_id, CntlValwithLock {cntVal, std::make_unique<SpinLock>()});
    return Status::OK();
}

Status RuntimeFilterMergeControllerEntity::_init_with_desc(
        const TRuntimeFilterDesc* runtime_filter_desc, const TQueryOptions* query_options,
        const std::vector<doris::TRuntimeFilterTargetParamsV2>* targetv2_info,
        const int producer_size) {
    std::unique_lock<std::shared_mutex> guard(_filter_map_mutex);
    std::shared_ptr<RuntimeFilterCntlVal> cntVal = std::make_shared<RuntimeFilterCntlVal>();
    // runtime_filter_desc and target will be released,
    // so we need to copy to cntVal
    cntVal->producer_size = producer_size;
    cntVal->runtime_filter_desc = *runtime_filter_desc;
    cntVal->targetv2_info = *targetv2_info;
    cntVal->pool.reset(new ObjectPool());
    cntVal->filter = cntVal->pool->add(
            new IRuntimeFilter(_state, &_state->get_query_ctx()->obj_pool, runtime_filter_desc));

    auto filter_id = runtime_filter_desc->filter_id;
    // LOG(INFO) << "entity filter id:" << filter_id;
    static_cast<void>(cntVal->filter->init_with_desc(&cntVal->runtime_filter_desc, query_options));
    _filter_map.emplace(filter_id, CntlValwithLock {cntVal, std::make_unique<SpinLock>()});
    return Status::OK();
}

Status RuntimeFilterMergeControllerEntity::init(UniqueId query_id, UniqueId fragment_instance_id,
                                                const TRuntimeFilterParams& runtime_filter_params,
                                                const TQueryOptions& query_options) {
    _query_id = query_id;
    _fragment_instance_id = fragment_instance_id;
    _mem_tracker = std::make_shared<MemTracker>("RuntimeFilterMergeControllerEntity",
                                                ExecEnv::GetInstance()->experimental_mem_tracker());
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

// merge data
Status RuntimeFilterMergeControllerEntity::merge(const PMergeFilterRequest* request,
                                                 butil::IOBufAsZeroCopyInputStream* attach_data,
                                                 bool opt_remote_rf) {
    _opt_remote_rf = _opt_remote_rf && opt_remote_rf;
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    std::shared_ptr<RuntimeFilterCntlVal> cntVal;
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
    // iter->second = pair{CntlVal,SpinLock}
    cntVal = iter->second.first;
    {
        std::lock_guard<SpinLock> l(*iter->second.second);
        MergeRuntimeFilterParams params(request, attach_data);
        ObjectPool* pool = cntVal->pool.get();
        RuntimeFilterWrapperHolder holder;
        RETURN_IF_ERROR(IRuntimeFilter::create_wrapper(_state, &params, pool, holder.getHandle()));
        RETURN_IF_ERROR(cntVal->filter->merge_from(holder.getHandle()->get()));
        cntVal->arrive_id.insert(UniqueId(request->fragment_id()));
        merged_size = cntVal->arrive_id.size();
        // TODO: avoid log when we had acquired a lock
        VLOG_ROW << "merge size:" << merged_size << ":" << cntVal->producer_size;
        DCHECK_LE(merged_size, cntVal->producer_size);
        cntVal->merge_time += (MonotonicMillis() - start_merge);
        if (merged_size < cntVal->producer_size) {
            return Status::OK();
        } else {
            merge_time = cntVal->merge_time;
        }
    }

    if (merged_size == cntVal->producer_size) {
        if (opt_remote_rf) {
            DCHECK_GT(cntVal->targetv2_info.size(), 0);
            DCHECK(cntVal->filter->is_bloomfilter());
            // Optimize merging phase iff:
            // 1. All BE has been upgraded (e.g. _opt_remote_rf)
            // 2. FE has been upgraded (e.g. cntVal->targetv2_info.size() > 0)
            // 3. This filter is bloom filter (only bloom filter should be used for merging)
            using PPublishFilterRpcContext =
                    AsyncRPCContext<PPublishFilterRequestV2, PPublishFilterResponse>;
            std::vector<std::unique_ptr<PPublishFilterRpcContext>> rpc_contexts;
            rpc_contexts.reserve(cntVal->targetv2_info.size());

            butil::IOBuf request_attachment;

            PPublishFilterRequestV2 apply_request;
            // serialize filter
            void* data = nullptr;
            int len = 0;
            bool has_attachment = false;
            RETURN_IF_ERROR(cntVal->filter->serialize(&apply_request, &data, &len));
            if (data != nullptr && len > 0) {
                request_attachment.append(data, len);
                has_attachment = true;
            }

            std::vector<TRuntimeFilterTargetParamsV2>& targets = cntVal->targetv2_info;
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
            rpc_contexts.reserve(cntVal->target_info.size());

            butil::IOBuf request_attachment;

            PPublishFilterRequest apply_request;
            // serialize filter
            void* data = nullptr;
            int len = 0;
            bool has_attachment = false;
            RETURN_IF_ERROR(cntVal->filter->serialize(&apply_request, &data, &len));
            if (data != nullptr && len > 0) {
                request_attachment.append(data, len);
                has_attachment = true;
            }

            std::vector<TRuntimeFilterTargetParams>& targets = cntVal->target_info;
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
                auto request_fragment_id = rpc_contexts[cur]->request.mutable_fragment_id();
                request_fragment_id->set_hi(targets[cur].target_fragment_instance_id.hi);
                request_fragment_id->set_lo(targets[cur].target_fragment_instance_id.lo);

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

Status RuntimeFilterMergeController::add_entity(
        const TExecPlanFragmentParams& params,
        std::shared_ptr<RuntimeFilterMergeControllerEntity>* handle, RuntimeState* state) {
    if (!params.params.__isset.runtime_filter_params ||
        params.params.runtime_filter_params.rid_to_runtime_filter.size() == 0) {
        return Status::OK();
    }

    runtime_filter_merge_entity_closer entity_closer =
            std::bind(runtime_filter_merge_entity_close, this, std::placeholders::_1);

    UniqueId query_id(params.params.query_id);
    std::string query_id_str = query_id.to_string();
    UniqueId fragment_instance_id = UniqueId(params.params.fragment_instance_id);
    uint32_t shard = _get_controller_shard_idx(query_id);
    std::lock_guard<std::mutex> guard(_controller_mutex[shard]);
    auto iter = _filter_controller_map[shard].find(query_id_str);

    if (iter == _filter_controller_map[shard].end()) {
        *handle = std::shared_ptr<RuntimeFilterMergeControllerEntity>(
                new RuntimeFilterMergeControllerEntity(state), entity_closer);
        _filter_controller_map[shard][query_id_str] = *handle;
        const TRuntimeFilterParams& filter_params = params.params.runtime_filter_params;
        RETURN_IF_ERROR(handle->get()->init(query_id, fragment_instance_id, filter_params,
                                            params.query_options));
    } else {
        *handle = _filter_controller_map[shard][query_id_str].lock();
    }
    return Status::OK();
}

Status RuntimeFilterMergeController::add_entity(
        const TPipelineFragmentParams& params, const TPipelineInstanceParams& local_params,
        std::shared_ptr<RuntimeFilterMergeControllerEntity>* handle, RuntimeState* state) {
    if (!local_params.__isset.runtime_filter_params ||
        local_params.runtime_filter_params.rid_to_runtime_filter.size() == 0) {
        return Status::OK();
    }

    runtime_filter_merge_entity_closer entity_closer =
            std::bind(runtime_filter_merge_entity_close, this, std::placeholders::_1);

    UniqueId query_id(params.query_id);
    std::string query_id_str = query_id.to_string();
    UniqueId fragment_instance_id = UniqueId(local_params.fragment_instance_id);
    uint32_t shard = _get_controller_shard_idx(query_id);
    std::lock_guard<std::mutex> guard(_controller_mutex[shard]);
    auto iter = _filter_controller_map[shard].find(query_id_str);

    if (iter == _filter_controller_map[shard].end()) {
        *handle = std::shared_ptr<RuntimeFilterMergeControllerEntity>(
                new RuntimeFilterMergeControllerEntity(state), entity_closer);
        _filter_controller_map[shard][query_id_str] = *handle;
        const TRuntimeFilterParams& filter_params = local_params.runtime_filter_params;
        RETURN_IF_ERROR(handle->get()->init(query_id, fragment_instance_id, filter_params,
                                            params.query_options));
    } else {
        *handle = _filter_controller_map[shard][query_id_str].lock();
    }
    return Status::OK();
}

Status RuntimeFilterMergeController::acquire(
        UniqueId query_id, std::shared_ptr<RuntimeFilterMergeControllerEntity>* handle) {
    uint32_t shard = _get_controller_shard_idx(query_id);
    std::lock_guard<std::mutex> guard(_controller_mutex[shard]);
    std::string query_id_str = query_id.to_string();
    auto iter = _filter_controller_map[shard].find(query_id_str);
    if (iter == _filter_controller_map[shard].end()) {
        LOG(WARNING) << "not found entity, query-id:" << query_id_str;
        return Status::InvalidArgument("not found entity");
    }
    *handle = _filter_controller_map[shard][query_id_str].lock();
    if (*handle == nullptr) {
        return Status::InvalidArgument("entity is closed");
    }
    return Status::OK();
}

Status RuntimeFilterMergeController::remove_entity(UniqueId query_id) {
    uint32_t shard = _get_controller_shard_idx(query_id);
    std::lock_guard<std::mutex> guard(_controller_mutex[shard]);
    _filter_controller_map[shard].erase(query_id.to_string());
    return Status::OK();
}

// auto called while call ~std::shared_ptr<RuntimeFilterMergeControllerEntity>
void runtime_filter_merge_entity_close(RuntimeFilterMergeController* controller,
                                       RuntimeFilterMergeControllerEntity* entity) {
    static_cast<void>(controller->remove_entity(entity->query_id()));
    delete entity;
}

} // namespace doris
