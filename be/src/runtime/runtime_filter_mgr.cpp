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

#include <string>

#include "client_cache.h"
#include "exprs/runtime_filter.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_state.h"
#include "service/brpc.h"
#include "util/brpc_client_cache.h"
#include "util/time.h"

namespace doris {

template <class RPCRequest, class RPCResponse>
struct async_rpc_context {
    RPCRequest request;
    RPCResponse response;
    brpc::Controller cntl;
    brpc::CallId cid;
};

RuntimeFilterMgr::RuntimeFilterMgr(const UniqueId& query_id, RuntimeState* state) : _state(state) {}

RuntimeFilterMgr::~RuntimeFilterMgr() {}

Status RuntimeFilterMgr::init() {
    DCHECK(_state->instance_mem_tracker().get() != nullptr);
    _tracker = _state->instance_mem_tracker().get();
    return Status::OK();
}

Status RuntimeFilterMgr::get_filter_by_role(const int filter_id, const RuntimeFilterRole role,
                                            IRuntimeFilter** target) {
    int32_t key = filter_id;
    std::map<int32_t, RuntimeFilterMgrVal>* filter_map = nullptr;

    if (role == RuntimeFilterRole::CONSUMER) {
        filter_map = &_consumer_map;
    } else {
        filter_map = &_producer_map;
    }

    auto iter = filter_map->find(key);
    if (iter == filter_map->end()) {
        LOG(WARNING) << "unknown filter...:" << key << ",role:" << (int)role;
        return Status::InvalidArgument("unknown filter");
    }
    *target = iter->second.filter;
    return Status::OK();
}

Status RuntimeFilterMgr::get_consume_filter(const int filter_id, IRuntimeFilter** consumer_filter) {
    return get_filter_by_role(filter_id, RuntimeFilterRole::CONSUMER, consumer_filter);
}

Status RuntimeFilterMgr::get_producer_filter(const int filter_id,
                                             IRuntimeFilter** producer_filter) {
    return get_filter_by_role(filter_id, RuntimeFilterRole::PRODUCER, producer_filter);
}

Status RuntimeFilterMgr::regist_filter(const RuntimeFilterRole role, const TRuntimeFilterDesc& desc,
                                       const TQueryOptions& options, int node_id) {
    DCHECK((role == RuntimeFilterRole::CONSUMER && node_id >= 0) ||
           role != RuntimeFilterRole::CONSUMER);
    int32_t key = desc.filter_id;

    std::map<int32_t, RuntimeFilterMgrVal>* filter_map = nullptr;
    if (role == RuntimeFilterRole::CONSUMER) {
        filter_map = &_consumer_map;
    } else {
        filter_map = &_producer_map;
    }
    // LOG(INFO) << "regist filter...:" << key << ",role:" << role;

    auto iter = filter_map->find(key);
    if (iter != filter_map->end()) {
        return Status::InvalidArgument("filter has registed");
    }

    RuntimeFilterMgrVal filter_mgr_val;
    filter_mgr_val.role = role;

    RETURN_IF_ERROR(IRuntimeFilter::create(_state, _tracker, &_pool, &desc, &options,
                                           role, node_id, &filter_mgr_val.filter));

    filter_map->emplace(key, filter_mgr_val);

    return Status::OK();
}

Status RuntimeFilterMgr::update_filter(const PPublishFilterRequest* request, const char* data) {
    UpdateRuntimeFilterParams params;
    params.request = request;
    params.data = data;
    params.pool = &_pool;
    int filter_id = request->filter_id();
    IRuntimeFilter* real_filter = nullptr;
    RETURN_IF_ERROR(get_consume_filter(filter_id, &real_filter));
    return real_filter->update_filter(&params);
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
        const TRuntimeFilterDesc* runtime_filter_desc,
        const TQueryOptions* query_options,
        const std::vector<doris::TRuntimeFilterTargetParams>* target_info,
        const int producer_size) {
    std::lock_guard<std::mutex> guard(_filter_map_mutex);
    std::shared_ptr<RuntimeFilterCntlVal> cntVal = std::make_shared<RuntimeFilterCntlVal>();
    // runtime_filter_desc and target will be released,
    // so we need to copy to cntVal
    // TODO: tracker should add a name
    cntVal->producer_size = producer_size;
    cntVal->runtime_filter_desc = *runtime_filter_desc;
    cntVal->target_info = *target_info;
    cntVal->pool.reset(new ObjectPool());
    cntVal->tracker = MemTracker::CreateTracker();
    cntVal->filter = cntVal->pool->add(
            new IRuntimeFilter(nullptr, cntVal->tracker.get(), cntVal->pool.get()));

    std::string filter_id = std::to_string(runtime_filter_desc->filter_id);
    // LOG(INFO) << "entity filter id:" << filter_id;
    cntVal->filter->init_with_desc(&cntVal->runtime_filter_desc, query_options, _fragment_instance_id);
    _filter_map.emplace(filter_id, cntVal);
    return Status::OK();
}

Status RuntimeFilterMergeControllerEntity::init(UniqueId query_id, UniqueId fragment_instance_id,
                                                const TRuntimeFilterParams& runtime_filter_params,
                                                const TQueryOptions& query_options) {
    _query_id = query_id;
    _fragment_instance_id = fragment_instance_id;
    for (auto& filterid_to_desc : runtime_filter_params.rid_to_runtime_filter) {
        int filter_id = filterid_to_desc.first;
        const auto& target_iter = runtime_filter_params.rid_to_target_param.find(filter_id);
        if (target_iter == runtime_filter_params.rid_to_target_param.end()) {
            return Status::InternalError("runtime filter params meet error");
        }
        const auto& build_iter = runtime_filter_params.runtime_filter_builder_num.find(filter_id);
        if (build_iter == runtime_filter_params.runtime_filter_builder_num.end()) {
            return Status::InternalError("runtime filter params meet error");
        }
        _init_with_desc(&filterid_to_desc.second, &query_options, &target_iter->second, build_iter->second);
    }
    return Status::OK();
}

// merge data
Status RuntimeFilterMergeControllerEntity::merge(const PMergeFilterRequest* request,
                                                 const char* data) {
    std::shared_ptr<RuntimeFilterCntlVal> cntVal;
    int merged_size = 0;
    {
        std::lock_guard<std::mutex> guard(_filter_map_mutex);
        auto iter = _filter_map.find(std::to_string(request->filter_id()));
        VLOG_ROW << "recv filter id:" << request->filter_id() << " " << request->ShortDebugString();
        if (iter == _filter_map.end()) {
            LOG(WARNING) << "unknown filter id:" << std::to_string(request->filter_id());
            return Status::InvalidArgument("unknown filter id");
        }
        cntVal = iter->second;
        MergeRuntimeFilterParams params;
        params.data = data;
        params.request = request;
        std::shared_ptr<MemTracker> tracker = iter->second->tracker;
        ObjectPool* pool = iter->second->pool.get();
        RuntimeFilterWrapperHolder holder;
        RETURN_IF_ERROR(
                IRuntimeFilter::create_wrapper(&params, tracker.get(), pool, holder.getHandle()));
        RETURN_IF_ERROR(cntVal->filter->merge_from(holder.getHandle()->get()));
        cntVal->arrive_id.insert(UniqueId(request->fragment_id()).to_string());
        merged_size = cntVal->arrive_id.size();
        // TODO: avoid log when we had acquired a lock
        VLOG_ROW << "merge size:" << merged_size << ":" << cntVal->producer_size;
        DCHECK_LE(merged_size, cntVal->producer_size);
        if (merged_size < cntVal->producer_size) {
            return Status::OK();
        }
    }

    if (merged_size == cntVal->producer_size) {
        // prepare rpc context
        using PPublishFilterRpcContext =
                async_rpc_context<PPublishFilterRequest, PPublishFilterResponse>;
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

        // TODO: async send publish rpc
        std::vector<TRuntimeFilterTargetParams>& targets = cntVal->target_info;
        for (size_t i = 0; i < targets.size(); i++) {
            rpc_contexts.emplace_back(new PPublishFilterRpcContext);
            rpc_contexts[i]->request = apply_request;
            rpc_contexts[i]->request.set_filter_id(request->filter_id());
            *rpc_contexts[i]->request.mutable_query_id() = request->query_id();
            if (has_attachment) {
                rpc_contexts[i]->cntl.request_attachment().append(request_attachment);
            }

            // set fragment-id
            auto request_fragment_id = rpc_contexts[i]->request.mutable_fragment_id();
            request_fragment_id->set_hi(targets[i].target_fragment_instance_id.hi);
            request_fragment_id->set_lo(targets[i].target_fragment_instance_id.lo);

            std::shared_ptr<PBackendService_Stub> stub(
                    ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(
                            targets[i].target_fragment_instance_addr));
            VLOG_NOTICE << "send filter " << rpc_contexts[i]->request.filter_id()
                        << " to:" << targets[i].target_fragment_instance_addr.hostname << ":"
                        << targets[i].target_fragment_instance_addr.port
                        << rpc_contexts[i]->request.ShortDebugString();
            if (stub == nullptr) {
                rpc_contexts.pop_back();
                continue;
            }
            stub->apply_filter(&rpc_contexts[i]->cntl, &rpc_contexts[i]->request,
                               &rpc_contexts[i]->response, nullptr);
        }
        /// TODO: use async and join rpc
    }
    return Status::OK();
}

Status RuntimeFilterMergeController::add_entity(
        const TExecPlanFragmentParams& params,
        std::shared_ptr<RuntimeFilterMergeControllerEntity>* handle) {
    runtime_filter_merge_entity_closer entity_closer =
            std::bind(runtime_filter_merge_entity_close, this, std::placeholders::_1);

    std::lock_guard<std::mutex> guard(_controller_mutex);
    UniqueId query_id(params.params.query_id);
    std::string query_id_str = query_id.to_string();
    auto iter = _filter_controller_map.find(query_id_str);
    UniqueId fragment_instance_id = UniqueId(params.params.fragment_instance_id);

    if (iter == _filter_controller_map.end()) {
        *handle = std::shared_ptr<RuntimeFilterMergeControllerEntity>(
                new RuntimeFilterMergeControllerEntity(), entity_closer);
        _filter_controller_map[query_id_str] = *handle;
        const TRuntimeFilterParams& filter_params = params.params.runtime_filter_params;
        if (params.params.__isset.runtime_filter_params) {
            RETURN_IF_ERROR(handle->get()->init(query_id, fragment_instance_id, filter_params, params.query_options));
        }
    } else {
        *handle = _filter_controller_map[query_id_str].lock();
    }
    return Status::OK();
}

Status RuntimeFilterMergeController::acquire(
        UniqueId query_id, std::shared_ptr<RuntimeFilterMergeControllerEntity>* handle) {
    std::lock_guard<std::mutex> guard(_controller_mutex);
    std::string query_id_str = query_id.to_string();
    auto iter = _filter_controller_map.find(query_id_str);
    if (iter == _filter_controller_map.end()) {
        LOG(WARNING) << "not found entity, query-id:" << query_id_str;
        return Status::InvalidArgument("not found entity");
    }
    *handle = _filter_controller_map[query_id_str].lock();
    if (*handle == nullptr) {
        return Status::InvalidArgument("entity is closed");
    }
    return Status::OK();
}

Status RuntimeFilterMergeController::remove_entity(UniqueId queryId) {
    std::lock_guard<std::mutex> guard(_controller_mutex);
    _filter_controller_map.erase(queryId.to_string());
    return Status::OK();
}

// auto called while call ~std::shared_ptr<RuntimeFilterMergeControllerEntity>
void runtime_filter_merge_entity_close(RuntimeFilterMergeController* controller,
                                       RuntimeFilterMergeControllerEntity* entity) {
    controller->remove_entity(entity->query_id());
    delete entity;
}

} // namespace doris
