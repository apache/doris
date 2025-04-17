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

#include "runtime_filter/runtime_filter_mgr.h"

#include <brpc/controller.h>
#include <butil/iobuf.h>
#include <butil/iobuf_inl.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>

#include <ostream>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "runtime_filter/runtime_filter.h"
#include "runtime_filter/runtime_filter_consumer.h"
#include "runtime_filter/runtime_filter_merger.h"
#include "runtime_filter/runtime_filter_producer.h"
#include "util/brpc_client_cache.h"
#include "util/brpc_closure.h"

namespace doris {
#include "common/compile_check_begin.h"
RuntimeFilterMgr::RuntimeFilterMgr(const bool is_global)
        : _is_global(is_global),
          _tracker(std::make_unique<MemTracker>("RuntimeFilterMgr(experimental)")) {}

std::vector<std::shared_ptr<RuntimeFilterConsumer>> RuntimeFilterMgr::get_consume_filters(
        int filter_id) {
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _consumer_map.find(filter_id);
    if (iter == _consumer_map.end()) {
        return {};
    }
    return iter->second;
}

Status RuntimeFilterMgr::register_consumer_filter(
        const QueryContext* query_ctx, const TRuntimeFilterDesc& desc, int node_id,
        std::shared_ptr<RuntimeFilterConsumer>* consumer) {
    SCOPED_CONSUME_MEM_TRACKER(_tracker.get());
    int32_t key = desc.filter_id;

    std::lock_guard<std::mutex> l(_lock);
    RETURN_IF_ERROR(RuntimeFilterConsumer::create(query_ctx, &desc, node_id, consumer));
    _consumer_map[key].push_back(*consumer);
    return Status::OK();
}

Status RuntimeFilterMgr::register_local_merger_producer_filter(
        const QueryContext* query_ctx, const TRuntimeFilterDesc& desc,
        std::shared_ptr<RuntimeFilterProducer> producer) {
    if (!_is_global) [[unlikely]] {
        return Status::InternalError(
                "A local merge filter can not be registered in Local RuntimeFilterMgr");
    }
    if (producer == nullptr) [[unlikely]] {
        return Status::InternalError(
                "Producer should be created in local RuntimeFilterMgr before registered in "
                "Global "
                "RuntimeFilterMgr");
    }
    SCOPED_CONSUME_MEM_TRACKER(_tracker.get());
    int32_t key = desc.filter_id;

    LocalMergeContext* context;
    {
        std::lock_guard<std::mutex> l(_lock);
        context = &_local_merge_map[key]; // may inplace construct default object
    }

    RETURN_IF_ERROR(context->register_producer(query_ctx, &desc, producer));
    return Status::OK();
}

Status LocalMergeContext::register_producer(const QueryContext* query_ctx,
                                            const TRuntimeFilterDesc* desc,
                                            std::shared_ptr<RuntimeFilterProducer> producer) {
    std::lock_guard<std::mutex> l(mtx);
    if (!merger) {
        RETURN_IF_ERROR(RuntimeFilterMerger::create(query_ctx, desc, &merger));
    }
    producers.emplace_back(producer);
    merger->set_expected_producer_num(cast_set<int>(producers.size()));
    return Status::OK();
}

Status RuntimeFilterMgr::get_local_merge_producer_filters(int filter_id,
                                                          LocalMergeContext** local_merge_filters) {
    if (!_is_global) [[unlikely]] {
        return Status::InternalError(
                "A local merge filter can not be registered in Local RuntimeFilterMgr");
    }
    std::lock_guard<std::mutex> l(_lock);
    auto iter = _local_merge_map.find(filter_id);
    if (iter == _local_merge_map.end()) {
        return Status::InternalError(
                "get_local_merge_producer_filters meet unknown filter: {}, role: "
                "LOCAL_MERGE_PRODUCER.",
                filter_id);
    }
    *local_merge_filters = &iter->second;
    DCHECK(iter->second.merger);
    return Status::OK();
}

Status RuntimeFilterMgr::register_producer_filter(
        const QueryContext* query_ctx, const TRuntimeFilterDesc& desc,
        std::shared_ptr<RuntimeFilterProducer>* producer) {
    if (_is_global) [[unlikely]] {
        return Status::InternalError(
                "A local producer filter should not be registered in Global RuntimeFilterMgr");
    }
    SCOPED_CONSUME_MEM_TRACKER(_tracker.get());
    int32_t key = desc.filter_id;

    std::lock_guard<std::mutex> l(_lock);
    if (_producer_id_set.contains(key)) {
        return Status::InvalidArgument("filter {} has been registered", key);
    }
    RETURN_IF_ERROR(RuntimeFilterProducer::create(query_ctx, &desc, producer));
    _producer_id_set.insert(key);
    return Status::OK();
}

bool RuntimeFilterMgr::set_runtime_filter_params(
        const TRuntimeFilterParams& runtime_filter_params) {
    std::lock_guard l(_lock);
    if (!_has_merge_addr) {
        _merge_addr = runtime_filter_params.runtime_filter_merge_addr;
        _has_merge_addr = true;
        return true;
    }
    return false;
}

Status RuntimeFilterMgr::get_merge_addr(TNetworkAddress* addr) {
    if (_has_merge_addr) {
        *addr = this->_merge_addr;
        return Status::OK();
    }
    return Status::InternalError("not found merge addr");
}

Status RuntimeFilterMergeControllerEntity::_init_with_desc(
        std::shared_ptr<QueryContext> query_ctx, const TRuntimeFilterDesc* runtime_filter_desc,
        const std::vector<TRuntimeFilterTargetParamsV2>&& targetv2_info, const int producer_size) {
    auto filter_id = runtime_filter_desc->filter_id;
    GlobalMergeContext* cnt_val;
    {
        std::unique_lock<std::shared_mutex> guard(_filter_map_mutex);
        cnt_val = &_filter_map[filter_id]; // may inplace construct default object
    }

    // runtime_filter_desc and target will be released,
    // so we need to copy to cnt_val
    cnt_val->runtime_filter_desc = *runtime_filter_desc;
    cnt_val->targetv2_info = targetv2_info;
    RETURN_IF_ERROR(
            RuntimeFilterMerger::create(query_ctx.get(), runtime_filter_desc, &cnt_val->merger));
    cnt_val->merger->set_expected_producer_num(producer_size);

    return Status::OK();
}

Status RuntimeFilterMergeControllerEntity::init(std::shared_ptr<QueryContext> query_ctx,
                                                const TRuntimeFilterParams& runtime_filter_params) {
    _mem_tracker = std::make_shared<MemTracker>("RuntimeFilterMergeControllerEntity(experimental)");
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    if (runtime_filter_params.__isset.rid_to_runtime_filter) {
        for (const auto& filterid_to_desc : runtime_filter_params.rid_to_runtime_filter) {
            int filter_id = filterid_to_desc.first;
            const auto& targetv2_iter = runtime_filter_params.rid_to_target_paramv2.find(filter_id);
            const auto& build_iter =
                    runtime_filter_params.runtime_filter_builder_num.find(filter_id);
            if (build_iter == runtime_filter_params.runtime_filter_builder_num.end()) {
                // This runtime filter has no builder info
                return Status::InternalError(
                        "Runtime filter has a wrong parameter. Maybe FE version is "
                        "mismatched.");
            }

            RETURN_IF_ERROR(_init_with_desc(
                    query_ctx, &filterid_to_desc.second,
                    targetv2_iter == runtime_filter_params.rid_to_target_paramv2.end()
                            ? std::vector<TRuntimeFilterTargetParamsV2> {}
                            : targetv2_iter->second,
                    build_iter->second));
        }
    }
    return Status::OK();
}

Status RuntimeFilterMergeControllerEntity::send_filter_size(std::shared_ptr<QueryContext> query_ctx,
                                                            const PSendFilterSizeRequest* request) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);

    auto filter_id = request->filter_id();
    std::map<int, GlobalMergeContext>::iterator iter;
    {
        std::shared_lock<std::shared_mutex> guard(_filter_map_mutex);
        iter = _filter_map.find(filter_id);
        if (iter == _filter_map.end()) {
            return Status::InvalidArgument("unknown filter id {}",
                                           std::to_string(request->filter_id()));
        }
    }
    auto& cnt_val = iter->second;
    std::unique_lock<std::mutex> l(iter->second.mtx);
    cnt_val.source_addrs.push_back(request->source_addr());

    Status st = Status::OK();
    // After all runtime filters' size are collected, we should send response to all producers.
    if (cnt_val.merger->add_rf_size(request->filter_size())) {
        auto ctx = query_ctx->ignore_runtime_filter_error() ? std::weak_ptr<QueryContext> {}
                                                            : query_ctx;
        for (auto addr : cnt_val.source_addrs) {
            std::shared_ptr<PBackendService_Stub> stub(
                    ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(addr));
            if (stub == nullptr) {
                LOG(WARNING) << "Failed to init rpc to " << addr.hostname() << ":" << addr.port();
                st = Status::InternalError("Failed to init rpc to {}:{}", addr.hostname(),
                                           addr.port());
                continue;
            }

            auto closure = AutoReleaseClosure<PSyncFilterSizeRequest,
                                              DummyBrpcCallback<PSyncFilterSizeResponse>>::
                    create_unique(std::make_shared<PSyncFilterSizeRequest>(),
                                  DummyBrpcCallback<PSyncFilterSizeResponse>::create_shared(), ctx);

            auto* pquery_id = closure->request_->mutable_query_id();
            pquery_id->set_hi(query_ctx->query_id().hi);
            pquery_id->set_lo(query_ctx->query_id().lo);
            closure->cntl_->set_timeout_ms(
                    get_execution_rpc_timeout_ms(query_ctx->execution_timeout()));
            if (config::execution_ignore_eovercrowded) {
                closure->cntl_->ignore_eovercrowded();
            }

            closure->request_->set_filter_id(filter_id);
            closure->request_->set_filter_size(cnt_val.merger->get_received_sum_size());

            stub->sync_filter_size(closure->cntl_.get(), closure->request_.get(),
                                   closure->response_.get(), closure.get());
            closure.release();
        }
    }
    return st;
}

Status RuntimeFilterMgr::sync_filter_size(const PSyncFilterSizeRequest* request) {
    LocalMergeContext* local_merge_filters = nullptr;
    RETURN_IF_ERROR(get_local_merge_producer_filters(request->filter_id(), &local_merge_filters));
    for (auto producer : local_merge_filters->producers) {
        producer->set_synced_size(request->filter_size());
    }
    return Status::OK();
}

// merge data
Status RuntimeFilterMergeControllerEntity::merge(std::shared_ptr<QueryContext> query_ctx,
                                                 const PMergeFilterRequest* request,
                                                 butil::IOBufAsZeroCopyInputStream* attach_data) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    int64_t merge_time = 0;
    auto filter_id = request->filter_id();
    std::map<int, GlobalMergeContext>::iterator iter;
    Status st = Status::OK();
    {
        std::shared_lock<std::shared_mutex> guard(_filter_map_mutex);
        iter = _filter_map.find(filter_id);
        VLOG_ROW << "recv filter id:" << request->filter_id() << " " << request->ShortDebugString();
        if (iter == _filter_map.end()) {
            return Status::InvalidArgument("unknown filter id {}",
                                           std::to_string(request->filter_id()));
        }
    }
    auto& cnt_val = iter->second;
    bool is_ready = false;
    {
        std::lock_guard<std::mutex> l(iter->second.mtx);
        // Skip the other broadcast join runtime filter
        if (cnt_val.arrive_id.size() == 1 && cnt_val.runtime_filter_desc.is_broadcast_join) {
            return Status::OK();
        }
        std::shared_ptr<RuntimeFilterProducer> tmp_filter;
        RETURN_IF_ERROR(RuntimeFilterProducer::create(query_ctx.get(), &cnt_val.runtime_filter_desc,
                                                      &tmp_filter));

        RETURN_IF_ERROR(tmp_filter->assign(*request, attach_data));

        RETURN_IF_ERROR(cnt_val.merger->merge_from(tmp_filter.get()));

        cnt_val.arrive_id.insert(UniqueId(request->fragment_instance_id()));
        is_ready = cnt_val.merger->ready(); // update is_ready in locked scope
    }

    if (is_ready) {
        DCHECK_GT(cnt_val.targetv2_info.size(), 0);

        butil::IOBuf request_attachment;

        PPublishFilterRequestV2 apply_request;
        // serialize filter
        void* data = nullptr;
        int len = 0;
        bool has_attachment = false;

        RETURN_IF_ERROR(cnt_val.merger->serialize(&apply_request, &data, &len));

        if (data != nullptr && len > 0) {
            void* allocated = malloc(len);
            memcpy(allocated, data, len);
            // control the memory by doris self to avoid using brpc's thread local storage
            // because the memory of tls will not be released
            request_attachment.append_user_data(allocated, len, [](void* ptr) { free(ptr); });
            has_attachment = true;
        }

        auto ctx = query_ctx->ignore_runtime_filter_error() ? std::weak_ptr<QueryContext> {}
                                                            : query_ctx;
        std::vector<TRuntimeFilterTargetParamsV2>& targets = cnt_val.targetv2_info;

        for (auto& target : targets) {
            auto closure = AutoReleaseClosure<PPublishFilterRequestV2,
                                              DummyBrpcCallback<PPublishFilterResponse>>::
                    create_unique(std::make_shared<PPublishFilterRequestV2>(apply_request),
                                  DummyBrpcCallback<PPublishFilterResponse>::create_shared(), ctx);

            closure->request_->set_merge_time(merge_time);
            *closure->request_->mutable_query_id() = request->query_id();
            if (has_attachment) {
                closure->cntl_->request_attachment().append(request_attachment);
            }

            closure->cntl_->set_timeout_ms(
                    get_execution_rpc_timeout_ms(query_ctx->execution_timeout()));
            if (config::execution_ignore_eovercrowded) {
                closure->cntl_->ignore_eovercrowded();
            }

            // set fragment-id
            if (target.__isset.target_fragment_ids) {
                for (auto& target_fragment_id : target.target_fragment_ids) {
                    closure->request_->add_fragment_ids(target_fragment_id);
                }
            } else {
                // FE not upgraded yet.
                for (auto& target_fragment_instance_id : target.target_fragment_instance_ids) {
                    PUniqueId* cur_id = closure->request_->add_fragment_instance_ids();
                    cur_id->set_hi(target_fragment_instance_id.hi);
                    cur_id->set_lo(target_fragment_instance_id.lo);
                }
            }

            std::shared_ptr<PBackendService_Stub> stub(
                    ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(
                            target.target_fragment_instance_addr));
            if (stub == nullptr) {
                LOG(WARNING) << "Failed to init rpc to "
                             << target.target_fragment_instance_addr.hostname << ":"
                             << target.target_fragment_instance_addr.port;
                st = Status::InternalError("Failed to init rpc to {}:{}",
                                           target.target_fragment_instance_addr.hostname,
                                           target.target_fragment_instance_addr.port);
                continue;
            }
            stub->apply_filterv2(closure->cntl_.get(), closure->request_.get(),
                                 closure->response_.get(), closure.get());
            closure.release();
        }
    }
    return st;
}
} // namespace doris
