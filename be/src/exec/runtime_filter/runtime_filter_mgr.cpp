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

#include "exec/runtime_filter/runtime_filter_mgr.h"

#include <brpc/controller.h>
#include <butil/iobuf.h>
#include <butil/iobuf_inl.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/runtime_filter/runtime_filter.h"
#include "exec/runtime_filter/runtime_filter_consumer.h"
#include "exec/runtime_filter/runtime_filter_merger.h"
#include "exec/runtime_filter/runtime_filter_producer.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/brpc_client_cache.h"
#include "util/brpc_closure.h"
#include "util/uid_util.h"

namespace doris {

namespace {

std::vector<RuntimeFilterPublishTarget> build_runtime_filter_publish_targets(
        const std::vector<TRuntimeFilterTargetParamsV2>& targets) {
    std::vector<RuntimeFilterPublishTarget> publish_targets;
    publish_targets.reserve(targets.size());
    for (const auto& target : targets) {
        DORIS_CHECK(target.__isset.target_fragment_ids);
        DORIS_CHECK(!target.target_fragment_ids.empty());
        RuntimeFilterPublishTarget publish_target;
        publish_target.addr.set_hostname(target.target_fragment_instance_addr.hostname);
        publish_target.addr.set_port(target.target_fragment_instance_addr.port);
        publish_target.fragment_ids = target.target_fragment_ids;
        publish_targets.emplace_back(std::move(publish_target));
    }
    return publish_targets;
}

class RuntimeFilterRelayRpcClosure final : public google::protobuf::Closure {
public:
    RuntimeFilterRelayRpcClosure(std::shared_ptr<PPublishFilterRequestV2> request,
                                 std::weak_ptr<QueryContext> query_ctx)
            : _request(std::move(request)),
              _callback(HandleErrorBrpcCallback<PPublishFilterResponse>::create_shared(
                      std::move(query_ctx))) {}

    void Run() override {
        std::unique_ptr<RuntimeFilterRelayRpcClosure> self(this);
        _callback->call();
    }

    brpc::Controller* cntl() { return _callback->cntl_.get(); }
    PPublishFilterRequestV2* request() { return _request.get(); }
    PPublishFilterResponse* response() { return _callback->response_.get(); }

private:
    std::shared_ptr<PPublishFilterRequestV2> _request;
    std::shared_ptr<HandleErrorBrpcCallback<PPublishFilterResponse>> _callback;
};

Status send_runtime_filter_relay_rpc(const RuntimeFilterPublishTask& task,
                                     const butil::IOBuf& request_attachment, int timeout_ms,
                                     std::weak_ptr<QueryContext> query_ctx) {
    std::shared_ptr<PBackendService_Stub> stub(
            ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(task.receiver.addr));
    if (stub == nullptr) {
        LOG(WARNING) << "Failed to init runtime filter relay rpc to "
                     << task.receiver.addr.hostname() << ":" << task.receiver.addr.port();
        return Status::InternalError("Failed to init runtime filter relay rpc to {}:{}",
                                     task.receiver.addr.hostname(), task.receiver.addr.port());
    }

    // brpc calls Run() exactly once; RuntimeFilterRelayRpcClosure deletes itself there.
    auto* closure = new RuntimeFilterRelayRpcClosure(
            std::make_shared<PPublishFilterRequestV2>(task.request), std::move(query_ctx));
    if (!request_attachment.empty()) {
        closure->cntl()->request_attachment().append(request_attachment);
    }
    closure->cntl()->set_timeout_ms(timeout_ms);
    if (config::execution_ignore_eovercrowded) {
        closure->cntl()->ignore_eovercrowded();
    }
    stub->apply_filterv2(closure->cntl(), closure->request(), closure->response(), closure);
    return Status::OK();
}

void set_request_direct_publish_target(const TRuntimeFilterTargetParamsV2& target,
                                       PPublishFilterRequestV2* request) {
    DORIS_CHECK(target.__isset.target_fragment_ids);
    DORIS_CHECK(!target.target_fragment_ids.empty());
    for (const auto& target_fragment_id : target.target_fragment_ids) {
        request->add_fragment_ids(target_fragment_id);
    }
}

} // namespace

std::vector<std::vector<RuntimeFilterPublishTarget>> split_runtime_filter_publish_targets(
        const std::vector<RuntimeFilterPublishTarget>& targets, int fanout) {
    DORIS_CHECK(!targets.empty());
    DORIS_CHECK(fanout > 0);
    size_t slice_count = std::min(targets.size(), static_cast<size_t>(fanout));
    std::vector<std::vector<RuntimeFilterPublishTarget>> slices;
    slices.reserve(slice_count);
    for (size_t offset = 0; offset < targets.size();) {
        size_t remaining_targets = targets.size() - offset;
        size_t remaining_slices = slice_count - slices.size();
        size_t slice_size = (remaining_targets + remaining_slices - 1) / remaining_slices;
        slices.emplace_back(targets.begin() + offset, targets.begin() + offset + slice_size);
        offset += slice_size;
    }
    return slices;
}

std::vector<RuntimeFilterPublishTask> build_runtime_filter_publish_tasks(
        const PPublishFilterRequestV2& base_request,
        const std::vector<RuntimeFilterPublishTarget>& targets, int fanout) {
    std::vector<RuntimeFilterPublishTask> tasks;
    auto slices = split_runtime_filter_publish_targets(targets, fanout);
    PPublishFilterRequestV2 request_template = base_request;
    request_template.clear_fragment_ids();
    request_template.clear_fragment_instance_ids();
    request_template.clear_forward_targets();
    tasks.reserve(slices.size());
    for (const auto& slice : slices) {
        DORIS_CHECK(!slice.empty());
        RuntimeFilterPublishTask task;
        task.receiver = slice.front();
        task.request = request_template;

        for (int32_t fragment_id : task.receiver.fragment_ids) {
            task.request.add_fragment_ids(fragment_id);
        }
        for (size_t i = 1; i < slice.size(); ++i) {
            DORIS_CHECK(!slice[i].fragment_ids.empty());
            PPublishFilterForwardTarget* forward_target = task.request.add_forward_targets();
            forward_target->mutable_target_addr()->CopyFrom(slice[i].addr);
            for (int32_t fragment_id : slice[i].fragment_ids) {
                forward_target->add_fragment_ids(fragment_id);
            }
        }
        tasks.emplace_back(std::move(task));
    }
    return tasks;
}

int calculate_tree_publish_fanout(int64_t serialized_filter_size, size_t target_count,
                                  int64_t max_send_bytes) {
    DORIS_CHECK(serialized_filter_size > 0);
    DORIS_CHECK(max_send_bytes >= 0);
    if (max_send_bytes == 0 || target_count <= 1) {
        return 0;
    }

    const int64_t direct_target_limit = max_send_bytes / serialized_filter_size;
    if (target_count <= static_cast<size_t>(direct_target_limit)) {
        return 0;
    }

    return static_cast<int>(std::max<int64_t>(1, direct_target_limit));
}

Status forward_runtime_filter(const PPublishFilterRequestV2& request,
                              const butil::IOBuf& request_attachment,
                              std::weak_ptr<QueryContext> query_ctx) {
    if (request.forward_targets().empty()) {
        return Status::OK();
    }

    std::vector<RuntimeFilterPublishTarget> targets;
    targets.reserve(request.forward_targets_size());
    for (const auto& forward_target : request.forward_targets()) {
        DORIS_CHECK(forward_target.has_target_addr());
        DORIS_CHECK(forward_target.fragment_ids_size() > 0);
        RuntimeFilterPublishTarget target;
        target.addr.CopyFrom(forward_target.target_addr());
        target.fragment_ids.assign(forward_target.fragment_ids().begin(),
                                   forward_target.fragment_ids().end());
        targets.emplace_back(std::move(target));
    }

    DORIS_CHECK(request.has_tree_publish_fanout());
    int fanout = request.tree_publish_fanout();
    DORIS_CHECK(fanout > 0);
    DORIS_CHECK(request.has_publish_rpc_timeout_ms());
    int timeout_ms = request.publish_rpc_timeout_ms();
    auto tasks = build_runtime_filter_publish_tasks(request, targets, fanout);
    VLOG_NOTICE << "Runtime filter relay publish filter_id=" << request.filter_id()
                << ", forward_targets=" << targets.size() << ", child_rpc_count=" << tasks.size()
                << ", fanout=" << fanout;
    auto st = Status::OK();
    for (const auto& task : tasks) {
        Status rpc_st =
                send_runtime_filter_relay_rpc(task, request_attachment, timeout_ms, query_ctx);
        if (!rpc_st.ok()) {
            LOG(WARNING) << "Failed to forward runtime filter, query_id="
                         << print_id(request.query_id()) << ", filter_id=" << request.filter_id()
                         << ", target=" << task.receiver.addr.hostname() << ":"
                         << task.receiver.addr.port() << ", status=" << rpc_st;
            st = std::move(rpc_st);
        }
    }
    return st;
}

RuntimeFilterMgr::RuntimeFilterMgr(const bool is_global)
        : _is_global(is_global),
          _tracker(std::make_unique<MemTracker>(
                  fmt::format("RuntimeFilterMgr({})", is_global ? "global" : "local"))) {}

std::vector<std::shared_ptr<RuntimeFilterConsumer>> RuntimeFilterMgr::get_consume_filters(
        int filter_id) {
    LockGuard l(_lock);
    auto iter = _consumer_map.find(filter_id);
    if (iter == _consumer_map.end()) {
        return {};
    }
    return iter->second;
}

Status RuntimeFilterMgr::register_consumer_filter(
        const RuntimeState* state, const TRuntimeFilterDesc& desc, int node_id,
        std::shared_ptr<RuntimeFilterConsumer>* consumer) {
    SCOPED_CONSUME_MEM_TRACKER(_tracker.get());
    int32_t key = desc.filter_id;

    std::shared_ptr<RuntimeFilterConsumer> new_consumer;
    RETURN_IF_ERROR(RuntimeFilterConsumer::create(state, &desc, node_id, &new_consumer));
    {
        LockGuard l(_lock);
        _consumer_map[key].push_back(new_consumer);
    }
    *consumer = new_consumer;
    return Status::OK();
}

Status RuntimeFilterMgr::register_local_merge_producer_filter(
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
    uint32_t producer_stage = producer->stage();

    std::shared_ptr<LocalMergeContext> context;
    std::shared_ptr<RuntimeFilterMerger> merger;
    int expected_producer_num = 0;
    {
        LockGuard l(_lock);
        auto iter = _local_merge_map.find(key);
        if (iter == _local_merge_map.end() || !iter->second ||
            producer_stage > iter->second->stage) {
            auto new_context = std::make_shared<LocalMergeContext>();
            RETURN_IF_ERROR(RuntimeFilterMerger::create(query_ctx, &desc, &new_context->merger));
            new_context->stage = producer_stage;
            _local_merge_map.insert_or_assign(key, new_context);
            context = new_context;
        } else {
            context = iter->second;
        }

        context->producers.emplace_back(producer);
        merger = context->merger;
        expected_producer_num = cast_set<int>(context->producers.size());
    }

    merger->increase_expected_producer_num(expected_producer_num);
    // Sync the local merger's stage from the producer so that outgoing merge RPCs
    // (via _push_to_remote) carry the correct recursive CTE round number.
    merger->set_stage(producer_stage);
    return Status::OK();
}

Status RuntimeFilterMgr::get_local_merge_context(int filter_id, uint32_t expected_stage,
                                                 std::shared_ptr<LocalMergeContext>* context) {
    if (!_is_global) [[unlikely]] {
        return Status::InternalError(
                "A local merge filter can not be registered in Local RuntimeFilterMgr");
    }
    context->reset();
    LockGuard l(_lock);
    auto iter = _local_merge_map.find(filter_id);
    if (iter == _local_merge_map.end()) {
        // Return OK with nullptr to let the caller skip gracefully.
        return Status::OK();
    }
    if (!iter->second) {
        return Status::InternalError("local merge context is nullptr for filter_id: {}", filter_id);
    }
    if (expected_stage != iter->second->stage) {
        return Status::OK();
    }
    *context = iter->second;
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

    {
        LockGuard l(_lock);
        if (_producer_id_set.contains(key)) {
            return Status::InvalidArgument("filter {} has been registered", key);
        }
    }
    std::shared_ptr<RuntimeFilterProducer> new_producer;
    RETURN_IF_ERROR(RuntimeFilterProducer::create(query_ctx, &desc, &new_producer));
    {
        LockGuard l(_lock);
        if (_producer_id_set.contains(key)) {
            return Status::InvalidArgument("filter {} has been registered", key);
        }
        _producer_id_set.insert(key);
    }
    *producer = new_producer;
    return Status::OK();
}

bool RuntimeFilterMgr::set_runtime_filter_params(
        const TRuntimeFilterParams& runtime_filter_params) {
    LockGuard l(_lock);
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
        LockGuard guard(_filter_map_mutex);
        cnt_val = &_filter_map[filter_id]; // may inplace construct default object
    }

    // runtime_filter_desc and target will be released,
    // so we need to copy to cnt_val
    cnt_val->runtime_filter_desc = *runtime_filter_desc;
    cnt_val->targetv2_info = targetv2_info;
    RETURN_IF_ERROR(
            RuntimeFilterMerger::create(query_ctx.get(), runtime_filter_desc, &cnt_val->merger));
    cnt_val->merger->increase_expected_producer_num(producer_size);

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
        SharedLockGuard guard(_filter_map_mutex);
        iter = _filter_map.find(filter_id);
        if (iter == _filter_map.end()) {
            return Status::InvalidArgument("unknown filter id {}",
                                           std::to_string(request->filter_id()));
        }
    }
    auto& cnt_val = iter->second;
    std::unique_lock<std::mutex> l(cnt_val.mtx);
    // Discard stale-stage runtime filter size requests from old recursive CTE rounds.
    // Each round increments the stage counter; only messages matching the current stage
    // should be processed. This prevents old PFC's runtime filters from corrupting
    // the merge state of the new round's filters.
    if (request->stage() != cnt_val.stage) {
        return Status::OK();
    }
    cnt_val.source_addrs.push_back(request->source_addr());

    Status st = Status::OK();
    // After all runtime filters' size are collected, we should send response to all producers.
    if (cnt_val.merger->add_rf_size(request->filter_size())) {
        cnt_val.sync_size_callbacks.resize(cnt_val.source_addrs.size());
        for (size_t i = 0; i < cnt_val.source_addrs.size(); ++i) {
            auto& addr = cnt_val.source_addrs[i];
            std::shared_ptr<PBackendService_Stub> stub(
                    ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(addr));
            if (stub == nullptr) {
                LOG(WARNING) << "Failed to init rpc to " << addr.hostname() << ":" << addr.port();
                st = Status::InternalError("Failed to init rpc to {}:{}", addr.hostname(),
                                           addr.port());
                continue;
            }

            auto sync_request = std::make_shared<PSyncFilterSizeRequest>();
            sync_request->set_stage(cnt_val.stage);

            auto callback = HandleErrorBrpcCallback<PSyncFilterSizeResponse>::create_shared(
                    query_ctx->ignore_runtime_filter_error() ? std::weak_ptr<QueryContext> {}
                                                             : query_ctx->weak_from_this());
            cnt_val.sync_size_callbacks[i] = callback;
            auto closure = AutoReleaseClosure<
                    PSyncFilterSizeRequest,
                    HandleErrorBrpcCallback<PSyncFilterSizeResponse>>::create_unique(sync_request,
                                                                                     callback);

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
    std::shared_ptr<LocalMergeContext> context;
    RETURN_IF_ERROR(get_local_merge_context(request->filter_id(), request->stage(), &context));
    if (!context) {
        // Filter was removed during a recursive CTE stage reset; discard stale request.
        return Status::OK();
    }
    for (const auto& producer : context->producers) {
        producer->set_synced_size(request->filter_size());
    }
    return Status::OK();
}

std::string RuntimeFilterMgr::debug_string() {
    std::string result = "Local Merger Info:\n";
    struct LocalMergeContextSnapshot {
        std::shared_ptr<RuntimeFilterMerger> merger;
        std::vector<std::shared_ptr<RuntimeFilterProducer>> producers;
    };
    std::vector<LocalMergeContextSnapshot> local_merge_contexts;
    std::vector<std::shared_ptr<RuntimeFilterConsumer>> consumers;
    {
        LockGuard l(_lock);
        for (const auto& [filter_id, ctx] : _local_merge_map) {
            DORIS_CHECK(ctx);
            DORIS_CHECK(ctx->merger);
            local_merge_contexts.push_back({ctx->merger, ctx->producers});
        }
        for (const auto& [filter_id, filter_consumers] : _consumer_map) {
            consumers.insert(consumers.end(), filter_consumers.begin(), filter_consumers.end());
        }
    }
    for (const auto& ctx : local_merge_contexts) {
        result += fmt::format("{}\n", ctx.merger->debug_string());
        for (const auto& producer : ctx.producers) {
            result += fmt::format("{}\n", producer->debug_string());
        }
    }
    result += "Consumer Info:\n";
    for (const auto& consumer : consumers) {
        result += fmt::format("{}\n", consumer->debug_string());
    }
    return result;
}

// merge data
Status RuntimeFilterMergeControllerEntity::merge(std::shared_ptr<QueryContext> query_ctx,
                                                 const PMergeFilterRequest* request,
                                                 butil::IOBufAsZeroCopyInputStream* attach_data) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    int64_t merge_time = 0;
    auto filter_id = request->filter_id();
    std::map<int, GlobalMergeContext>::iterator iter;
    {
        SharedLockGuard guard(_filter_map_mutex);
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
        std::lock_guard<std::mutex> l(cnt_val.mtx);
        // Discard stale-stage merge requests from old recursive CTE rounds.
        if (request->stage() != cnt_val.stage) {
            return Status::OK();
        }
        if (cnt_val.merger == nullptr) {
            return Status::InternalError("Merger is null for filter id {}",
                                         std::to_string(request->filter_id()));
        }
        // Skip the other broadcast join runtime filter
        if (cnt_val.arrive_id.size() == 1 && cnt_val.runtime_filter_desc.is_broadcast_join) {
            return Status::OK();
        }
        std::shared_ptr<RuntimeFilterProducer> tmp_filter;
        RETURN_IF_ERROR(RuntimeFilterProducer::create(query_ctx.get(), &cnt_val.runtime_filter_desc,
                                                      &tmp_filter));

        RETURN_IF_ERROR(tmp_filter->assign(*request, attach_data));

        RETURN_IF_ERROR(cnt_val.merger->merge_from(tmp_filter.get(), &is_ready));

        cnt_val.arrive_id.insert(UniqueId(request->fragment_instance_id()));
    }

    if (is_ready) {
        return _send_rf_to_target(cnt_val,
                                  query_ctx->ignore_runtime_filter_error()
                                          ? std::weak_ptr<QueryContext> {}
                                          : query_ctx,
                                  merge_time, request->query_id(), query_ctx->execution_timeout(),
                                  query_ctx->query_options());
    }
    return Status::OK();
}

Status RuntimeFilterMergeControllerEntity::_send_rf_to_target(
        GlobalMergeContext& cnt_val, std::weak_ptr<QueryContext> ctx, int64_t merge_time,
        PUniqueId query_id, int execution_timeout, const TQueryOptions& query_options) {
    if (cnt_val.targetv2_info.empty()) {
        return Status::InternalError(
                "_send_rf_to_target called with empty targetv2_info, filter: {}",
                cnt_val.merger ? cnt_val.merger->debug_string() : "unknown");
    }

    if (cnt_val.done) {
        return Status::InternalError("Runtime filter has been sent, filter: {}",
                                     cnt_val.merger->debug_string());
    }
    cnt_val.done = true;

    butil::IOBuf request_attachment;

    PPublishFilterRequestV2 apply_request;
    apply_request.set_stage(cnt_val.stage);

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

    std::vector<TRuntimeFilterTargetParamsV2>& targets = cnt_val.targetv2_info;
    int timeout_ms = get_execution_rpc_timeout_ms(execution_timeout);
    apply_request.set_merge_time(merge_time);
    *apply_request.mutable_query_id() = query_id;
    const int64_t serialized_filter_size =
            static_cast<int64_t>(apply_request.ByteSizeLong()) + std::max(0, len);
    int fanout = 0;
    if (query_options.__isset.runtime_filter_tree_publish_max_send_bytes) {
        int64_t max_send_bytes = query_options.runtime_filter_tree_publish_max_send_bytes;
        DORIS_CHECK(max_send_bytes >= 0);
        fanout = calculate_tree_publish_fanout(serialized_filter_size, targets.size(),
                                               max_send_bytes);
    }
    const bool use_tree_publish = fanout > 0 && targets.size() > static_cast<size_t>(fanout);

    if (use_tree_publish) {
        apply_request.set_tree_publish_fanout(fanout);
        apply_request.set_publish_rpc_timeout_ms(timeout_ms);
        auto publish_targets = build_runtime_filter_publish_targets(targets);
        auto tasks = build_runtime_filter_publish_tasks(apply_request, publish_targets, fanout);
        cnt_val.publish_callbacks.resize(tasks.size());
        VLOG_NOTICE << "Runtime filter tree publish filter_id=" << apply_request.filter_id()
                    << ", serialized_bytes=" << serialized_filter_size
                    << ", target_count=" << targets.size() << ", child_rpc_count=" << tasks.size()
                    << ", fanout=" << fanout;
        auto st = Status::OK();
        for (size_t i = 0; i < tasks.size(); ++i) {
            const auto& task = tasks[i];
            auto callback = HandleErrorBrpcCallback<PPublishFilterResponse>::create_shared(ctx);
            cnt_val.publish_callbacks[i] = callback;
            auto closure = AutoReleaseClosure<PPublishFilterRequestV2,
                                              HandleErrorBrpcCallback<PPublishFilterResponse>>::
                    create_unique(std::make_shared<PPublishFilterRequestV2>(task.request),
                                  callback);

            if (has_attachment) {
                closure->cntl_->request_attachment().append(request_attachment);
            }
            closure->cntl_->set_timeout_ms(timeout_ms);
            if (config::execution_ignore_eovercrowded) {
                closure->cntl_->ignore_eovercrowded();
            }

            std::shared_ptr<PBackendService_Stub> stub(
                    ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(
                            task.receiver.addr));
            if (stub == nullptr) {
                LOG(WARNING) << "Failed to init rpc to " << task.receiver.addr.hostname() << ":"
                             << task.receiver.addr.port();
                st = Status::InternalError("Failed to init rpc to {}:{}",
                                           task.receiver.addr.hostname(),
                                           task.receiver.addr.port());
                continue;
            }
            stub->apply_filterv2(closure->cntl_.get(), closure->request_.get(),
                                 closure->response_.get(), closure.get());
            closure.release();
        }
        return st;
    }

    auto st = Status::OK();
    cnt_val.publish_callbacks.resize(targets.size());
    for (size_t i = 0; i < targets.size(); ++i) {
        auto& target = targets[i];
        auto callback = HandleErrorBrpcCallback<PPublishFilterResponse>::create_shared(ctx);
        cnt_val.publish_callbacks[i] = callback;
        auto closure = AutoReleaseClosure<PPublishFilterRequestV2,
                                          HandleErrorBrpcCallback<PPublishFilterResponse>>::
                create_unique(std::make_shared<PPublishFilterRequestV2>(apply_request), callback);

        if (has_attachment) {
            closure->cntl_->request_attachment().append(request_attachment);
        }

        closure->cntl_->set_timeout_ms(timeout_ms);
        if (config::execution_ignore_eovercrowded) {
            closure->cntl_->ignore_eovercrowded();
        }

        set_request_direct_publish_target(target, closure->request_.get());

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
    return st;
}

// Reset merge context for the next recursive CTE round.
// Recreates the merger to clear accumulated state, preserving expected producer count.
// Increments the stage counter so stale merge/size RPCs from old rounds are discarded.
Status GlobalMergeContext::reset(QueryContext* query_ctx) {
    std::unique_lock<std::mutex> lock(mtx);
    // Merger must exist: reset() is only called on fully initialized merge contexts.
    DORIS_CHECK(merger);
    int producer_size = merger->get_expected_producer_num();
    RETURN_IF_ERROR(RuntimeFilterMerger::create(query_ctx, &runtime_filter_desc, &merger));
    merger->increase_expected_producer_num(producer_size);
    arrive_id.clear();
    source_addrs.clear();
    sync_size_callbacks.clear();
    publish_callbacks.clear();
    done = false;
    stage++;
    // Keep the Merger's own stage in sync for consistent debug output.
    merger->set_stage(stage);
    return Status::OK();
}

Status RuntimeFilterMergeControllerEntity::reset_global_rf(
        QueryContext* query_ctx, const google::protobuf::RepeatedField<int32_t>& filter_ids) {
    for (const auto& filter_id : filter_ids) {
        GlobalMergeContext* cnt_val;
        {
            LockGuard guard(_filter_map_mutex);
            cnt_val = &_filter_map[filter_id]; // may inplace construct default object
        }
        RETURN_IF_ERROR(cnt_val->reset(query_ctx));
    }
    return Status::OK();
}

std::string RuntimeFilterMergeControllerEntity::debug_string() {
    std::string result = "RuntimeFilterMergeControllerEntity Info:\n";
    SharedLockGuard guard(_filter_map_mutex);
    for (const auto& [filter_id, ctx] : _filter_map) {
        result += fmt::format("filter_id: {}, stage: {}, {}\n", filter_id, ctx.stage,
                              ctx.merger->debug_string());
    }
    return result;
}
} // namespace doris
