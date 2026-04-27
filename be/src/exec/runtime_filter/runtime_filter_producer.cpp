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

#include "exec/runtime_filter/runtime_filter_producer.h"

#include <glog/logging.h>

#include "exec/runtime_filter/runtime_filter_consumer.h"
#include "exec/runtime_filter/runtime_filter_merger.h"
#include "exec/runtime_filter/runtime_filter_wrapper.h"
#include "util/brpc_client_cache.h"
#include "util/brpc_closure.h"

namespace doris {
Status RuntimeFilterProducer::_send_to_remote_targets(RuntimeState* state,
                                                      RuntimeFilter* merger_filter) {
    TNetworkAddress addr;
    RETURN_IF_ERROR(state->global_runtime_filter_mgr()->get_merge_addr(&addr));
    return merger_filter->_push_to_remote(state, &addr);
};

Status RuntimeFilterProducer::_send_to_local_targets(RuntimeState* state, RuntimeFilter* source,
                                                     bool global) {
    std::vector<std::shared_ptr<RuntimeFilterConsumer>> filters =
            global ? state->global_runtime_filter_mgr()->get_consume_filters(_wrapper->filter_id())
                   : state->local_runtime_filter_mgr()->get_consume_filters(_wrapper->filter_id());
    for (auto filter : filters) {
        filter->signal(source);
    }
    return Status::OK();
};

Status RuntimeFilterProducer::publish(RuntimeState* state, bool build_hash_table) {
    std::unique_lock<std::recursive_mutex> l(_rmtx);
    _check_state({State::READY_TO_PUBLISH});

    auto do_merge = [&]() {
        if (!_need_do_merge(state)) {
            // when global consumer not exist, send_to_local_targets will do nothing, so merge rf is useless
            return Status::OK();
        }
        LocalMergeContext* context = nullptr;
        RETURN_IF_ERROR(state->global_runtime_filter_mgr()->get_local_merge_producer_filters(
                _wrapper->filter_id(), &context));
        if (context == nullptr) {
            // Filter was removed during a recursive CTE stage reset; this producer is stale.
            return Status::OK();
        }
        std::lock_guard l(context->mtx);
        RETURN_IF_ERROR(context->merger->merge_from(this));
        if (context->merger->ready()) {
            if (_has_remote_target) {
                RETURN_IF_ERROR(_send_to_remote_targets(state, context->merger.get()));
            } else {
                RETURN_IF_ERROR(_send_to_local_targets(state, context->merger.get(), true));
            }
        }
        return Status::OK();
    };

    if (!_has_remote_target) {
        // A runtime filter may have multiple targets and some of those are local-merge RF and others are not.
        // So for all runtime filters' producers, `publish` should notify all consumers in global RF mgr which manages local-merge RF and local RF mgr which manages others.
        RETURN_IF_ERROR(do_merge());
        RETURN_IF_ERROR(_send_to_local_targets(state, this, false));
    } else if (build_hash_table) {
        if (_is_broadcast_join) {
            RETURN_IF_ERROR(_send_to_remote_targets(state, this));
        } else {
            RETURN_IF_ERROR(do_merge());
        }
    } else {
        if (!_is_broadcast_join) {
            return Status::InternalError(
                    "Expected broadcast join for non-build hash table path in publish, filter: {}",
                    debug_string());
        }
    }

    // wrapper may moved to rf merger, release wrapper here to make sure thread safe
    _wrapper.reset();
    set_state(State::PUBLISHED);
    return Status::OK();
}

void RuntimeFilterProducer::latch_dependency(
        const std::shared_ptr<CountedFinishDependency>& dependency) {
    std::unique_lock<std::recursive_mutex> l(_rmtx);
    if (_rf_state != State::WAITING_FOR_SEND_SIZE) {
        return;
    }
    if (dependency == nullptr) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "dependency is nullptr in latch_dependency, filter: {}", debug_string());
    }
    _dependency = dependency;
    _dependency->add();
}

Status RuntimeFilterProducer::send_size(RuntimeState* state, uint64_t local_filter_size) {
    std::unique_lock<std::recursive_mutex> l(_rmtx);
    if (_rf_state != State::WAITING_FOR_SEND_SIZE) {
        return Status::OK();
    }
    if (_dependency == nullptr) {
        return Status::InternalError("_dependency is nullptr in send_size, filter: {}",
                                     debug_string());
    }
    set_state(State::WAITING_FOR_SYNCED_SIZE);

    if (_need_do_merge(state)) {
        LocalMergeContext* merger_context = nullptr;
        RETURN_IF_ERROR(state->global_runtime_filter_mgr()->get_local_merge_producer_filters(
                _wrapper->filter_id(), &merger_context));
        if (merger_context == nullptr) {
            // Filter was removed during a recursive CTE stage reset; this producer is stale.
            return Status::OK();
        }
        std::lock_guard merger_lock(merger_context->mtx);
        if (merger_context->merger->add_rf_size(local_filter_size)) {
            if (!_has_remote_target) {
                for (auto filter : merger_context->producers) {
                    filter->set_synced_size(merger_context->merger->get_received_sum_size());
                }
                return Status::OK();
            } else {
                local_filter_size = merger_context->merger->get_received_sum_size();
            }
        } else {
            return Status::OK();
        }

    } else if (!_has_remote_target) {
        set_synced_size(local_filter_size);
        return Status::OK();
    }

    TNetworkAddress addr;
    RETURN_IF_ERROR(state->global_runtime_filter_mgr()->get_merge_addr(&addr));
    std::shared_ptr<PBackendService_Stub> stub(
            state->get_query_ctx()->exec_env()->brpc_internal_client_cache()->get_client(addr));
    if (!stub) {
        return Status::InternalError("Get rpc stub failed, host={}, port={}", addr.hostname,
                                     addr.port);
    }

    auto request = std::make_shared<PSendFilterSizeRequest>();
    request->set_stage(_stage);
    // when failed, will check `ignore_runtime_filter_error` in callback to decide cancel or not
    _sync_size_callback = SyncSizeCallback::create_shared(_dependency, _wrapper,
                                                          state->get_query_ctx()->weak_from_this());
    // RuntimeFilter maybe deconstructed before the rpc finished, so that could not use
    // a raw pointer in closure. Has to use the context's shared ptr.
    auto closure = AutoReleaseClosure<PSendFilterSizeRequest, SyncSizeCallback>::create_unique(
            request, _sync_size_callback);
    auto* pquery_id = request->mutable_query_id();
    pquery_id->set_hi(state->get_query_ctx()->query_id().hi);
    pquery_id->set_lo(state->get_query_ctx()->query_id().lo);

    auto* source_addr = request->mutable_source_addr();
    source_addr->set_hostname(BackendOptions::get_local_backend().host);
    source_addr->set_port(BackendOptions::get_local_backend().brpc_port);

    request->set_filter_size(local_filter_size);
    request->set_filter_id(_wrapper->filter_id());

    _sync_size_callback->cntl_->set_timeout_ms(
            get_execution_rpc_timeout_ms(state->execution_timeout()));
    if (config::execution_ignore_eovercrowded) {
        _sync_size_callback->cntl_->ignore_eovercrowded();
    }

    if (config::enable_debug_points &&
        DebugPoints::instance()->is_enable("RuntimeFilterProducer::send_size.rpc_fail")) {
        closure->cntl_->SetFailed("inject RuntimeFilterProducer::send_size.rpc_fail");
    }

    stub->send_filter_size(closure->cntl_.get(), closure->request_.get(), closure->response_.get(),
                           closure.get());
    closure.release();
    return Status::OK();
}

void RuntimeFilterProducer::set_synced_size(uint64_t global_size) {
    std::unique_lock<std::recursive_mutex> l(_rmtx);
    if (!set_state(State::WAITING_FOR_DATA)) {
        _check_wrapper_state({RuntimeFilterWrapper::State::DISABLED});
    }

    _synced_size = global_size;
    if (_dependency == nullptr) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "_dependency is nullptr in set_synced_size, filter: {}", debug_string());
    }
    _dependency->sub();
}

Status RuntimeFilterProducer::init(size_t local_size) {
    return _wrapper->init(_synced_size != -1 ? _synced_size : local_size);
}

} // namespace doris
