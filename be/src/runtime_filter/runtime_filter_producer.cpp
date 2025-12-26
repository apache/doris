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

#include "runtime_filter/runtime_filter_producer.h"

#include <glog/logging.h>

#include "runtime_filter/runtime_filter_consumer.h"
#include "runtime_filter/runtime_filter_merger.h"
#include "runtime_filter/runtime_filter_wrapper.h"
#include "util/brpc_client_cache.h"
#include "util/brpc_closure.h"

namespace doris {
#include "common/compile_check_begin.h"
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
        // two case we need do local merge:
        // 1. has remote target
        // 2. has local target and has global consumer (means target scan has local shuffle)
        if (!_has_remote_target && state->global_runtime_filter_mgr()
                                           ->get_consume_filters(_wrapper->filter_id())
                                           .empty()) {
            // when global consumer not exist, send_to_local_targets will do nothing, so merge rf is useless
            return Status::OK();
        }
        LocalMergeContext* context = nullptr;
        RETURN_IF_ERROR(state->global_runtime_filter_mgr()->get_local_merge_producer_filters(
                _wrapper->filter_id(), &context));
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
        DCHECK(_is_broadcast_join);
    }

    // wrapper may moved to rf merger, release wrapper here to make sure thread safe
    _wrapper.reset();
    set_state(State::PUBLISHED);
    return Status::OK();
}

class SyncSizeClosure : public AutoReleaseClosure<PSendFilterSizeRequest,
                                                  DummyBrpcCallback<PSendFilterSizeResponse>> {
    std::shared_ptr<pipeline::Dependency> _dependency;
    // Should use weak ptr here, because when query context deconstructs, should also delete runtime filter
    // context, it not the memory is not released. And rpc is in another thread, it will hold rf context
    // after query context because the rpc is not returned.
    std::weak_ptr<RuntimeFilterWrapper> _wrapper;
    using Base =
            AutoReleaseClosure<PSendFilterSizeRequest, DummyBrpcCallback<PSendFilterSizeResponse>>;
    friend class RuntimeFilterProducer;
    ENABLE_FACTORY_CREATOR(SyncSizeClosure);

    void _process_if_rpc_failed() override {
        Defer defer {[&]() {
            Base::_process_if_rpc_failed();
            ((pipeline::CountedFinishDependency*)_dependency.get())->sub();
        }};
        auto wrapper = _wrapper.lock();
        if (!wrapper) {
            return;
        }

        wrapper->set_state(RuntimeFilterWrapper::State::DISABLED, cntl_->ErrorText());
    }

    void _process_if_meet_error_status(const Status& status) override {
        Defer defer {[&]() {
            Base::_process_if_meet_error_status(status);
            ((pipeline::CountedFinishDependency*)_dependency.get())->sub();
        }};
        auto wrapper = _wrapper.lock();
        if (!wrapper) {
            return;
        }

        wrapper->set_state(RuntimeFilterWrapper::State::DISABLED, status.to_string());
    }

public:
    SyncSizeClosure(std::shared_ptr<PSendFilterSizeRequest> req,
                    std::shared_ptr<DummyBrpcCallback<PSendFilterSizeResponse>> callback,
                    std::shared_ptr<pipeline::Dependency> dependency,
                    std::shared_ptr<RuntimeFilterWrapper> wrapper,
                    std::weak_ptr<QueryContext> context)
            : Base(req, callback, context), _dependency(std::move(dependency)), _wrapper(wrapper) {}
};

void RuntimeFilterProducer::latch_dependency(
        const std::shared_ptr<pipeline::CountedFinishDependency>& dependency) {
    std::unique_lock<std::recursive_mutex> l(_rmtx);
    if (_rf_state != State::WAITING_FOR_SEND_SIZE) {
        return;
    }
    DCHECK(dependency != nullptr);
    _dependency = dependency;
    _dependency->add();
}

Status RuntimeFilterProducer::send_size(RuntimeState* state, uint64_t local_filter_size) {
    std::unique_lock<std::recursive_mutex> l(_rmtx);
    if (_rf_state != State::WAITING_FOR_SEND_SIZE) {
        return Status::OK();
    }
    DCHECK(_dependency != nullptr);
    set_state(State::WAITING_FOR_SYNCED_SIZE);

    // two case we need do local merge:
    // 1. has remote target
    // 2. has local target and has global consumer (means target scan has local shuffle)
    if (_has_remote_target ||
        !state->global_runtime_filter_mgr()->get_consume_filters(_wrapper->filter_id()).empty()) {
        LocalMergeContext* merger_context = nullptr;
        RETURN_IF_ERROR(state->global_runtime_filter_mgr()->get_local_merge_producer_filters(
                _wrapper->filter_id(), &merger_context));
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
    auto callback = DummyBrpcCallback<PSendFilterSizeResponse>::create_shared();
    // RuntimeFilter maybe deconstructed before the rpc finished, so that could not use
    // a raw pointer in closure. Has to use the context's shared ptr.
    auto closure = SyncSizeClosure::create_unique(request, callback, _dependency, _wrapper,
                                                  state->query_options().ignore_runtime_filter_error
                                                          ? std::weak_ptr<QueryContext> {}
                                                          : state->get_query_ctx_weak());
    auto* pquery_id = request->mutable_query_id();
    pquery_id->set_hi(state->get_query_ctx()->query_id().hi);
    pquery_id->set_lo(state->get_query_ctx()->query_id().lo);

    auto* source_addr = request->mutable_source_addr();
    source_addr->set_hostname(BackendOptions::get_local_backend().host);
    source_addr->set_port(BackendOptions::get_local_backend().brpc_port);

    request->set_filter_size(local_filter_size);
    request->set_filter_id(_wrapper->filter_id());

    callback->cntl_->set_timeout_ms(get_execution_rpc_timeout_ms(state->execution_timeout()));
    if (config::execution_ignore_eovercrowded) {
        callback->cntl_->ignore_eovercrowded();
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
    DCHECK(_dependency != nullptr);
    _dependency->sub();
}

Status RuntimeFilterProducer::init(size_t local_size) {
    return _wrapper->init(_synced_size != -1 ? _synced_size : local_size);
}

} // namespace doris
