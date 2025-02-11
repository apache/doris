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
#include "util/ref_count_closure.h"

namespace doris {

Status RuntimeFilterProducer::init_with_size(size_t local_size) {
    size_t real_size = need_sync_filter_size() ? get_synced_size() : local_size;
    if (_runtime_filter_type == RuntimeFilterType::IN_OR_BLOOM_FILTER &&
        local_size > _wrapper->_max_in_num) {
        RETURN_IF_ERROR(_wrapper->change_to_bloom_filter());
    }

    if (get_real_type() == RuntimeFilterType::BLOOM_FILTER) {
        RETURN_IF_ERROR(_wrapper->init_bloom_filter(real_size));
    }
    return Status::OK();
}

Status RuntimeFilterProducer::_send_to_remote_targets(RuntimeState* state, RuntimeFilter* filter,
                                                      uint64_t local_merge_time) {
    TNetworkAddress addr;
    DCHECK(_state != nullptr);
    RETURN_IF_ERROR(_state->global_runtime_filter_mgr()->get_merge_addr(&addr));
    return filter->_push_to_remote(state, &addr, local_merge_time);
};

Status RuntimeFilterProducer::_send_to_local_targets(std::shared_ptr<RuntimeFilterWrapper> wrapper,
                                                     bool global, uint64_t local_merge_time) {
    std::vector<std::shared_ptr<RuntimeFilterConsumer>> filters =
            global ? _state->global_runtime_filter_mgr()->get_consume_filters(filter_id())
                   : _state->local_runtime_filter_mgr()->get_consume_filters(filter_id());
    for (auto filter : filters) {
        filter->_wrapper = wrapper;
        filter->update_runtime_filter_type_to_profile(local_merge_time);
        filter->signal();
    }
    return Status::OK();
};

Status RuntimeFilterProducer::publish(RuntimeState* state, bool publish_local) {
    DCHECK(_rf_state == State::READY_TO_PUBLISH);
    _rf_state = State::PUBLISHED;

    auto do_merge = [&]() {
        // two case we need do local merge:
        // 1. has remote target
        // 2. has local target and has global consumer (means target scan has local shuffle)
        if (_has_local_target &&
            _state->global_runtime_filter_mgr()->get_consume_filters(filter_id()).empty()) {
            // when global consumer not exist, send_to_local_targets will do nothing, so merge rf is useless
            return Status::OK();
        }
        LocalMergeFilters* local_merge_filters = nullptr;
        RETURN_IF_ERROR(_state->global_runtime_filter_mgr()->get_local_merge_producer_filters(
                filter_id(), &local_merge_filters));
        local_merge_filters->merge_watcher.start();
        std::lock_guard l(*local_merge_filters->lock);
        RETURN_IF_ERROR(local_merge_filters->merger->merge_from(this));
        local_merge_filters->merge_time--;
        local_merge_filters->merge_watcher.stop();
        if (local_merge_filters->merge_time == 0) {
            if (_has_local_target) {
                RETURN_IF_ERROR(
                        _send_to_local_targets(local_merge_filters->merger->_wrapper, true,
                                               local_merge_filters->merge_watcher.elapsed_time()));
            } else {
                RETURN_IF_ERROR(
                        _send_to_remote_targets(state, local_merge_filters->merger.get(),
                                                local_merge_filters->merge_watcher.elapsed_time()));
            }
        }
        return Status::OK();
    };

    if (_has_local_target) {
        // A runtime filter may have multiple targets and some of those are local-merge RF and others are not.
        // So for all runtime filters' producers, `publish` should notify all consumers in global RF mgr which manages local-merge RF and local RF mgr which manages others.
        RETURN_IF_ERROR(do_merge());
        RETURN_IF_ERROR(_send_to_local_targets(_wrapper, false, 0));
    } else if (!publish_local) {
        if (_is_broadcast_join) {
            RETURN_IF_ERROR(_send_to_remote_targets(state, this, 0));
        } else {
            RETURN_IF_ERROR(do_merge());
        }
    } else {
        // remote broadcast join only push onetime in build shared hash table
        // publish_local only set true on copy shared hash table
        DCHECK(_is_broadcast_join);
    }
    return Status::OK();
}

class SyncSizeClosure : public AutoReleaseClosure<PSendFilterSizeRequest,
                                                  DummyBrpcCallback<PSendFilterSizeResponse>> {
    std::shared_ptr<pipeline::Dependency> _dependency;
    // Should use weak ptr here, because when query context deconstructs, should also delete runtime filter
    // context, it not the memory is not released. And rpc is in another thread, it will hold rf context
    // after query context because the rpc is not returned.
    std::weak_ptr<RuntimeFilterWrapper> _rf_context;
    using Base =
            AutoReleaseClosure<PSendFilterSizeRequest, DummyBrpcCallback<PSendFilterSizeResponse>>;
    ENABLE_FACTORY_CREATOR(SyncSizeClosure);

    void _process_if_rpc_failed() override {
        Defer defer {[&]() { ((pipeline::CountedFinishDependency*)_dependency.get())->sub(); }};
        auto ctx = _rf_context.lock();
        if (!ctx) {
            return;
        }

        ctx->_err_msg = cntl_->ErrorText();
        Base::_process_if_rpc_failed();
    }

    void _process_if_meet_error_status(const Status& status) override {
        Defer defer {[&]() { ((pipeline::CountedFinishDependency*)_dependency.get())->sub(); }};
        auto ctx = _rf_context.lock();
        if (!ctx) {
            return;
        }

        if (status.is<ErrorCode::END_OF_FILE>()) {
            // rf merger backend may finished before rf's send_filter_size, we just ignore filter in this case.
            ctx->_state = RuntimeFilterWrapper::State::IGNORED;
        } else {
            ctx->_err_msg = status.to_string();
            Base::_process_if_meet_error_status(status);
        }
    }

public:
    SyncSizeClosure(std::shared_ptr<PSendFilterSizeRequest> req,
                    std::shared_ptr<DummyBrpcCallback<PSendFilterSizeResponse>> callback,
                    std::shared_ptr<pipeline::Dependency> dependency,
                    std::shared_ptr<RuntimeFilterWrapper> rf_context,
                    std::weak_ptr<QueryContext> context)
            : Base(req, callback, context),
              _dependency(std::move(dependency)),
              _rf_context(rf_context) {}
};

Status RuntimeFilterProducer::send_filter_size(RuntimeState* state, uint64_t local_filter_size) {
    // two case we need do local merge:
    // 1. has remote target
    // 2. has local target and has global consumer (means target scan has local shuffle)
    if (_has_remote_target ||
        !_state->global_runtime_filter_mgr()->get_consume_filters(filter_id()).empty()) {
        LocalMergeFilters* local_merge_filters = nullptr;
        RETURN_IF_ERROR(_state->global_runtime_filter_mgr()->get_local_merge_producer_filters(
                filter_id(), &local_merge_filters));
        std::lock_guard l(*local_merge_filters->lock);
        local_merge_filters->merge_size_times--;
        local_merge_filters->local_merged_size += local_filter_size;
        if (local_merge_filters->merge_size_times) {
            return Status::OK();
        } else {
            if (_has_local_target) {
                for (auto filter : local_merge_filters->producers) {
                    filter->set_synced_size(local_merge_filters->local_merged_size);
                }
                return Status::OK();
            } else {
                local_filter_size = local_merge_filters->local_merged_size;
            }
        }
    } else if (_has_local_target) {
        set_synced_size(local_filter_size);
        return Status::OK();
    }

    TNetworkAddress addr;
    DCHECK(_state != nullptr);
    RETURN_IF_ERROR(_state->global_runtime_filter_mgr()->get_merge_addr(&addr));
    std::shared_ptr<PBackendService_Stub> stub(
            _state->get_query_ctx()->exec_env()->brpc_internal_client_cache()->get_client(addr));
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
    pquery_id->set_hi(_state->get_query_ctx()->query_id().hi);
    pquery_id->set_lo(_state->get_query_ctx()->query_id().lo);

    auto* source_addr = request->mutable_source_addr();
    source_addr->set_hostname(BackendOptions::get_local_backend().host);
    source_addr->set_port(BackendOptions::get_local_backend().brpc_port);

    request->set_filter_size(local_filter_size);
    request->set_filter_id(filter_id());

    callback->cntl_->set_timeout_ms(get_execution_rpc_timeout_ms(state->execution_timeout()));
    if (config::execution_ignore_eovercrowded) {
        callback->cntl_->ignore_eovercrowded();
    }

    stub->send_filter_size(closure->cntl_.get(), closure->request_.get(), closure->response_.get(),
                           closure.get());
    closure.release();
    return Status::OK();
}

void RuntimeFilterProducer::set_finish_dependency(
        const std::shared_ptr<pipeline::CountedFinishDependency>& dependency) {
    _dependency = dependency;
    _dependency->add();
    CHECK(_dependency);
}

void RuntimeFilterProducer::set_synced_size(uint64_t global_size) {
    _synced_size = global_size;
    if (_dependency) {
        _dependency->sub();
    }
}

} // namespace doris
