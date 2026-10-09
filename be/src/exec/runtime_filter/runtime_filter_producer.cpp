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
namespace {
// Nothing other than the merger can read the wrapper `merge_from` is about to receive: either no
// plain local consumer of `state` exists at all (`_send_to_local_targets(state, this, false)` is
// then a no-op), or `merger` expects only this one producer, so `merge_from` becomes ready on
// this very call and never merges another producer into the wrapper it adopts here, even though
// a plain local consumer also reads it.
bool wrapper_has_no_other_reader(RuntimeState* state, int filter_id, RuntimeFilterMerger* merger) {
    return state->local_runtime_filter_mgr()->get_consume_filters(filter_id).empty() ||
           merger->get_expected_producer_num() == 1;
}
} // namespace

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

// Pre-existing complexity debt from the target/broadcast/remote branching below, already over
// threshold before this change; the ownership refinement above is factored out to keep the
// increase small, but not addressed further here to keep this diff focused.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
Status RuntimeFilterProducer::publish(RuntimeState* state, bool build_hash_table) {
    std::unique_lock<std::recursive_mutex> l(_rmtx);
    _check_state({State::READY_TO_PUBLISH});

    // `allow_relaxed_ownership` is only meaningful when `other_wrapper_exclusively_owned` is
    // false: it lets this call refine that decision using the local-merge context it fetches
    // below, instead of the caller guessing it without the context. It must stay false for the
    // remote-target call site, whose `true` already holds unconditionally for a different
    // reason (see its call below), and for the broadcast case, whose producers alias one shared
    // wrapper (see `RuntimeFilterProducerTest.publish_mixed_targets_shared_wrapper`).
    auto do_merge = [&](bool other_wrapper_exclusively_owned, bool allow_relaxed_ownership) {
        if (!_need_do_merge(state)) {
            // when global consumer not exist, send_to_local_targets will do nothing, so merge rf is useless
            return Status::OK();
        }
        std::shared_ptr<LocalMergeContext> context;
        RETURN_IF_ERROR(state->global_runtime_filter_mgr()->get_local_merge_context(
                _wrapper->filter_id(), _stage, &context));
        if (!context) {
            // Filter was removed during a recursive CTE stage reset; this producer is stale.
            return Status::OK();
        }
        if (!other_wrapper_exclusively_owned && allow_relaxed_ownership) {
            other_wrapper_exclusively_owned = wrapper_has_no_other_reader(
                    state, _wrapper->filter_id(), context->merger.get());
        }
        bool ready = false;
        RETURN_IF_ERROR(context->merger->merge_from(this, &ready, other_wrapper_exclusively_owned));
        if (ready) {
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
        // The merger never writes this wrapper while a plain local consumer may still read it
        // (see `RuntimeFilterMerger::merge_from`), so by default the consumers in local RF mgr
        // can use it right away while the merge of the other producers goes on. `do_merge` may
        // still find no such consumer exists (or this is the only producer) and adopt the
        // wrapper directly. Broadcast producers alias one shared wrapper across instances (see
        // `publish_mixed_targets_shared_wrapper`), so relaxing ownership is unsound for them;
        // `build_bf_by_runtime_size` is excluded too, to keep this call's precondition the same
        // simple "nothing else can reach this wrapper" rule as the remote-target call below,
        // without re-deriving it for a filter whose size-sync step this branch never exercises.
        RETURN_IF_ERROR(do_merge(/*other_wrapper_exclusively_owned=*/false,
                                 /*allow_relaxed_ownership=*/!_is_broadcast_join &&
                                         !_wrapper->build_bf_by_runtime_size()));
        RETURN_IF_ERROR(_send_to_local_targets(state, this, false));
    } else if (build_hash_table) {
        if (_is_broadcast_join) {
            RETURN_IF_ERROR(_send_to_remote_targets(state, this));
        } else {
            // This path never hands `_wrapper` to a plain local consumer (that only happens in
            // the `!_has_remote_target` branch above), and the shared-wrapper broadcast-join
            // case is the other arm of this `if`, so no other producer aliases it either. The
            // only remaining reference after this call is the one `_wrapper.reset()` below
            // drops, so the merger may take `_wrapper` over directly instead of cloning it.
            RETURN_IF_ERROR(do_merge(/*other_wrapper_exclusively_owned=*/true,
                                     /*allow_relaxed_ownership=*/false));
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
        std::shared_ptr<LocalMergeContext> context;
        RETURN_IF_ERROR(state->global_runtime_filter_mgr()->get_local_merge_context(
                _wrapper->filter_id(), _stage, &context));
        if (!context) {
            // Filter was removed during a recursive CTE stage reset; this producer is stale.
            return Status::OK();
        }
        uint64_t received_sum_size = 0;
        bool ready_to_sync = context->merger->add_rf_size(local_filter_size);
        if (!ready_to_sync) {
            return Status::OK();
        }
        received_sum_size = context->merger->get_received_sum_size();
        if (!_has_remote_target) {
            for (const auto& filter : context->producers) {
                filter->set_synced_size(received_sum_size);
            }
            return Status::OK();
        }
        local_filter_size = received_sum_size;

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
