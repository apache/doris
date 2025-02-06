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

#include "exprs/runtime_filter/runtime_filter.h"

#include "common/status.h"
#include "exprs/bitmapfilter_predicate.h"
#include "exprs/create_predicate_function.h"
#include "exprs/hybrid_set.h"
#include "exprs/minmax_predicate.h"
#include "util/brpc_client_cache.h"
#include "util/ref_count_closure.h"
#include "vec/exprs/vbitmap_predicate.h"
#include "vec/exprs/vbloom_predicate.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vruntimefilter_wrapper.h"

namespace doris {

Status RuntimeFilter::create(RuntimeFilterParamsContext* state, const TRuntimeFilterDesc* desc,
                             const RuntimeFilterRole role, int node_id,
                             std::shared_ptr<RuntimeFilter>* res) {
    *res = std::shared_ptr<RuntimeFilter>(new RuntimeFilter(state, desc, role));
    return (*res)->_init_with_desc(desc, &state->get_query_ctx()->query_options(), node_id);
}

RuntimeFilterContextSPtr& RuntimeFilter::get_shared_context_ref() {
    return _wrapper->_context;
}

void RuntimeFilter::insert_batch(const vectorized::ColumnPtr column, size_t start) {
    _wrapper->insert_batch(column, start);
}

Status RuntimeFilter::publish(RuntimeState* state, bool publish_local) {
    auto send_to_remote_targets = [&](RuntimeFilter* filter, uint64_t local_merge_time) {
        TNetworkAddress addr;
        DCHECK(_state != nullptr);
        RETURN_IF_ERROR(_state->global_runtime_filter_mgr()->get_merge_addr(&addr));
        return filter->push_to_remote(state, &addr, local_merge_time);
    };
    auto send_to_local_targets = [&](std::shared_ptr<RuntimePredicateWrapper> wrapper, bool global,
                                     uint64_t local_merge_time = 0) {
        std::vector<std::shared_ptr<RuntimeFilter>> filters =
                global ? _state->global_runtime_filter_mgr()->get_consume_filters(_filter_id)
                       : _state->local_runtime_filter_mgr()->get_consume_filters(_filter_id);
        for (auto filter : filters) {
            filter->_wrapper = wrapper;
            filter->update_runtime_filter_type_to_profile(local_merge_time);
            filter->signal();
        }
        return Status::OK();
    };
    auto do_merge = [&]() {
        // two case we need do local merge:
        // 1. has remote target
        // 2. has local target and has global consumer (means target scan has local shuffle)
        if (_has_local_target &&
            _state->global_runtime_filter_mgr()->get_consume_filters(_filter_id).empty()) {
            // when global consumer not exist, send_to_local_targets will do nothing, so merge rf is useless
            return Status::OK();
        }
        LocalMergeFilters* local_merge_filters = nullptr;
        RETURN_IF_ERROR(_state->global_runtime_filter_mgr()->get_local_merge_producer_filters(
                _filter_id, &local_merge_filters));
        local_merge_filters->merge_watcher.start();
        std::lock_guard l(*local_merge_filters->lock);
        RETURN_IF_ERROR(local_merge_filters->filters[0]->merge_from(this));
        local_merge_filters->merge_time--;
        local_merge_filters->merge_watcher.stop();
        if (local_merge_filters->merge_time == 0) {
            if (_has_local_target) {
                RETURN_IF_ERROR(
                        send_to_local_targets(local_merge_filters->filters[0]->_wrapper, true,
                                              local_merge_filters->merge_watcher.elapsed_time()));
            } else {
                RETURN_IF_ERROR(
                        send_to_remote_targets(local_merge_filters->filters[0].get(),
                                               local_merge_filters->merge_watcher.elapsed_time()));
            }
        }
        return Status::OK();
    };

    if (_has_local_target) {
        // A runtime filter may have multiple targets and some of those are local-merge RF and others are not.
        // So for all runtime filters' producers, `publish` should notify all consumers in global RF mgr which manages local-merge RF and local RF mgr which manages others.
        RETURN_IF_ERROR(do_merge());
        RETURN_IF_ERROR(send_to_local_targets(_wrapper, false));
    } else if (!publish_local) {
        if (_is_broadcast_join || _state->get_query_ctx()->be_exec_version() < USE_NEW_SERDE) {
            RETURN_IF_ERROR(send_to_remote_targets(this, 0));
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
    std::weak_ptr<RuntimeFilterContext> _rf_context;
    using Base =
            AutoReleaseClosure<PSendFilterSizeRequest, DummyBrpcCallback<PSendFilterSizeResponse>>;
    ENABLE_FACTORY_CREATOR(SyncSizeClosure);

    void _process_if_rpc_failed() override {
        Defer defer {[&]() { ((pipeline::CountedFinishDependency*)_dependency.get())->sub(); }};
        auto ctx = _rf_context.lock();
        if (!ctx) {
            return;
        }

        ctx->err_msg = cntl_->ErrorText();
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
            ctx->ignored = true;
        } else {
            ctx->err_msg = status.to_string();
            Base::_process_if_meet_error_status(status);
        }
    }

public:
    SyncSizeClosure(std::shared_ptr<PSendFilterSizeRequest> req,
                    std::shared_ptr<DummyBrpcCallback<PSendFilterSizeResponse>> callback,
                    std::shared_ptr<pipeline::Dependency> dependency,
                    RuntimeFilterContextSPtr rf_context, std::weak_ptr<QueryContext> context)
            : Base(req, callback, context),
              _dependency(std::move(dependency)),
              _rf_context(rf_context) {}
};

Status RuntimeFilter::send_filter_size(RuntimeState* state, uint64_t local_filter_size) {
    // two case we need do local merge:
    // 1. has remote target
    // 2. has local target and has global consumer (means target scan has local shuffle)
    if (_has_remote_target ||
        !_state->global_runtime_filter_mgr()->get_consume_filters(_filter_id).empty()) {
        LocalMergeFilters* local_merge_filters = nullptr;
        RETURN_IF_ERROR(_state->global_runtime_filter_mgr()->get_local_merge_producer_filters(
                _filter_id, &local_merge_filters));
        std::lock_guard l(*local_merge_filters->lock);
        local_merge_filters->merge_size_times--;
        local_merge_filters->local_merged_size += local_filter_size;
        if (local_merge_filters->merge_size_times) {
            return Status::OK();
        } else {
            if (_has_local_target) {
                for (auto filter : local_merge_filters->filters) {
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
    auto closure = SyncSizeClosure::create_unique(
            request, callback, _dependency, _wrapper->_context,
            state->query_options().ignore_runtime_filter_error ? std::weak_ptr<QueryContext> {}
                                                               : state->get_query_ctx_weak());
    auto* pquery_id = request->mutable_query_id();
    pquery_id->set_hi(_state->get_query_ctx()->query_id().hi);
    pquery_id->set_lo(_state->get_query_ctx()->query_id().lo);

    auto* source_addr = request->mutable_source_addr();
    source_addr->set_hostname(BackendOptions::get_local_backend().host);
    source_addr->set_port(BackendOptions::get_local_backend().brpc_port);

    request->set_filter_size(local_filter_size);
    request->set_filter_id(_filter_id);

    callback->cntl_->set_timeout_ms(get_execution_rpc_timeout_ms(state->execution_timeout()));
    if (config::execution_ignore_eovercrowded) {
        callback->cntl_->ignore_eovercrowded();
    }

    stub->send_filter_size(closure->cntl_.get(), closure->request_.get(), closure->response_.get(),
                           closure.get());
    closure.release();
    return Status::OK();
}

Status RuntimeFilter::push_to_remote(RuntimeState* state, const TNetworkAddress* addr,
                                     uint64_t local_merge_time) {
    std::shared_ptr<PBackendService_Stub> stub(
            _state->get_query_ctx()->exec_env()->brpc_internal_client_cache()->get_client(*addr));
    if (!stub) {
        return Status::InternalError(
                fmt::format("Get rpc stub failed, host={}, port={}", addr->hostname, addr->port));
    }

    auto merge_filter_request = std::make_shared<PMergeFilterRequest>();
    auto merge_filter_callback = DummyBrpcCallback<PMergeFilterResponse>::create_shared();
    auto merge_filter_closure =
            AutoReleaseClosure<PMergeFilterRequest, DummyBrpcCallback<PMergeFilterResponse>>::
                    create_unique(merge_filter_request, merge_filter_callback,
                                  state->query_options().ignore_runtime_filter_error
                                          ? std::weak_ptr<QueryContext> {}
                                          : state->get_query_ctx_weak());
    void* data = nullptr;
    int len = 0;

    auto* pquery_id = merge_filter_request->mutable_query_id();
    pquery_id->set_hi(_state->get_query_ctx()->query_id().hi);
    pquery_id->set_lo(_state->get_query_ctx()->query_id().lo);

    auto* pfragment_instance_id = merge_filter_request->mutable_fragment_instance_id();
    pfragment_instance_id->set_hi(BackendOptions::get_local_backend().id);
    pfragment_instance_id->set_lo((int64_t)this);

    merge_filter_request->set_filter_id(_filter_id);
    merge_filter_request->set_local_merge_time(local_merge_time);
    auto column_type = _wrapper->column_type();
    RETURN_IF_CATCH_EXCEPTION(merge_filter_request->set_column_type(to_proto(column_type)));

    merge_filter_callback->cntl_->set_timeout_ms(
            get_execution_rpc_timeout_ms(_state->get_query_ctx()->execution_timeout()));
    if (config::execution_ignore_eovercrowded) {
        merge_filter_callback->cntl_->ignore_eovercrowded();
    }

    if (get_ignored() || get_disabled()) {
        merge_filter_request->set_filter_type(PFilterType::UNKNOW_FILTER);
        merge_filter_request->set_ignored(get_ignored());
        merge_filter_request->set_disabled(get_disabled());
    } else {
        RETURN_IF_ERROR(serialize(merge_filter_request.get(), &data, &len));
    }

    if (len > 0) {
        DCHECK(data != nullptr);
        merge_filter_callback->cntl_->request_attachment().append(data, len);
    }

    stub->merge_filter(merge_filter_closure->cntl_.get(), merge_filter_closure->request_.get(),
                       merge_filter_closure->response_.get(), merge_filter_closure.get());
    // the closure will be released by brpc during closure->Run.
    merge_filter_closure.release();
    return Status::OK();
}

Status RuntimeFilter::get_push_expr_ctxs(std::list<vectorized::VExprContextSPtr>& probe_ctxs,
                                         std::vector<vectorized::VRuntimeFilterPtr>& push_exprs,
                                         bool is_late_arrival) {
    auto origin_size = push_exprs.size();
    if (!_wrapper->is_ignored() && !_wrapper->is_disabled()) {
        _set_push_down(!is_late_arrival);
        RETURN_IF_ERROR(_wrapper->get_push_exprs(probe_ctxs, push_exprs, _probe_expr));
    }
    _profile->add_info_string("Info", formatted_state());
    // The runtime filter is pushed down, adding filtering information.
    auto* expr_filtered_rows_counter = ADD_COUNTER(_profile, "ExprFilteredRows", TUnit::UNIT);
    auto* expr_input_rows_counter = ADD_COUNTER(_profile, "ExprInputRows", TUnit::UNIT);
    auto* always_true_counter = ADD_COUNTER(_profile, "AlwaysTruePassRows", TUnit::UNIT);
    for (auto i = origin_size; i < push_exprs.size(); i++) {
        push_exprs[i]->attach_profile_counter(expr_filtered_rows_counter, expr_input_rows_counter,
                                              always_true_counter);
    }
    return Status::OK();
}

void RuntimeFilter::update_state() {
    auto execution_timeout = _state->get_query_ctx()->execution_timeout() * 1000;
    auto runtime_filter_wait_time_ms = _state->get_query_ctx()->runtime_filter_wait_time_ms();
    // bitmap filter is precise filter and only filter once, so it must be applied.
    int64_t wait_times_ms = _runtime_filter_type == RuntimeFilterType::BITMAP_FILTER
                                    ? execution_timeout
                                    : runtime_filter_wait_time_ms;
    auto expected = _rf_state.load(std::memory_order_acquire);
    // In pipelineX, runtime filters will be ready or timeout before open phase.
    if (expected == RuntimeFilterState::NOT_READY) {
        DCHECK(MonotonicMillis() - _registration_time >= wait_times_ms);
        COUNTER_SET(_wait_timer,
                    int64_t((MonotonicMillis() - _registration_time) * NANOS_PER_MILLIS));
        _rf_state = RuntimeFilterState::TIMEOUT;
    }
}

void RuntimeFilter::signal() {
    if (!_wrapper->is_ignored() && !_wrapper->is_disabled() && _wrapper->is_bloomfilter() &&
        !_wrapper->get_bloomfilter()->inited()) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "bf not inited and not ignored/disabled, rf: {}",
                        debug_string());
    }

    COUNTER_SET(_wait_timer, int64_t((MonotonicMillis() - _registration_time) * NANOS_PER_MILLIS));
    _rf_state.store(RuntimeFilterState::READY);
    if (!_filter_timer.empty()) {
        for (auto& timer : _filter_timer) {
            timer->call_ready();
        }
    }

    if (_wrapper->get_real_type() == RuntimeFilterType::IN_FILTER) {
        _profile->add_info_string("InFilterSize", std::to_string(_wrapper->get_in_filter_size()));
    }
    if (_wrapper->get_real_type() == RuntimeFilterType::BITMAP_FILTER) {
        auto bitmap_filter = _wrapper->get_bitmap_filter();
        _profile->add_info_string("BitmapSize", std::to_string(bitmap_filter->size()));
        _profile->add_info_string("IsNotIn", bitmap_filter->is_not_in() ? "true" : "false");
    }
    if (_wrapper->get_real_type() == RuntimeFilterType::BLOOM_FILTER) {
        _profile->add_info_string("BloomFilterSize",
                                  std::to_string(_wrapper->get_bloom_filter_size()));
    }
}
std::shared_ptr<pipeline::RuntimeFilterTimer> RuntimeFilter::create_filter_timer(
        std::shared_ptr<pipeline::RuntimeFilterDependency> dependencie) {
    auto timer = std::make_shared<pipeline::RuntimeFilterTimer>(_registration_time,
                                                                _rf_wait_time_ms, dependencie);
    std::unique_lock lock(_inner_mutex);
    _filter_timer.push_back(timer);
    return timer;
}

void RuntimeFilter::set_finish_dependency(
        const std::shared_ptr<pipeline::CountedFinishDependency>& dependency) {
    _dependency = dependency;
    _dependency->add();
    CHECK(_dependency);
}

void RuntimeFilter::set_synced_size(uint64_t global_size) {
    _synced_size = global_size;
    if (_dependency) {
        _dependency->sub();
    }
}

void RuntimeFilter::set_ignored() {
    _wrapper->set_ignored();
}

bool RuntimeFilter::get_ignored() {
    return _wrapper->is_ignored();
}

void RuntimeFilter::set_disabled() {
    _wrapper->set_disabled();
}

bool RuntimeFilter::get_disabled() const {
    return _wrapper->is_disabled();
}

std::string RuntimeFilter::formatted_state() const {
    return fmt::format(
            "[Id = {}, IsPushDown = {}, RuntimeFilterState = {}, HasRemoteTarget = {}, "
            "HasLocalTarget = {}, Ignored = {}, Disabled = {}, Type = {}, WaitTimeMS = {}]",
            _filter_id, _is_push_down, to_string(_rf_state), _has_remote_target, _has_local_target,
            _wrapper->_context->ignored, _wrapper->_context->disabled, _wrapper->get_real_type(),
            _rf_wait_time_ms);
}

Status RuntimeFilter::_init_with_desc(const TRuntimeFilterDesc* desc, const TQueryOptions* options,
                                      int node_id) {
    // if node_id == -1 , it shouldn't be a consumer
    DCHECK(node_id >= 0 || (node_id == -1 && _role != RuntimeFilterRole::CONSUMER));

    vectorized::VExprContextSPtr build_ctx;
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(desc->src_expr, build_ctx));

    RuntimeFilterParams params;
    params.filter_id = _filter_id;
    params.filter_type = _runtime_filter_type;
    params.column_return_type = build_ctx->root()->type().type;
    params.max_in_num = options->runtime_filter_max_in_num;
    params.runtime_bloom_filter_min_size = options->__isset.runtime_bloom_filter_min_size
                                                   ? options->runtime_bloom_filter_min_size
                                                   : 0;
    params.runtime_bloom_filter_max_size = options->__isset.runtime_bloom_filter_max_size
                                                   ? options->runtime_bloom_filter_max_size
                                                   : 0;

    params.build_bf_exactly = desc->__isset.build_bf_exactly && desc->build_bf_exactly;
    params.bloom_filter_size_calculated_by_ndv = desc->bloom_filter_size_calculated_by_ndv;

    if (desc->__isset.bloom_filter_size_bytes) {
        params.bloom_filter_size = desc->bloom_filter_size_bytes;
    }
    params.null_aware = desc->__isset.null_aware && desc->null_aware;
    params.enable_fixed_len_to_uint32_v2 = options->__isset.enable_fixed_len_to_uint32_v2 &&
                                           options->enable_fixed_len_to_uint32_v2;
    if (_runtime_filter_type == RuntimeFilterType::BITMAP_FILTER) {
        if (_has_remote_target) {
            return Status::InternalError("bitmap filter do not support remote target");
        }
        if (!build_ctx->root()->type().is_bitmap_type()) {
            return Status::InternalError("Unexpected src expr type:{} for bitmap filter.",
                                         build_ctx->root()->type().debug_string());
        }
        if (!desc->__isset.bitmap_target_expr) {
            return Status::InternalError("Unknown bitmap filter target expr.");
        }
        vectorized::VExprContextSPtr bitmap_target_ctx;
        RETURN_IF_ERROR(
                vectorized::VExpr::create_expr_tree(desc->bitmap_target_expr, bitmap_target_ctx));
        params.column_return_type = bitmap_target_ctx->root()->type().type;

        if (desc->__isset.bitmap_filter_not_in) {
            params.bitmap_filter_not_in = desc->bitmap_filter_not_in;
        }
    }

    if (node_id >= 0) {
        const auto iter = desc->planId_to_target_expr.find(node_id);
        if (iter == desc->planId_to_target_expr.end()) {
            return Status::InternalError("not found a node id:{}", node_id);
        }
        _probe_expr = iter->second;
    }

    _wrapper = std::make_shared<RuntimePredicateWrapper>(&params);
    return Status::OK();
}

Status RuntimeFilter::init_with_size(size_t local_size) {
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

void RuntimeFilter::init_profile(RuntimeProfile* parent_profile) {
    if (_profile_init) {
        parent_profile->add_child(_profile.get(), true, nullptr);
    } else {
        _profile_init = true;
        parent_profile->add_child(_profile.get(), true, nullptr);
        _profile->add_info_string("Info", formatted_state());
        _wait_timer = ADD_TIMER(_profile, "WaitTime");
    }
}

void RuntimeFilter::update_runtime_filter_type_to_profile(uint64_t local_merge_time) {
    _profile->add_info_string("RealRuntimeFilterType", to_string(_wrapper->get_real_type()));
    _profile->add_info_string("LocalMergeTime",
                              std::to_string((double)local_merge_time / NANOS_PER_SEC) + " s");
}

std::string RuntimeFilter::debug_string() const {
    return fmt::format(
            "RuntimeFilter: (id = {}, type = {}, is_broadcast: {}, ignored: {}, disabled: {}, "
            "build_bf_cardinality: {}, dependency: {}, synced_size: {}, has_local_target: {}, "
            "has_remote_target: {}, error_msg: [{}]",
            _filter_id, to_string(_runtime_filter_type), _is_broadcast_join,
            _wrapper->_context->ignored, _wrapper->_context->disabled,
            _wrapper->get_build_bf_cardinality(),
            _dependency ? _dependency->debug_string() : "none", _synced_size, _has_local_target,
            _has_remote_target, _wrapper->_context->err_msg);
}

Status RuntimeFilter::merge_from(const RuntimeFilter* other) {
    auto status = _wrapper->merge(other->_wrapper.get());
    if (!status) {
        return Status::InternalError("runtime filter merge failed: {}, error_msg: {}",
                                     debug_string(), status.msg());
    }
    return Status::OK();
}

void RuntimeFilter::_to_protobuf(PInFilter* filter) {
    filter->set_column_type(to_proto(_wrapper->column_type()));
    _wrapper->_context->hybrid_set->to_pb(filter);
}

void RuntimeFilter::_to_protobuf(PMinMaxFilter* filter) {
    filter->set_column_type(to_proto(_wrapper->column_type()));
    _wrapper->_context->minmax_func->to_pb(filter);
}

RuntimeFilterType RuntimeFilter::get_real_type() {
    return _wrapper->get_real_type();
}

bool RuntimeFilter::need_sync_filter_size() {
    return _wrapper->get_build_bf_cardinality() && !_is_broadcast_join;
}

void RuntimeFilter::update_filter(const RuntimeFilter* other, int64_t merge_time,
                                  int64_t start_apply, uint64_t local_merge_time) {
    _profile->add_info_string("UpdateTime",
                              std::to_string(MonotonicMillis() - start_apply) + " ms");
    _profile->add_info_string("MergeTime", std::to_string(merge_time) + " ms");
    _wrapper = other->_wrapper;
    update_runtime_filter_type_to_profile(local_merge_time);
    signal();
}

} // namespace doris
