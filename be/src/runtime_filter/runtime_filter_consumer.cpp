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

#include "runtime_filter/runtime_filter_consumer.h"

#include "exprs/minmax_predicate.h"
#include "util/runtime_profile.h"
#include "vec/exprs/vbitmap_predicate.h"
#include "vec/exprs/vbloom_predicate.h"
#include "vec/exprs/vdirect_in_predicate.h"

namespace doris {
#include "common/compile_check_begin.h"
Status RuntimeFilterConsumer::_apply_ready_expr(
        std::vector<vectorized::VRuntimeFilterPtr>& push_exprs) {
    _check_state({State::READY});
    _set_state(State::APPLIED);

    if (_wrapper->get_state() != RuntimeFilterWrapper::State::READY) {
        _wrapper->check_state({RuntimeFilterWrapper::State::DISABLED});
        return Status::OK();
    }

    auto origin_size = push_exprs.size();
    RETURN_IF_ERROR(_get_push_exprs(push_exprs, _probe_expr));

    for (auto i = origin_size; i < push_exprs.size(); i++) {
        push_exprs[i]->attach_profile_counter(_rf_input, _rf_filter, _always_true_counter);
    }
    return Status::OK();
}

Status RuntimeFilterConsumer::acquire_expr(std::vector<vectorized::VRuntimeFilterPtr>& push_exprs) {
    std::unique_lock<std::recursive_mutex> l(_rmtx);
    if (_rf_state == State::READY) {
        RETURN_IF_ERROR(_apply_ready_expr(push_exprs));
    }
    if (_rf_state != State::APPLIED && _rf_state != State::TIMEOUT) {
        _set_state(State::TIMEOUT);
    }
    return Status::OK();
}

void RuntimeFilterConsumer::signal(RuntimeFilter* other) {
    std::unique_lock<std::recursive_mutex> l(_rmtx);
    COUNTER_SET(_wait_timer, int64_t((MonotonicMillis() - _registration_time) * NANOS_PER_MILLIS));
    _set_state(State::READY, other->_wrapper);
    if (!_filter_timer.empty()) {
        for (auto& timer : _filter_timer) {
            timer->call_ready();
        }
    }
}

std::shared_ptr<pipeline::RuntimeFilterTimer> RuntimeFilterConsumer::create_filter_timer(
        std::shared_ptr<pipeline::Dependency> dependencies) {
    std::unique_lock<std::recursive_mutex> l(_rmtx);
    auto timer = std::make_shared<pipeline::RuntimeFilterTimer>(_registration_time,
                                                                _rf_wait_time_ms, dependencies);
    _filter_timer.push_back(timer);
    return timer;
}

Status RuntimeFilterConsumer::_get_push_exprs(std::vector<vectorized::VRuntimeFilterPtr>& container,
                                              const TExpr& probe_expr) {
    // TODO: `VExprContextSPtr` is not need, we should just create an expr.
    vectorized::VExprContextSPtr probe_ctx;
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(probe_expr, probe_ctx));

    auto real_filter_type = _wrapper->get_real_type();
    bool null_aware = _wrapper->contain_null();
    switch (real_filter_type) {
    case RuntimeFilterType::IN_FILTER: {
        TTypeDesc type_desc = create_type_desc(PrimitiveType::TYPE_BOOLEAN);
        type_desc.__set_is_nullable(false);
        TExprNode node;
        node.__set_type(type_desc);
        // NULL_AWARE_IN_PRED predicate will do not push down to olap
        node.__set_node_type(null_aware ? TExprNodeType::NULL_AWARE_IN_PRED
                                        : TExprNodeType::IN_PRED);
        node.in_predicate.__set_is_not_in(false);
        node.__set_opcode(TExprOpcode::FILTER_IN);
        node.__set_is_nullable(false);
        auto in_pred = vectorized::VDirectInPredicate::create_shared(node, _wrapper->hybrid_set());
        in_pred->add_child(probe_ctx->root());
        auto wrapper = vectorized::VRuntimeFilterWrapper::create_shared(
                node, in_pred, get_in_list_ignore_thredhold(_wrapper->hybrid_set()->size()),
                null_aware, _wrapper->filter_id());
        container.push_back(wrapper);
        break;
    }
    case RuntimeFilterType::MIN_FILTER: {
        // create min filter
        vectorized::VExprSPtr min_pred;
        TExprNode min_pred_node;
        RETURN_IF_ERROR(create_vbin_predicate(probe_ctx->root()->data_type(), TExprOpcode::GE,
                                              min_pred, &min_pred_node, null_aware));
        vectorized::VExprSPtr min_literal;
        RETURN_IF_ERROR(create_literal(probe_ctx->root()->data_type(),
                                       _wrapper->minmax_func()->get_min(), min_literal));
        min_pred->add_child(probe_ctx->root());
        min_pred->add_child(min_literal);
        DCHECK(null_aware == false) << "only min predicate do not support null aware";
        container.push_back(vectorized::VRuntimeFilterWrapper::create_shared(
                min_pred_node, min_pred, get_comparison_ignore_thredhold(), null_aware,
                _wrapper->filter_id()));
        break;
    }
    case RuntimeFilterType::MAX_FILTER: {
        vectorized::VExprSPtr max_pred;
        // create max filter
        TExprNode max_pred_node;
        RETURN_IF_ERROR(create_vbin_predicate(probe_ctx->root()->data_type(), TExprOpcode::LE,
                                              max_pred, &max_pred_node, null_aware));
        vectorized::VExprSPtr max_literal;
        RETURN_IF_ERROR(create_literal(probe_ctx->root()->data_type(),
                                       _wrapper->minmax_func()->get_max(), max_literal));
        max_pred->add_child(probe_ctx->root());
        max_pred->add_child(max_literal);
        DCHECK(null_aware == false) << "only max predicate do not support null aware";
        container.push_back(vectorized::VRuntimeFilterWrapper::create_shared(
                max_pred_node, max_pred, get_comparison_ignore_thredhold(), null_aware,
                _wrapper->filter_id()));
        break;
    }
    case RuntimeFilterType::MINMAX_FILTER: {
        vectorized::VExprSPtr max_pred;
        // create max filter
        TExprNode max_pred_node;
        RETURN_IF_ERROR(create_vbin_predicate(probe_ctx->root()->data_type(), TExprOpcode::LE,
                                              max_pred, &max_pred_node, null_aware));
        vectorized::VExprSPtr max_literal;
        RETURN_IF_ERROR(create_literal(probe_ctx->root()->data_type(),
                                       _wrapper->minmax_func()->get_max(), max_literal));
        max_pred->add_child(probe_ctx->root());
        max_pred->add_child(max_literal);
        container.push_back(vectorized::VRuntimeFilterWrapper::create_shared(
                max_pred_node, max_pred, get_comparison_ignore_thredhold(), null_aware,
                _wrapper->filter_id()));

        vectorized::VExprContextSPtr new_probe_ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(probe_expr, new_probe_ctx));

        // create min filter
        vectorized::VExprSPtr min_pred;
        TExprNode min_pred_node;
        RETURN_IF_ERROR(create_vbin_predicate(new_probe_ctx->root()->data_type(), TExprOpcode::GE,
                                              min_pred, &min_pred_node, null_aware));
        vectorized::VExprSPtr min_literal;
        RETURN_IF_ERROR(create_literal(new_probe_ctx->root()->data_type(),
                                       _wrapper->minmax_func()->get_min(), min_literal));
        min_pred->add_child(new_probe_ctx->root());
        min_pred->add_child(min_literal);
        container.push_back(vectorized::VRuntimeFilterWrapper::create_shared(
                min_pred_node, min_pred, get_comparison_ignore_thredhold(), null_aware,
                _wrapper->filter_id()));
        break;
    }
    case RuntimeFilterType::BLOOM_FILTER: {
        // create a bloom filter
        TTypeDesc type_desc = create_type_desc(PrimitiveType::TYPE_BOOLEAN);
        type_desc.__set_is_nullable(false);
        TExprNode node;
        node.__set_type(type_desc);
        node.__set_node_type(TExprNodeType::BLOOM_PRED);
        node.__set_opcode(TExprOpcode::RT_FILTER);
        node.__set_is_nullable(false);
        auto bloom_pred = vectorized::VBloomPredicate::create_shared(node);
        bloom_pred->set_filter(_wrapper->bloom_filter_func());
        bloom_pred->add_child(probe_ctx->root());
        auto wrapper = vectorized::VRuntimeFilterWrapper::create_shared(
                node, bloom_pred, get_bloom_filter_ignore_thredhold(), null_aware,
                _wrapper->filter_id());
        container.push_back(wrapper);
        break;
    }
    case RuntimeFilterType::BITMAP_FILTER: {
        // create a bitmap filter
        TTypeDesc type_desc = create_type_desc(PrimitiveType::TYPE_BOOLEAN);
        type_desc.__set_is_nullable(false);
        TExprNode node;
        node.__set_type(type_desc);
        node.__set_node_type(TExprNodeType::BITMAP_PRED);
        node.__set_opcode(TExprOpcode::RT_FILTER);
        node.__set_is_nullable(false);
        auto bitmap_pred = vectorized::VBitmapPredicate::create_shared(node);
        bitmap_pred->set_filter(_wrapper->bitmap_filter_func());
        bitmap_pred->add_child(probe_ctx->root());
        DCHECK(null_aware == false) << "bitmap predicate do not support null aware";
        auto wrapper = vectorized::VRuntimeFilterWrapper::create_shared(
                node, bitmap_pred, 0, null_aware, _wrapper->filter_id());
        container.push_back(wrapper);
        break;
    }
    default:
        DCHECK(false);
        break;
    }
    return Status::OK();
}

void RuntimeFilterConsumer::collect_realtime_profile(RuntimeProfile* parent_operator_profile) {
    std::unique_lock<std::recursive_mutex> l(_rmtx);
    DCHECK(parent_operator_profile != nullptr);
    int filter_id = -1;
    {
        // since debug_string will read from  RuntimeFilter::_wrapper
        // and it is a shared_ptr, instead of a atomic_shared_ptr
        // so it is not thread safe
        filter_id = _wrapper->filter_id();
        parent_operator_profile->add_description(fmt::format("RF{} Info", filter_id),
                                                 debug_string(), "RuntimeFilterInfo");
    }

    // Counter* is owned by RuntimeProfile, so no need to free.
    RuntimeProfile::Counter* c = parent_operator_profile->add_counter(
            fmt::format("RF{} InputRows", filter_id), TUnit::UNIT, "RuntimeFilterInfo", 1);
    c->update(_rf_input->value());

    c = parent_operator_profile->add_counter(fmt::format("RF{} FilterRows", filter_id), TUnit::UNIT,
                                             "RuntimeFilterInfo", 1);
    c->update(_rf_filter->value());
    c = parent_operator_profile->add_counter(fmt::format("RF{} WaitTime", filter_id),
                                             TUnit::TIME_NS, "RuntimeFilterInfo", 2);
    c->update(_wait_timer->value());

    c = parent_operator_profile->add_counter(fmt::format("RF{} AlwaysTrueFilterRows", filter_id),
                                             TUnit::UNIT, "RuntimeFilterInfo", 2);
    c->update(_always_true_counter->value());
}

} // namespace doris
