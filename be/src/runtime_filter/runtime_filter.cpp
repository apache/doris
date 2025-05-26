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

#include "runtime_filter/runtime_filter.h"

#include "common/status.h"
#include "runtime/runtime_state.h"
#include "util/brpc_client_cache.h"
#include "util/brpc_closure.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
#include "common/compile_check_begin.h"
Status RuntimeFilter::_push_to_remote(RuntimeState* state, const TNetworkAddress* addr) {
    std::shared_ptr<PBackendService_Stub> stub(
            state->get_query_ctx()->exec_env()->brpc_internal_client_cache()->get_client(*addr));
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
    pquery_id->set_hi(state->get_query_ctx()->query_id().hi);
    pquery_id->set_lo(state->get_query_ctx()->query_id().lo);

    auto* pfragment_instance_id = merge_filter_request->mutable_fragment_instance_id();
    pfragment_instance_id->set_hi(BackendOptions::get_local_backend().id);
    pfragment_instance_id->set_lo((int64_t)this);

    merge_filter_callback->cntl_->set_timeout_ms(
            get_execution_rpc_timeout_ms(state->get_query_ctx()->execution_timeout()));
    if (config::execution_ignore_eovercrowded) {
        merge_filter_callback->cntl_->ignore_eovercrowded();
    }

    RETURN_IF_ERROR(serialize(merge_filter_request.get(), &data, &len));

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

Status RuntimeFilter::_init_with_desc(const TRuntimeFilterDesc* desc,
                                      const TQueryOptions* options) {
    vectorized::VExprContextSPtr build_ctx;
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(desc->src_expr, build_ctx));

    RuntimeFilterParams params;
    params.filter_id = desc->filter_id;
    params.filter_type = _runtime_filter_type;
    params.column_return_type = build_ctx->root()->data_type()->get_primitive_type();
    params.max_in_num = options->runtime_filter_max_in_num;
    params.runtime_bloom_filter_min_size = options->__isset.runtime_bloom_filter_min_size
                                                   ? options->runtime_bloom_filter_min_size
                                                   : 0;
    params.runtime_bloom_filter_max_size = options->__isset.runtime_bloom_filter_max_size
                                                   ? options->runtime_bloom_filter_max_size
                                                   : 0;

    params.build_bf_by_runtime_size =
            desc->__isset.build_bf_by_runtime_size && desc->build_bf_by_runtime_size;
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
        if (build_ctx->root()->data_type()->get_primitive_type() != PrimitiveType::TYPE_OBJECT) {
            return Status::InternalError("Unexpected src expr type:{} for bitmap filter.",
                                         build_ctx->root()->data_type()->get_name());
        }
        if (!desc->__isset.bitmap_target_expr) {
            return Status::InternalError("Unknown bitmap filter target expr.");
        }
        vectorized::VExprContextSPtr bitmap_target_ctx;
        RETURN_IF_ERROR(
                vectorized::VExpr::create_expr_tree(desc->bitmap_target_expr, bitmap_target_ctx));
        params.column_return_type = bitmap_target_ctx->root()->data_type()->get_primitive_type();

        if (desc->__isset.bitmap_filter_not_in) {
            params.bitmap_filter_not_in = desc->bitmap_filter_not_in;
        }
    }

    _wrapper = std::make_shared<RuntimeFilterWrapper>(&params);
    return Status::OK();
}

std::string RuntimeFilter::_debug_string() const {
    return fmt::format("{}, mode: {}", _wrapper ? _wrapper->debug_string() : "<null wrapper>",
                       _has_remote_target ? "GLOBAL" : "LOCAL");
}

void RuntimeFilter::_check_wrapper_state(
        const std::vector<RuntimeFilterWrapper::State>& assumed_states) {
    // _wrapper is null mean rf is published
    if (!_wrapper) {
        return;
    }
    try {
        _wrapper->check_state(assumed_states);
    } catch (const Exception& e) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "rf wrapper meet invalid state, {}, {}",
                        e.what(), debug_string());
    }
}

} // namespace doris
