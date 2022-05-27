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

#include "common/config.h"
#include "common/status.h"
#include "exprs/runtime_filter.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

// for rpc
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "service/brpc.h"
#include "util/brpc_client_cache.h"

namespace doris {

struct IRuntimeFilter::rpc_context {
    PMergeFilterRequest request;
    PMergeFilterResponse response;
    brpc::Controller cntl;
    brpc::CallId cid;
};

Status IRuntimeFilter::push_to_remote(RuntimeState* state, const TNetworkAddress* addr) {
    DCHECK(is_producer());
    DCHECK(_rpc_context == nullptr);
    std::shared_ptr<PBackendService_Stub> stub(
            state->exec_env()->brpc_internal_client_cache()->get_client(*addr));
    if (!stub) {
        std::string msg =
                fmt::format("Get rpc stub failed, host={},  port=", addr->hostname, addr->port);
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    _rpc_context = std::make_shared<IRuntimeFilter::rpc_context>();
    void* data = nullptr;
    int len = 0;

    auto pquery_id = _rpc_context->request.mutable_query_id();
    pquery_id->set_hi(_state->query_id().hi);
    pquery_id->set_lo(_state->query_id().lo);

    auto pfragment_instance_id = _rpc_context->request.mutable_fragment_id();
    pfragment_instance_id->set_hi(state->fragment_instance_id().hi);
    pfragment_instance_id->set_lo(state->fragment_instance_id().lo);

    _rpc_context->request.set_filter_id(_filter_id);
    _rpc_context->cntl.set_timeout_ms(1000);
    _rpc_context->cid = _rpc_context->cntl.call_id();

    Status serialize_status = serialize(&_rpc_context->request, &data, &len);
    if (serialize_status.ok()) {
        VLOG_NOTICE << "Producer:" << _rpc_context->request.ShortDebugString() << addr->hostname
                    << ":" << addr->port;
        if (len > 0) {
            DCHECK(data != nullptr);
            _rpc_context->cntl.request_attachment().append(data, len);
        }
        if (config::runtime_filter_use_async_rpc) {
            stub->merge_filter(&_rpc_context->cntl, &_rpc_context->request, &_rpc_context->response,
                               brpc::DoNothing());
        } else {
            stub->merge_filter(&_rpc_context->cntl, &_rpc_context->request, &_rpc_context->response,
                               nullptr);
            _rpc_context.reset();
        }

    } else {
        // we should reset context
        _rpc_context.reset();
    }
    return serialize_status;
}

Status IRuntimeFilter::join_rpc() {
    DCHECK(is_producer());
    if (_rpc_context != nullptr) {
        brpc::Join(_rpc_context->cid);
        if (_rpc_context->cntl.Failed()) {
            LOG(WARNING) << "runtimefilter rpc err:" << _rpc_context->cntl.ErrorText();
            // reset stub cache
            ExecEnv::GetInstance()->brpc_internal_client_cache()->erase(
                    _rpc_context->cntl.remote_side());
        }
    }
    return Status::OK();
}
} // namespace doris
