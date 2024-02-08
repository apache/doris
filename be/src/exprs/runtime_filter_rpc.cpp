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

#include <brpc/controller.h>
#include <butil/iobuf.h>
#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>

#include <memory>
#include <ostream>
#include <string>

#include "common/config.h"
#include "common/status.h"
#include "exprs/runtime_filter.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
// for rpc
#include <gen_cpp/internal_service.pb.h>

#include "common/logging.h"
#include "util/brpc_client_cache.h"
#include "util/ref_count_closure.h"

namespace doris {

Status IRuntimeFilter::push_to_remote(RuntimeFilterParamsContext* state,
                                      const TNetworkAddress* addr, bool opt_remote_rf) {
    DCHECK(is_producer());
    std::shared_ptr<PBackendService_Stub> stub(
            state->exec_env->brpc_internal_client_cache()->get_client(*addr));
    if (!stub) {
        std::string msg =
                fmt::format("Get rpc stub failed, host={},  port=", addr->hostname, addr->port);
        return Status::InternalError(msg);
    }

    auto merge_filter_request = std::make_shared<PMergeFilterRequest>();
    auto merge_filter_callback = DummyBrpcCallback<PMergeFilterResponse>::create_shared();
    auto merge_filter_closure =
            AutoReleaseClosure<PMergeFilterRequest, DummyBrpcCallback<PMergeFilterResponse>>::
                    create_unique(merge_filter_request, merge_filter_callback);
    void* data = nullptr;
    int len = 0;

    auto pquery_id = merge_filter_request->mutable_query_id();
    pquery_id->set_hi(_state->query_id.hi());
    pquery_id->set_lo(_state->query_id.lo());

    auto pfragment_instance_id = merge_filter_request->mutable_fragment_instance_id();
    pfragment_instance_id->set_hi(state->fragment_instance_id().hi());
    pfragment_instance_id->set_lo(state->fragment_instance_id().lo());

    merge_filter_request->set_filter_id(_filter_id);
    merge_filter_request->set_opt_remote_rf(opt_remote_rf);
    merge_filter_request->set_is_pipeline(state->enable_pipeline_exec);
    merge_filter_callback->cntl_->set_timeout_ms(wait_time_ms());

    Status serialize_status = serialize(merge_filter_request.get(), &data, &len);
    if (serialize_status.ok()) {
        VLOG_NOTICE << "Producer:" << merge_filter_request->ShortDebugString() << addr->hostname
                    << ":" << addr->port;
        if (len > 0) {
            DCHECK(data != nullptr);
            merge_filter_callback->cntl_->request_attachment().append(data, len);
        }

        stub->merge_filter(merge_filter_closure->cntl_.get(), merge_filter_closure->request_.get(),
                           merge_filter_closure->response_.get(), merge_filter_closure.get());
        // the closure will be released by brpc during closure->Run.
        merge_filter_closure.release();
    }
    return serialize_status;
}

} // namespace doris
