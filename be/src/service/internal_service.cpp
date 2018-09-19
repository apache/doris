// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "service/internal_service.h"

#include "gen_cpp/BackendService.h"
#include "runtime/exec_env.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/fragment_mgr.h"
#include "service/brpc.h"
#include "util/thrift_util.h"
#include "runtime/buffer_control_block.h"
#include "runtime/result_buffer_mgr.h"

namespace palo {

PInternalServiceImpl::PInternalServiceImpl(ExecEnv* exec_env) : _exec_env(exec_env) {
}

PInternalServiceImpl::~PInternalServiceImpl() {
}

void PInternalServiceImpl::transmit_data(google::protobuf::RpcController* cntl_base,
                                         const PTransmitDataParams* request,
                                         PTransmitDataResult* response,
                                         google::protobuf::Closure* done) {
    bool eos = request->eos();
    if (request->has_row_batch()) {
        _exec_env->stream_mgr()->add_data(
            request->finst_id(), request->node_id(),
            request->row_batch(), request->sender_id(),
            request->be_number(), request->packet_seq(),
            eos ? nullptr : &done);
    }
    if (eos) {
        TUniqueId finst_id;
        finst_id.__set_hi(request->finst_id().hi());
        finst_id.__set_lo(request->finst_id().lo());
        _exec_env->stream_mgr()->close_sender(
            finst_id, request->node_id(),
            request->sender_id(), request->be_number());
    }
    if (done != nullptr) {
        done->Run();
    }
}

void PInternalServiceImpl::exec_plan_fragment(
        google::protobuf::RpcController* cntl_base,
        const PExecPlanFragmentRequest* request,
        PExecPlanFragmentResult* response,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    auto st = _exec_plan_fragment(cntl);
    if (!st.ok()) {
        LOG(WARNING) << "exec plan fragment failed, errmsg=" << st.get_error_msg();
    }
    st.to_protobuf(response->mutable_status());
}

Status PInternalServiceImpl::_exec_plan_fragment(brpc::Controller* cntl) {
    auto ser_request = cntl->request_attachment().to_string();
    TExecPlanFragmentParams t_request;
    {
        const uint8_t* buf = (const uint8_t*)ser_request.data();
        uint32_t len = ser_request.size();
        RETURN_IF_ERROR(deserialize_thrift_msg(buf, &len, false, &t_request));
    }
    LOG(INFO) << "exec plan fragment, finst_id=" << t_request.params.fragment_instance_id
        << ", coord=" << t_request.coord << ", backend=" << t_request.backend_num;
    return _exec_env->fragment_mgr()->exec_plan_fragment(t_request);
}

void PInternalServiceImpl::cancel_plan_fragment(
        google::protobuf::RpcController* cntl_base,
        const PCancelPlanFragmentRequest* request,
        PCancelPlanFragmentResult* result,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    TUniqueId tid;
    tid.__set_hi(request->finst_id().hi());
    tid.__set_lo(request->finst_id().lo());
    LOG(INFO) << "cancel framgent, finst_id=" << tid;
    auto st = _exec_env->fragment_mgr()->cancel(tid);
    if (!st.ok()) {
        LOG(WARNING) << "cancel plan fragment failed, errmsg=" << st.get_error_msg();
    }
    st.to_protobuf(result->mutable_status());
}

void PInternalServiceImpl::fetch_data(
        google::protobuf::RpcController* cntl_base,
        const PFetchDataRequest* request,
        PFetchDataResult* result,
        google::protobuf::Closure* done) {
    brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
    GetResultBatchCtx* ctx = new GetResultBatchCtx(cntl, result, done);
    _exec_env->result_mgr()->fetch_data(request->finst_id(), ctx);
}

void PInternalServiceImpl::fetch_fragment_exec_infos(
        google::protobuf::RpcController* controller,
        const PFetchFragmentExecInfoRequest* request,
        PFetchFragmentExecInfosResult* result,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    auto status = _exec_env->fragment_mgr()->fetch_fragment_exec_infos(result, request);
    if (!status.ok()) {
        LOG(WARNING) << "fetch fragment exec status failed:" << status.get_error_msg();
    }
    status.to_protobuf(result->mutable_status());
}

}
