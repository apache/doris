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

#include "runtime/exec_env.h"
#include "runtime/data_stream_mgr.h"
#include "service/brpc.h"

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
            eos ? nullptr : done);
    }
    if (eos) {
        TUniqueId finst_id;
        finst_id.__set_hi(request->finst_id().hi());
        finst_id.__set_lo(request->finst_id().lo());
        _exec_env->stream_mgr()->close_sender(
            finst_id, request->node_id(),
            request->sender_id(), request->be_number());
        done->Run();
    }
}

}
