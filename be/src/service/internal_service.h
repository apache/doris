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

#pragma once

#include "common/status.h"
#include "gen_cpp/internal_service.pb.h"

namespace brpc {
class Controller;
}

namespace palo {

class ExecEnv;

class PInternalServiceImpl : public PInternalService {
public:
    PInternalServiceImpl(ExecEnv* exec_env);
    virtual ~PInternalServiceImpl();

    void transmit_data(::google::protobuf::RpcController* controller,
                       const ::palo::PTransmitDataParams* request,
                       ::palo::PTransmitDataResult* response,
                       ::google::protobuf::Closure* done) override;
    void exec_plan_fragment(
        google::protobuf::RpcController* controller,
        const PExecPlanFragmentRequest* request,
        PExecPlanFragmentResult* result,
        google::protobuf::Closure* done) override;

    void cancel_plan_fragment(
        google::protobuf::RpcController* controller,
        const PCancelPlanFragmentRequest* request,
        PCancelPlanFragmentResult* result,
        google::protobuf::Closure* done) override;

    void fetch_data(
        google::protobuf::RpcController* controller,
        const PFetchDataRequest* request,
        PFetchDataResult* result,
        google::protobuf::Closure* done) override;
private:
    Status _exec_plan_fragment(brpc::Controller* cntl);

private:
    ExecEnv* _exec_env;
};

}
