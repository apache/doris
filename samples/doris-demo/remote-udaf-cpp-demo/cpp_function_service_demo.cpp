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

#include <brpc/server.h>
#include <butil/logging.h>

#include "function_service.pb.h"

#include<sys/time.h>


namespace doris {
class FunctionServiceImpl : public PFunctionService {
public:
    FunctionServiceImpl() {}
    virtual ~FunctionServiceImpl() {}

    void fn_call(::google::protobuf::RpcController* controller,
                 const ::doris::PFunctionCallRequest* request,
                 ::doris::PFunctionCallResponse* response,
                 ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard closure_guard(done);
        std::string fun_name = request->function_name();
        auto* result = response->add_result();
        if(fun_name=="rpc_sum_merge"){
            result->mutable_type()->set_id(PGenericType::INT32);
            int sum=0;
            for (size_t i = 0; i < request->args_size(); ++i) {
                sum += request->args(i).int32_value(0);
            }
            result->add_int32_value(sum);
        }
        if(fun_name=="rpc_sum_finalize"){
             result->mutable_type()->set_id(PGenericType::INT32);
             result->add_int32_value(request->context().function_context().args_data(0).int32_value(0));
        }
        if(fun_name=="rpc_sum_update"){
            result->mutable_type()->set_id(PGenericType::INT32);
            int sum=0;
            for (size_t i = 0; i < request->args(0).int32_value_size(); ++i) {
                sum += request->args(0).int32_value(i);
            }
            if(request->has_context() && request->context().has_function_context()){
                sum += request->context().function_context().args_data(0).int32_value(0);
            }
            result->add_int32_value(sum);
        }
        if(fun_name=="rpc_avg_update"){
            result->mutable_type()->set_id(PGenericType::DOUBLE);
            double sum=0;
            int64_t size = request->args(0).int32_value_size();
            for (size_t i = 0; i < request->args(0).int32_value_size(); ++i) {
                sum += request->args(0).int32_value(i);
            }
            if(request->has_context() && request->context().has_function_context()){
                sum += request->context().function_context().args_data(0).double_value(0);
                size += request->context().function_context().args_data(0).int32_value(0);
            }
            result->add_double_value(sum);
            result->add_int32_value(size);
        }
        if(fun_name=="rpc_avg_merge"){
            result->mutable_type()->set_id(PGenericType::INT32);
            double sum= 0;
            int32_t size = 0;
            for (size_t i = 0; i < request->args_size(); ++i) {
                sum += request->args(i).double_value(0);
                size += request->args(i).int32_value(0);
            }
            result->add_double_value(sum);
            result->add_int32_value(size);
        }
        if(fun_name=="rpc_avg_finalize"){
             result->mutable_type()->set_id(PGenericType::DOUBLE);
             double sum = request->context().function_context().args_data(0).double_value(0);
             int64_t size = request->context().function_context().args_data(0).int32_value(0);
             double avg = sum / size;
             result->add_double_value(avg);
        }
        response->mutable_status()->set_status_code(0);
    }

    void check_fn(google::protobuf::RpcController* controller, const PCheckFunctionRequest* request,
                  PCheckFunctionResponse* response, google::protobuf::Closure* done) override {
        brpc::ClosureGuard closure_guard(done);
        response->mutable_status()->set_status_code(0);
    }

    void hand_shake(google::protobuf::RpcController* controller, const PHandShakeRequest* request,
                    PHandShakeResponse* response, google::protobuf::Closure* done) override {
        brpc::ClosureGuard closure_guard(done);
        if (request->has_hello()) {
            response->set_hello(request->hello());
        }
        response->mutable_status()->set_status_code(0);
    }
};
} // namespace doris

int main(int argc, char** argv) {
    int port = 9000;
    if (argc > 1) {
        try {
            port = std::stoi(argv[1], 0, 10);
        } catch (const std::exception& e) {
            std::cerr << "port " << argv[1] << " must be an integer." << std::endl;
            return -1;
        }
    }

    brpc::Server server;

    doris::FunctionServiceImpl function_service_impl;

    if (server.AddService(&function_service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    // Start the server.
    brpc::ServerOptions options;
    options.idle_timeout_sec = 1000;
    if (server.Start(port, &options) != 0) {
        LOG(ERROR) << "Fail to start EchoServer";
        return -1;
    }

    // Wait until Ctrl-C is pressed, then Stop() and Join() the server.
    server.RunUntilAskedToQuit();
    return 0;
}
