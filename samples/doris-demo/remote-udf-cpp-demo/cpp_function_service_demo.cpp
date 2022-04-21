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
        if (fun_name == "int32_add") {
            result->mutable_type()->set_id(PGenericType::INT32);
            for (size_t i = 0; i < request->args(0).int32_value_size(); ++i) {
                result->add_int32_value(request->args(0).int32_value(i) +
                                        request->args(1).int32_value(i));
            }
        } else if (fun_name == "int64_add") {
            result->mutable_type()->set_id(PGenericType::INT64);
            for (size_t i = 0; i < request->args(0).int64_value_size(); ++i) {
                result->add_int64_value(request->args(0).int64_value(i) +
                                        request->args(1).int64_value(i));
            }
        } else if (fun_name == "int128_add") {
            result->mutable_type()->set_id(PGenericType::INT128);
            for (size_t i = 0; i < request->args(0).bytes_value_size(); ++i) {
                __int128 v1;
                memcpy(&v1, request->args(0).bytes_value(i).data(), sizeof(__int128));
                __int128 v2;
                memcpy(&v2, request->args(1).bytes_value(i).data(), sizeof(__int128));
                __int128 v = v1 + v2;
                char buffer[sizeof(__int128)];
                memcpy(buffer, &v, sizeof(__int128));
                result->add_bytes_value(buffer, sizeof(__int128));
            }
        } else if (fun_name == "float_add") {
            result->mutable_type()->set_id(PGenericType::FLOAT);
            for (size_t i = 0; i < request->args(0).float_value_size(); ++i) {
                result->add_float_value(request->args(0).float_value(i) +
                                        request->args(1).float_value(i));
            }
        } else if (fun_name == "double_add") {
            result->mutable_type()->set_id(PGenericType::DOUBLE);
            for (size_t i = 0; i < request->args(0).double_value_size(); ++i) {
                result->add_double_value(request->args(0).double_value(i) +
                                         request->args(1).double_value(i));
            }
        } else if (fun_name == "str_add") {
            result->mutable_type()->set_id(PGenericType::STRING);
            for (size_t i = 0; i < request->args(0).string_value_size(); ++i) {
                result->add_string_value(request->args(0).string_value(i) + " + " +
                                         request->args(1).string_value(i));
            }
        }
        response->mutable_status()->set_status_code(0);
        std::cout << response->DebugString();
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
