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

#pragma once

#include <gen_cpp/internal_service.pb.h>

#include <string>

#include "common/status.h"
#include "util/threadpool.h"

namespace google::protobuf {
class Closure;
class RpcController;
} // namespace google::protobuf

namespace doris {

class ExecEnv;

class CdcClientManager {
public:
    explicit CdcClientManager(ExecEnv* exec_env);
    ~CdcClientManager();

    void stop();

    // Request CDC client to handle a request 
    void request_cdc_client_impl(const PRequestCdcClientRequest* request,
                                 PRequestCdcClientResult* result,
                                 google::protobuf::Closure* done);

    // Execute CDC scan and commit transaction
    void execute_cdc_scan_commit_impl(const PRequestCdcClientRequest* request,
                                      PRequestCdcClientResult* result,
                                      google::protobuf::Closure* done);

private:
    Status send_request_to_cdc_client(const std::string& api,
                                            const std::string& params_body,
                                            std::string* response);

    Status extract_meta_from_response(const std::string& cdc_response,
                                          std::string* meta_json);

    Status commit_transaction(const std::string& txn_id, const std::string& meta_json);

    ExecEnv* _exec_env = nullptr;
    std::unique_ptr<doris::ThreadPool> _thread_pool;
};

} // namespace doris

