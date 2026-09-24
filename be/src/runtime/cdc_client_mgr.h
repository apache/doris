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

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>

#include "common/status.h"

namespace google::protobuf {
class Closure;
class RpcController;
} // namespace google::protobuf

namespace doris {

class CdcClientMgr {
public:
    CdcClientMgr();
    ~CdcClientMgr();

    void stop();

    // Request CDC client to handle a request
    void request_cdc_client_impl(const PRequestCdcClientRequest* request,
                                 PRequestCdcClientResult* result, google::protobuf::Closure* done);

    Status send_request_to_cdc_client(const std::string& api, const std::string& params_body,
                                      std::string* response);

    Status start_cdc_client(PRequestCdcClientResult* result);

#ifdef BE_TEST
    // For testing only: get current child PID
    pid_t get_child_pid() const { return _get_child_pid(); }
    // For testing only: publish a PID and return its generation-qualified identity.
    uint64_t set_child_pid_for_test(pid_t pid);
    uint64_t get_child_identity_for_test() const { return _get_child_identity(); }
    // For testing only: run the production cleanup gate for an exact identity.
    bool terminate_child_identity_for_test(uint64_t identity);
    // For testing only: invoke the installed handler body deterministically.
    static void invoke_sigchld_handler_for_test();
    // For testing only: pause a deterministic handler after it has copied the published identity.
    static void pause_sigchld_handler_for_test(bool pause);
    static bool sigchld_handler_paused_for_test();
    // For testing only: inspect / drive the adopt-external flag
    bool get_adopted_external_for_test() const { return _adopted_external.load(); }
    void set_adopted_external_for_test(bool v) { _adopted_external.store(v); }
#endif

private:
    uint64_t _get_child_identity() const;
    pid_t _get_child_pid() const;
    uint64_t _publish_child_pid(pid_t pid);
    bool _terminate_child_identity(uint64_t identity);

    std::mutex _start_mutex;
    std::atomic<bool> _adopted_external {false};
};

} // namespace doris
