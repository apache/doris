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
#include "check_rpc_channel_action.h"

#include <gen_cpp/internal_service.pb.h>

#include <string>

#include "service/brpc.h"
#include "util/brpc_client_cache.h"
#include "util/md5.h"

namespace doris {
CheckRPCChannelAction::CheckRPCChannelAction(ExecEnv* exec_env)
        : BaseHttpHandler("check_rpc_channel", exec_env) {}

void CheckRPCChannelAction::handle_sync(brpc::Controller* cntl) {
    const std::string req_ip = *get_param(cntl, "ip");
    const std::string* req_port_ptr = get_param(cntl, "port");
    const std::string* req_payload_size_ptr = get_param(cntl, "payload_size");
    uint64_t port = 0;
    uint64_t payload_size = 0;
    try {
        port = std::stoull(*req_port_ptr);
        payload_size = std::stoull(*req_payload_size_ptr);
        if (port > 65535) {
            on_error(cntl,
                     fmt::format("invalid argument port, should between 0-65535, actrual is {0}",
                                 port));
            return;
        }
    } catch (const std::exception& e) {
        std::string err =
                fmt::format("invalid argument. port:{0}, payload_size: {1}", port, payload_size);
        LOG(WARNING) << err;
        on_error(cntl, err);
        return;
    }

    PCheckRPCChannelRequest req;
    PCheckRPCChannelResponse resp;
    brpc::Controller ck_cntl;
    std::string* buf = req.mutable_data();
    buf->resize(payload_size);
    req.set_size(payload_size);
    Md5Digest digest;
    digest.update(static_cast<const void*>(buf->c_str()), payload_size);
    digest.digest();
    req.set_md5(digest.hex());
    std::shared_ptr<PBackendService_Stub> stub(
            get_exec_env()->brpc_internal_client_cache()->get_client(req_ip, port));
    if (!stub) {
        on_error(cntl, fmt::format("cannot find valid connection to {0}:{1}.", req_ip, port));
        return;
    }
    stub->check_rpc_channel(&ck_cntl, &req, &resp, nullptr);
    if (ck_cntl.Failed()) {
        std::string err = fmt::format("open brpc connection to {0}:{1} failed: {2}", req_ip, port,
                                      ck_cntl.ErrorText());
        LOG(WARNING) << err;
        on_error(cntl, err);
        return;
    }
    if (resp.status().status_code() == 0) {
        std::string msg = fmt::format("open brpc connection to {0}:{1} succcess.", req_ip, port);
        LOG(INFO) << msg;
        on_succ(cntl, msg);
    } else {
        std::string err = fmt::format("open brpc connection to {0}:{1} failed.", req_ip, port);
        LOG(WARNING) << err;
        on_succ(cntl, err);
    }
}
} // namespace doris