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

#include "http/action/check_rpc_channel_action.h"

#include <brpc/controller.h>
#include <fmt/format.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <stdint.h>

#include <exception>
#include <memory>
#include <string>

#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "runtime/exec_env.h"
#include "util/brpc_client_cache.h"
#include "util/md5.h"

namespace doris {
CheckRPCChannelAction::CheckRPCChannelAction(ExecEnv* exec_env, TPrivilegeHier::type hier,
                                             TPrivilegeType::type type)
        : HttpHandlerWithAuth(exec_env, hier, type) {}
void CheckRPCChannelAction::handle(HttpRequest* req) {
    std::string req_ip = req->param("ip");
    std::string req_port = req->param("port");
    std::string req_payload_size = req->param("payload_size");
    uint64_t port = 0;
    uint64_t payload_size = 0;
    try {
        port = std::stoull(req_port);
        payload_size = std::stoull(req_payload_size);
        if (port > 65535) {
            HttpChannel::send_reply(
                    req, HttpStatus::INTERNAL_SERVER_ERROR,
                    fmt::format("invalid argument port, should between 0-65535, actrual is {0}",
                                req_port));
            return;
        }
        if (payload_size > (10 * 2 << 20) /* 10M */ || payload_size == 0) {
            HttpChannel::send_reply(
                    req, HttpStatus::INTERNAL_SERVER_ERROR,
                    fmt::format(
                            "invalid argument payload_size, should between 1-10M, actrual is {0}",
                            req_payload_size));
            return;
        }
    } catch (const std::exception& e) {
        std::string err = fmt::format("invalid argument. port: {0}, payload_size: {1}, reason: {}",
                                      req_port, req_payload_size, e.what());
        LOG(WARNING) << err;
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, err);
        return;
    }
    PCheckRPCChannelRequest request;
    PCheckRPCChannelResponse response;
    brpc::Controller cntl;
    std::string* buf = request.mutable_data();
    buf->resize(payload_size);
    request.set_size(payload_size);
    Md5Digest digest;
    digest.update(static_cast<const void*>(buf->c_str()), payload_size);
    digest.digest();
    request.set_md5(digest.hex());
    std::shared_ptr<PBackendService_Stub> stub(
            _exec_env->brpc_internal_client_cache()->get_client(req_ip, port));
    if (!stub) {
        HttpChannel::send_reply(
                req, HttpStatus::INTERNAL_SERVER_ERROR,
                fmt::format("cannot find valid connection to {0}:{1}.", req_ip, req_port));
        return;
    }
    stub->check_rpc_channel(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        std::string err = fmt::format("open brpc connection to {0}:{1} failed: {2}", req_ip,
                                      req_port, cntl.ErrorText());
        LOG(WARNING) << err;
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, err);
        return;
    }
    if (response.status().status_code() == 0) {
        std::string err = fmt::format("open brpc connection to {0}:{1} success.", req_ip, req_port);
        LOG(WARNING) << err;
        HttpChannel::send_reply(req, HttpStatus::OK, err);
    } else {
        std::string err = fmt::format("open brpc connection to {0}:{1} failed.", req_ip, req_port);
        LOG(WARNING) << err;
        HttpChannel::send_reply(req, HttpStatus::OK, err);
    }
}

} // namespace doris
