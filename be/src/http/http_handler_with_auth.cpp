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

#include "http_handler_with_auth.h"

#include <gen_cpp/HeartbeatService_types.h>

#include "http/http_channel.h"
#include "runtime/client_cache.h"
#include "util/thrift_rpc_helper.h"
#include "utils.h"

namespace doris {

class TPrivilegeType;
class TPrivilegeHier;
class ThriftRpcHelper;

HttpHandlerWithAuth::HttpHandlerWithAuth(ExecEnv* exec_env, TPrivilegeHier::type hier,
                                         TPrivilegeType::type type)
        : _exec_env(exec_env), _hier(hier), _type(type) {}

int HttpHandlerWithAuth::on_header(HttpRequest* req) {
    TCheckAuthRequest auth_request;
    TCheckAuthResult auth_result;
    AuthInfo auth_info;

    if (!config::enable_all_http_auth) {
        return 0;
    }

    if (!parse_basic_auth(*req, &auth_info)) {
        LOG(WARNING) << "parse basic authorization failed"
                     << ", request: " << req->debug_string();
        evhttp_add_header(evhttp_request_get_output_headers(req->get_evhttp_request()),
                          "WWW-Authenticate", "Basic realm=\"Restricted\"");
        HttpChannel::send_reply(req, HttpStatus::UNAUTHORIZED);
        return -1;
    }

    auth_request.user = auth_info.user;
    auth_request.passwd = auth_info.passwd;
    auth_request.__set_cluster(auth_info.cluster);
    auth_request.__set_user_ip(auth_info.user_ip);
    auth_request.__set_thrift_rpc_timeout_ms(config::thrift_rpc_timeout_ms);

    if (!on_privilege(*req, auth_request)) {
        LOG(WARNING) << "invalid privilege, request: " << req->debug_string();
        HttpChannel::send_error(req, HttpStatus::BAD_REQUEST);
        return -1;
    }

#ifndef BE_TEST
    TNetworkAddress master_addr = _exec_env->master_info()->network_address;
    {
        auto status = ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr.hostname, master_addr.port,
                [&auth_result, &auth_request](FrontendServiceConnection& client) {
                    client->checkAuth(auth_result, auth_request);
                });
        if (!status) {
            return -1;
        }
    }
#else
    if (auth_request.user == "root" && auth_request.passwd.empty()) {
        auth_result.status.status_code = TStatusCode::type::OK;
        auth_result.status.error_msgs.clear();
    } else {
        return -1;
    }
#endif
    Status status(Status::create(auth_result.status));
    if (!status.ok()) {
        LOG(WARNING) << "permission verification failed, request: " << auth_request;
        HttpChannel::send_reply(req, HttpStatus::FORBIDDEN);
        return -1;
    }
    return 0;
}

} // namespace doris
