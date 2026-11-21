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

#include "server/cloud_server_starter_factory.h"

#include <brpc/server.h>
// IWYU pragma: no_include <bthread/errno.h>
#include <butil/endpoint.h>
#include <errno.h> // IWYU pragma: keep
#include <string.h>

#include <memory>
#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "common/network_util.h"

namespace doris::cloud {
namespace {

class OssMetaBrpcServerStarter final : public ICloudServerStarter {
public:
    OssMetaBrpcServerStarter(brpc::Server* server, int port) : _server(server), _port(port) {}

    bool validate_config() override {
        if (_server == nullptr) {
            LOG(ERROR) << "meta brpc server is null";
            return false;
        }
        if (config::enable_tls) {
            LOG(ERROR) << "Cloud TLS requires TLS module";
            return false;
        }
        return true;
    }

    bool start() override {
        if (!validate_config()) {
            return false;
        }

        brpc::ServerOptions options;
        if (config::brpc_idle_timeout_sec != -1) {
            options.idle_timeout_sec = config::brpc_idle_timeout_sec;
        }
        if (config::brpc_num_threads != -1) {
            options.num_threads = config::brpc_num_threads;
        }
        if (config::brpc_internal_listen_port > 0) {
            options.internal_port = config::brpc_internal_listen_port;
        }
        // Bind "[::0]" when an IPv6 address is selected, so IPv6-only hosts
        // are reachable (mirrors BE be_server_starter_factory).
        const std::string bind_addr = get_service_bind_address(config::priority_networks);
        butil::EndPoint point;
        if (butil::str2endpoint(bind_addr.c_str(), _port, &point) < 0) {
            LOG(WARNING) << "convert address failed, host=" << bind_addr << ", port=" << _port;
            return false;
        }
        LOG(INFO) << "cloud brpc server bind to host: " << bind_addr << ", port: " << _port;
        if (_server->Start(point, &options) != 0) {
            char buf[64];
            LOG(WARNING) << "failed to start cloud brpc, errno=" << errno
                         << ", errmsg=" << strerror_r(errno, buf, 64) << ", port=" << _port;
            return false;
        }
        return true;
    }

    void stop() override {}

    void join() override {}

private:
    brpc::Server* _server;
    int _port;
};

} // namespace

bool create_meta_brpc_starter(brpc::Server* server, int port,
                              std::unique_ptr<ICloudServerStarter>* out) {
    if (out == nullptr) {
        LOG(ERROR) << "meta brpc starter output parameter is null";
        return false;
    }
    *out = std::make_unique<OssMetaBrpcServerStarter>(server, port);
    return true;
}

} // namespace doris::cloud
