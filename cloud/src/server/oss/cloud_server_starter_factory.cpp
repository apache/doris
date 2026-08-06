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
#include <errno.h> // IWYU pragma: keep
#include <string.h>

#include <memory>

#include "common/config.h"
#include "common/logging.h"

namespace doris::cloud {
namespace {

class OssMetaBrpcServerStarter final : public ICloudServerStarter {
public:
    OssMetaBrpcServerStarter(brpc::Server* server, int port) : _server(server), _port(port) {}

    bool start() override {
        if (_server == nullptr) {
            LOG(ERROR) << "meta brpc server is null";
            return false;
        }
        if (config::enable_tls) {
            LOG(ERROR) << "Cloud TLS requires TLS module";
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
        if (_server->Start(_port, &options) != 0) {
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
