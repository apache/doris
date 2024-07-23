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

#include "service/brpc_service.h"

#include <brpc/server.h>
#include <brpc/ssl_options.h>
#include <butil/endpoint.h>
// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <gflags/gflags_declare.h>
#include <string.h>

#include <ostream>

#include "cloud/cloud_internal_service.h"
#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "service/internal_service.h"
#include "util/mem_info.h"

namespace brpc {

DECLARE_uint64(max_body_size);
DECLARE_int64(socket_max_unwritten_bytes);

} // namespace brpc

namespace doris {

BRpcService::BRpcService(ExecEnv* exec_env) : _exec_env(exec_env), _server(new brpc::Server()) {
    // Set config
    brpc::FLAGS_max_body_size = config::brpc_max_body_size;
    brpc::FLAGS_socket_max_unwritten_bytes =
            config::brpc_socket_max_unwritten_bytes != -1
                    ? config::brpc_socket_max_unwritten_bytes
                    : std::max((int64_t)1073741824, (MemInfo::mem_limit() / 1024) * 20);
}

BRpcService::~BRpcService() {
    join();
}

Status BRpcService::start(int port, int num_threads) {
    // Add service
    if (config::is_cloud_mode()) {
        _server->AddService(
                new CloudInternalServiceImpl(_exec_env->storage_engine().to_cloud(), _exec_env),
                brpc::SERVER_OWNS_SERVICE);
    } else {
        _server->AddService(
                new PInternalServiceImpl(_exec_env->storage_engine().to_local(), _exec_env),
                brpc::SERVER_OWNS_SERVICE);
    }
    // start service
    brpc::ServerOptions options;
    if (num_threads != -1) {
        options.num_threads = num_threads;
    }
    options.idle_timeout_sec = config::brpc_idle_timeout_sec;

    if (config::enable_https) {
        auto sslOptions = options.mutable_ssl_options();
        sslOptions->default_cert.certificate = config::ssl_certificate_path;
        sslOptions->default_cert.private_key = config::ssl_private_key_path;
    }

    butil::EndPoint point;
    if (butil::str2endpoint(BackendOptions::get_service_bind_address(), port, &point) < 0) {
        return Status::InternalError("convert address failed, host={}, port={}", "[::0]", port);
    }
    LOG(INFO) << "BRPC server bind to host: " << BackendOptions::get_service_bind_address()
              << ", port: " << port;
    if (_server->Start(point, &options) != 0) {
        char buf[64];
        LOG(WARNING) << "start brpc failed, errno=" << errno
                     << ", errmsg=" << strerror_r(errno, buf, 64) << ", port=" << port;
        return Status::InternalError("start brpc service failed");
    }
    return Status::OK();
}

void BRpcService::join() {
    int stop_succeed = _server->Stop(1000);

    if (stop_succeed == 0) {
        _server->Join();
    } else {
        LOG(WARNING) << "Failed to stop brpc service, "
                     << "not calling brpc server join since it will never retrun."
                     << "maybe something bad will happen, let us know if you meet something error.";
    }

    _server->ClearServices();
}

} // namespace doris
