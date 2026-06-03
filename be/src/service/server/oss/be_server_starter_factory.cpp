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

#include "service/server/be_server_starter_factory.h"

#include <brpc/server.h>
#include <brpc/ssl_options.h>
#include <butil/endpoint.h>
#include <gen_cpp/BackendService.h>
#include <gen_cpp/HeartbeatService.h>
// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <gflags/gflags_declare.h>
#include <string.h>

#include <algorithm>
#include <memory>

#include "agent/heartbeat_server.h"
#include "cloud/cloud_internal_service.h"
#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "runtime/exec_env.h"
#include "service/arrow_flight/flight_sql_service.h"
#include "service/backend_options.h"
#include "service/backend_service.h"
#include "service/http_service.h"
#include "service/internal_service.h"
#include "storage/storage_engine.h"
#include "util/mem_info.h"
#include "util/thrift_server.h"

namespace brpc {
DECLARE_uint64(max_body_size);
DECLARE_int64(socket_max_unwritten_bytes);
DECLARE_bool(usercode_in_pthread);
} // namespace brpc

namespace doris::server {
namespace {

class OssBrpcServiceStarter final : public IServerStarter {
public:
    OssBrpcServiceStarter(ExecEnv* env, int port, int num_threads)
            : _env(env),
              _server(std::make_unique<brpc::Server>()),
              _port(port),
              _num_threads(num_threads) {
        if (config::brpc_usercode_in_pthread) {
            brpc::FLAGS_usercode_in_pthread = true;
        }
        brpc::FLAGS_max_body_size = config::brpc_max_body_size;
        brpc::FLAGS_socket_max_unwritten_bytes =
                config::brpc_socket_max_unwritten_bytes != -1
                        ? config::brpc_socket_max_unwritten_bytes
                        : std::max((int64_t)1073741824, (MemInfo::mem_limit() / 1024) * 20);
    }

    ~OssBrpcServiceStarter() override { join(); }

    Status start() override {
        if (config::enable_tls) {
            return Status::NotSupported("BE BRPC TLS requires TLS module");
        }

        if (config::is_cloud_mode()) {
            _server->AddService(
                    new CloudInternalServiceImpl(_env->storage_engine().to_cloud(), _env),
                    brpc::SERVER_OWNS_SERVICE);
        } else {
            _server->AddService(new PInternalServiceImpl(_env->storage_engine().to_local(), _env),
                                brpc::SERVER_OWNS_SERVICE);
        }

        brpc::ServerOptions options;
        if (_num_threads != -1) {
            options.num_threads = _num_threads;
        }
        options.idle_timeout_sec = config::brpc_idle_timeout_sec;
        if (config::enable_https) {
            auto* ssl_options = options.mutable_ssl_options();
            ssl_options->default_cert.certificate = config::ssl_certificate_path;
            ssl_options->default_cert.private_key = config::ssl_private_key_path;
        }
        options.has_builtin_services = config::enable_brpc_builtin_services;

        butil::EndPoint point;
        if (butil::str2endpoint(BackendOptions::get_service_bind_address(), _port, &point) < 0) {
            return Status::InternalError("convert address failed, host={}, port={}", "[::0]",
                                         _port);
        }
        LOG(INFO) << "BRPC server bind to host: " << BackendOptions::get_service_bind_address()
                  << ", port: " << _port;
        if (_server->Start(point, &options) != 0) {
            char buf[64];
            LOG(WARNING) << "start brpc failed, errno=" << errno
                         << ", errmsg=" << strerror_r(errno, buf, 64) << ", port=" << _port;
            return Status::InternalError("start brpc service failed");
        }
        return Status::OK();
    }

    void stop() override {
        if (_server != nullptr) {
            static_cast<void>(_server->Stop(1000));
            _stop_requested = true;
        }
    }

    void join() override {
        if (_server == nullptr) {
            return;
        }
        int stop_succeed = _stop_requested ? 0 : _server->Stop(1000);
        if (stop_succeed == 0) {
            _server->Join();
        } else {
            LOG(WARNING) << "Failed to stop brpc service, not calling brpc server join.";
        }
        _server->ClearServices();
        _server.reset();
    }

private:
    ExecEnv* _env;
    std::unique_ptr<brpc::Server> _server;
    int _port;
    int _num_threads;
    bool _stop_requested = false;
};

class OssHttpServiceStarter final : public IServerStarter {
public:
    OssHttpServiceStarter(ExecEnv* env, int port, int num_workers)
            : _service(std::make_unique<HttpService>(env, port, num_workers)) {}

    Status start() override {
        if (config::enable_tls) {
            return Status::NotSupported("BE HTTP TLS requires TLS module");
        }
        return _service->start();
    }

    void stop() override {
        if (_service != nullptr) {
            _service->stop();
        }
    }

    void join() override { _service.reset(); }

private:
    std::unique_ptr<HttpService> _service;
};

class OssFlightServiceStarter final : public IServerStarter {
public:
    explicit OssFlightServiceStarter(int port) : _port(port) {}

    Status start() override {
        if (config::enable_tls) {
            return Status::NotSupported("BE Arrow Flight TLS requires TLS module");
        }
        if (_service == nullptr) {
            auto result = flight::FlightSqlServer::create();
            if (!result.ok()) {
                return Status::InternalError(result.status().ToString());
            }
            _service = result.ValueOrDie();
        }
        return _service->init(_port);
    }

    void stop() override {
        if (_service != nullptr) {
            static_cast<void>(_service->join());
        }
    }

    void join() override { _service.reset(); }

private:
    std::shared_ptr<flight::FlightSqlServer> _service;
    int _port;
};

class OssBackendThriftStarter final : public IServerStarter {
public:
    OssBackendThriftStarter(int port, std::shared_ptr<BaseBackendService> service)
            : _port(port), _service(std::move(service)) {}

    Status start() override {
        if (config::enable_tls) {
            return Status::NotSupported("BE backend thrift TLS requires TLS module");
        }
        RETURN_IF_ERROR(_service->start_thrift_dependencies());
        auto processor = std::make_shared<BackendServiceProcessor>(_service);
        _server = std::make_unique<ThriftServer>("backend", processor, _port,
                                                 config::be_service_threads);
        return _server->start();
    }

    void stop() override {
        if (_server != nullptr) {
            _server->stop();
        }
    }

    void join() override { _server.reset(); }

private:
    int _port;
    std::shared_ptr<BaseBackendService> _service;
    std::unique_ptr<ThriftServer> _server;
};

class OssHeartbeatThriftStarter final : public IServerStarter {
public:
    OssHeartbeatThriftStarter(int port, uint32_t worker_thread_num, ClusterInfo* cluster_info)
            : _port(port), _worker_thread_num(worker_thread_num), _cluster_info(cluster_info) {}

    Status start() override {
        if (config::enable_tls) {
            return Status::NotSupported("BE heartbeat thrift TLS requires TLS module");
        }
        auto* heartbeat_server = new HeartbeatServer(_cluster_info);
        heartbeat_server->init_cluster_id();
        std::shared_ptr<HeartbeatServer> handler(heartbeat_server);
        auto processor = std::make_shared<HeartbeatServiceProcessor>(handler);
        _server = std::make_unique<ThriftServer>("heartbeat", processor, _port, _worker_thread_num);
        return _server->start();
    }

    void stop() override {
        if (_server != nullptr) {
            _server->stop();
        }
    }

    void join() override { _server.reset(); }

private:
    int _port;
    uint32_t _worker_thread_num;
    ClusterInfo* _cluster_info;
    std::unique_ptr<ThriftServer> _server;
};

} // namespace

Status create_brpc_starter(ExecEnv* env, int port, int num_threads,
                           std::unique_ptr<IServerStarter>* out) {
    if (out == nullptr) {
        return Status::InvalidArgument("BRPC starter output parameter is null");
    }
    *out = std::make_unique<OssBrpcServiceStarter>(env, port, num_threads);
    return Status::OK();
}

Status create_http_starter(ExecEnv* env, int port, int num_workers,
                           std::unique_ptr<IServerStarter>* out) {
    if (out == nullptr) {
        return Status::InvalidArgument("HTTP starter output parameter is null");
    }
    *out = std::make_unique<OssHttpServiceStarter>(env, port, num_workers);
    return Status::OK();
}

Status create_backend_thrift_starter(ExecEnv* env, int port,
                                     std::shared_ptr<BaseBackendService> service,
                                     std::unique_ptr<IServerStarter>* out) {
    if (out == nullptr) {
        return Status::InvalidArgument("Backend thrift starter output parameter is null");
    }
    (void)env;
    *out = std::make_unique<OssBackendThriftStarter>(port, std::move(service));
    return Status::OK();
}

Status create_heartbeat_thrift_starter(ExecEnv* env, int port, uint32_t worker_thread_num,
                                       ClusterInfo* cluster_info,
                                       std::unique_ptr<IServerStarter>* out) {
    (void)env;
    if (out == nullptr) {
        return Status::InvalidArgument("Heartbeat thrift starter output parameter is null");
    }
    *out = std::make_unique<OssHeartbeatThriftStarter>(port, worker_thread_num, cluster_info);
    return Status::OK();
}

Status create_flight_starter(int port, std::unique_ptr<IServerStarter>* out) {
    if (out == nullptr) {
        return Status::InvalidArgument("Flight starter output parameter is null");
    }
    *out = std::make_unique<OssFlightServiceStarter>(port);
    return Status::OK();
}

} // namespace doris::server
