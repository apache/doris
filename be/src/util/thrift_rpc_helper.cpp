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

#include "util/thrift_rpc_helper.h"

#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <thrift/Thrift.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TTransportException.h>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <sstream>
#include <thread>

#include "common/status.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h" // IWYU pragma: keep
#include "util/debug_points.h"
#include "util/network_util.h"

namespace apache {
namespace thrift {
namespace protocol {
class TProtocol;
} // namespace protocol
namespace transport {
class TBufferedTransport;
class TSocket;
class TTransport;
} // namespace transport
} // namespace thrift
} // namespace apache

namespace doris {

using apache::thrift::protocol::TProtocol;
using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::transport::TSocket;
using apache::thrift::transport::TTransport;
using apache::thrift::transport::TBufferedTransport;

ExecEnv* ThriftRpcHelper::_s_exec_env = nullptr;

void ThriftRpcHelper::setup(ExecEnv* exec_env) {
    _s_exec_env = exec_env;
}

template <typename T>
Status ThriftRpcHelper::rpc(const std::string& ip, const int32_t port,
                            std::function<void(ClientConnection<T>&)> callback, int timeout_ms) {
    return rpc<T>([ip, port]() { return make_network_address(ip, port); }, callback, timeout_ms);
}

template <typename T>
Status ThriftRpcHelper::rpc(std::function<TNetworkAddress()> address_provider,
                            std::function<void(ClientConnection<T>&)> callback, int timeout_ms) {
    TNetworkAddress address = address_provider();
    if (address.hostname.empty() || address.port == 0) {
        return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>("FE address is not available");
    }
    Status status;
    DBUG_EXECUTE_IF("thriftRpcHelper.rpc.error", { timeout_ms = 30000; });
    ClientConnection<T> client(_s_exec_env->get_client_cache<T>(), address, timeout_ms, &status);
    if (!status.ok()) {
#ifndef ADDRESS_SANITIZER
        LOG(WARNING) << "Connect frontend failed, address=" << address << ", status=" << status;
#endif
        return status;
    }
    try {
        try {
            callback(client);
        } catch (apache::thrift::transport::TTransportException& e) {
            std::cerr << "thrift error, reason=" << e.what();
#ifndef ADDRESS_SANITIZER
            LOG(WARNING) << "retrying call frontend service after "
                         << config::thrift_client_retry_interval_ms << " ms, address=" << address
                         << ", reason=" << e.what();
#endif
            std::this_thread::sleep_for(
                    std::chrono::milliseconds(config::thrift_client_retry_interval_ms));
            TNetworkAddress retry_address = address_provider();
            if (retry_address.hostname.empty() || retry_address.port == 0) {
                return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>("FE address is not available");
            }
            if (retry_address.hostname != address.hostname || retry_address.port != address.port) {
#ifndef ADDRESS_SANITIZER
                LOG(INFO) << "retrying call frontend service with new address=" << retry_address;
#endif
                Status retry_status;
                ClientConnection<T> retry_client(_s_exec_env->get_client_cache<T>(), retry_address,
                                                 timeout_ms, &retry_status);
                if (!retry_status.ok()) {
#ifndef ADDRESS_SANITIZER
                    LOG(WARNING) << "Connect frontend failed, address=" << retry_address
                                 << ", status=" << retry_status;
#endif
                    return retry_status;
                }
                callback(retry_client);
            } else {
                status = client.reopen(timeout_ms);
                if (!status.ok()) {
#ifndef ADDRESS_SANITIZER
                    LOG(WARNING) << "client reopen failed. address=" << address
                                 << ", status=" << status;
#endif
                    return status;
                }
                callback(client);
            }
        }
    } catch (apache::thrift::TException& e) {
#ifndef ADDRESS_SANITIZER
        LOG(WARNING) << "call frontend service failed, address=" << address
                     << ", reason=" << e.what();
#endif
        std::this_thread::sleep_for(
                std::chrono::milliseconds(config::thrift_client_retry_interval_ms * 2));
        // just reopen to disable this connection
        static_cast<void>(client.reopen(timeout_ms));
        return Status::RpcError("failed to call frontend service, FE address={}:{}, reason: {}",
                                address.hostname, address.port, e.what());
    }
    return Status::OK();
}

template Status ThriftRpcHelper::rpc<FrontendServiceClient>(
        const std::string& ip, const int32_t port,
        std::function<void(ClientConnection<FrontendServiceClient>&)> callback, int timeout_ms);

template Status ThriftRpcHelper::rpc<FrontendServiceClient>(
        std::function<TNetworkAddress()> address_provider,
        std::function<void(ClientConnection<FrontendServiceClient>&)> callback, int timeout_ms);

template Status ThriftRpcHelper::rpc<BackendServiceClient>(
        const std::string& ip, const int32_t port,
        std::function<void(ClientConnection<BackendServiceClient>&)> callback, int timeout_ms);

template Status ThriftRpcHelper::rpc<TPaloBrokerServiceClient>(
        const std::string& ip, const int32_t port,
        std::function<void(ClientConnection<TPaloBrokerServiceClient>&)> callback, int timeout_ms);

} // namespace doris
