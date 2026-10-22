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
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h" // IWYU pragma: keep
#include "util/client_cache.h"
#include "util/debug_points.h"
#include "util/dns_cache.h"
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
        LOG(WARNING) << "Connect frontend failed, address=" << address << ", status=" << status;
        return status;
    }
    try {
        try {
            callback(client);
        } catch (apache::thrift::transport::TTransportException& e) {
            LOG(WARNING) << "retrying call frontend service after "
                         << config::thrift_client_retry_interval_ms << " ms, address=" << address
                         << ", reason=" << e.what();
            std::this_thread::sleep_for(
                    std::chrono::milliseconds(config::thrift_client_retry_interval_ms));
            TNetworkAddress retry_address = address_provider();
            if (retry_address.hostname.empty() || retry_address.port == 0) {
                return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>("FE address is not available");
            }
            if (retry_address.hostname != address.hostname || retry_address.port != address.port) {
                LOG(INFO) << "retrying call frontend service with new address=" << retry_address;
                Status retry_status;
                ClientConnection<T> retry_client(_s_exec_env->get_client_cache<T>(), retry_address,
                                                 timeout_ms, &retry_status);
                if (!retry_status.ok()) {
                    LOG(WARNING) << "Connect frontend failed, address=" << retry_address
                                 << ", status=" << retry_status;
                    return retry_status;
                }
                callback(retry_client);
            } else {
                status = client.reopen(timeout_ms);
                if (!status.ok()) {
                    LOG(WARNING) << "client reopen failed. address=" << address
                                 << ", status=" << status;
                    return status;
                }
                callback(client);
            }
        }
    } catch (apache::thrift::TException& e) {
        LOG(WARNING) << "call frontend service failed, address=" << address
                     << ", reason=" << e.what();
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

Status ThriftRpcHelper::rpc_fe_with_master_refresh(
        std::function<TNetworkAddress()> address_provider,
        std::function<void(ClientConnection<FrontendServiceClient>&)> callback,
        std::function<TStatus()> response_status,
        std::function<const TNetworkAddress*()> master_hint, int timeout_ms) {
    const bool enabled = _s_exec_env->cluster_info()->in_graceful_shutdown();
    return _retry_fe_rpc(
            address_provider,
            [&](const TNetworkAddress& address) {
                auto status = rpc<FrontendServiceClient>(address.hostname, address.port, callback,
                                                         timeout_ms);
                // A restarted FE can keep its hostname but get a different pod IP.
                // Reopening a socket against the cached old IP cannot recover.
                if (enabled && !status.ok()) {
                    _s_exec_env->dns_cache()->invalidate(address.hostname);
                }
                return status;
            },
            response_status, master_hint, enabled, 1000);
}

Status ThriftRpcHelper::_retry_fe_rpc(const std::function<TNetworkAddress()>& address_provider,
                                      const std::function<Status(const TNetworkAddress&)>& attempt,
                                      const std::function<TStatus()>& response_status,
                                      const std::function<const TNetworkAddress*()>& master_hint,
                                      bool enabled, int retry_interval_ms) {
    constexpr int max_retries = 5;
    TNetworkAddress heartbeat_address = address_provider();
    TNetworkAddress target = heartbeat_address;
    for (int retry = 0;; ++retry) {
        Status st = attempt(target);
        if (!enabled || (st.ok() && response_status().status_code != TStatusCode::NOT_MASTER) ||
            retry == max_retries) {
            return st;
        }
        // Keep the redirect local to this request. Heartbeat alone owns the cached
        // master and its epoch; an old follower's hint must not overwrite that state.
        if (st.ok()) {
            const auto* hint = master_hint();
            // During election the old master may have no elected successor yet.
            if (hint != nullptr && !hint->hostname.empty() && hint->port > 0) {
                target = *hint;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(retry_interval_ms * (2 * retry + 1)));
        auto current_address = address_provider();
        // A newer heartbeat supersedes any redirect retained from a failed attempt.
        if (current_address != heartbeat_address) {
            heartbeat_address = current_address;
            target = current_address;
        }
        LOG(INFO) << "retry stream-load FE RPC " << retry + 1 << "/" << max_retries
                  << ", target=" << target;
    }
}

} // namespace doris
