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
#include "runtime/exec_env.h" // IWYU pragma: keep
#include "util/client_cache.h"
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

template <typename T>
Status ThriftRpcHelper::rpc(std::function<TNetworkAddress()> address_provider,
                            std::function<void(ClientConnection<T>&)> callback, int timeout_ms) {
    TNetworkAddress address = address_provider();
    if (address.hostname.empty() || address.port == 0) {
        return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>("FE address is not available");
    }
    Status status;
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

Status ThriftRpcHelper::rpc_fe_with_master_refresh(
        std::function<TNetworkAddress()> address_provider,
        std::function<void(ClientConnection<FrontendServiceClient>&)> callback,
        std::function<TStatus()> status_extractor,
        std::function<const TNetworkAddress*()> master_addr_extractor, int timeout_ms) {
    // First attempt: drives the existing one-shot transport-level retry inside
    // rpc<FrontendServiceClient>.
    Status st = rpc<FrontendServiceClient>(address_provider, callback, timeout_ms);

    bool transport_failed = !st.ok();
    bool not_master = false;
    TNetworkAddress new_master;
    bool have_new_master = false;

    if (st.ok()) {
        TStatus resp_status = status_extractor();
        if (resp_status.status_code == TStatusCode::NOT_MASTER) {
            not_master = true;
            const TNetworkAddress* addr = master_addr_extractor();
            if (addr != nullptr && !addr->hostname.empty() && addr->port != 0) {
                new_master = *addr;
                have_new_master = true;
            }
        }
    }

    if (!transport_failed && !not_master) {
        return st;
    }

    TNetworkAddress old_master = address_provider();
    if (have_new_master) {
        // Refresh cached master immediately from the response. No extra RPC.
        // Note: unprotected write, same pattern as HeartbeatServer; the field
        // is also written by heartbeat thread with epoch checks, which will
        // self-correct any stale value we might race-write.
        _s_exec_env->cluster_info()->master_fe_addr = new_master;
        LOG(INFO) << "fe RPC got NOT_MASTER from " << old_master
                  << ", refreshed master_fe_addr to " << new_master;
    } else {
        // Scenario A: transport failure; or NOT_MASTER without master_address
        // (old FE without thrift change). Brief backoff so heartbeat thread
        // may have time to push a new master.
        std::this_thread::sleep_for(
                std::chrono::milliseconds(config::thrift_client_retry_interval_ms * 2));
        TNetworkAddress refreshed = address_provider();
        if (refreshed.hostname == old_master.hostname && refreshed.port == old_master.port) {
            // Heartbeat has not learned about a new master yet. Return the
            // original error and let upper layer (stream load executor) decide.
            return st.ok() ? Status::Error<ErrorCode::NOT_MASTER>(
                                     "FE returned NOT_MASTER but no new master address available")
                           : st;
        }
        LOG(INFO) << "fe RPC failed against " << old_master
                  << ", heartbeat now reports master=" << refreshed << ", retrying";
    }

    // Second (and only) attempt against the new master address.
    Status retry_st = rpc<FrontendServiceClient>(address_provider, callback, timeout_ms);
    if (!retry_st.ok()) {
        LOG(WARNING) << "fe RPC retry after master refresh still failed: " << retry_st;
    }
    return retry_st;
}

} // namespace doris
