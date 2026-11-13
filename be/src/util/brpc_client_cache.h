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

#pragma once

#include <brpc/adaptive_connection_type.h>
#include <brpc/adaptive_protocol_type.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <butil/endpoint.h>
#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <google/protobuf/service.h>
#include <parallel_hashmap/phmap.h>
#include <stddef.h>

#include <cerrno>
#include <functional>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/status.h"
#include "runtime/cluster_info.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "util/client_connection_provider.h"
#include "util/dns_cache.h"
#include "util/network_util.h"

namespace doris {
class PBackendService_Stub;
class PFunctionService_Stub;
} // namespace doris

// Entry that holds both resolved IP and stub, similar to Java's BackendServiceClientExtIp
template <typename T>
struct StubEntry {
    std::string real_ip;
    std::shared_ptr<T> stub;
};

template <typename T>
using StubMap = phmap::parallel_flat_hash_map<
        std::string, StubEntry<T>, std::hash<std::string>, std::equal_to<std::string>,
        std::allocator<std::pair<const std::string, StubEntry<T>>>, 8, std::mutex>;

namespace doris {
#include "common/compile_check_begin.h"
inline bool is_brpc_network_fault(int error) {
    return error == EHOSTDOWN || error == EHOSTUNREACH || error == ENETUNREACH ||
           error == ECONNREFUSED || error == ECONNRESET || error == ENOTCONN || error == EPIPE;
}

inline void on_brpc_network_fault(const std::shared_ptr<AtomicStatus>& channel_status,
                                  const std::string& hostname, brpc::Controller* controller) {
    auto status = Status::NetworkError("brpc network failure: {}", controller->ErrorText());
    channel_status->update(status);
    // Refresh DNS before the next connection after a possible pod replacement.
    // Socket errors alone must not discard the fallback IP during a resolver outage.
    if (!hostname.empty() && !is_valid_ip(hostname)) {
        auto* dns_cache = ExecEnv::GetInstance()->dns_cache();
        // get_client also supports startup / test environments without a DNS cache.
        if (dns_cache != nullptr) {
            dns_cache->invalidate(hostname);
        }
    }
}

class FailureDetectClosure : public ::google::protobuf::Closure {
public:
    FailureDetectClosure(std::shared_ptr<AtomicStatus>& channel_st,
                         ::google::protobuf::RpcController* controller,
                         ::google::protobuf::Closure* done, std::string hostname = "")
            : _channel_st(channel_st),
              _controller(controller),
              _done(done),
              _hostname(std::move(hostname)) {}

    void Run() override {
        Defer defer {[&]() { delete this; }};
        // All brpc related API will use brpc::Controller, so that it is safe
        // to do static cast here.
        auto* cntl = static_cast<brpc::Controller*>(_controller);
        if (cntl->Failed() && is_brpc_network_fault(cntl->ErrorCode())) {
            on_brpc_network_fault(_channel_st, _hostname, cntl);
        }
        // Sometimes done == nullptr, for example hand_shake API.
        if (_done != nullptr) {
            _done->Run();
        }
        // _done->Run may throw exception, so that move delete this to Defer.
        // delete this;
    }

private:
    std::shared_ptr<AtomicStatus> _channel_st;
    ::google::protobuf::RpcController* _controller;
    ::google::protobuf::Closure* _done;
    std::string _hostname;
};

// This channel will use FailureDetectClosure to wrap the original closure
// If some non-recoverable rpc failure happens, it will save the error status in
// _channel_st.
// And brpc client cache will depend on it to detect if the client is health.
class FailureDetectChannel : public ::brpc::Channel {
public:
    explicit FailureDetectChannel(std::string hostname = "")
            : ::brpc::Channel(), _hostname(std::move(hostname)) {
        _channel_st = std::make_shared<AtomicStatus>(); // default OK
    }
    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request, google::protobuf::Message* response,
                    google::protobuf::Closure* done) override {
        FailureDetectClosure* failure_detect_closure = nullptr;
        if (done != nullptr) {
            // If done == nullptr, then it means the call is sync call, so that should not
            // gen a failure detect closure for it. Or it will core.
            failure_detect_closure =
                    new FailureDetectClosure(_channel_st, controller, done, _hostname);
        }
        ::brpc::Channel::CallMethod(method, controller, request, response, failure_detect_closure);
        // Done == nullptr, it is a sync call, should also deal with the bad channel.
        if (done == nullptr) {
            auto* cntl = static_cast<brpc::Controller*>(controller);
            if (cntl->Failed() && is_brpc_network_fault(cntl->ErrorCode())) {
                on_brpc_network_fault(_channel_st, _hostname, cntl);
            }
        }
    }

    std::shared_ptr<AtomicStatus> channel_status() { return _channel_st; }

private:
    std::shared_ptr<AtomicStatus> _channel_st;
    std::string _hostname;
};

template <class T>
class BrpcClientCache {
public:
    BrpcClientCache(std::string protocol = "baidu_std", std::string connection_type = "",
                    std::string connection_group = "");
    virtual ~BrpcClientCache();

    std::shared_ptr<T> get_client(const butil::EndPoint& endpoint) {
        return get_client(butil::endpoint2str(endpoint).c_str());
    }

#ifdef BE_TEST
    virtual std::shared_ptr<T> get_client(const TNetworkAddress& taddr) {
        std::string host_port = fmt::format("{}:{}", taddr.hostname, taddr.port);
        return get_client(host_port);
    }
#else
    std::shared_ptr<T> get_client(const TNetworkAddress& taddr) {
        return get_client(taddr.hostname, taddr.port);
    }
#endif

    std::shared_ptr<T> get_client(const PNetworkAddress& paddr) {
        return get_client(paddr.hostname(), paddr.port());
    }

    std::shared_ptr<T> get_client(const std::string& host, int port) {
        auto* cluster_info = ExecEnv::GetInstance()->cluster_info();
        // Startup and cache unit tests can run before ExecEnv initializes ClusterInfo.
        return get_client(host, port,
                          std::is_same_v<T, PBackendService_Stub> && cluster_info != nullptr &&
                                  cluster_info->in_graceful_shutdown());
    }

    std::shared_ptr<T> get_client(const std::string& host, int port, bool enable_handshake) {
        const int max_attempts = enable_handshake ? 5 : 1;
        for (int attempt = 0; attempt < max_attempts; ++attempt) {
            auto stub = _get_client(host, port, enable_handshake);
            if (stub != nullptr) {
                return stub;
            }
            // A pod may answer heartbeats before its brpc port or DNS is ready.
            // Retry a fresh resolution and channel, leaving healthy cache hits unchanged.
            if (attempt + 1 < max_attempts) {
                auto* dns_cache = ExecEnv::GetInstance()->dns_cache();
                if (dns_cache != nullptr && !is_valid_ip(host)) {
                    dns_cache->invalidate(host);
                }
                bthread_usleep(1000000L * (attempt + 1));
            }
        }
        return nullptr;
    }

private:
    std::shared_ptr<T> _get_client(const std::string& host, int port, bool enable_handshake) {
        std::string realhost = host;
        auto dns_cache = ExecEnv::GetInstance()->dns_cache();
        if (dns_cache == nullptr) {
            LOG(WARNING) << "DNS cache is not initialized, skipping hostname resolve";
        } else if (!is_valid_ip(host)) {
            Status status = dns_cache->get(host, &realhost);
            if (!status.ok()) {
                LOG(WARNING) << "failed to get ip from host:" << status.to_string();
                return nullptr;
            }
        }

        // Use original host:port as key (like Java's TNetworkAddress address)
        // This allows us to detect IP changes when DNS resolution changes
        std::string host_port = fmt::format("{}:{}", host, port);

        std::shared_ptr<T> stub_ptr;
        bool need_remove = false;

        auto check_entry = [&](const auto& v) {
            const StubEntry<T>& entry = v.second;
            // Check if cached IP matches current resolved IP
            if (entry.real_ip != realhost) {
                // IP changed (DNS resolution changed)
                LOG(WARNING) << "Cached ip changed for " << host << ", before ip: " << entry.real_ip
                             << ", current ip: " << realhost;
                need_remove = true;
            } else if (!static_cast<FailureDetectChannel*>(entry.stub->channel())
                                ->channel_status()
                                ->ok()) {
                // Client is not in normal state, need to recreate
                // At this point we cannot judge the progress of reconnecting the underlying channel.
                // In the worst case, it may take two minutes. But we can't stand the connection refused
                // for two minutes, so rebuild the channel directly.
                need_remove = true;
            } else {
                // Cache hit: IP matches and client is healthy
                stub_ptr = entry.stub;
            }
        };

        if (LIKELY(_stub_map.if_contains(host_port, check_entry))) {
            if (stub_ptr != nullptr) {
                return stub_ptr;
            }
            // IP changed or client unhealthy, need to remove old entry
            if (need_remove) {
                _stub_map.erase(host_port);
            }
        }

        // Create new stub using resolved IP for actual connection
        std::string real_host_port = get_host_port(realhost, port);
        auto stub = get_new_client_no_cache(real_host_port, "", "", "", 2000, 10, host);
        // In rolling mode, do not publish a new channel until the peer answers the
        // probe. Healthy cache hits, including channels opened before the mode was
        // enabled, must not pay an extra RPC per lookup.
        if (stub == nullptr || (enable_handshake && !available(stub, real_host_port))) {
            return nullptr;
        }
        StubEntry<T> entry {realhost, stub};
        _stub_map.try_emplace_l(
                host_port, [&stub](const auto& v) { stub = v.second.stub; }, entry);
        return stub;
    }

public:
    std::shared_ptr<T> get_client(const std::string& host_port) {
        const auto pos = host_port.rfind(':');
        std::string host = host_port.substr(0, pos);
        int port = 0;
        try {
            port = stoi(host_port.substr(pos + 1));
        } catch (const std::exception& err) {
            LOG(WARNING) << "failed to parse port from " << host_port << ": " << err.what();
            return nullptr;
        }
        return get_client(host, port);
    }

    std::shared_ptr<T> get_new_client_no_cache(const std::string& host_port,
                                               const std::string& protocol = "",
                                               const std::string& connection_type = "",
                                               const std::string& connection_group = "",
                                               int connect_timeout_ms = 2000, int max_retry = 10,
                                               const std::string& original_hostname = "") {
        brpc::ChannelOptions options;
        Status status = doris::client::configure_brpc_channel_options(&options);
        if (!status.ok()) {
            throw status;
        }
        if (protocol != "") {
            options.protocol = protocol;
        } else if (_protocol != "") {
            options.protocol = _protocol;
        }
        if (connection_type != "") {
            options.connection_type = connection_type;
        } else if (_connection_type != "") {
            options.connection_type = _connection_type;
        }
        if (connection_group != "") {
            options.connection_group = connection_group;
        } else if (_connection_group != "") {
            options.connection_group = _connection_group;
        }
        // Add random connection id to connection_group to make sure use new socket
        options.connection_group += std::to_string(_connection_id.fetch_add(1));
        options.connect_timeout_ms = connect_timeout_ms;
        options.timeout_ms = 2000;
        options.max_retry = max_retry;

        std::unique_ptr<FailureDetectChannel> channel(new FailureDetectChannel(original_hostname));
        int ret_code = 0;
        if (host_port.find("://") == std::string::npos) {
            ret_code = channel->Init(host_port.c_str(), &options);
        } else {
            ret_code =
                    channel->Init(host_port.c_str(), config::rpc_load_balancer.c_str(), &options);
        }
        if (ret_code) {
            LOG(WARNING) << "Failed to initialize brpc Channel to " << host_port;
            return nullptr;
        }
        return std::make_shared<T>(channel.release(), google::protobuf::Service::STUB_OWNS_CHANNEL);
    }

    size_t size() { return _stub_map.size(); }

    void clear() { _stub_map.clear(); }

    size_t erase(const std::string& host_port) { return _stub_map.erase(host_port); }

    size_t erase(const std::string& host, int port) {
        std::string host_port = fmt::format("{}:{}", host, port);
        return erase(host_port);
    }

    size_t erase(const butil::EndPoint& endpoint) {
        return _stub_map.erase(butil::endpoint2str(endpoint).c_str());
    }

    bool exist(const std::string& host_port) {
        return _stub_map.find(host_port) != _stub_map.end();
    }

    void get_all(std::vector<std::string>* endpoints) {
        for (auto it = _stub_map.begin(); it != _stub_map.end(); ++it) {
            endpoints->emplace_back(it->first.c_str());
        }
    }

    bool available(std::shared_ptr<T> stub, const butil::EndPoint& endpoint) {
        return available(stub, butil::endpoint2str(endpoint).c_str());
    }

    bool available(std::shared_ptr<T> stub, const std::string& host_port) {
        if (!stub) {
            LOG(WARNING) << "stub is null to: " << host_port;
            return false;
        }
        std::string message = "hello doris!";
        PHandShakeRequest request;
        request.set_hello(message);
        PHandShakeResponse response;
        brpc::Controller cntl;
        // Bound a recovery probe independently of the channel's transport retries.
        cntl.set_timeout_ms(2000);
        cntl.set_max_retry(0);
        stub->hand_shake(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            LOG(WARNING) << "open brpc connection to " << host_port
                         << " failed: " << cntl.ErrorText();
            return false;
        } else if (response.has_status() && response.has_hello() && response.hello() == message &&
                   response.status().status_code() == 0) {
            return true;
        } else {
            LOG(WARNING) << "open brpc connection to " << host_port
                         << " failed: " << response.DebugString();
            return false;
        }
    }

    bool available(std::shared_ptr<T> stub, const std::string& host, int port) {
        std::string host_port = fmt::format("{}:{}", host, port);
        return available(stub, host_port);
    }

private:
    StubMap<T> _stub_map;
    const std::string _protocol;
    const std::string _connection_type;
    const std::string _connection_group;
    // use to generate unique connection id for each connection
    // to prevent the connection problem of brpc: https://github.com/apache/brpc/issues/2146
    std::atomic<int64_t> _connection_id {0};
};

using InternalServiceClientCache = BrpcClientCache<PBackendService_Stub>;
using FunctionServiceClientCache = BrpcClientCache<PFunctionService_Stub>;
#include "common/compile_check_end.h"
} // namespace doris
