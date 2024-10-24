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
#include <butil/endpoint.h>
#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <google/protobuf/service.h>
#include <parallel_hashmap/phmap.h>
#include <stddef.h>

#include <functional>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "runtime/exec_env.h"
#include "util/dns_cache.h"
#include "util/network_util.h"

namespace doris {
class PBackendService_Stub;
class PFunctionService_Stub;
} // namespace doris

template <typename T>
using StubMap = phmap::parallel_flat_hash_map<
        std::string, std::shared_ptr<T>, std::hash<std::string>, std::equal_to<std::string>,
        std::allocator<std::pair<const std::string, std::shared_ptr<T>>>, 8, std::mutex>;

namespace doris {

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
        std::string host_port = get_host_port(realhost, port);
        std::shared_ptr<T> stub_ptr;
        auto get_value = [&stub_ptr](const auto& v) { stub_ptr = v.second; };
        if (LIKELY(_stub_map.if_contains(host_port, get_value))) {
            DCHECK(stub_ptr != nullptr);
            return stub_ptr;
        }

        // new one stub and insert into map
        auto stub = get_new_client_no_cache(host_port);
        if (stub != nullptr) {
            _stub_map.try_emplace_l(
                    host_port, [&stub](const auto& v) { stub = v.second; }, stub);
        }
        return stub;
    }

    std::shared_ptr<T> get_client(const std::string& host_port) {
        int pos = host_port.rfind(':');
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
                                               const std::string& connection_group = "") {
        brpc::ChannelOptions options;
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
        options.connect_timeout_ms = 2000;
        options.timeout_ms = 2000;
        options.max_retry = 10;

        std::unique_ptr<brpc::Channel> channel(new brpc::Channel());
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
};

using InternalServiceClientCache = BrpcClientCache<PBackendService_Stub>;
using FunctionServiceClientCache = BrpcClientCache<PFunctionService_Stub>;
} // namespace doris
