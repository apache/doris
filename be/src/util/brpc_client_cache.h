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

#include <parallel_hashmap/phmap.h>

#include <memory>
#include <mutex>

#include "common/config.h"
#include "gen_cpp/Types_types.h" // TNetworkAddress
#include "gen_cpp/function_service.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "service/brpc.h"
#include "util/doris_metrics.h"

template <typename T>
using StubMap = phmap::parallel_flat_hash_map<
        std::string, std::shared_ptr<T>, std::hash<std::string>, std::equal_to<std::string>,
        std::allocator<std::pair<const std::string, std::shared_ptr<T>>>, 8, std::mutex>;

namespace doris {

template <class T>
class BrpcClientCache {
public:
    BrpcClientCache();
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
        std::string host_port = fmt::format("{}:{}", taddr.hostname, taddr.port);
        return get_client(host_port);
    }
#endif

    std::shared_ptr<T> get_client(const std::string& host, int port) {
        std::string host_port = fmt::format("{}:{}", host, port);
        return get_client(host_port);
    }

    std::shared_ptr<T> get_client(const std::string& host_port) {
        std::shared_ptr<T> stub_ptr;
        auto get_value = [&stub_ptr](typename StubMap<T>::mapped_type& v) { stub_ptr = v; };
        if (LIKELY(_stub_map.if_contains(host_port, get_value))) {
            return stub_ptr;
        }

        // new one stub and insert into map
        brpc::ChannelOptions options;
        if constexpr (std::is_same_v<T, PFunctionService_Stub>) {
            options.protocol = config::function_service_protocol;
        }
        std::unique_ptr<brpc::Channel> channel(new brpc::Channel());
        int ret_code = 0;
        if (host_port.find("://") == std::string::npos) {
            ret_code = channel->Init(host_port.c_str(), &options);
        } else {
            ret_code =
                    channel->Init(host_port.c_str(), config::rpc_load_balancer.c_str(), &options);
        }
        if (ret_code) {
            return nullptr;
        }
        auto stub = std::make_shared<T>(channel.release(),
                                        google::protobuf::Service::STUB_OWNS_CHANNEL);
        _stub_map.try_emplace_l(
                host_port, [&stub](typename StubMap<T>::mapped_type& v) { stub = v; }, stub);
        return stub;
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
};

using InternalServiceClientCache = BrpcClientCache<PBackendService_Stub>;
using FunctionServiceClientCache = BrpcClientCache<PFunctionService_Stub>;
} // namespace doris
