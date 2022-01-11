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
#include "gen_cpp/internal_service.pb.h"
#include "service/brpc.h"
#include "util/doris_metrics.h"

namespace std {
template <>
struct hash<butil::EndPoint> {
    std::size_t operator()(butil::EndPoint const& p) const {
        return phmap::HashState().combine(0, butil::ip2int(p.ip), p.port);
    }
};
} // namespace std
using SubMap = phmap::parallel_flat_hash_map<
        butil::EndPoint, std::shared_ptr<doris::PBackendService_Stub>, std::hash<butil::EndPoint>,
        std::equal_to<butil::EndPoint>,
        std::allocator<
                std::pair<const butil::EndPoint, std::shared_ptr<doris::PBackendService_Stub>>>,
        8, std::mutex>;
namespace doris {

class BrpcStubCache {
public:
    BrpcStubCache();
    virtual ~BrpcStubCache();

    inline std::shared_ptr<PBackendService_Stub> get_stub(const butil::EndPoint& endpoint) {
        auto stub_ptr = _stub_map.find(endpoint);
        if (LIKELY(stub_ptr != _stub_map.end())) {
            return stub_ptr->second;
        }
        // new one stub and insert into map
        brpc::ChannelOptions options;
        std::unique_ptr<brpc::Channel> channel(new brpc::Channel());
        if (channel->Init(endpoint, &options)) {
            return nullptr;
        }
        auto stub = std::make_shared<PBackendService_Stub>(
                channel.release(), google::protobuf::Service::STUB_OWNS_CHANNEL);
        _stub_map[endpoint] = stub;
        return stub;
    }

    virtual std::shared_ptr<PBackendService_Stub> get_stub(const TNetworkAddress& taddr) {
        butil::EndPoint endpoint;
        if (str2endpoint(taddr.hostname.c_str(), taddr.port, &endpoint)) {
            LOG(WARNING) << "unknown endpoint, hostname=" << taddr.hostname
                         << ", port=" << taddr.port;
            return nullptr;
        }
        return get_stub(endpoint);
    }

    inline std::shared_ptr<PBackendService_Stub> get_stub(const std::string& host, int port) {
        butil::EndPoint endpoint;
        if (str2endpoint(host.c_str(), port, &endpoint)) {
            LOG(WARNING) << "unknown endpoint, hostname=" << host << ", port=" << port;
            return nullptr;
        }
        return get_stub(endpoint);
    }

    inline size_t size() { return _stub_map.size(); }

    inline void clear() { _stub_map.clear(); }

    inline size_t erase(const std::string& host_port) {
        butil::EndPoint endpoint;
        if (str2endpoint(host_port.c_str(), &endpoint)) {
            LOG(WARNING) << "unknown endpoint: " << host_port;
            return 0;
        }
        return erase(endpoint);
    }

    size_t erase(const std::string& host, int port) {
        butil::EndPoint endpoint;
        if (str2endpoint(host.c_str(), port, &endpoint)) {
            LOG(WARNING) << "unknown endpoint, hostname=" << host << ", port=" << port;
            return 0;
        }
        return erase(endpoint);
    }

    inline size_t erase(const butil::EndPoint& endpoint) { return _stub_map.erase(endpoint); }

    inline bool exist(const std::string& host_port) {
        butil::EndPoint endpoint;
        if (str2endpoint(host_port.c_str(), &endpoint)) {
            LOG(WARNING) << "unknown endpoint: " << host_port;
            return false;
        }
        return _stub_map.find(endpoint) != _stub_map.end();
    }

    inline void get_all(std::vector<std::string>* endpoints) {
        for (SubMap::const_iterator it = _stub_map.begin(); it != _stub_map.end(); ++it) {
            endpoints->emplace_back(endpoint2str(it->first).c_str());
        }
    }

    inline bool available(std::shared_ptr<PBackendService_Stub> stub,
                          const butil::EndPoint& endpoint) {
        if (!stub) {
            return false;
        }
        PHandShakeRequest request;
        PHandShakeResponse response;
        brpc::Controller cntl;
        stub->hand_shake(&cntl, &request, &response, nullptr);
        if (!cntl.Failed()) {
            return true;
        } else {
            LOG(WARNING) << "open brpc connection to  " << endpoint2str(endpoint).c_str()
                         << " failed: " << cntl.ErrorText();
            return false;
        }
    }

    inline bool available(std::shared_ptr<PBackendService_Stub> stub, const std::string& host,
                          int port) {
        butil::EndPoint endpoint;
        if (str2endpoint(host.c_str(), port, &endpoint)) {
            LOG(WARNING) << "unknown endpoint, hostname=" << host;
            return false;
        }
        return available(stub, endpoint);
    }

private:
    SubMap _stub_map;
};

} // namespace doris
