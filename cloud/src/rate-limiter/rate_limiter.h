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

#include <brpc/server.h>
#include <bthread/mutex.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "common/config.h"

namespace doris::cloud {

class RpcRateLimiter;

class RateLimiter {
public:
    RateLimiter() = default;
    ~RateLimiter() = default;
    void init(google::protobuf::Service* service);
    std::shared_ptr<RpcRateLimiter> get_rpc_rate_limiter(const std::string& rpc_name);

private:
    // rpc_name -> RpcRateLimiter
    std::unordered_map<std::string, std::shared_ptr<RpcRateLimiter>> limiters_;
};

class RpcRateLimiter {
public:
    RpcRateLimiter(const std::string rpc_name, const int64_t max_qps_limit)
            : rpc_name_(rpc_name), max_qps_limit_(max_qps_limit) {}

    ~RpcRateLimiter() = default;

    /**
     * @brief Get the qps token by instance_id
     *
     * @param instance_id
     * @param get_bvar_qps a function that cat get the qps
     */
    bool get_qps_token(const std::string& instance_id, std::function<int()>& get_bvar_qps);

    // Todo: Recycle outdated instance_id

private:
    class QpsToken {
    public:
        QpsToken(const int64_t max_qps_limit) : max_qps_limit_(max_qps_limit) {}

        bool get_token(std::function<int()>& get_bvar_qps);

    private:
        bthread::Mutex mutex_;
        std::chrono::steady_clock::time_point last_update_time_;
        int64_t access_count_ {0};
        int64_t current_qps_ {0};
        int64_t max_qps_limit_;
    };

private:
    bthread::Mutex mutex_;
    // instance_id -> QpsToken
    std::unordered_map<std::string, std::shared_ptr<QpsToken>> qps_limiter_;
    std::string rpc_name_;
    int64_t max_qps_limit_;
};

} // namespace doris::cloud