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
#include <google/protobuf/service.h>

#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <string_view>
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

    void reset_rate_limit(google::protobuf::Service* service, int64_t default_qps_limit,
                          const std::string& specific_max_qps_limit);

    void for_each_rpc_limiter(
            std::function<void(std::string_view, std::shared_ptr<RpcRateLimiter>)> cb);

private:
    // rpc_name -> RpcRateLimiter
    std::unordered_map<std::string, std::shared_ptr<RpcRateLimiter>> limiters_;
    std::unordered_set<std::string> rpc_with_specific_limit_;
    std::shared_mutex shared_mtx_;
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

    std::string_view rpc_name() const { return rpc_name_; }

    int64_t max_qps_limit() const { return max_qps_limit_; }

    void reset_max_qps_limit(int64_t max_qps_limit);

    // Todo: Recycle outdated instance_id

private:
    class QpsToken {
    public:
        QpsToken(const int64_t max_qps_limit) : max_qps_limit_(max_qps_limit) {}

        bool get_token(std::function<int()>& get_bvar_qps);

        void reset_max_qps_limit(int64_t max_qps_limit);

    private:
        bthread::Mutex mutex_;
        std::chrono::steady_clock::time_point last_update_time_;
        int64_t access_count_ {0};
        int64_t current_qps_ {0};
        int64_t max_qps_limit_;
    };

    bthread::Mutex mutex_;
    // instance_id -> QpsToken
    std::unordered_map<std::string, std::shared_ptr<QpsToken>> qps_limiter_;
    std::string rpc_name_;
    int64_t max_qps_limit_;
};

} // namespace doris::cloud
