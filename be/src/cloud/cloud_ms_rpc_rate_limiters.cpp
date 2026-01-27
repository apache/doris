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

#include "cloud/cloud_ms_rpc_rate_limiters.h"

#include <glog/logging.h>

#include <algorithm>

#include "cloud/config.h"

namespace doris::cloud {

RpcRateLimiter::RpcRateLimiter(int qps, const std::string& op_name) {
    latency_recorder = std::make_unique<bvar::LatencyRecorder>("host_level_ms_rpc_rate_limit_sleep", op_name);
    limiter = std::make_unique<S3RateLimiterHolder>(
            qps, qps, /*limit=*/0,
            [this](int64_t sleep_ns) {
                if (sleep_ns > 0) {
                    // Convert ns to us for LatencyRecorder
                    *latency_recorder << (sleep_ns / 1000);
                }
            });
}

void RpcRateLimiter::reset(int qps) {
    limiter->reset(qps, qps, 0);
}

HostLevelMSRpcRateLimiters::HostLevelMSRpcRateLimiters(int qps) {
    qps = std::max(qps, 1);

    LOG(INFO) << "Initializing MS RPC rate limiters with qps=" << qps;

    // Pre-create rate limiters for all known RPC methods
    add_limiter("get tablet meta", qps);
    add_limiter("get rowset", qps);
    add_limiter("prepare rowset", qps);
    add_limiter("commit rowset", qps);
    add_limiter("update tmp rowset", qps);
    add_limiter("commit txn", qps);
    add_limiter("abort txn", qps);
    add_limiter("precommit txn", qps);
    add_limiter("get obj store info", qps);
    add_limiter("start tablet job", qps);
    add_limiter("finish tablet job", qps);
    add_limiter("get delete bitmap", qps);
    add_limiter("update delete bitmap", qps);
    add_limiter("get delete bitmap update lock", qps);
    add_limiter("remove delete bitmap update lock", qps);
    add_limiter("get instance", qps);
    add_limiter("prepare restore job", qps);
    add_limiter("commit restore job", qps);
    add_limiter("finish restore job", qps);
    add_limiter("list snapshots", qps);
    add_limiter("update packed file info", qps);
}

void HostLevelMSRpcRateLimiters::add_limiter(const std::string& op_name, int qps) {
    _limiters[op_name] = std::make_unique<RpcRateLimiter>(qps, op_name);
}

int64_t HostLevelMSRpcRateLimiters::limit(std::string_view op_name) {
    if (!config::enable_ms_rpc_host_level_rate_limit) {
        return 0;
    }

    S3RateLimiterHolder* limiter = nullptr;
    {
        std::shared_lock<std::shared_mutex> lock(_mutex);
        auto it = _limiters.find(op_name);
        if (it != _limiters.end()) {
            limiter = it->second->limiter.get();
        }
    }

    if (limiter) {
        return limiter->add(1);
    }
    return 0;
}

void HostLevelMSRpcRateLimiters::reset(int qps) {
    qps = std::max(qps, 1);
    LOG(INFO) << "Resetting MS RPC rate limiters with qps=" << qps;
    std::unique_lock<std::shared_mutex> lock(_mutex);
    for (auto& [op_name, limiter] : _limiters) {
        limiter->reset(qps);
    }
}

} // namespace doris::cloud
