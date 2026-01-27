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

#include <bvar/bvar.h>

#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>

#include "cpp/s3_rate_limiter.h"

namespace doris::cloud {

// Rate limiter with associated metrics for a single RPC method
struct RpcRateLimiter {
    std::unique_ptr<S3RateLimiterHolder> limiter;
    std::unique_ptr<bvar::LatencyRecorder> latency_recorder;

    RpcRateLimiter(int qps, const std::string& op_name);

    // Reset the rate limiter with new QPS
    void reset(int qps);
};

// Host-level rate limiters for MS RPCs to prevent burst traffic
// Each RPC method has its own rate limiter and bvar metrics
// Key is op_name string
class HostLevelMSRpcRateLimiters {
public:
    // Constructor with QPS parameter
    explicit HostLevelMSRpcRateLimiters(int qps);
    ~HostLevelMSRpcRateLimiters() = default;

    // Rate limit the specified RPC method by op_name, returns actual sleep time in nanoseconds
    // Thread-safe: uses shared lock, allowing concurrent reads
    int64_t limit(std::string_view op_name);

    // Reset all rate limiters with new QPS
    // Thread-safe: uses exclusive lock
    void reset(int qps);

private:
    void add_limiter(const std::string& op_name, int qps);

    mutable std::shared_mutex _mutex;
    std::map<std::string, std::unique_ptr<RpcRateLimiter>, std::less<>> _limiters;
};

} // namespace doris::cloud
