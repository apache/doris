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
#include "util/cpu_info.h"
#include "util/time.h"

namespace doris::cloud {

// Display names for each RPC type (used in bvar metrics)
static constexpr std::string_view META_SERVICE_RPC_DISPLAY_NAMES[] = {
#define DEFINE_DISPLAY_NAME(enum_name, config_suffix, display_name) display_name,
        META_SERVICE_RPC_TYPES(DEFINE_DISPLAY_NAME)
#undef DEFINE_DISPLAY_NAME
};

std::string_view meta_service_rpc_display_name(MetaServiceRPC rpc) {
    size_t idx = static_cast<size_t>(rpc);
    if (idx < static_cast<size_t>(MetaServiceRPC::COUNT)) {
        return META_SERVICE_RPC_DISPLAY_NAMES[idx];
    }
    return "unknown";
}

// Get QPS config value for each RPC type
// Returns actual QPS (already multiplied by num_cores)
// Returns 0 if rate limiting should be disabled for this RPC
static int get_rpc_qps_from_config(MetaServiceRPC rpc) {
    int num_cores = CpuInfo::num_cores();
    int qps_per_core = 0;

    // Get the per-RPC config value, -1 means use default
#define GET_RPC_QPS_CONFIG(enum_name, config_suffix, display_name) \
    case MetaServiceRPC::enum_name:                                \
        qps_per_core = config::ms_rpc_qps_##config_suffix;         \
        break;

    switch (rpc) {
        META_SERVICE_RPC_TYPES(GET_RPC_QPS_CONFIG)
    default:
        return 0;
    }
#undef GET_RPC_QPS_CONFIG

    // -1 means use default config
    if (qps_per_core < 0) {
        qps_per_core = config::ms_rpc_qps_default;
    }

    // 0 means disabled
    if (qps_per_core <= 0) {
        return 0;
    }

    return std::max(1, qps_per_core * num_cores);
}

RpcRateLimiter::RpcRateLimiter(int qps, std::string_view op_name) {
    latency_recorder = std::make_unique<bvar::LatencyRecorder>("host_level_ms_rpc_rate_limit_sleep",
                                                               std::string(op_name));
    limiter = std::make_unique<TokenBucketRateLimiterHolder>(
            qps, qps, /*limit=*/0, [this](int64_t sleep_ns) {
                if (sleep_ns > 0) {
                    // Convert ns to us for LatencyRecorder
                    *latency_recorder << (sleep_ns / 1000);
                }
            });
}

bool RpcRateLimiter::should_log(int64_t now_us) {
    int64_t next_log_time_us = _next_log_time_us.load(std::memory_order_relaxed);
    return now_us >= next_log_time_us &&
           _next_log_time_us.compare_exchange_strong(next_log_time_us, now_us + MICROS_PER_SEC,
                                                     std::memory_order_relaxed);
}

void RpcRateLimiter::reset(int qps) {
    limiter->reset(qps, qps, 0);
}

HostLevelMSRpcRateLimiters::HostLevelMSRpcRateLimiters() {
    init_from_config();
}

HostLevelMSRpcRateLimiters::HostLevelMSRpcRateLimiters(int uniform_qps) {
    init_with_uniform_qps(uniform_qps);
}

void HostLevelMSRpcRateLimiters::init_from_config() {
    LOG(INFO) << "Initializing MS RPC rate limiters from config";

    // Initialize rate limiters for all RPC types
    for (size_t i = 0; i < static_cast<size_t>(MetaServiceRPC::COUNT); ++i) {
        MetaServiceRPC rpc = static_cast<MetaServiceRPC>(i);
        int qps = get_rpc_qps_from_config(rpc);
        if (qps > 0) {
            _limiters[i].store(
                    std::make_shared<RpcRateLimiter>(qps, meta_service_rpc_display_name(rpc)));
            LOG(INFO) << "  " << meta_service_rpc_display_name(rpc) << ": qps=" << qps;
        } else {
            _limiters[i].store(nullptr);
            LOG(INFO) << "  " << meta_service_rpc_display_name(rpc) << ": disabled";
        }
    }
}

void HostLevelMSRpcRateLimiters::init_with_uniform_qps(int qps) {
    qps = std::max(qps, 1);
    LOG(INFO) << "Initializing MS RPC rate limiters with uniform qps=" << qps;

    // Initialize rate limiters for all RPC types with the same QPS
    for (size_t i = 0; i < static_cast<size_t>(MetaServiceRPC::COUNT); ++i) {
        MetaServiceRPC rpc = static_cast<MetaServiceRPC>(i);
        _limiters[i].store(
                std::make_shared<RpcRateLimiter>(qps, meta_service_rpc_display_name(rpc)));
    }
}

int64_t HostLevelMSRpcRateLimiters::limit(MetaServiceRPC rpc) {
    const bool dry_run = config::enable_ms_rpc_host_level_rate_limit_dry_run;
    if (!config::enable_ms_rpc_host_level_rate_limit && !dry_run) {
        return 0;
    }

    size_t idx = static_cast<size_t>(rpc);
    if (idx >= static_cast<size_t>(MetaServiceRPC::COUNT)) {
        return 0;
    }

    auto limiter = _limiters[idx].load();
    if (!limiter) {
        return 0;
    }
    DCHECK(limiter->limiter);

    auto result = dry_run ? limiter->limiter->reserve_with_config(1)
                          : limiter->limiter->add_with_config(1);
    if (result.sleep_duration > 0 && limiter->should_log(MonotonicMicros())) {
        LOG(INFO) << "[ms-throttle] host-level rate limiter "
                  << (dry_run ? "dry run would throttle" : "throttled") << " MS RPC request"
                  << ", rpc=" << meta_service_rpc_display_name(rpc)
                  << (dry_run ? ", estimated_wait_ns=" : ", sleep_ns=") << result.sleep_duration
                  << ", qps_limit=" << result.max_speed;
    }
    return dry_run ? 0 : result.sleep_duration;
}

void HostLevelMSRpcRateLimiters::reset(MetaServiceRPC rpc, int qps) {
    size_t idx = static_cast<size_t>(rpc);
    if (idx >= static_cast<size_t>(MetaServiceRPC::COUNT)) {
        return;
    }

    qps = std::max(qps, 1);
    LOG(INFO) << "Resetting MS RPC rate limiter for " << meta_service_rpc_display_name(rpc)
              << " with qps=" << qps;

    auto limiter = _limiters[idx].load();
    if (limiter) {
        limiter->reset(qps);
    } else {
        _limiters[idx].store(
                std::make_shared<RpcRateLimiter>(qps, meta_service_rpc_display_name(rpc)));
    }
}

void HostLevelMSRpcRateLimiters::reset_all() {
    LOG(INFO) << "Resetting all MS RPC rate limiters from config";

    for (size_t i = 0; i < static_cast<size_t>(MetaServiceRPC::COUNT); ++i) {
        MetaServiceRPC rpc = static_cast<MetaServiceRPC>(i);
        int qps = get_rpc_qps_from_config(rpc);
        if (qps > 0) {
            auto limiter = _limiters[i].load();
            if (limiter) {
                limiter->reset(qps);
            } else {
                _limiters[i].store(
                        std::make_shared<RpcRateLimiter>(qps, meta_service_rpc_display_name(rpc)));
            }
            LOG(INFO) << "  " << meta_service_rpc_display_name(rpc) << ": qps=" << qps;
        } else {
            _limiters[i].store(nullptr);
            LOG(INFO) << "  " << meta_service_rpc_display_name(rpc) << ": disabled";
        }
    }
}

} // namespace doris::cloud
