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

#include "io/object_storage/obj_storage_rate_limiter_manager.h"

#include <algorithm>
#include <limits>
#include <thread>
#include <utility>

#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "cpp/obj-storage/rate_limited_obj_storage_client.h"
#include "util/cgroup_util.h"

namespace doris {

bvar::Adder<int64_t> s3_get_bytes_rate_limit_sleep_ns("s3_get_bytes_rate_limit_sleep_ns");
bvar::Adder<int64_t> s3_get_bytes_rate_limit_sleep_count("s3_get_bytes_rate_limit_sleep_count");
bvar::Adder<int64_t> s3_get_bytes_rate_limit_rejected_count(
        "s3_get_bytes_rate_limit_rejected_count");
bvar::Adder<int64_t> s3_put_bytes_rate_limit_sleep_ns("s3_put_bytes_rate_limit_sleep_ns");
bvar::Adder<int64_t> s3_put_bytes_rate_limit_sleep_count("s3_put_bytes_rate_limit_sleep_count");
bvar::Adder<int64_t> s3_put_bytes_rate_limit_rejected_count(
        "s3_put_bytes_rate_limit_rejected_count");

namespace {

constexpr int64_t kBytesRateLimitLongSleepNs = 500 * 1000 * 1000;
constexpr int kBytesRateLimitLongSleepLogInterval = 50;

std::function<void(int64_t)> bytes_rate_limiter_metric_func_with_log(
        ObjStorageRateLimitType type, bvar::Adder<int64_t>& sleep_ns_bvar,
        bvar::Adder<int64_t>& sleep_count_bvar, bvar::Adder<int64_t>& rejected_count_bvar) {
    auto metric_func = metric_func_factory(sleep_ns_bvar, sleep_count_bvar, &rejected_count_bvar);
    return [type, metric_func = std::move(metric_func)](int64_t sleep_ns) {
        metric_func(sleep_ns);
        LOG_IF_EVERY_N(WARNING, sleep_ns > kBytesRateLimitLongSleepNs,
                       kBytesRateLimitLongSleepLogInterval)
                << "S3 " << to_string(type) << " request is throttled by bytes rate limiter"
                << ", sleep_ms=" << sleep_ns / 1000000;
    };
}

std::function<void(int64_t)> bytes_rate_limiter_metric_func(ObjStorageRateLimitType type) {
    switch (type) {
    case ObjStorageRateLimitType::GET:
        return bytes_rate_limiter_metric_func_with_log(type, s3_get_bytes_rate_limit_sleep_ns,
                                                       s3_get_bytes_rate_limit_sleep_count,
                                                       s3_get_bytes_rate_limit_rejected_count);
    case ObjStorageRateLimitType::PUT:
        return bytes_rate_limiter_metric_func_with_log(type, s3_put_bytes_rate_limit_sleep_ns,
                                                       s3_put_bytes_rate_limit_sleep_count,
                                                       s3_put_bytes_rate_limit_rejected_count);
    default:
        return [](int64_t) {};
    }
}

// min(per_core * cores, cap) with overflow protection; cap <= 0 means no cap.
int64_t cap_multiply(int64_t per_core, int64_t cores, int64_t cap) {
    cap = cap > 0 ? cap : std::numeric_limits<int64_t>::max();
    if (per_core > cap / cores) {
        return cap;
    }
    return per_core * cores;
}

size_t index_of(ObjStorageRateLimitType type) {
    DCHECK(type == ObjStorageRateLimitType::GET || type == ObjStorageRateLimitType::PUT)
            << to_string(type);
    return static_cast<size_t>(type);
}

ObjStorageResponse make_be_rate_limit_response(ObjStorageRateLimitType type,
                                               ObjStorageRateLimitRejectReason reason) {
    const auto* limit_type = reason == ObjStorageRateLimitRejectReason::QPS ? "QPS" : "bytes";
    // A local admission rejection is not an S3 HTTP 429. Keep the merged #65420 behavior so S3
    // readers do not retry it as provider throttling.
    return ObjStorageResponse::rate_limit(
            ErrorCode::EXCEEDED_LIMIT, 0,
            fmt::format("s3 {} request exceeds {} limit, rejected by BE rate limiter",
                        to_string(type), limit_type));
}

class BeObjStorageRateLimitPolicy final : public ObjStorageRateLimitPolicy {
public:
    ObjStorageAdmission acquire(ObjStorageRateLimitType type,
                                size_t estimated_bytes) const override {
        auto guard = std::make_shared<ObjStorageRateLimitGuard>(type, estimated_bytes);
        if (!guard->ok()) {
            return ObjStorageAdmission {
                    .resp = make_be_rate_limit_response(type, guard->reject_reason()),
            };
        }
        return ObjStorageAdmission {
                .settle = [guard = std::move(guard)](
                                  size_t actual_bytes) { guard->settle(actual_bytes); },
        };
    }
};

} // namespace

ObjStorageRateLimitSettings resolve_obj_storage_rate_limit(ObjStorageRateLimitType type,
                                                           int64_t cores) {
    const bool is_get = type == ObjStorageRateLimitType::GET;
    const int64_t qps_per_core = is_get ? config::s3_get_requests_per_second_per_core
                                        : config::s3_put_requests_per_second_per_core;
    const int64_t qps_max = is_get ? config::s3_get_requests_per_second_max
                                   : config::s3_put_requests_per_second_max;
    const int64_t bytes_per_core = is_get ? config::s3_get_bytes_per_second_per_core
                                          : config::s3_put_bytes_per_second_per_core;
    const int64_t bytes_max =
            is_get ? config::s3_get_bytes_per_second_max : config::s3_put_bytes_per_second_max;
    cores = std::max<int64_t>(1, cores);

    ObjStorageRateLimitSettings limit;
    if (qps_per_core < 0) {
        // Unset: the legacy absolute configs stay in charge, bit-for-bit compatible.
        limit.qps = is_get ? config::s3_get_token_per_second : config::s3_put_token_per_second;
        limit.burst = is_get ? config::s3_get_bucket_tokens : config::s3_put_bucket_tokens;
        limit.count_limit = is_get ? config::s3_get_token_limit : config::s3_put_token_limit;
    } else if (qps_per_core > 0) {
        limit.qps = cap_multiply(qps_per_core, cores, qps_max);
        limit.burst = limit.qps; // burst = 1 second worth of quota
    }                            // qps_per_core == 0: QPS limiting disabled, all fields stay 0.

    if (bytes_per_core > 0) {
        limit.bytes_per_second = cap_multiply(bytes_per_core, cores, bytes_max);
    }
    return limit;
}

int reset_obj_storage_rate_limiter(ObjStorageRateLimitType type, size_t max_speed, size_t max_burst,
                                   size_t limit) {
    if (type == ObjStorageRateLimitType::UNKNOWN) {
        return -1;
    }
    return ObjStorageRateLimiterManager::instance().qps_limiter(type)->reset(max_speed, max_burst,
                                                                             limit);
}

int64_t obj_storage_rate_limiter_cpu_cores() {
    if (int32_t overridden = config::s3_rate_limiter_cpu_cores_override; overridden > 0) {
        return overridden;
    }
    int physical = static_cast<int>(std::thread::hardware_concurrency());
    // Re-read the cgroup quota on every call: serverless BEs can be resized in place,
    // and the daemon refresh thread picks the change up through here.
    int limited = CGroupUtil::get_cgroup_limited_cpu_number(physical);
    return std::max(1, limited);
}

ObjStorageRateLimiterManager::ObjStorageRateLimiterManager() {
    const int64_t cores = obj_storage_rate_limiter_cpu_cores();
    for (auto type : {ObjStorageRateLimitType::GET, ObjStorageRateLimitType::PUT}) {
        auto limit = resolve_obj_storage_rate_limit(type, cores);
        _qps_limiters[index_of(type)] = std::make_unique<S3RateLimiterHolder>(
                limit.qps, limit.burst, limit.count_limit, s3_rate_limiter_metric_func(type));
        _bytes_limiters[index_of(type)] = std::make_unique<S3RateLimiterHolder>(
                limit.bytes_per_second, limit.bytes_per_second, 0,
                bytes_rate_limiter_metric_func(type));
    }
}

std::shared_ptr<const ObjStorageRateLimitPolicy> ObjStorageRateLimiterManager::policy_for(
        bool is_internal_bucket) {
    if (config::is_cloud_mode() && !is_internal_bucket) {
        return nullptr;
    }
    static const auto policy = std::make_shared<BeObjStorageRateLimitPolicy>();
    return policy;
}

ObjStorageRateLimiterManager& ObjStorageRateLimiterManager::instance() {
    static ObjStorageRateLimiterManager ret;
    return ret;
}

S3RateLimiterHolder* ObjStorageRateLimiterManager::qps_limiter(ObjStorageRateLimitType type) {
    return _qps_limiters[index_of(type)].get();
}

S3RateLimiterHolder* ObjStorageRateLimiterManager::bytes_limiter(ObjStorageRateLimitType type) {
    return _bytes_limiters[index_of(type)].get();
}

void ObjStorageRateLimiterManager::refresh() {
    std::lock_guard guard(_refresh_lock);
    const int64_t cores = obj_storage_rate_limiter_cpu_cores();
    for (auto type : {ObjStorageRateLimitType::GET, ObjStorageRateLimitType::PUT}) {
        const auto limit = resolve_obj_storage_rate_limit(type, cores);

        auto* qps = qps_limiter(type);
        if (qps->get_max_speed() != static_cast<size_t>(limit.qps) ||
            qps->get_max_burst() != static_cast<size_t>(limit.burst) ||
            qps->get_limit() != static_cast<size_t>(limit.count_limit)) {
            qps->reset(limit.qps, limit.burst, limit.count_limit);
            LOG(INFO) << "reset S3 " << to_string(type) << " QPS rate limiter, qps=" << limit.qps
                      << ", burst=" << limit.burst << ", count_limit=" << limit.count_limit
                      << ", cores=" << cores;
        }

        auto* bytes = bytes_limiter(type);
        if (bytes->get_max_speed() != static_cast<size_t>(limit.bytes_per_second)) {
            bytes->reset(limit.bytes_per_second, limit.bytes_per_second, 0);
            LOG(INFO) << "reset S3 " << to_string(type)
                      << " bytes rate limiter, bytes_per_second=" << limit.bytes_per_second
                      << ", cores=" << cores;
        }
    }
}

ObjStorageRateLimitGuard::ObjStorageRateLimitGuard(ObjStorageRateLimitType type,
                                                   size_t estimated_bytes) {
    if (!config::enable_s3_rate_limiter) {
        return;
    }
    auto& mgr = ObjStorageRateLimiterManager::instance();

    auto* qps = mgr.qps_limiter(type);
    if (qps->is_enabled() &&
        apply_s3_rate_limit(type, qps, config::s3_rate_limiter_log_interval) < 0) {
        _ok = false;
        _reject_reason = ObjStorageRateLimitRejectReason::QPS;
        return;
    }

    if (estimated_bytes == 0) {
        return;
    }
    auto* bytes = mgr.bytes_limiter(type);
    if (!bytes->is_enabled()) {
        return;
    }
    // Clamp the reservation to 1 second worth of bandwidth so a single oversized IO
    // (e.g. a whole-file read_at) cannot create unbounded upfront debt. The clamped
    // remainder is intentionally not accounted; effective quotas below the single-IO
    // upper bound are excluded by the config contract (see config.cpp).
    _reserved = std::min(estimated_bytes, bytes->get_max_speed());
    if (_reserved > 0) {
        // Pin the admitted bucket generation for settle().
        _charged_bucket = bytes->charge(_reserved);
        if (_charged_bucket == nullptr) {
            _ok = false;
            _reject_reason = ObjStorageRateLimitRejectReason::BYTES;
        }
    }
}

void ObjStorageRateLimitGuard::settle(size_t actual_bytes) {
    if (_settled) {
        return;
    }
    _settled = true;
    if (_charged_bucket != nullptr && _reserved > actual_bytes) {
        _charged_bucket->refund(_reserved - actual_bytes);
    }
}

} // namespace doris
