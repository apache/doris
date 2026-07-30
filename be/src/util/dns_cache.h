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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "common/status.h"

namespace doris {

// Same as
// fe/fe-core/src/main/java/org/apache/doris/common/DNSCache.java
class DNSCache {
public:
    using Resolver = std::function<Status(const std::string&, std::string&, bool)>;

    DNSCache();

    // Test-only constructor: uses a custom resolver and does NOT start the
    // background refresh thread.  Call refresh_for_test() to drive one cycle.
    explicit DNSCache(Resolver resolver);

    ~DNSCache();

    // get ip by hostname
    Status get(const std::string& hostname, std::string* ip);

private:
    // Resolve hostname to IP address.
    // If resolution fails, falls back to cached IP if available.
    // Returns the resolved IP, or cached IP on failure, or empty string if no cache available.
    // *is_fresh is set to true when DNS returned a live result, false when the
    // returned IP is the stale cached fallback from a failed lookup.
    std::string _resolve_hostname(const std::string& hostname, bool* is_fresh = nullptr);

    // update the ip of hostname in cache; out_failures (if non-null) is set to
    // the current consecutive failure count read under the same lock; out_ip (if
    // non-null) receives the resolved IP so callers can use it without a second
    // cache lookup (avoids operator[] mutation under shared_lock).
    Status _update(const std::string& hostname, uint32_t* out_failures = nullptr,
                   std::string* out_ip = nullptr);

    // erase a hostname from cache unconditionally (with unique_lock)
    void _erase(const std::string& hostname);

    // Erase a hostname from cache only if failure_count still meets threshold.
    // Re-reads the live counter under the same lock that performs the erase, so
    // a concurrent successful resolution that cleared the counter is not lost.
    // Returns true if the host was erased, false if the counter was already reset.
    bool _erase_if_still_failing(const std::string& hostname, uint32_t threshold);

    // one refresh cycle: update every cached hostname and evict if needed
    void _refresh_once();

    // a function for refresh daemon thread
    // update cache at fix internal
    void _refresh_cache();

    // ── test helpers (accessible via friend class DNSCacheTest) ──────────────
    size_t size_for_test() const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        return cache.size();
    }

    size_t negative_cache_size_for_test() const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        return _negative_cache.size();
    }

    uint32_t failure_count_for_test(const std::string& hostname) const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        auto it = failure_count.find(hostname);
        return it != failure_count.end() ? it->second : 0;
    }

    // Run one refresh cycle synchronously (no sleep).  Only meaningful when
    // the object was constructed with the test constructor (no background thread).
    void refresh_for_test() { _refresh_once(); }

    // Backdate all negative-cache entries far into the past so they appear
    // expired without removing them.  Use this to simulate TTL expiry in tests
    // that need the re-arm path in get() to trigger.
    // Backdating relative to now() (rather than to steady_clock's epoch, which is
    // typically boot time) keeps this correct on a freshly booted host.
    void _expire_negative_cache_for_test() {
        std::unique_lock<std::shared_mutex> lock(mutex);
        auto backdated = std::chrono::steady_clock::now() - std::chrono::hours(24 * 365);
        for (auto& [k, v] : _negative_cache) {
            v = backdated;
        }
    }

    friend class DNSCacheTest;

private:
    Resolver _resolver; // null → use global hostname_to_ip
    // hostname -> ip
    std::unordered_map<std::string, std::string> cache;
    // hostname -> consecutive resolution failure count
    std::unordered_map<std::string, uint32_t> failure_count;
    // hostname -> eviction timestamp; effective deadline is computed as
    // eviction_time + dns_cache_negative_ttl_seconds to honor live config changes.
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> _negative_cache;
    mutable std::shared_mutex mutex;
    std::thread refresh_thread;
    // Protects stop_refresh and signals _refresh_cache to wake early on destroy.
    std::mutex _cv_mutex;
    std::condition_variable _cv;
    std::atomic<bool> stop_refresh {false};
};

} // end of namespace doris
