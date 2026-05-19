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

#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
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

    // visible for testing
    size_t size_for_test() const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        return cache.size();
    }

    uint32_t failure_count_for_test(const std::string& hostname) const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        auto it = failure_count.find(hostname);
        return it != failure_count.end() ? it->second : 0;
    }

    // Run one refresh cycle synchronously (no sleep).  Only meaningful when
    // the object was constructed with the test constructor (no background thread).
    void refresh_for_test() { _refresh_once(); }

private:
    // Resolve hostname to IP address.
    // If resolution fails, falls back to cached IP if available.
    // Returns the resolved IP, or cached IP on failure, or empty string if no cache available.
    std::string _resolve_hostname(const std::string& hostname);

    // update the ip of hostname in cache
    Status _update(const std::string& hostname);

    // erase a hostname from cache (with unique_lock)
    void _erase(const std::string& hostname);

    // one refresh cycle: update every cached hostname and evict if needed
    void _refresh_once();

    // a function for refresh daemon thread
    // update cache at fix internal
    void _refresh_cache();

private:
    Resolver _resolver; // null → use global hostname_to_ip
    // hostname -> ip
    std::unordered_map<std::string, std::string> cache;
    // hostname -> consecutive resolution failure count
    std::unordered_map<std::string, uint32_t> failure_count;
    mutable std::shared_mutex mutex;
    std::thread refresh_thread;
    bool stop_refresh = false;
};

} // end of namespace doris

} // end of namespace doris
