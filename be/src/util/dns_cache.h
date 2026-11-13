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
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "common/status.h"

namespace doris {

// Same as
// fe/fe-core/src/main/java/org/apache/doris/common/DNSCache.java
class DNSCache {
public:
    using Resolver = std::function<Status(const std::string&, std::string&, bool, int*)>;
    DNSCache();
    // Deterministic tests supply a resolver and drive refreshes without a background thread.
    explicit DNSCache(Resolver resolver);
    ~DNSCache();

    // get ip by hostname
    Status get(const std::string& hostname, std::string* ip);

    // Force a lookup after a socket fault, keeping the last IP as DNS-failure fallback.
    void invalidate(const std::string& hostname);

private:
    // Resolve hostname to IP address.
    // If resolution fails, falls back to cached IP if available.
    // Returns the resolved IP, or cached IP on failure, or empty string if no cache available.
    std::string _resolve_hostname(const std::string& hostname);

    // update the ip of hostname in cache
    Status _update(const std::string& hostname, std::string* ip = nullptr);

    // a function for refresh daemon thread
    // update cache at fix internal
    void _refresh_cache();

private:
    // hostname -> ip
    std::unordered_map<std::string, std::string> cache;
    std::unordered_set<std::string> _refresh_pending;
    std::unordered_set<std::string> _refresh_inflight;
    std::unordered_map<std::string, std::shared_ptr<std::mutex>> _refresh_locks;
    mutable std::shared_mutex mutex;
    Resolver _resolver;
    std::thread refresh_thread;
    std::atomic<bool> stop_refresh {false};
};

} // end of namespace doris
