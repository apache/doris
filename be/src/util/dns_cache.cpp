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

#include "util/dns_cache.h"

#include "service/backend_options.h"
#include "util/network_util.h"

namespace doris {

DNSCache::DNSCache() {
    refresh_thread = std::thread(&DNSCache::_refresh_cache, this);
}

DNSCache::~DNSCache() {
    stop_refresh = true;
    if (refresh_thread.joinable()) {
        refresh_thread.join();
    }
}

Status DNSCache::get(const std::string& hostname, std::string* ip) {
    {
        std::shared_lock<std::shared_mutex> lock(mutex);
        auto it = cache.find(hostname);
        if (it != cache.end()) {
            *ip = it->second;
            return Status::OK();
        }
    }
    // Update if not found
    RETURN_IF_ERROR(_update(hostname));
    {
        std::shared_lock<std::shared_mutex> lock(mutex);
        *ip = cache[hostname];
        return Status::OK();
    }
}

// Resolve hostname to IP address, similar to Java's DNSCache.resolveHostname.
// If resolution fails, falls back to cached IP if available.
// Returns the resolved IP, or cached IP on failure, or empty string if no cache available.
std::string DNSCache::_resolve_hostname(const std::string& hostname) {
    // Get cached IP first (if any)
    std::string cached_ip;
    {
        std::shared_lock<std::shared_mutex> lock(mutex);
        auto it = cache.find(hostname);
        if (it != cache.end()) {
            cached_ip = it->second;
        }
    }

    // Try to resolve hostname
    std::string resolved_ip;
    Status status = hostname_to_ip(hostname, resolved_ip, BackendOptions::is_bind_ipv6());

    if (!status.ok() || resolved_ip.empty()) {
        // Resolution failed
        if (!cached_ip.empty()) {
            LOG(WARNING) << "Failed to resolve hostname " << hostname
                         << ", use cached ip: " << cached_ip;
            return cached_ip;
        } else {
            LOG(WARNING) << "Failed to resolve hostname " << hostname << ", no cached ip available";
            return "";
        }
    }

    return resolved_ip;
}

Status DNSCache::_update(const std::string& hostname) {
    std::string real_ip = _resolve_hostname(hostname);
    if (real_ip.empty()) {
        return Status::InternalError("Failed to resolve hostname {} and no cached ip available",
                                     hostname);
    }

    std::unique_lock<std::shared_mutex> lock(mutex);
    auto it = cache.find(hostname);
    if (it == cache.end() || it->second != real_ip) {
        cache[hostname] = real_ip;
        LOG(INFO) << "update hostname " << hostname << "'s ip to " << real_ip;
    }
    return Status::OK();
}

void DNSCache::_refresh_cache() {
    while (!stop_refresh) {
        // refresh every 1 min
        std::this_thread::sleep_for(std::chrono::minutes(1));
        std::unordered_set<std::string> keys;
        {
            std::shared_lock<std::shared_mutex> lock(mutex);
            std::transform(cache.begin(), cache.end(), std::inserter(keys, keys.end()),
                           [](const auto& pair) { return pair.first; });
        }
        for (auto& key : keys) {
            Status st = _update(key);
            if (!st.ok()) {
                LOG(WARNING) << "Failed to update DNS cache for hostname " << key << ": "
                             << st.to_string();
            }
        }
    }
}

} // end of namespace doris
