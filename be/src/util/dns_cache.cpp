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

#include <algorithm>
#include <unordered_set>

#include "common/config.h"
#include "service/backend_options.h"
#include "util/network_util.h"

namespace doris {

DNSCache::DNSCache() {
    refresh_thread = std::thread(&DNSCache::_refresh_cache, this);
}

DNSCache::DNSCache(Resolver resolver) : _resolver(std::move(resolver)) {}

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
    Status status = _resolver
                            ? _resolver(hostname, resolved_ip, BackendOptions::is_bind_ipv6())
                            : hostname_to_ip(hostname, resolved_ip, BackendOptions::is_bind_ipv6());

    if (!status.ok() || resolved_ip.empty()) {
        if (!cached_ip.empty()) {
            // Only track failure counts for hosts that are currently in the cache.
            // Hosts that were never cached or have already been evicted are not
            // tracked, which prevents unbounded growth of failure_count.
            uint32_t failures = 0;
            {
                std::unique_lock<std::shared_mutex> lock(mutex);
                failures = ++failure_count[hostname];
            }
            // Throttle the log: only every N failures or the first failure.
            int32_t every_n = std::max(1, config::dns_cache_log_every_n_failures);
            if (failures == 1 || failures % static_cast<uint32_t>(every_n) == 0) {
                LOG(WARNING) << "Failed to resolve hostname " << hostname
                             << " (consecutive failures: " << failures
                             << "), use cached ip: " << cached_ip;
            }
            return cached_ip;
        } else {
            LOG(WARNING) << "Failed to resolve hostname " << hostname << ", no cached ip available";
            return "";
        }
    }

    // Resolution succeeded - clear failure counter for this hostname.
    {
        std::unique_lock<std::shared_mutex> lock(mutex);
        failure_count.erase(hostname);
    }
    return resolved_ip;
}

void DNSCache::_erase(const std::string& hostname) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    cache.erase(hostname);
    failure_count.erase(hostname);
}

Status DNSCache::_update(const std::string& hostname, uint32_t* out_failures) {
    std::string real_ip = _resolve_hostname(hostname);
    if (real_ip.empty()) {
        if (out_failures) *out_failures = 0;
        return Status::InternalError("Failed to resolve hostname {} and no cached ip available",
                                     hostname);
    }

    std::unique_lock<std::shared_mutex> lock(mutex);
    auto it = cache.find(hostname);
    if (it == cache.end() || it->second != real_ip) {
        cache[hostname] = real_ip;
        LOG(INFO) << "update hostname " << hostname << "'s ip to " << real_ip;
    }
    // Read failure_count under the same lock we already hold, so _refresh_once
    // does not need a second lock acquisition to decide on eviction.
    if (out_failures) {
        auto fc_it = failure_count.find(hostname);
        *out_failures = fc_it != failure_count.end() ? fc_it->second : 0;
    }
    return Status::OK();
}

void DNSCache::_refresh_once() {
    std::unordered_set<std::string> keys;
    {
        std::shared_lock<std::shared_mutex> lock(mutex);
        std::transform(cache.begin(), cache.end(), std::inserter(keys, keys.end()),
                       [](const auto& pair) { return pair.first; });
    }
    for (auto& key : keys) {
        uint32_t failures = 0;
        Status st = _update(key, &failures);
        if (!st.ok()) {
            // _update only returns an error when _resolve_hostname returns an
            // empty string, which happens only if the hostname has never been
            // successfully resolved (no cached IP to fall back to).  Keys in
            // the refresh loop come from `cache`, so they all have a prior IP;
            // during DNS failure _resolve_hostname returns that cached IP and
            // _update returns OK.  This branch is therefore effectively dead
            // under normal refresh semantics — the eviction logic below handles
            // the long-running failure case instead.
            LOG(WARNING) << "Failed to update DNS cache for hostname " << key << ": "
                         << st.to_string();
        }
        // Evict hostnames that have failed to resolve for too long.
        // This avoids two pathological symptoms after a backend is dropped
        // from the cluster and its DNS record is removed:
        //   1) be.WARNING gets flooded with `failed to get ip from host`.
        //   2) brpc keeps re-using the stale IP from cache, producing
        //      `Fail to wait EPOLLOUT ... Connection timed out`.
        int32_t threshold = config::dns_cache_max_consecutive_failures;
        if (threshold > 0 && failures >= static_cast<uint32_t>(threshold)) {
            LOG(WARNING) << "Evicting hostname " << key << " from DNS cache after " << failures
                         << " consecutive resolution failures";
            _erase(key);
        }
    }
}

void DNSCache::_refresh_cache() {
    while (!stop_refresh) {
        // refresh every 1 min
        std::this_thread::sleep_for(std::chrono::minutes(1));
        _refresh_once();
    }
}

} // end of namespace doris
