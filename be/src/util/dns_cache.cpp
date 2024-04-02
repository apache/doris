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

Status DNSCache::_update(const std::string& hostname) {
    std::string real_ip = "";
    RETURN_IF_ERROR(hostname_to_ip(hostname, real_ip, BackendOptions::is_bind_ipv6()));
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
        Status st;
        for (auto& key : keys) {
            st = _update(key);
        }
    }
}

} // end of namespace doris
