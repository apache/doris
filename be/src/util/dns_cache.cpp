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

#include <netdb.h>

#include <algorithm>
#include <atomic>
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
    {
        std::lock_guard<std::mutex> lk(_cv_mutex);
        stop_refresh = true;
    }
    _cv.notify_all();
    if (refresh_thread.joinable()) {
        refresh_thread.join();
    }
}

Status DNSCache::get(const std::string& hostname, std::string* ip) {
    bool has_negative_entry = false;
    {
        std::shared_lock<std::shared_mutex> lock(mutex);
        auto it = cache.find(hostname);
        if (it != cache.end()) {
            *ip = it->second;
            return Status::OK();
        }
        auto neg_it = _negative_cache.find(hostname);
        if (neg_it != _negative_cache.end()) {
            int32_t ttl = config::dns_cache_negative_ttl_seconds;
            if (ttl > 0) {
                auto deadline = neg_it->second + std::chrono::seconds(ttl);
                if (std::chrono::steady_clock::now() < deadline) {
                    // No stack trace: this is an expected steady state, not an anomaly, and
                    // it is returned once per caller request for as long as the host stays
                    // unresolvable. Capturing a stack here would make Status::Error() log a
                    // WARNING per call (and every caller logs status.to_string() again),
                    // recreating exactly the be.WARNING flood this cache exists to stop.
                    return Status::InternalError<false>(
                            "Hostname {} is in negative DNS cache (recently evicted or "
                            "unresolvable), skipping resolve",
                            hostname);
                }
            }
            has_negative_entry = true;
        }
    }

    // If the host was in the negative cache with an expired (or disabled) TTL,
    // claim the single-flight retry under unique_lock before the blocking DNS
    // call.  Re-arming the eviction_time to now() makes concurrent callers see
    // an unexpired entry, bounding retries to one per host per TTL period.
    if (has_negative_entry) {
        std::unique_lock<std::shared_mutex> lock(mutex);
        auto neg_it = _negative_cache.find(hostname);
        if (neg_it != _negative_cache.end()) {
            int32_t ttl = config::dns_cache_negative_ttl_seconds;
            if (ttl <= 0) {
                _negative_cache.erase(neg_it);
            } else {
                auto deadline = neg_it->second + std::chrono::seconds(ttl);
                if (std::chrono::steady_clock::now() >= deadline) {
                    neg_it->second = std::chrono::steady_clock::now();
                } else {
                    // Lost the single-flight race; see above for why this carries no stack.
                    return Status::InternalError<false>(
                            "Hostname {} is in negative DNS cache (recently evicted or "
                            "unresolvable), skipping resolve",
                            hostname);
                }
            }
        }
    }

    // First access (or negative TTL expired): resolve and populate the cache.
    // Consume the IP returned by _update() directly to avoid a second cache
    // lookup — operator[] under a shared_lock would mutate the map and could
    // reinsert an empty entry if a concurrent refresh cycle evicted the hostname
    // between _update() and here.
    return _update(hostname, nullptr, ip);
}

// Resolve hostname to IP address, similar to Java's DNSCache.resolveHostname.
// If resolution fails, falls back to cached IP if available.
// Returns the resolved IP, or cached IP on failure, or empty string if no cache available.
// *is_fresh (if non-null) is set to true when DNS returned a live result, false
// when the IP comes from the stale cached fallback path.
std::string DNSCache::_resolve_hostname(const std::string& hostname, bool* is_fresh) {
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
    int gai_err = 0;
    Status status =
            _resolver ? _resolver(hostname, resolved_ip, BackendOptions::is_bind_ipv6(), &gai_err)
                      : hostname_to_ip(hostname, resolved_ip, BackendOptions::is_bind_ipv6(),
                                       &gai_err);

    if (!status.ok() || resolved_ip.empty()) {
        if (is_fresh) {
            *is_fresh = false;
        }
        // EAI_NONAME is the resolver authoritatively answering "this name does not exist",
        // which is the only evidence that a backend is really gone. Everything else
        // (EAI_AGAIN = resolver unreachable or timed out, EAI_SYSTEM, EAI_FAIL, ...) means
        // DNS itself is unhealthy while the host is most likely still up at its last known
        // address, so those failures must never lead to eviction — otherwise a resolver
        // outage would wipe every hostname at once and turn a DNS incident into a
        // cluster-wide RPC outage.
        const bool authoritative = (gai_err == EAI_NONAME);
        if (!cached_ip.empty()) {
            // Only track failure counts for hosts that are currently in the cache.
            // Hosts that were never cached or have already been evicted are not
            // tracked, which prevents unbounded growth of failure_count.
            uint32_t failures = 0;
            {
                std::unique_lock<std::shared_mutex> lock(mutex);
                // Re-check that the host is still cached under the unique_lock:
                // it may have been evicted by the refresh thread between our
                // earlier shared_lock read of cached_ip and now (hostname_to_ip
                // can block for seconds on DNS timeout, widening the window).
                // Skipping the bump here preserves keys(failure_count) ⊆ keys(cache).
                if (cache.find(hostname) != cache.end()) {
                    FailureState& state = failure_count[hostname];
                    // The counter tracks failures of any kind so the throttled log below
                    // stays informative during a resolver outage; only `last_authoritative`
                    // gates eviction.
                    failures = ++state.count;
                    state.last_authoritative = authoritative;
                }
            }
            // Throttle the log: only every N failures or the first failure.
            if (failures > 0) {
                int32_t every_n = std::max(1, config::dns_cache_log_every_n_failures);
                if (failures == 1 || failures % static_cast<uint32_t>(every_n) == 0) {
                    LOG(WARNING) << "Failed to resolve hostname " << hostname
                                 << " (consecutive failures: " << failures << ", error: "
                                 << (authoritative ? "NXDOMAIN, host is gone"
                                                   : "transient, DNS unhealthy")
                                 << "), use cached ip: " << cached_ip;
                }
            }
            return cached_ip;
        } else {
            // Throttle to avoid flooding be.WARNING when callers repeatedly
            // query an evicted or never-resolvable hostname.  This branch
            // deliberately does not maintain a per-hostname counter (that
            // would break the keys(failure_count) ⊆ keys(cache) invariant),
            // so the throttle is a coarse global rate limit shared across
            // all hostnames hitting this code path.
            static std::atomic<uint64_t> no_cache_warn_counter {0};
            uint64_t n = no_cache_warn_counter.fetch_add(1, std::memory_order_relaxed) + 1;
            int32_t every_n = std::max(1, config::dns_cache_log_every_n_failures);
            if (n == 1 || n % static_cast<uint64_t>(every_n) == 0) {
                LOG(WARNING) << "Failed to resolve hostname " << hostname
                             << ", no cached ip available";
            }
            return "";
        }
    }

    // Resolution succeeded - clear failure counter for this hostname.
    if (is_fresh) {
        *is_fresh = true;
    }
    {
        std::unique_lock<std::shared_mutex> lock(mutex);
        failure_count.erase(hostname);
    }
    return resolved_ip;
}

void DNSCache::_evict_locked(const std::string& hostname) {
    cache.erase(hostname);
    failure_count.erase(hostname);
    int32_t ttl = config::dns_cache_negative_ttl_seconds;
    if (ttl > 0) {
        _negative_cache[hostname] = std::chrono::steady_clock::now();
    }
}

void DNSCache::_remember_unresolvable(const std::string& hostname) {
    int32_t ttl = config::dns_cache_negative_ttl_seconds;
    if (ttl <= 0) {
        return;
    }
    std::unique_lock<std::shared_mutex> lock(mutex);
    // try_emplace, not operator[]: get()'s single-flight path may have just re-armed this
    // tombstone to now(); overwriting it would be harmless there but would also let two
    // callers racing on the same host each reset the deadline, loosening the rate limit.
    _negative_cache.try_emplace(hostname, std::chrono::steady_clock::now());
}

void DNSCache::_erase(const std::string& hostname) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    _evict_locked(hostname);
}

bool DNSCache::_erase_if_still_failing(const std::string& hostname, uint32_t threshold) {
    std::unique_lock<std::shared_mutex> lock(mutex);
    auto fc_it = failure_count.find(hostname);
    if (fc_it == failure_count.end() || fc_it->second.count < threshold ||
        !fc_it->second.last_authoritative) {
        // Either a concurrent successful resolution cleared or reset the counter between
        // _update() returning and this call — do not erase a now-healthy entry — or the
        // most recent failure was transient (resolver unreachable) rather than an
        // authoritative NXDOMAIN, in which case the host is probably still alive.
        return false;
    }
    _evict_locked(hostname);
    return true;
}

Status DNSCache::_update(const std::string& hostname, FailureState* out_state,
                         std::string* out_ip) {
    bool is_fresh = false;
    std::string real_ip = _resolve_hostname(hostname, &is_fresh);
    if (real_ip.empty()) {
        if (out_state) {
            *out_state = FailureState {};
        }
        if (out_ip) {
            out_ip->clear();
        }
        // The host could not be resolved and has no cached IP to fall back on, so it never
        // entered `cache` and will therefore never reach the eviction path that writes a
        // tombstone. Record one here as well: otherwise every single get() on a hostname
        // that has never resolved (a typo in the FE, a backend registered before its DNS
        // record propagated) pays a full blocking getaddrinfo, and many of those calls run
        // on bthreads where long blocking is especially costly.
        _remember_unresolvable(hostname);
        return Status::InternalError<false>(
                "Failed to resolve hostname {} and no cached ip available", hostname);
    }

    std::unique_lock<std::shared_mutex> lock(mutex);
    // _resolve_hostname may have captured a stale cached_ip before a concurrent
    // eviction completed.  If the host is now in the negative cache we must not
    // reinsert the stale IP: that would silently undo the eviction and clear the
    // tombstone, defeating the whole purpose of eviction.  Only a fresh DNS
    // result (is_fresh == true, meaning DNS actually resolved) may override an
    // eviction — which indicates the backend is genuinely back.
    if (!is_fresh && _negative_cache.count(hostname)) {
        if (out_state) {
            *out_state = FailureState {};
        }
        if (out_ip) {
            out_ip->clear();
        }
        // No stack trace: like the negative-cache hits in get(), this is an expected
        // outcome that can repeat on every request while the host stays evicted.
        return Status::InternalError<false>(
                "Hostname {} was concurrently evicted; stale-fallback not reinserted", hostname);
    }
    auto it = cache.find(hostname);
    if (it == cache.end() || it->second != real_ip) {
        cache[hostname] = real_ip;
        LOG(INFO) << "update hostname " << hostname << "'s ip to " << real_ip;
    }
    // DNS resolved successfully — remove any negative cache tombstone so
    // subsequent get() calls go straight to the main cache.
    _negative_cache.erase(hostname);
    if (out_ip) {
        *out_ip = real_ip;
    }
    // Read failure_count under the same lock we already hold, so _refresh_once
    // does not need a second lock acquisition to decide on eviction.
    if (out_state) {
        auto fc_it = failure_count.find(hostname);
        *out_state = fc_it != failure_count.end() ? fc_it->second : FailureState {};
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
        // Each _update() below performs a blocking getaddrinfo, so one cycle over a
        // cluster whose DNS is timing out can take minutes. Without this check the
        // destructor's join() would be held up for exactly that long, which would
        // undo the point of making the wait itself interruptible.
        if (stop_refresh.load(std::memory_order_acquire)) {
            break;
        }
        FailureState state;
        Status st = _update(key, &state);
        if (!st.ok()) {
            // _update returns an error either when _resolve_hostname returns ""
            // (no fallback IP) or when a stale fallback was suppressed because
            // the host was concurrently evicted.  Either way, log and move on;
            // the threshold check below handles the normal eviction path.
            LOG(WARNING) << "Failed to update DNS cache for hostname " << key << ": "
                         << st.to_string();
        }
        // Evict hostnames that have failed to resolve for too long.
        // This avoids two pathological symptoms after a backend is dropped
        // from the cluster and its DNS record is removed:
        //   1) be.WARNING gets flooded with `failed to get ip from host`.
        //   2) brpc keeps re-using the stale IP from cache, producing
        //      `Fail to wait EPOLLOUT ... Connection timed out`.
        // `last_authoritative` keeps this restricted to hosts the resolver has positively
        // reported as non-existent; a DNS outage yields transient errors for every host at
        // once and must leave the cache (and the stale-IP fallback) intact.
        int32_t threshold = config::dns_cache_max_consecutive_failures;
        if (threshold > 0 && state.last_authoritative &&
            state.count >= static_cast<uint32_t>(threshold)) {
            // Re-read failure_count under the mutex that also performs the erase
            // to fence any concurrent success that cleared the counter between
            // _update() returning and this point.
            if (_erase_if_still_failing(key, static_cast<uint32_t>(threshold))) {
                LOG(WARNING) << "Evicting hostname " << key << " from DNS cache after "
                             << state.count << " consecutive resolution failures";
            }
        }
    }
}

void DNSCache::_refresh_cache() {
    while (!stop_refresh) {
        {
            std::unique_lock<std::mutex> lk(_cv_mutex);
            // Wake up either after 1 minute or when the destructor signals stop.
            _cv.wait_for(lk, std::chrono::minutes(1), [this] { return stop_refresh.load(); });
        }
        if (!stop_refresh) {
            _refresh_once();
        }
    }
}

} // end of namespace doris
