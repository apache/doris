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

package org.apache.doris.common;

import org.apache.doris.common.util.NetUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.UnknownHostException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * DNSCache is a class that caches DNS lookups and periodically refreshes them.
 * It uses a ConcurrentHashMap to store the hostname to IP address mappings and a ScheduledExecutorService
 * to periodically refresh these mappings.
 */
public class DNSCache {
    private static final Logger LOG = LogManager.getLogger(DNSCache.class);

    // An RPC failure marks its host for a forced lookup at most once per interval, so a backend that
    // keeps refusing connections at an unchanged address does not turn every failed RPC into a DNS query.
    private static final long RPC_FAILURE_INVALIDATE_INTERVAL_MS = 1000L;

    private final ConcurrentHashMap<String, String> cache = new ConcurrentHashMap<>();
    // Hosts whose next get() must resolve again. A marker may exist before the host's first
    // lookup has published an entry, so an invalidation that races that lookup is not lost.
    private final Set<String> refreshPending = ConcurrentHashMap.newKeySet();
    // Hosts with a get() computation running. Warm readers queue behind it instead of returning
    // the address that the computation is replacing.
    private final ConcurrentHashMap<String, Object> refreshInFlight = new ConcurrentHashMap<>();
    // Bumped under the host's map lock each time a lookup publishes. The periodic refresh resolves
    // outside the lock and publishes only if no other lookup published since it started.
    private final ConcurrentHashMap<String, Long> versions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> lastRpcFailureInvalidateNanos = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executor = ThreadPoolManager.newDaemonScheduledThreadPool(1,
            "dns_cache_pool", true);

    /**
     * Check if the enable_fqdn_mode configuration is set.
     * If it is, it schedules a task to refresh the DNS cache every 60 seconds,
     * starting after an initial delay of 120 seconds.
     */
    public void start() {
        if (Config.enable_fqdn_mode) {
            executor.scheduleAtFixedRate(this::refresh, 120, 60, TimeUnit.SECONDS);
        }
    }

    /**
     * The get method retrieves the IP address for a given hostname from the cache.
     * If the hostname is not in the cache, it resolves the hostname to an IP address and stores it in the cache.
     *
     * @param hostname The hostname for which to get the IP address.
     * @return The IP address for the given hostname.
     */
    public String get(String hostname) {
        if (!refreshPending.contains(hostname) && !refreshInFlight.containsKey(hostname)) {
            // Read the IP after the markers: a forced lookup may have just published a new value.
            String cachedIp = cache.get(hostname);
            if (cachedIp != null) {
                return cachedIp;
            }
        }
        Object refresh = new Object();
        try {
            // compute serializes lookups per host with refresh(), so neither can publish an older result
            // over a newer one.
            return cache.compute(hostname, (host, currentIp) -> {
                // Mark in-flight before claiming pending work, so warm readers cannot
                // bypass this computation and reuse the old IP while DNS is running.
                refreshInFlight.put(host, refresh);
                boolean needsRefresh = refreshPending.remove(host);
                if (currentIp == null) {
                    if (needsRefresh) {
                        // An invalidation raced this first lookup; the JVM may still hold the old address.
                        NetUtils.evictJvmDnsCache(host);
                    }
                    versions.merge(host, 1L, Long::sum);
                    return resolveHostname(host);
                }
                if (needsRefresh) {
                    // A forced lookup must not be answered from the JVM's own cache, which may
                    // still hold the address being replaced.
                    NetUtils.evictJvmDnsCache(host);
                    versions.merge(host, 1L, Long::sum);
                    String resolvedIp = resolveHostname(host);
                    if (!resolvedIp.equals(currentIp)) {
                        LOG.info("IP for hostname {} has changed from {} to {}", host, currentIp, resolvedIp);
                    }
                    return resolvedIp;
                }
                return currentIp;
            });
        } finally {
            // compute has published the result. A following computation may already
            // own this marker, so remove only this call's token.
            refreshInFlight.remove(hostname, refresh);
        }
    }

    /**
     * Forces the next get() of hostname to resolve it again. The cached IP stays in place as the
     * fallback if that lookup fails. Invalidating a host whose first lookup is still running
     * forces one more lookup after it.
     */
    public void invalidate(String hostname) {
        refreshPending.add(hostname);
    }

    /**
     * Forces a new lookup of hostname after an RPC to failedIp could not reach it.
     * Does nothing when the cache already maps hostname to another address (the failure came from
     * a channel built before the last lookup), or when the host was invalidated for an RPC failure
     * less than a second ago.
     *
     * @return true if the host was marked for a new lookup.
     */
    public boolean invalidateAfterRpcFailure(String hostname, String failedIp) {
        String cachedIp = cache.get(hostname);
        if (cachedIp != null && !cachedIp.isEmpty() && !cachedIp.equals(failedIp)) {
            return false;
        }
        long now = System.nanoTime();
        long intervalNanos = TimeUnit.MILLISECONDS.toNanos(RPC_FAILURE_INVALIDATE_INTERVAL_MS);
        boolean[] claimed = new boolean[1];
        lastRpcFailureInvalidateNanos.compute(hostname, (host, last) -> {
            if (last != null && now - last < intervalNanos) {
                return last;
            }
            claimed[0] = true;
            return now;
        });
        if (claimed[0]) {
            invalidate(hostname);
            LOG.info("RPC to {} ({}) failed, re-resolve the hostname on its next use", hostname, failedIp);
        }
        return claimed[0];
    }

    /**
     * The resolveHostname method resolves a hostname to an IP address.
     * If the hostname cannot be resolved, it returns an empty string.
     *
     * @param hostname The hostname to resolve.
     * @return The IP address for the given hostname, or an empty string if the hostname cannot be resolved.
     */
    private String resolveHostname(String hostname) {
        String cachedIp = cache.get(hostname);
        try {
            String ip = NetUtils.getIpByHost(hostname, 0);
            if (ip == null || ip.isEmpty()) {
                if (cachedIp != null && !cachedIp.isEmpty()) {
                    LOG.warn("Failed to resolve hostname {}, use cached ip: {}", hostname, cachedIp);
                    return cachedIp;
                } else {
                    LOG.warn("Failed to resolve hostname {}, no cached ip available", hostname);
                    return "";
                }
            }
            return ip;
        } catch (UnknownHostException e) {
            if (cachedIp != null && !cachedIp.isEmpty()) {
                LOG.warn("Failed to resolve hostname {}, use cached ip: {}", hostname, cachedIp);
                return cachedIp;
            } else {
                LOG.warn("Failed to resolve hostname {}, no cached ip available", hostname);
                return "";
            }
        }
    }

    /**
     * The refresh method periodically refreshes the DNS cache.
     * It iterates over each hostname in the cache, resolves the hostname to an IP address,
     * and compares it with the current IP address in the cache.
     * If they are different, it updates the cache with the new IP address and logs the change.
     */
    private void refresh() {
        for (String hostname : cache.keySet()) {
            Long capturedVersion = versions.get(hostname);
            // Resolve outside the map's lock, so a slow resolver does not block lookups of other hosts.
            String resolvedIp = resolveHostname(hostname);
            cache.computeIfPresent(hostname, (host, currentIp) -> {
                // A lookup that published while this one ran has the newer answer, even if it
                // resolved the same address this refresh started from.
                if (!Objects.equals(versions.get(host), capturedVersion)) {
                    return currentIp;
                }
                // A pending invalidation stays pending: get() still forces its own lookup, and this
                // answer becomes that lookup's fallback.
                versions.merge(host, 1L, Long::sum);
                if (!resolvedIp.equals(currentIp)) {
                    LOG.info("IP for hostname {} has changed from {} to {}", host, currentIp, resolvedIp);
                }
                return resolvedIp;
            });
        }
    }
}
