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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

/**
 * DNSCache is a class that caches DNS lookups and periodically refreshes them.
 * It uses a ConcurrentHashMap to store the hostname to IP address mappings and a ScheduledExecutorService
 * to periodically refresh these mappings.
 */
public class DNSCache {
    private static final Logger LOG = LogManager.getLogger(DNSCache.class);

    private final ConcurrentHashMap<String, String> cache = new ConcurrentHashMap<>();
    private final ScheduledExecutorService executor = ThreadPoolManager.newDaemonScheduledThreadPool(1,
            "dns_cache_pool", true);

    /**
     * Check if the enable_fqdn_mode configuration is set.
     * If it is, it schedules a task to refresh the DNS cache every 60 seconds,
     * starting after an initial delay of 120 seconds.
     */
    public void start() {
        if (Config.enable_fqdn_mode) {
            executor.scheduleAtFixedRate(this::refresh, 120, 60, java.util.concurrent.TimeUnit.SECONDS);
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
        return cache.computeIfAbsent(hostname, this::resolveHostname);
    }

    /**
     * The resolveHostname method resolves a hostname to an IP address.
     * If the hostname cannot be resolved, it returns an empty string.
     *
     * @param hostname The hostname to resolve.
     * @return The IP address for the given hostname, or an empty string if the hostname cannot be resolved.
     */
    private String resolveHostname(String hostname) {
        try {
            return NetUtils.getIpByHost(hostname, 0);
        } catch (UnknownHostException e) {
            return "";
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
            String resolvedHostname = resolveHostname(hostname);
            String currentHostname = cache.get(hostname);
            if (!resolvedHostname.equals(currentHostname)) {
                cache.put(hostname, resolvedHostname);
                LOG.info("IP for hostname {} has changed from {} to {}", hostname, currentHostname,
                        resolvedHostname);
            }
        }
    }
}
