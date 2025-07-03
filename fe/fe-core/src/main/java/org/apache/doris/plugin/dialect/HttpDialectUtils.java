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

package org.apache.doris.plugin.dialect;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This class is used to convert sql with different dialects using sql convertor service.
 * The sql convertor service is a http service which is used to convert sql.
 * <p>
 * Features:
 * - Support multiple URLs (comma separated)
 * - Blacklist mechanism for failed URLs
 * - Automatic failover and retry
 * - URL caching and smart selection
 */
public class HttpDialectUtils {
    private static final Logger LOG = LogManager.getLogger(HttpDialectUtils.class);

    // Cache URL manager instances to avoid duplicate parsing
    private static final ConcurrentHashMap<String, UrlManager> urlManagerCache = new ConcurrentHashMap<>();

    // Blacklist recovery time (ms): 5 minutes
    private static final long BLACKLIST_RECOVERY_TIME_MS = 5 * 60 * 1000;
    // Connection timeout period (ms): 3 seconds
    private static final int CONNECTION_TIMEOUT_MS = 3000;
    // Read timeout period (ms): 10 seconds
    private static final int READ_TIMEOUT_MS = 10000;

    public static String convertSql(String targetURLs, String originStmt, String dialect,
            String[] features, String config) {
        if (targetURLs == null || targetURLs.trim().isEmpty()) {
            LOG.warn("Target URLs is empty, return original SQL");
            return originStmt;
        }

        UrlManager urlManager = getOrCreateUrlManager(targetURLs);
        ConvertRequest convertRequest = new ConvertRequest(originStmt, dialect, features, config);
        String requestStr = convertRequest.toJson();

        // Try to convert SQL using intelligent URL selection strategy
        return tryConvertWithIntelligentSelection(urlManager, requestStr, originStmt);
    }

    /**
     * Try to convert SQL using intelligent URL selection strategy
     * CRITICAL: This method ensures 100% success rate when ANY service is available
     */
    private static String tryConvertWithIntelligentSelection(
            UrlManager urlManager, String requestStr, String originStmt) {
        // Strategy: Try ALL URLs in intelligent order, regardless of blacklist status
        // This ensures 100% success rate when any service is actually available

        List<String> allUrls = urlManager.getAllUrlsInPriorityOrder();

        for (String url : allUrls) {
            try {
                String result = doConvertSql(url, requestStr);
                if (result != null && !result.equals(originStmt)) {
                    // Conversion succeeded, mark URL as healthy (remove from blacklist)
                    urlManager.markUrlAsHealthy(url);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Successfully converted SQL using URL: {}", url);
                    }
                    return result;
                }
            } catch (Exception e) {
                LOG.warn("Failed to convert SQL using URL: {}, error: {}", url, e.getMessage());
                // Add failed URL to blacklist for future optimization
                urlManager.markUrlAsBlacklisted(url);
                // Continue trying next URL - this is CRITICAL for 100% success rate
            }
        }

        LOG.warn("All URLs failed to convert SQL, return original SQL");
        return originStmt;
    }

    /**
     * Get or create a URL manager
     */
    private static UrlManager getOrCreateUrlManager(String targetURLs) {
        return urlManagerCache.computeIfAbsent(targetURLs, UrlManager::new);
    }

    /**
     * Perform SQL conversion for individual URL
     */
    private static String doConvertSql(String targetURL, String requestStr) throws Exception {
        HttpURLConnection connection = null;
        try {
            URL url = new URL(targetURL);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setUseCaches(false);
            connection.setDoOutput(true);
            connection.setConnectTimeout(CONNECTION_TIMEOUT_MS);
            connection.setReadTimeout(READ_TIMEOUT_MS);

            try (OutputStream outputStream = connection.getOutputStream()) {
                outputStream.write(requestStr.getBytes(StandardCharsets.UTF_8));
            }

            int responseCode = connection.getResponseCode();
            if (LOG.isDebugEnabled()) {
                LOG.debug("POST Response Code: {}, URL: {}, post data: {}", responseCode, targetURL, requestStr);
            }

            if (responseCode == HttpURLConnection.HTTP_OK) {
                try (InputStreamReader inputStreamReader
                        = new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8);
                        BufferedReader in = new BufferedReader(inputStreamReader)) {
                    String inputLine;
                    StringBuilder response = new StringBuilder();

                    while ((inputLine = in.readLine()) != null) {
                        response.append(inputLine);
                    }

                    Type type = new TypeToken<ConvertResponse>() {
                    }.getType();
                    ConvertResponse result = new Gson().fromJson(response.toString(), type);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Convert response: {}, URL: {}", result, targetURL);
                    }
                    if (result.code == 0) {
                        if (!"v1".equals(result.version)) {
                            LOG.warn("Failed to convert sql, response version is not v1: {}, URL: {}",
                                    result.version, targetURL);
                            return null;
                        }
                        return result.data;
                    } else {
                        LOG.warn("Failed to convert sql, response: {}, URL: {}", result, targetURL);
                        return null;
                    }
                }
            } else {
                throw new Exception("HTTP response code: " + responseCode);
            }
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    /**
     * URL Manager - Responsible for URL parsing, caching, blacklist management, and smart selection
     */
    private static class UrlManager {
        private final List<String> parsedUrls;
        private final ConcurrentHashMap<String, BlacklistEntry> blacklist;

        public UrlManager(String urls) {
            this.parsedUrls = parseUrls(urls);
            this.blacklist = new ConcurrentHashMap<>();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Created UrlManager with URLs: {}, parsed: {}", urls, parsedUrls);
            }
        }

        /**
         * Parse comma separated URL strings
         */
        private List<String> parseUrls(String urls) {
            List<String> result = Lists.newArrayList();
            if (urls != null && !urls.trim().isEmpty()) {
                String[] urlArray = urls.split(",");
                for (String url : urlArray) {
                    String trimmedUrl = url.trim();
                    if (!trimmedUrl.isEmpty()) {
                        result.add(trimmedUrl);
                    }
                }
            }
            return result;
        }

        /**
         * Mark URL as healthy (remove from blacklist)
         */
        public void markUrlAsHealthy(String url) {
            if (blacklist.remove(url) != null) {
                LOG.info("Removed URL from blacklist due to successful request: {}", url);
            }
        }

        /**
         * Add URL to blacklist
         */
        public void markUrlAsBlacklisted(String url) {
            long currentTime = System.currentTimeMillis();
            long recoverTime = currentTime + BLACKLIST_RECOVERY_TIME_MS;
            BlacklistEntry existingEntry = blacklist.get(url);
            if (existingEntry != null) {
                // If URL is already in blacklist, limit maximum recovery time to avoid infinite extension
                // Maximum recovery time is 2 times the original recovery time
                long maxRecoverTime = currentTime + (BLACKLIST_RECOVERY_TIME_MS * 2);
                recoverTime = Math.min(maxRecoverTime, existingEntry.recoverTime + BLACKLIST_RECOVERY_TIME_MS);
            }
            blacklist.put(url, new BlacklistEntry(currentTime, recoverTime));
            LOG.warn("Added URL to blacklist: {}, will recover at: {}", url, new Date(recoverTime));
        }

        /**
         * Get list of healthy URLs (not in blacklist)
         */
        public List<String> getHealthyUrls() {
            List<String> healthy = Lists.newArrayList();
            long currentTime = System.currentTimeMillis();
            for (String url : parsedUrls) {
                BlacklistEntry entry = blacklist.get(url);
                if (entry == null) {
                    // URL is not in blacklist, consider it healthy
                    healthy.add(url);
                } else if (currentTime >= entry.recoverTime) {
                    // URL has reached recovery time, remove from blacklist and add to healthy list
                    blacklist.remove(url);
                    healthy.add(url);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("URL recovered from blacklist: {}", url);
                    }
                }
            }

            // Randomly shuffle the order to avoid always trying from the first URL
            Collections.shuffle(healthy, ThreadLocalRandom.current());

            if (LOG.isDebugEnabled()) {
                LOG.debug("Healthy URLs: {}", healthy);
            }

            return healthy;
        }

        /**
         * Get list of blacklisted URLs (for immediate retry)
         */
        public List<String> getBlacklistedUrls() {
            List<String> blacklisted = Lists.newArrayList();
            long currentTime = System.currentTimeMillis();

            for (String url : parsedUrls) {
                BlacklistEntry entry = blacklist.get(url);
                if (entry != null && currentTime < entry.recoverTime) {
                    // URL is in blacklist and has not reached recovery time yet
                    blacklisted.add(url);
                }
            }

            // Sort by recovery time, prioritize URLs that should recover earlier
            blacklisted.sort((url1, url2) -> {
                BlacklistEntry entry1 = blacklist.get(url1);
                BlacklistEntry entry2 = blacklist.get(url2);
                return Long.compare(entry1.recoverTime, entry2.recoverTime);
            });

            if (LOG.isDebugEnabled()) {
                LOG.debug("Blacklisted URLs for immediate retry: {}", blacklisted);
            }

            return blacklisted;
        }

        /**
         * Get ALL URLs in priority order for 100% success guarantee
         * CRITICAL: This method ensures we try every URL when any service might be available
         * <p>
         * Priority order:
         * 1. Healthy URLs (not in blacklist or recovered) - randomly shuffled for load balancing
         * 2. Blacklisted URLs (sorted by recovery time) - still try them for guaranteed coverage
         */
        public List<String> getAllUrlsInPriorityOrder() {
            List<String> prioritizedUrls = Lists.newArrayList();

            // First: Add all healthy URLs
            List<String> healthyUrls = getHealthyUrls();
            prioritizedUrls.addAll(healthyUrls);

            // Second: Add all blacklisted URLs that haven't been tried yet
            List<String> blacklistedUrls = getBlacklistedUrls();
            for (String blacklistedUrl : blacklistedUrls) {
                if (!prioritizedUrls.contains(blacklistedUrl)) {
                    prioritizedUrls.add(blacklistedUrl);
                }
            }

            // Ensure we have all URLs - add any missing ones (safety net)
            for (String url : parsedUrls) {
                if (!prioritizedUrls.contains(url)) {
                    prioritizedUrls.add(url);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("All URLs in priority order: {}", prioritizedUrls);
            }

            return prioritizedUrls;
        }
    }

    /**
     * Blacklist entry
     */
    private static class BlacklistEntry {
        final long blacklistedTime;
        final long recoverTime;

        BlacklistEntry(long blacklistedTime, long recoverTime) {
            this.blacklistedTime = blacklistedTime;
            this.recoverTime = recoverTime;
        }
    }

    @Data
    private static class ConvertRequest {
        private String version;    // CHECKSTYLE IGNORE THIS LINE
        private String sql_query;   // CHECKSTYLE IGNORE THIS LINE
        private String from;    // CHECKSTYLE IGNORE THIS LINE
        private String to;   // CHECKSTYLE IGNORE THIS LINE
        private String source;  // CHECKSTYLE IGNORE THIS LINE
        private String case_sensitive;  // CHECKSTYLE IGNORE THIS LINE
        private String[] enable_sql_convertor_features; // CHECKSTYLE IGNORE THIS LINE
        private String config; // CHECKSTYLE IGNORE THIS LINE

        public ConvertRequest(String originStmt, String dialect, String[] features, String config) {
            this.version = "v1";
            this.sql_query = originStmt;
            this.from = dialect;
            this.to = "doris";
            this.source = "text";
            this.case_sensitive = "0";
            this.enable_sql_convertor_features = features;
            this.config = config;
        }

        public String toJson() {
            return new Gson().toJson(this);
        }
    }

    @Data
    private static class ConvertResponse {
        private String version;    // CHECKSTYLE IGNORE THIS LINE
        private String data;    // CHECKSTYLE IGNORE THIS LINE
        private int code;   // CHECKSTYLE IGNORE THIS LINE
        private String message; // CHECKSTYLE IGNORE THIS LINE

        public String toJson() {
            return new Gson().toJson(this);
        }

        @Override
        public String toString() {
            return toJson();
        }
    }
}
