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

package org.apache.doris.sdk.load.internal;

import org.apache.doris.sdk.load.config.DorisConfig;
import org.apache.doris.sdk.load.config.GroupCommitMode;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Builds HttpPut requests for Doris stream load.
 * Handles header assembly, label generation, and group commit logic.
 */
public class RequestBuilder {

    private static final Logger log = LoggerFactory.getLogger(RequestBuilder.class);
    private static final String STREAM_LOAD_PATTERN = "http://%s/api/%s/%s/_stream_load";
    private static final Random RANDOM = new Random();

    /**
     * Builds an HttpPut request for the given config and data.
     *
     * @param config  DorisConfig
     * @param data    pre-buffered (and optionally pre-compressed) request body bytes
     * @param attempt 0 = first attempt, >0 = retry number
     */
    public static HttpPut build(DorisConfig config, byte[] data, int attempt) throws Exception {
        String host = pickEndpoint(config.getEndpoints());
        String url = String.format(STREAM_LOAD_PATTERN, host, config.getDatabase(), config.getTable());

        HttpPut request = new HttpPut(url);
        request.setEntity(new ByteArrayEntity(data));

        // Basic auth
        String credentials = config.getUser() + ":" + config.getPassword();
        String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        request.setHeader("Authorization", "Basic " + encoded);
        request.setHeader("Expect", "100-continue");

        // Build and apply all stream load headers
        Map<String, String> allHeaders = buildStreamLoadHeaders(config);
        for (Map.Entry<String, String> entry : allHeaders.entrySet()) {
            request.setHeader(entry.getKey(), entry.getValue());
        }

        // Label handling: skip labels when group commit is enabled
        boolean groupCommitEnabled = allHeaders.containsKey("group_commit");
        if (groupCommitEnabled) {
            if (config.getLabel() != null && !config.getLabel().isEmpty()) {
                log.warn("Custom label '{}' specified but group_commit is enabled. Removing label.", config.getLabel());
            }
            if (config.getLabelPrefix() != null && !config.getLabelPrefix().isEmpty()) {
                log.warn("Label prefix '{}' specified but group_commit is enabled. Removing label prefix.", config.getLabelPrefix());
            }
            // Also remove any label header that may have been passed through options
            if (request.containsHeader("label")) {
                log.warn("Label header found in options but group_commit is enabled. Removing label.");
                request.removeHeaders("label");
            }
            log.info("Group commit enabled - labels removed from request headers");
        } else {
            String label = generateLabel(config, attempt);
            request.setHeader("label", label);
            if (attempt > 0) {
                log.debug("Generated retry label for attempt {}: {}", attempt, label);
            } else {
                log.debug("Generated label: {}", label);
            }
        }

        return request;
    }

    private static Map<String, String> buildStreamLoadHeaders(DorisConfig config) {
        Map<String, String> headers = new HashMap<>();

        // User-defined options first (lowest priority)
        if (config.getOptions() != null) {
            headers.putAll(config.getOptions());
        }

        // Format-specific headers
        if (config.getFormat() != null) {
            headers.putAll(config.getFormat().getHeaders());
        }

        // Group commit
        switch (config.getGroupCommit()) {
            case SYNC:
                headers.put("group_commit", "sync_mode");
                break;
            case ASYNC:
                headers.put("group_commit", "async_mode");
                break;
            case OFF:
            default:
                break;
        }

        // Gzip compression header
        if (config.isEnableGzip()) {
            if (headers.containsKey("compress_type")) {
                log.warn("Both enableGzip and options[compress_type] are set; enableGzip takes precedence.");
            }
            headers.put("compress_type", "gz");
        }

        return headers;
    }

    private static String generateLabel(DorisConfig config, int attempt) {
        long now = System.currentTimeMillis();
        String uuid = UUID.randomUUID().toString();

        if (config.getLabel() != null && !config.getLabel().isEmpty()) {
            if (attempt == 0) {
                return config.getLabel();
            } else {
                return config.getLabel() + "_retry_" + attempt + "_" + now + "_" + uuid.substring(0, 8);
            }
        }

        String prefix = (config.getLabelPrefix() != null && !config.getLabelPrefix().isEmpty())
                ? config.getLabelPrefix() : "load";

        if (attempt == 0) {
            return prefix + "_" + config.getDatabase() + "_" + config.getTable() + "_" + now + "_" + uuid;
        } else {
            return prefix + "_" + config.getDatabase() + "_" + config.getTable()
                    + "_" + now + "_retry_" + attempt + "_" + uuid;
        }
    }

    /** Picks a random endpoint and strips the http:// scheme to return host:port. */
    static String pickEndpoint(List<String> endpoints) {
        String endpoint = endpoints.get(RANDOM.nextInt(endpoints.size()));
        if (endpoint.startsWith("http://")) return endpoint.substring(7);
        if (endpoint.startsWith("https://")) return endpoint.substring(8);
        return endpoint;
    }
}
