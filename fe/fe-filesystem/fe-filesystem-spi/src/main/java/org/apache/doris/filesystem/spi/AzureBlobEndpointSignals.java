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

package org.apache.doris.filesystem.spi;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The single source of truth for "does this endpoint belong to Azure Blob Storage?" during
 * routing guesses.
 *
 * <p>Verbatim port of fe-core's legacy {@code AzureProperties.guessIsMe} endpoint leg +
 * {@code AzurePropertyUtils.isAzureBlobEndpoint}: the HOST is extracted from the endpoint
 * (URI parse when a scheme is present, otherwise path/port stripping), lowercased, and matched
 * with dot-anchored {@code endsWith} against the recognised Azure Blob/DFS host suffixes.
 *
 * <p>The live suffix list is fe-core's admin-extensible {@code Config.azure_blob_host_suffixes},
 * injected by fe-core's {@code FileSystemPluginManager} into every {@code supportsGuess} probe
 * view under {@link #HOST_SUFFIXES_PROBE_KEY} (plugins cannot see fe-core Config). When the
 * probe key is absent (plugin used standalone / in tests) the builtin default equals that
 * Config default.
 *
 * <p>Lives in fe-filesystem-spi so that BOTH the Azure provider (positive claim) and the
 * S3-compatible fallback providers such as MinIO (mutual exclusion — legacy
 * {@code MinioProperties.guessIsMe} consulted {@code AzureProperties.guessIsMe} directly) share
 * one predicate and can never drift apart, which would double-bind a map to AZURE and MINIO.
 */
public final class AzureBlobEndpointSignals {

    /**
     * Probe-context key carrying fe-core's {@code Config.azure_blob_host_suffixes} into the
     * routing guess. Comma-separated suffix list.
     */
    public static final String HOST_SUFFIXES_PROBE_KEY = "_AZURE_HOST_SUFFIXES_";

    /**
     * Endpoint aliases consulted by the routing guess — exactly the legacy
     * {@code AzureProperties.guessIsMe} list.
     */
    private static final String[] GUESS_ENDPOINT_KEYS = {
            "azure.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"};

    /**
     * Default host suffixes for the routing guess, equal to fe-core's
     * {@code Config.azure_blob_host_suffixes} defaults (blob + dfs across sovereign clouds).
     * Used only when no probe-context override is present.
     */
    private static final List<String> DEFAULT_GUESS_HOST_SUFFIXES = Arrays.asList(
            ".blob.core.windows.net",
            ".dfs.core.windows.net",
            ".blob.core.chinacloudapi.cn",
            ".dfs.core.chinacloudapi.cn",
            ".blob.core.usgovcloudapi.net",
            ".dfs.core.usgovcloudapi.net",
            ".blob.core.cloudapi.de",
            ".dfs.core.cloudapi.de");

    private AzureBlobEndpointSignals() {
    }

    /**
     * The endpoint leg of Azure's routing guess over the full (probe view of the) properties:
     * takes the first present endpoint alias and matches its host against the live suffix list.
     * Used positively by the Azure provider and negatively (exclusion) by MinIO.
     */
    public static boolean guessIsAzureBlobEndpoint(Map<String, String> properties) {
        String endpoint = firstPresent(properties, GUESS_ENDPOINT_KEYS);
        if (endpoint == null || endpoint.isEmpty()) {
            return false;
        }
        return isAzureBlobEndpoint(endpoint, properties);
    }

    /**
     * Whether the given endpoint's host carries a recognised Azure Blob/DFS suffix, honouring
     * the {@link #HOST_SUFFIXES_PROBE_KEY} override in {@code probeContext} when present.
     */
    public static boolean isAzureBlobEndpoint(String endpoint, Map<String, String> probeContext) {
        String host = extractHost(endpoint);
        if (host == null || host.isEmpty()) {
            return false;
        }
        String normalizedHost = host.toLowerCase(Locale.ROOT);
        for (String suffix : hostSuffixes(probeContext)) {
            if (matchesSuffix(normalizedHost, suffix)) {
                return true;
            }
        }
        return false;
    }

    private static List<String> hostSuffixes(Map<String, String> properties) {
        String override = properties == null ? null : properties.get(HOST_SUFFIXES_PROBE_KEY);
        if (override == null || override.trim().isEmpty()) {
            return DEFAULT_GUESS_HOST_SUFFIXES;
        }
        return Arrays.asList(override.split(","));
    }

    private static boolean matchesSuffix(String normalizedHost, String suffix) {
        if (suffix == null) {
            return false;
        }
        String normalizedSuffix = suffix.trim().toLowerCase(Locale.ROOT);
        if (normalizedSuffix.isEmpty()) {
            return false;
        }
        if (!normalizedSuffix.startsWith(".")) {
            normalizedSuffix = "." + normalizedSuffix;
        }
        return normalizedHost.endsWith(normalizedSuffix);
    }

    /** Verbatim port of legacy AzurePropertyUtils.extractHost. */
    private static String extractHost(String endpointOrHost) {
        String normalized = endpointOrHost.trim();
        if (normalized.isEmpty()) {
            return null;
        }
        if (normalized.contains("://")) {
            try {
                return new URI(normalized).getHost();
            } catch (URISyntaxException e) {
                return null;
            }
        }
        int slashIndex = normalized.indexOf('/');
        if (slashIndex >= 0) {
            normalized = normalized.substring(0, slashIndex);
        }
        int colonIndex = normalized.indexOf(':');
        if (colonIndex >= 0) {
            normalized = normalized.substring(0, colonIndex);
        }
        return normalized;
    }

    private static String firstPresent(Map<String, String> properties, String[] names) {
        for (String name : names) {
            String value = properties.get(name);
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        return null;
    }
}
