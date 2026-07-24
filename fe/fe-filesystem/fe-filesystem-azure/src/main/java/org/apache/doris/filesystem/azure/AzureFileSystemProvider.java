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

package org.apache.doris.filesystem.azure;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * SPI provider for Azure Blob Storage.
 *
 * <p>Registered via META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider.
 *
 * <p>Identified by the presence of {@code AZURE_ACCOUNT_NAME}, {@code azure.account_name},
 * or an endpoint that contains a known Azure Blob Storage host suffix from one of the
 * sovereign clouds.
 */
public class AzureFileSystemProvider implements FileSystemProvider<AzureFileSystemProperties> {

    private static final String STORAGE_TYPE_KEY = "_STORAGE_TYPE_";
    private static final String STORAGE_TYPE_AZURE = "AZURE";
    private static final String PROVIDER_KEY = "provider";
    private static final String[] ACCOUNT_NAME_KEYS = {
            AzureFileSystemProperties.ACCOUNT_NAME, "azure.access_key", "AZURE_ACCOUNT_NAME"};
    private static final String[] ENDPOINT_KEYS = {
            AzureFileSystemProperties.ENDPOINT, "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT",
            "AZURE_ENDPOINT"};

    /**
     * Recognised Azure Blob Storage host suffixes across sovereign clouds.
     * Includes Azure Public, Azure China, Azure US Government, and the deprecated
     * Azure Germany cloud (still spec'd for completeness).
     */
    private static final List<String> AZURE_BLOB_HOST_SUFFIXES = Arrays.asList(
            "blob.core.windows.net",
            "blob.core.chinacloudapi.cn",
            "blob.core.usgovcloudapi.net",
            "blob.core.cloudapi.de");

    @Override
    public boolean supports(Map<String, String> properties) {
        if (isExplicitAzure(properties)) {
            return true;
        }
        if (firstPresent(properties, ACCOUNT_NAME_KEYS) != null) {
            return true;
        }
        String endpoint = firstPresent(properties, ENDPOINT_KEYS);
        if (endpoint == null) {
            return false;
        }
        for (String suffix : AZURE_BLOB_HOST_SUFFIXES) {
            if (endpoint.contains(suffix)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public AzureFileSystemProperties bind(Map<String, String> properties) {
        return AzureFileSystemProperties.of(properties);
    }

    @Override
    public FileSystem create(AzureFileSystemProperties properties) throws IOException {
        return new AzureFileSystem(new AzureObjStorage(properties));
    }

    @Override
    public boolean supportsExplicit(Map<String, String> properties) {
        return Boolean.parseBoolean(properties.getOrDefault("fs.azure.support", "false"));
    }

    /**
     * Probe-context key carrying fe-core's {@code Config.azure_blob_host_suffixes} into the
     * routing guess (the plugin cannot see fe-core Config; fe-core's bind registry injects the
     * live, admin-extensible list into a probe view of the properties). Comma-separated.
     */
    public static final String HOST_SUFFIXES_PROBE_KEY = "_AZURE_HOST_SUFFIXES_";

    /**
     * Endpoint aliases consulted by the routing guess — exactly the legacy
     * {@code AzureProperties.guessIsMe} list (deliberately narrower than {@link #ENDPOINT_KEYS}:
     * legacy never consulted {@code AZURE_ENDPOINT} for guessing).
     */
    private static final String[] GUESS_ENDPOINT_KEYS = {
            "azure.endpoint", "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"};

    /**
     * Default host suffixes for the routing guess, equal to fe-core's
     * {@code Config.azure_blob_host_suffixes} defaults (blob + dfs across sovereign clouds).
     * Used only when no probe-context override is present (plugin used standalone / in tests).
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

    @Override
    public boolean supportsGuess(Map<String, String> properties) {
        // Verbatim port of fe-core AzureProperties.guessIsMe + AzurePropertyUtils
        // .isAzureBlobEndpoint: provider=azure, or an endpoint alias whose HOST (extracted from
        // the URI, lowercased) ends with a dot-anchored recognised Azure Blob/DFS suffix.
        if ("azure".equalsIgnoreCase(properties.get(PROVIDER_KEY))) {
            return true;
        }
        String endpoint = firstPresent(properties, GUESS_ENDPOINT_KEYS);
        if (endpoint == null || endpoint.isEmpty()) {
            return false;
        }
        String host = extractHost(endpoint);
        if (host == null || host.isEmpty()) {
            return false;
        }
        String normalizedHost = host.toLowerCase(Locale.ROOT);
        for (String suffix : guessHostSuffixes(properties)) {
            if (matchesSuffix(normalizedHost, suffix)) {
                return true;
            }
        }
        return false;
    }

    private static List<String> guessHostSuffixes(Map<String, String> properties) {
        String override = properties.get(HOST_SUFFIXES_PROBE_KEY);
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

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return create(bind(properties));
    }

    @Override
    public String name() {
        return "AZURE";
    }

    @Override
    public Set<String> sensitivePropertyKeys() {
        return ConnectorPropertiesUtils.getSensitiveKeys(AzureFileSystemProperties.class);
    }

    private boolean isExplicitAzure(Map<String, String> properties) {
        return STORAGE_TYPE_AZURE.equalsIgnoreCase(properties.get(STORAGE_TYPE_KEY))
                || "azure".equalsIgnoreCase(properties.get(PROVIDER_KEY));
    }

    private String firstPresent(Map<String, String> properties, String[] names) {
        for (String name : names) {
            String value = properties.get(name);
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        return null;
    }
}
