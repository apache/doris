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

package org.apache.doris.datasource.credentials;

import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.datasource.storage.StorageTypeId;
import org.apache.doris.foundation.property.StoragePropertiesException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for Credential operations
 */
public class CredentialUtils {

    private static final String ADLS_SAS_TOKEN_PREFIX = "adls.sas-token.";
    private static final String ADLS_SAS_EXPIRY_PREFIX = "adls.sas-token-expires-at-ms.";
    private static final String ADLS_PROPERTY_PREFIX = "adls.";

    /** Provider-specific failure that must not be silently downgraded to static credentials. */
    public static class AzureSasCredentialException extends StoragePropertiesException {
        public AzureSasCredentialException(String message) {
            super(message);
        }

        public AzureSasCredentialException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Supported cloud storage prefixes for filtering vended credentials
     */
    private static final Set<String> CLOUD_STORAGE_PREFIXES = new HashSet<>(Arrays.asList(
            "fs.",           // file system
            "s3.",           // Amazon S3
            "oss.",          // Alibaba OSS
            "cos.",          // Tencent COS
            "obs.",          // Huawei OBS
            "gs.",           // Google Cloud Storage
            "azure.",        // Microsoft Azure
            "azure_",        // Native Azure backend aliases
            "adls.",         // Apache Iceberg ADLS vended credentials
            "client.",       // Iceberg client properties (e.g., client.region)
            "iceberg.rest."  // Iceberg REST catalog properties (e.g., iceberg.rest.access-key-id)
    ));

    /**
     * Filter cloud storage properties from raw vended credentials
     * Only keeps properties with supported cloud storage prefixes
     *
     * @param rawVendedCredentials Raw vended credentials map
     * @return Filtered cloud storage properties
     */
    public static Map<String, String> filterCloudStorageProperties(Map<String, String> rawVendedCredentials) {
        if (rawVendedCredentials == null || rawVendedCredentials.isEmpty()) {
            return new HashMap<>();
        }

        Map<String, String> filtered = new HashMap<>();
        rawVendedCredentials.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getValue() != null)
                .filter(entry -> isSupportedCloudStorageKey(entry.getKey()))
                .forEach(entry -> filtered.put(entry.getKey(), entry.getValue()));

        return filtered;
    }

    private static boolean isSupportedCloudStorageKey(String key) {
        String lowerKey = key.toLowerCase(Locale.ROOT);
        return CLOUD_STORAGE_PREFIXES.stream()
                .anyMatch(prefix -> lowerKey.startsWith(prefix.toLowerCase(Locale.ROOT)))
                || "provider".equals(lowerKey);
    }

    /**
     * Converts Iceberg ADLS vended SAS properties to the native Azure binding consumed by BE.
     * Unity Catalog/ADLSFileIO exposes account-scoped values as
     * {@code adls.sas-token.<account-host>} and
     * {@code adls.sas-token-expires-at-ms.<account-host>}. The native Doris reader receives
     * provider-owned {@code AZURE_*} keys instead of Hadoop {@code fs.azure.*} settings.
     *
     * <p>Only one account is accepted per scan. A table whose files span multiple Azure accounts
     * must be planned as separate storage bindings; silently selecting one token would make the
     * other files fail (or, worse, use the wrong credential).</p>
     */
    public static Map<String, String> normalizeCloudStorageProperties(
            Map<String, String> rawVendedCredentials) {
        Map<String, String> filtered = filterCloudStorageProperties(rawVendedCredentials);
        Map<String, String> normalized = new HashMap<>(filtered);
        String tokenHost = null;
        String token = null;
        String expiry = null;
        String container = null;
        for (Map.Entry<String, String> entry : filtered.entrySet()) {
            String key = entry.getKey();
            String lowerKey = key.toLowerCase(Locale.ROOT);
            if (lowerKey.startsWith(ADLS_SAS_TOKEN_PREFIX)) {
                String host = key.substring(ADLS_SAS_TOKEN_PREFIX.length());
                if (host.isBlank() || entry.getValue().isBlank()) {
                    throw new AzureSasCredentialException(
                            "Azure ADLS SAS credential requires a non-empty account host and token");
                }
                if (tokenHost != null && !tokenHost.equalsIgnoreCase(host)) {
                    throw new AzureSasCredentialException(
                            "Multiple Azure ADLS SAS account hosts are not supported in one scan");
                }
                tokenHost = host;
                token = stripSasPrefix(entry.getValue());
            } else if (lowerKey.startsWith(ADLS_SAS_EXPIRY_PREFIX)) {
                String host = key.substring(ADLS_SAS_EXPIRY_PREFIX.length());
                if (host.isBlank() || entry.getValue().isBlank()) {
                    throw new AzureSasCredentialException("Azure ADLS SAS expiry is incomplete");
                }
                if (tokenHost != null && !tokenHost.equalsIgnoreCase(host)) {
                    throw new AzureSasCredentialException(
                            "Azure ADLS SAS token and expiry refer to different account hosts");
                }
                tokenHost = host;
                expiry = entry.getValue().trim();
            } else if (lowerKey.equals("adls.container") || lowerKey.equals("adls.container-name")
                    || lowerKey.equals("azure.container") || lowerKey.equals("azure.bucket")) {
                container = entry.getValue().trim();
            }
        }
        if (tokenHost == null) {
            return normalized;
        }
        if (token == null || token.isBlank()) {
            throw new AzureSasCredentialException(
                    "Azure ADLS SAS expiry was supplied without a SAS token for " + tokenHost);
        }
        validateSasExpiry(expiry, tokenHost);

        // Drop all Azure aliases from the raw FileIO map before adding the canonical binding. In
        // particular, an unrelated azure.auth_type=OAuth2 must not win alias precedence over the
        // SAS token we just validated. Generic fs./client./iceberg.rest.* properties remain intact
        // for their existing consumers.
        normalized.keySet().removeIf(key -> {
            String lowerKey = key.toLowerCase(Locale.ROOT);
            return lowerKey.startsWith(ADLS_PROPERTY_PREFIX) || lowerKey.startsWith("azure.")
                    || lowerKey.startsWith("azure_");
        });
        normalized.put("provider", "azure");
        normalized.put("AZURE_AUTH_TYPE", "SAS");
        normalized.put("AZURE_ACCOUNT_NAME", accountName(tokenHost));
        normalized.put("AZURE_ENDPOINT", blobEndpoint(tokenHost));
        normalized.put("AZURE_SAS_TOKEN", token);
        if (container != null && !container.isBlank()) {
            normalized.put("AZURE_CONTAINER", container);
        }
        if (expiry != null) {
            normalized.put("AZURE_SAS_EXPIRY_MS", expiry);
        }
        return normalized;
    }

    private static void validateSasExpiry(String expiry, String accountHost) {
        if (expiry == null) {
            return;
        }
        final long expiryMs;
        try {
            expiryMs = Long.parseLong(expiry);
        } catch (NumberFormatException e) {
            throw new AzureSasCredentialException(
                    "Invalid Azure ADLS SAS expiry for " + accountHost + ": " + expiry, e);
        }
        if (expiryMs <= 0) {
            throw new AzureSasCredentialException(
                    "Azure ADLS SAS expiry must be a positive Unix timestamp for " + accountHost);
        }
        if (expiryMs <= System.currentTimeMillis()) {
            throw new AzureSasCredentialException(
                    "Azure ADLS SAS credential is expired for " + accountHost);
        }
    }

    private static String accountName(String accountHost) {
        int dot = accountHost.indexOf('.');
        return dot > 0 ? accountHost.substring(0, dot) : accountHost;
    }

    private static String blobEndpoint(String accountHost) {
        String endpoint = accountHost.trim();
        String lower = endpoint.toLowerCase(Locale.ROOT);
        int dfs = lower.indexOf(".dfs.");
        if (dfs >= 0) {
            endpoint = endpoint.substring(0, dfs) + ".blob" + endpoint.substring(dfs + 4);
        }
        return endpoint.startsWith("http://") || endpoint.startsWith("https://")
                ? endpoint : "https://" + endpoint;
    }

    private static String stripSasPrefix(String token) {
        String normalized = token.trim();
        while (normalized.startsWith("?") || normalized.startsWith("&")) {
            normalized = normalized.substring(1);
        }
        return normalized;
    }

    /**
     * Extract backend properties from StorageAdapter map
     * Reference: CatalogProperty.getBackendStorageProperties()
     *
     * @param storagePropertiesMap Map of storage adapters
     * @return Backend properties with null values filtered out
     */
    public static Map<String, String> getBackendPropertiesFromStorageMap(
            Map<StorageTypeId, StorageAdapter> storagePropertiesMap) {
        Map<String, String> result = new HashMap<>();
        for (StorageAdapter sp : storagePropertiesMap.values()) {
            Map<String, String> backendProps = sp.getBackendConfigProperties();
            // the backend property's value can not be null, because it will be serialized to thrift,
            // which does not support null value.
            backendProps.entrySet().stream().filter(e -> e.getValue() != null)
                    .forEach(e -> result.put(e.getKey(), e.getValue()));
        }
        return result;
    }
}
