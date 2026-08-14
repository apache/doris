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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for Credential operations
 */
public class CredentialUtils {

    private static final String ADLS_SAS_TOKEN_PREFIX = "adls.sas-token.";
    private static final String ADLS_PROPERTY_PREFIX = "adls.";
    private static final String HADOOP_AZURE_ACCOUNT_AUTH_TYPE_PREFIX = "fs.azure.account.auth.type.";
    private static final String HADOOP_AZURE_FIXED_SAS_TOKEN_PREFIX = "fs.azure.sas.fixed.token.";

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
            "adls.",         // Iceberg Azure ADLS vended credentials
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
                .filter(entry -> CLOUD_STORAGE_PREFIXES.stream().anyMatch(prefix -> entry.getKey().startsWith(prefix)))
                .forEach(entry -> filtered.put(entry.getKey(), entry.getValue()));

        return filtered;
    }

    /**
     * Convert cloud storage credentials to the properties consumed by Doris storage adapters.
     * Databricks Unity Catalog returns Azure SAS credentials using Iceberg's
     * {@code adls.sas-token.<account-host>} property. Hadoop ABFS instead consumes an account-scoped
     * authentication type and fixed SAS token, so translate that representation before selecting the
     * storage adapter. Remove the raw Iceberg {@code adls.*} property names after translating them to
     * the equivalent backend configuration.
     *
     * @param rawVendedCredentials Raw vended credentials map
     * @return Normalized cloud storage properties
     */
    public static Map<String, String> normalizeCloudStorageProperties(
            Map<String, String> rawVendedCredentials) {
        Map<String, String> normalized = filterCloudStorageProperties(rawVendedCredentials);
        Map<String, String> adlsSasTokens = new HashMap<>();
        normalized.forEach((key, value) -> {
            if (key.startsWith(ADLS_SAS_TOKEN_PREFIX)) {
                String accountHost = key.substring(ADLS_SAS_TOKEN_PREFIX.length());
                adlsSasTokens.put(accountHost, value);
            }
        });
        normalized.keySet().removeIf(key -> key.startsWith(ADLS_PROPERTY_PREFIX));
        adlsSasTokens.forEach((accountHost, sasToken) -> {
            normalized.put(HADOOP_AZURE_ACCOUNT_AUTH_TYPE_PREFIX + accountHost, "SAS");
            normalized.put(HADOOP_AZURE_FIXED_SAS_TOKEN_PREFIX + accountHost, sasToken);
        });
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
