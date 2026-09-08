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

package org.apache.doris.iceberg;

import com.google.common.base.Preconditions;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Rebinds serialized Iceberg metadata tasks to the storage credentials selected by Doris.
 *
 * <p>REST vended credentials are carried by the Doris scan range, while the serialized
 * {@code $all_manifests} task contains the FileIO that was used to plan it. A REST catalog can
 * therefore leave a {@code HadoopFileIO} in the serialized task even though the scan range has a
 * valid Azure SAS credential. Replacing only the task's FileIO keeps the Iceberg task shape and
 * residual unchanged, and lets the Iceberg Azure SDK read the manifest list directly.</p>
 */
final class IcebergSysTableFileIOResolver {
    private static final String ALL_MANIFESTS_TASK =
            "org.apache.iceberg.AllManifestsTable$ManifestListReadTask";
    private static final String AZURE_PROVIDER = "azure";
    private static final String AZURE_AUTH_TYPE = "AZURE_AUTH_TYPE";
    private static final String AZURE_ENDPOINT = "AZURE_ENDPOINT";
    private static final String AZURE_ACCOUNT_NAME = "AZURE_ACCOUNT_NAME";
    private static final String AZURE_ACCOUNT_KEY = "AZURE_ACCOUNT_KEY";
    private static final String AZURE_SAS_TOKEN = "AZURE_SAS_TOKEN";
    private static final String AZURE_SAS_EXPIRY_MS = "AZURE_SAS_EXPIRY_MS";
    private static final String TASK_FILE_IO_FIELD = "io";
    private static final String ADLS_SAS_TOKEN_PREFIX = "adls.sas-token.";
    private static final String ADLS_SAS_EXPIRY_PREFIX = "adls.sas-token-expires-at-ms.";
    private static final String ADLS_SHARED_KEY_ACCOUNT_NAME = "adls.auth.shared-key.account.name";
    private static final String ADLS_SHARED_KEY_ACCOUNT_KEY = "adls.auth.shared-key.account.key";

    private IcebergSysTableFileIOResolver() {
    }

    static FileScanTask resolve(FileScanTask task, Map<String, String> storageProperties) {
        Map<String, String> fileIOProperties = azureFileIOProperties(task, storageProperties);
        if (fileIOProperties.isEmpty()) {
            return task;
        }

        ADLSFileIO fileIO = new ADLSFileIO();
        fileIO.initialize(fileIOProperties);
        try {
            // Iceberg's parser cannot be used here: CatalogUtil.loadFileIO() loads implementations
            // with CatalogUtil's parent classloader, which cannot see classes bundled only in this
            // JNI extension. ManifestListReadTask has no public FileIO replacement API, so update
            // its private final field before the task is published to the scanner thread.
            Field ioField = task.getClass().getDeclaredField(TASK_FILE_IO_FIELD);
            ioField.setAccessible(true);
            ioField.set(task, fileIO);
            return task;
        } catch (ReflectiveOperationException e) {
            fileIO.close();
            throw new IllegalStateException("Failed to rebind Iceberg all-manifests task FileIO", e);
        }
    }

    static Map<String, String> azureFileIOProperties(FileScanTask task, Map<String, String> storageProperties) {
        if (task == null || !ALL_MANIFESTS_TASK.equals(task.getClass().getName()) || storageProperties == null
                || !AZURE_PROVIDER.equalsIgnoreCase(storageProperties.get("provider"))) {
            return Map.of();
        }

        String authType = storageProperties.get(AZURE_AUTH_TYPE);
        String accountName = storageProperties.get(AZURE_ACCOUNT_NAME);
        if (isSas(authType, storageProperties)) {
            String accountHost = accountHost(storageProperties.get(AZURE_ENDPOINT), accountName);
            String token = storageProperties.get(AZURE_SAS_TOKEN);
            Preconditions.checkArgument(token != null && !token.isBlank(),
                    "Azure SAS metadata task requires AZURE_SAS_TOKEN");
            Map<String, String> properties = new HashMap<>();
            properties.put(ADLS_SAS_TOKEN_PREFIX + accountHost, token);
            String expiry = storageProperties.get(AZURE_SAS_EXPIRY_MS);
            if (expiry != null && !expiry.isBlank()) {
                validateExpiry(expiry);
                properties.put(ADLS_SAS_EXPIRY_PREFIX + accountHost, expiry);
            }
            return properties;
        }
        if ("SHARED_KEY".equalsIgnoreCase(authType) || "SharedKey".equalsIgnoreCase(authType)) {
            String key = storageProperties.get(AZURE_ACCOUNT_KEY);
            Preconditions.checkArgument(accountName != null && !accountName.isBlank() && key != null
                            && !key.isBlank(),
                    "Azure SharedKey metadata task requires AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY");
            return Map.of(ADLS_SHARED_KEY_ACCOUNT_NAME, accountName, ADLS_SHARED_KEY_ACCOUNT_KEY, key);
        }
        // OAuth2 is intentionally not rebound: the native metadata path has no complete, safe
        // credential construction yet, and treating service-principal material as SAS/SharedKey
        // would be an authentication downgrade.
        return Map.of();
    }

    private static boolean isSas(String authType, Map<String, String> storageProperties) {
        return "SAS".equalsIgnoreCase(authType)
                || (authType == null && storageProperties.containsKey(AZURE_SAS_TOKEN));
    }

    private static String accountHost(String endpoint, String accountName) {
        if (endpoint != null && !endpoint.isBlank()) {
            URI uri = endpoint.contains("://") ? URI.create(endpoint) : URI.create("https://" + endpoint);
            String host = uri.getHost();
            Preconditions.checkArgument(host != null && !host.isBlank(),
                    "Azure metadata task has an invalid AZURE_ENDPOINT");
            String lowerHost = host.toLowerCase(Locale.ROOT);
            int blobMarker = lowerHost.indexOf(".blob.");
            if (blobMarker >= 0) {
                return host.substring(0, blobMarker) + ".dfs" + host.substring(blobMarker + 5);
            }
            return host;
        }
        Preconditions.checkArgument(accountName != null && !accountName.isBlank(),
                "Azure metadata task requires AZURE_ENDPOINT or AZURE_ACCOUNT_NAME");
        return accountName + ".dfs.core.windows.net";
    }

    private static void validateExpiry(String expiry) {
        final long expiryMs;
        try {
            expiryMs = Long.parseLong(expiry.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid Azure SAS expiry value: " + expiry, e);
        }
        Preconditions.checkArgument(expiryMs > System.currentTimeMillis(),
                "Azure SAS credential is expired");
    }
}
