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
import org.apache.doris.filesystem.spi.AzureBlobEndpointSignals;
import org.apache.doris.filesystem.spi.FileSystemProvider;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * SPI provider for Azure Blob Storage.
 *
 * <p>Registered via META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider.
 *
 * <p>Identified by the presence of Azure provider-owned account/SAS properties, or an endpoint
 * whose host matches a known Azure Blob/DFS suffix from one of the sovereign clouds.
 */
public class AzureFileSystemProvider implements FileSystemProvider<AzureFileSystemProperties> {

    private static final String STORAGE_TYPE_KEY = "_STORAGE_TYPE_";
    private static final String STORAGE_TYPE_AZURE = "AZURE";
    private static final String PROVIDER_KEY = "provider";
    private static final String[] ACCOUNT_NAME_KEYS = {
            AzureFileSystemProperties.ACCOUNT_NAME, "azure.access_key", "AZURE_ACCOUNT_NAME"};
    private static final String[] SAS_TOKEN_KEYS = {
            AzureFileSystemProperties.SAS_TOKEN, "azure.sas-token"};
    private static final String[] ENDPOINT_KEYS = {
            AzureFileSystemProperties.ENDPOINT, "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT",
            "AZURE_ENDPOINT"};

    @Override
    public boolean supports(Map<String, String> properties) {
        if (isExplicitAzure(properties)) {
            return true;
        }
        if (firstPresent(properties, ACCOUNT_NAME_KEYS) != null) {
            return true;
        }
        if (firstPresent(properties, SAS_TOKEN_KEYS) != null) {
            return true;
        }
        String endpoint = firstPresent(properties, ENDPOINT_KEYS);
        return endpoint != null && AzureBlobEndpointSignals.isAzureBlobEndpoint(endpoint, properties);
    }

    @Override
    public AzureFileSystemProperties bind(Map<String, String> properties) {
        return AzureFileSystemProperties.of(properties);
    }

    @Override
    public Optional<AzureFileSystemProperties> bindVended(
            Map<String, String> credentials, Map<String, String> catalogProperties) {
        Optional<AzureVendedSas> sas = AzureVendedSas.parse(credentials);
        // A vended credential may be bound after the catalog has already selected this provider.
        // Also retain catalog defaults recognized only by the raw compatibility hook, such as a
        // historical AZURE_ENDPOINT standard host, without widening the routing guess contract.
        Map<String, String> connectionDefaults = supportsExplicit(catalogProperties)
                || supportsGuess(catalogProperties) || supports(catalogProperties) ? catalogProperties : Map.of();
        if (sas.isPresent()) {
            return Optional.of(AzureFileSystemProperties.withVendedSas(sas.get(), credentials, connectionDefaults));
        }
        return AzureFileIOSharedKey.parse(credentials)
                .map(sharedKey -> AzureFileSystemProperties.withFileIOSharedKey(sharedKey, connectionDefaults));
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
     * Referenced by fe-core; the value is owned by the shared predicate.
     */
    public static final String HOST_SUFFIXES_PROBE_KEY = AzureBlobEndpointSignals.HOST_SUFFIXES_PROBE_KEY;

    @Override
    public boolean supportsGuess(Map<String, String> properties) {
        // Verbatim port of fe-core AzureProperties.guessIsMe: provider=azure, or an endpoint
        // alias whose HOST carries a recognised Azure Blob/DFS suffix. The suffix predicate
        // (endpoint alias list, host extraction, dot-anchored endsWith, probe-injected live
        // suffix list) is shared with the S3-compatible fallback providers via
        // AzureBlobEndpointSignals so their mutual exclusion can never drift from this claim.
        if ("azure".equalsIgnoreCase(properties.get(PROVIDER_KEY))
                || firstPresent(properties, ACCOUNT_NAME_KEYS) != null
                || firstPresent(properties, SAS_TOKEN_KEYS) != null) {
            return true;
        }
        return AzureBlobEndpointSignals.guessIsAzureBlobEndpoint(properties);
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
        Set<String> keys = ConnectorPropertiesUtils.getSensitiveKeys(AzureFileSystemProperties.class);
        // Wire secrets must stay masked independently of which spellings the input binder accepts.
        keys.addAll(Set.of(AzureFileSystemProperties.BACKEND_ACCOUNT_KEY,
                AzureFileSystemProperties.BACKEND_CLIENT_SECRET, AzureFileSystemProperties.BACKEND_SAS_TOKEN,
                "s3.secret_key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY"));
        return keys;
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
