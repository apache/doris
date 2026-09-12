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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.filesystem.FileSystemType;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.properties.FileSystemProperties;
import org.apache.doris.filesystem.properties.StorageProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

/** Provider-owned FileIO properties enter the official REST table/FileIO construction path here. */
final class IcebergRestFileIOProperties {
    private final ConnectorStorageContext storage;
    private final Map<String, String> clientProperties;
    // Only loadTable creates a new FileIO after a GET. Refresh and lazy snapshot GETs retain
    // the existing one, so they must not resolve or validate a replacement credential.
    private final ThreadLocal<TableLoadContext> tableLoad = new ThreadLocal<>();
    // Set during catalog initialization, before the catalog is published. No table credentials
    // are retained here: each response chooses its own complete credential group.
    private ConfigResponse serverConfig = ConfigResponse.builder().build();

    private static final class TableLoadContext {
        private Map<String, String> hadoopProperties = Collections.emptyMap();
    }

    IcebergRestFileIOProperties(ConnectorStorageContext storage, Map<String, String> clientProperties) {
        this.storage = storage;
        this.clientProperties = Collections.unmodifiableMap(new HashMap<>(clientProperties));
    }

    Map<String, String> catalogProperties() {
        // RESTUtil.merge cannot remove an inherited key. Keep authentication out of that
        // per-key merge and restore only the selected complete group in the table response.
        return withoutAzureAuthentication(clientProperties);
    }

    <T> T withTableLoad(Supplier<T> operation) {
        TableLoadContext previous = tableLoad.get();
        tableLoad.set(new TableLoadContext());
        try {
            return operation.get();
        } finally {
            if (previous == null) {
                tableLoad.remove();
            } else {
                tableLoad.set(previous);
            }
        }
    }

    void configureTableFileIO(FileIO fileIO) {
        Map<String, String> hadoopProperties = tableLoad.get().hadoopProperties;
        if (!hadoopProperties.isEmpty()) {
            // Only the official HadoopFileIO is selected here: initialize stores properties
            // without opening files. REST creates a table-owned IO for this nonempty config.
            // Copy before adding the static view, never mutate another table's Configuration.
            HadoopFileIO hadoopFileIO = (HadoopFileIO) fileIO;
            Configuration conf = new Configuration(hadoopFileIO.getConf());
            IcebergCatalogFactory.applyHadoopProperties(conf, clientProperties, hadoopProperties);
            hadoopFileIO.setConf(conf);
        }
    }

    RESTResponse adaptGet(RESTResponse response) {
        if (response instanceof LoadTableResponse && tableLoad.get() == null) {
            return response;
        }
        return adapt(response);
    }

    RESTResponse adapt(RESTResponse response) {
        if (response instanceof ConfigResponse) {
            serverConfig = (ConfigResponse) response;
            return ConfigResponse.builder().withDefaults(withoutAzureAuthentication(serverConfig.defaults()))
                    .withOverrides(withoutAzureAuthentication(serverConfig.overrides()))
                    .withEndpoints(serverConfig.endpoints()).build();
        }
        if (!(response instanceof LoadTableResponse)) {
            return response;
        }
        LoadTableResponse table = (LoadTableResponse) response;
        TableLoadContext load = tableLoad.get();
        if (load != null) {
            load.hadoopProperties = Collections.emptyMap();
        }
        Map<String, String> authentication = selectAuthentication(table);
        Map<String, String> providerAuthentication = providerAuthenticationProperties(authentication);
        Map<String, String> fileIOProperties = new HashMap<>(authentication);
        Map<String, String> configured = withoutAzureAuthentication(serverConfig.merge(clientProperties));
        configured.putAll(withoutAzureAuthentication(table.config()));
        String metadataLocation = metadataLocation(table);
        for (StorageProperties binding : storage.resolveStorageProperties(providerAuthentication)) {
            if (binding.type() == FileSystemType.AZURE) {
                FileSystemProperties filesystem = (FileSystemProperties) binding;
                if (!filesystem.getSupportedSchemes().contains(
                        Location.of(metadataLocation).scheme().toLowerCase(Locale.ROOT))
                        && !filesystem.claimsUri(metadataLocation)) {
                    // Do not access an unused credential or force its FileIO requirement onto
                    // metadata in another store. Raw SDK properties remain available to FileIO.
                    continue;
                }
                filesystem.validateAndNormalizeUri(metadataLocation);
                if (HadoopFileIO.class.getName().equals(configured.get(CatalogProperties.FILE_IO_IMPL))) {
                    if (!authentication.isEmpty()) {
                        throw new DorisConnectorException("HadoopFileIO cannot consume vended Azure"
                                + " FileIO authentication; its static Hadoop configuration"
                                + " must not silently reuse the previous identity");
                    }
                    if (load != null) {
                        load.hadoopProperties = binding.toHadoopProperties().orElseThrow(() ->
                                new DorisConnectorException("Azure storage has no static Hadoop configuration"))
                                .toHadoopConfigurationMap();
                    }
                }
                fileIOProperties.keySet().removeIf(IcebergRestFileIOProperties::isProviderAuthenticationProperty);
            }
            if (binding.type() == FileSystemType.AZURE && !authentication.isEmpty()
                    && providerAuthentication.isEmpty()) {
                // adls.token belongs to the Java FileIO only. Do not read or validate the old
                // native credential merely to retain the provider's connection parameters.
                fileIOProperties.putAll(binding.toIcebergFileIOConnectionProperties());
                continue;
            }
            Map<String, String> providerProperties = binding.toIcebergFileIOProperties();
            validateFileIOChoice(configured, providerProperties);
            fileIOProperties.putAll(providerProperties);
        }
        if (fileIOProperties.isEmpty()) {
            return response;
        }
        // Native provider defaults do not override explicit SDK connection/FileIO settings.
        fileIOProperties.putAll(configured);
        // Preserve the complete metadata, its file location and every scoped credential. The
        // SDK still owns FileIO selection, SupportsStorageCredentials injection and lifecycle.
        return LoadTableResponse.builder().withTableMetadata(table.tableMetadata())
                .addAllConfig(fileIOProperties).addAllCredentials(table.credentials()).build();
    }

    private static void validateFileIOChoice(Map<String, String> configured, Map<String, String> provider) {
        String selected = configured.get(CatalogProperties.FILE_IO_IMPL);
        if (HadoopFileIO.class.getName().equals(provider.get(CatalogProperties.FILE_IO_IMPL))
                && (ADLSFileIO.class.getName().equals(selected) || ResolvingFileIO.class.getName().equals(selected))) {
            throw new DorisConnectorException("Azure client-secret metadata authentication requires HadoopFileIO; "
                    + "the explicitly configured io-impl cannot consume that identity");
        }
    }

    private static String metadataLocation(LoadTableResponse table) {
        if (table.metadataLocation() != null) {
            return table.metadataLocation();
        }
        // Stage-create has no metadata file yet. Match RESTTableOperations' metadata directory
        // selection instead of assuming that the data root also holds the metadata files.
        return table.tableMetadata().properties().getOrDefault(
                TableProperties.WRITE_METADATA_LOCATION, table.tableMetadata().location() + "/metadata");
    }

    private Map<String, String> selectAuthentication(LoadTableResponse table) {
        Map<String, String> scopedAuthentication = scopedSasAuthentication(table);
        if (!scopedAuthentication.isEmpty()) {
            return scopedAuthentication;
        }
        // REST precedence applies to a credential generation, not to individual aliases or
        // expiry fields. A new token with unknown expiry must not inherit an old expiry.
        for (Map<String, String> source : List.of(table.config(), serverConfig.overrides(), clientProperties)) {
            Map<String, String> authentication = authenticationProperties(source);
            if (!authentication.isEmpty()) {
                return authentication;
            }
        }
        // Native catalog credentials are also client options. Defaults cannot override them
        // merely because the provider has not emitted its Iceberg dialect yet. Do not emit
        // that static view before higher-priority vended credentials have been considered.
        if (storage.getStorageProperties().stream().anyMatch(binding -> binding.type() == FileSystemType.AZURE)) {
            return Collections.emptyMap();
        }
        return authenticationProperties(serverConfig.defaults());
    }

    private static Map<String, String> scopedSasAuthentication(LoadTableResponse table) {
        Location metadata = Location.of(metadataLocation(table));
        Credential selected = null;
        Map<String, String> authentication = Collections.emptyMap();
        for (Credential credential : table.credentials()) {
            if (!credential.config().keySet().stream().anyMatch(IcebergRestFileIOProperties::isAzureSasProperty)
                    || !metadata.startsWith(Location.of(credential.prefix()))) {
                continue;
            }
            Map<String, String> candidate = authenticationProperties(credential.config());
            if (selected != null && (!selected.prefix().equals(credential.prefix())
                    || !authentication.equals(candidate))) {
                // ADLSFileIO accepts one SAS per account host, not a prefix-aware credential
                // list. Do not flatten conflicting scopes into a table-wide last-token-wins map.
                throw new DorisConnectorException("Multiple scoped Azure metadata credentials are not supported");
            }
            selected = credential;
            authentication = candidate;
        }
        // ResolvingFileIO preserves the original list, but ADLSFileIO does not implement
        // SupportsStorageCredentials. Put only this metadata location's selected group through
        // the provider bridge before the SDK constructs FileIO. A data-only scope stays in the
        // original list and cannot replace metadata authentication. A scoped table credential
        // overrides unscoped defaults as a complete group, including an absent/unknown expiry.
        return authentication;
    }

    private static Map<String, String> authenticationProperties(Map<String, String> source) {
        Map<String, String> authentication = new HashMap<>();
        source.forEach((key, value) -> {
            if (isAzureAuthenticationProperty(key)) {
                authentication.put(key, value);
            }
        });
        return authentication;
    }

    private static Map<String, String> providerAuthenticationProperties(Map<String, String> source) {
        Map<String, String> authentication = new HashMap<>();
        source.forEach((key, value) -> {
            if (isProviderAuthenticationProperty(key)) {
                authentication.put(key, value);
            }
        });
        return authentication;
    }

    private static Map<String, String> withoutAzureAuthentication(Map<String, String> properties) {
        Map<String, String> result = new HashMap<>(properties);
        result.keySet().removeIf(IcebergRestFileIOProperties::isAzureAuthenticationProperty);
        return result;
    }

    // Shared with native credential extraction: preserve one Iceberg authentication-group vocabulary.
    static boolean isAzureAuthenticationProperty(String key) {
        return isProviderAuthenticationProperty(key) || AzureProperties.ADLS_TOKEN.equalsIgnoreCase(key);
    }

    private static boolean isProviderAuthenticationProperty(String key) {
        return isAzureSasProperty(key)
                || AzureProperties.ADLS_SHARED_KEY_ACCOUNT_NAME.equalsIgnoreCase(key)
                || AzureProperties.ADLS_SHARED_KEY_ACCOUNT_KEY.equalsIgnoreCase(key);
    }

    private static boolean isAzureSasProperty(String key) {
        return key.regionMatches(true, 0, AzureProperties.ADLS_SAS_TOKEN_PREFIX, 0,
                AzureProperties.ADLS_SAS_TOKEN_PREFIX.length())
                || key.regionMatches(true, 0, AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX, 0,
                        AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX.length());
    }
}
