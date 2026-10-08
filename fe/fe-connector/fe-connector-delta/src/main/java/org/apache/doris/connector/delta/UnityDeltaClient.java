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

package org.apache.doris.connector.delta;

import org.apache.doris.connector.spi.DorisConnectorException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.delta.kernel.Snapshot;
import io.delta.kernel.commit.Committer;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.transaction.CreateTableTransactionBuilder;
import io.delta.kernel.types.StructType;
import io.delta.kernel.unitycatalog.UCCatalogManagedClient;
import io.delta.kernel.unitycatalog.UCCatalogManagedCommitter;
import io.delta.kernel.unitycatalog.UCTableIdentifier;
import io.delta.storage.commit.CommitFailedException;
import io.delta.storage.commit.uccommitcoordinator.UCClient;
import io.delta.storage.commit.uccommitcoordinator.UCDeltaTokenBasedRestClient;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.delta.api.DeltaConfigurationApi;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.api.DeltaTemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.DeltaCatalogConfig;
import io.unitycatalog.client.delta.model.DeltaCreateStagingTableRequest;
import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.client.delta.model.DeltaLoadTableResponse;
import io.unitycatalog.client.delta.model.DeltaStagingTableResponse;
import io.unitycatalog.client.delta.model.DeltaStorageCredentialConfig;
import io.unitycatalog.client.delta.model.DeltaTableType;
import io.unitycatalog.client.model.ListSchemasResponse;
import io.unitycatalog.client.model.SchemaInfo;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** Thin wrapper around the official Unity Catalog Java client. */
final class UnityDeltaClient {
    private static final int PAGE_SIZE = 1000;
    // Databricks Tables API limits max_results to 50; schemas use the larger generic limit.
    private static final int TABLE_PAGE_SIZE = 50;
    private static final String DELTA_PROTOCOL_VERSION = "1.0";
    private static final String APP_NAME = "Apache-Doris";
    private static final String APP_VERSION = "native-delta";
    private static final Map<String, String> APP_VERSIONS =
            Map.of(APP_NAME, APP_VERSION);

    private final String workspaceUri;
    private final TokenProvider tokenProvider;
    private final ApiClient apiClient;
    private final SchemasApi schemasApi;
    private final DeltaConfigurationApi configurationApi;
    private final DeltaTablesApi deltaTablesApi;
    private final DeltaTemporaryCredentialsApi credentialsApi;
    private String negotiatedCatalog;

    static UnityDeltaClient create(String workspaceUri, String token) {
        return create(workspaceUri, Map.of("type", "static", "token", token));
    }

    static UnityDeltaClient create(Map<String, String> properties) {
        String workspaceUri = requireProperty(properties, DeltaConnectorProperties.UNITY_URI);
        long connectTimeoutMs = DeltaConnectorProperties.positiveLongProperty(properties,
                DeltaConnectorProperties.UNITY_CONNECT_TIMEOUT_MS,
                DeltaConnectorProperties.DEFAULT_UNITY_CONNECT_TIMEOUT_MS);
        long readTimeoutMs = DeltaConnectorProperties.positiveLongProperty(properties,
                DeltaConnectorProperties.UNITY_READ_TIMEOUT_MS,
                DeltaConnectorProperties.DEFAULT_UNITY_READ_TIMEOUT_MS);
        String authType = properties.getOrDefault(
                DeltaConnectorProperties.UNITY_AUTH_TYPE, "pat").trim().toLowerCase(
                        java.util.Locale.ROOT);
        Map<String, String> authProperties;
        if ("pat".equals(authType)) {
            authProperties = Map.of("type", "static",
                    "token", requireProperty(properties, DeltaConnectorProperties.UNITY_TOKEN));
        } else if ("oauth".equals(authType)) {
            authProperties = Map.of("type", "oauth",
                    "oauth.uri", requireProperty(properties, DeltaConnectorProperties.UNITY_OAUTH_URI),
                    "oauth.clientId", requireProperty(
                            properties, DeltaConnectorProperties.UNITY_OAUTH_CLIENT_ID),
                    "oauth.clientSecret", requireProperty(
                            properties, DeltaConnectorProperties.UNITY_OAUTH_CLIENT_SECRET));
        } else {
            throw new IllegalArgumentException(
                    "Unsupported Unity authentication type '" + authType + "'");
        }
        return create(workspaceUri, authProperties, connectTimeoutMs, readTimeoutMs);
    }

    private static UnityDeltaClient create(String workspaceUri,
            Map<String, String> authProperties) {
        return create(workspaceUri, authProperties,
                DeltaConnectorProperties.DEFAULT_UNITY_CONNECT_TIMEOUT_MS,
                DeltaConnectorProperties.DEFAULT_UNITY_READ_TIMEOUT_MS);
    }

    private static UnityDeltaClient create(String workspaceUri,
            Map<String, String> authProperties, long connectTimeoutMs, long readTimeoutMs) {
        String normalizedUri = stripTrailingSlash(workspaceUri);
        TokenProvider tokenProvider = TokenProvider.create(authProperties);
        ApiClient apiClient = ApiClientBuilder.create()
                .uri(normalizedUri)
                .tokenProvider(tokenProvider)
                .addAppVersion(APP_NAME, APP_VERSION)
                .build();
        apiClient.setConnectTimeout(Duration.ofMillis(connectTimeoutMs));
        apiClient.setReadTimeout(Duration.ofMillis(readTimeoutMs));
        return new UnityDeltaClient(normalizedUri, tokenProvider, apiClient);
    }

    private static String requireProperty(Map<String, String> properties, String key) {
        String value = properties.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required Unity property '" + key + "'");
        }
        return value;
    }

    UnityDeltaClient(String workspaceUri, TokenProvider tokenProvider, ApiClient apiClient) {
        this.workspaceUri = workspaceUri;
        this.tokenProvider = tokenProvider;
        this.apiClient = apiClient;
        registerCredentialConfigDeserializer(apiClient);
        this.schemasApi = new SchemasApi(apiClient);
        this.configurationApi = new DeltaConfigurationApi(apiClient);
        this.deltaTablesApi = new DeltaTablesApi(apiClient);
        this.credentialsApi = new DeltaTemporaryCredentialsApi(apiClient);
    }

    List<String> listSchemas(String catalogName) {
        List<String> schemaNames = new ArrayList<>();
        String pageToken = null;
        do {
            try {
                ListSchemasResponse response = schemasApi.listSchemas(
                        catalogName, PAGE_SIZE, pageToken);
                for (SchemaInfo schema : response.getSchemas()) {
                    schemaNames.add(schema.getName());
                }
                pageToken = response.getNextPageToken();
            } catch (ApiException e) {
                throw requestFailure("list schemas in catalog '" + catalogName + "'", e);
            }
        } while (pageToken != null && !pageToken.isEmpty());
        return schemaNames;
    }

    List<String> listDeltaTables(String catalogName, String schemaName) {
        List<String> tableNames = new ArrayList<>();
        String pageToken = null;
        do {
            try {
                JsonNode response = listTablesWithManifestCapabilities(
                        catalogName, schemaName, pageToken);
                JsonNode tables = response.get("tables");
                if (tables == null || !tables.isArray()) {
                    throw new ApiException("Unity Catalog list tables response has no tables array");
                }
                for (JsonNode table : tables) {
                    if (isReadableDeltaTable(table)) {
                        tableNames.add(table.get("name").asText());
                    }
                }
                JsonNode nextPageToken = response.get("next_page_token");
                pageToken = nextPageToken == null || nextPageToken.isNull()
                        ? null : nextPageToken.asText();
            } catch (ApiException e) {
                throw requestFailure("list tables in '" + catalogName + "." + schemaName + "'", e);
            } catch (IOException e) {
                throw new DorisConnectorException(
                        "Unity Catalog request failed while attempting to list tables in '"
                                + catalogName + "." + schemaName + "'", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DorisConnectorException(
                        "Unity Catalog request was interrupted while attempting to list tables in '"
                                + catalogName + "." + schemaName + "'", e);
            }
        } while (pageToken != null && !pageToken.isEmpty());
        return tableNames;
    }

    Optional<Set<String>> getDeltaTableCapabilities(
            String catalogName, String schemaName, String tableName) {
        String pageToken = null;
        do {
            try {
                JsonNode response = listTablesWithManifestCapabilities(
                        catalogName, schemaName, pageToken);
                JsonNode tables = response.get("tables");
                if (tables == null || !tables.isArray()) {
                    throw new ApiException("Unity Catalog list tables response has no tables array");
                }
                for (JsonNode table : tables) {
                    if (tableName.equals(textValue(table, "name"))
                            && isReadableDeltaTable(table)) {
                        return Optional.of(tableCapabilities(table));
                    }
                }
                JsonNode nextPageToken = response.get("next_page_token");
                pageToken = nextPageToken == null || nextPageToken.isNull()
                        ? null : nextPageToken.asText();
            } catch (ApiException e) {
                throw requestFailure("find Delta table '" + catalogName + "."
                        + schemaName + "." + tableName + "'", e);
            } catch (IOException e) {
                throw new DorisConnectorException(
                        "Unity Catalog request failed while finding Delta table '"
                                + catalogName + "." + schemaName + "." + tableName + "'", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DorisConnectorException(
                        "Unity Catalog request was interrupted while finding Delta table '"
                                + catalogName + "." + schemaName + "." + tableName + "'", e);
            }
        } while (pageToken != null && !pageToken.isEmpty());
        return Optional.empty();
    }

    private JsonNode listTablesWithManifestCapabilities(
            String catalogName, String schemaName, String pageToken)
            throws IOException, InterruptedException, ApiException {
        StringBuilder uri = new StringBuilder(apiClient.getBaseUri()).append("/tables?")
                .append("catalog_name=").append(ApiClient.urlEncode(catalogName))
                .append("&schema_name=").append(ApiClient.urlEncode(schemaName))
                .append("&max_results=").append(TABLE_PAGE_SIZE)
                .append("&include_manifest_capabilities=true");
        if (pageToken != null && !pageToken.isEmpty()) {
            uri.append("&page_token=").append(ApiClient.urlEncode(pageToken));
        }
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(uri.toString()))
                .header("Accept", "application/json");
        if (apiClient.getReadTimeout() != null) {
            request.timeout(apiClient.getReadTimeout());
        }
        Consumer<HttpRequest.Builder> interceptor = apiClient.getRequestInterceptor();
        if (interceptor != null) {
            interceptor.accept(request);
        }
        HttpResponse<String> response = apiClient.getHttpClient().send(
                request.GET().build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() / 100 != 2) {
            throw new ApiException(response.statusCode(), response.body());
        }
        return apiClient.getObjectMapper().readTree(response.body());
    }

    Optional<DeltaLoadTableResponse> loadTable(
            String catalogName, String schemaName, String tableName) {
        negotiateDeltaProtocol(catalogName);
        try {
            return Optional.of(deltaTablesApi.loadTable(catalogName, schemaName, tableName));
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                return Optional.empty();
            }
            throw requestFailure(
                    "load Delta table '" + catalogName + "." + schemaName + "." + tableName + "'", e);
        }
    }

    void deleteTable(String catalogName, String schemaName, String tableName) {
        negotiateDeltaProtocol(catalogName);
        try {
            deltaTablesApi.deleteTable(catalogName, schemaName, tableName);
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                return;
            }
            throw requestFailure(
                    "delete Delta table '" + catalogName + "." + schemaName + "."
                            + tableName + "'", e);
        }
    }

    Configuration buildReadHadoopConfiguration(String catalogName, String schemaName,
            String tableName, String location, Configuration baseConfiguration) {
        return buildHadoopConfiguration(catalogName, schemaName, tableName, location,
                baseConfiguration, UCCredentialHadoopConfs.TableOperation.READ);
    }

    Configuration buildWriteHadoopConfiguration(String catalogName, String schemaName,
            String tableName, String location, Configuration baseConfiguration) {
        return buildHadoopConfiguration(catalogName, schemaName, tableName, location,
                baseConfiguration, UCCredentialHadoopConfs.TableOperation.READ_WRITE);
    }

    private Configuration buildHadoopConfiguration(String catalogName, String schemaName,
            String tableName, String location, Configuration baseConfiguration,
            UCCredentialHadoopConfs.TableOperation operation) {
        negotiateDeltaProtocol(catalogName);
        Configuration configuration = new Configuration(baseConfiguration);
        String scheme = storageScheme(location);
        if ("file".equals(scheme)) {
            return configuration;
        }
        try {
            Map<String, String> credentialProperties = UCCredentialHadoopConfs
                    .builder(workspaceUri, scheme)
                    .tokenProvider(tokenProvider)
                    .apiClient(apiClient)
                    .enableCredentialRenewal(true)
                    .enableCredentialScopedFs(true)
                    .hadoopConf(configuration)
                    .addAppVersions(APP_VERSIONS)
                    .buildForTable(catalogName, schemaName, tableName,
                            operation, location);
            credentialProperties.forEach(configuration::set);
            return configuration;
        } catch (ApiException e) {
            throw requestFailure(
                    "vend read credentials for Delta table '" + catalogName + "."
                            + schemaName + "." + tableName + "'", e);
        }
    }

    DeltaCredentialsResponse getReadCredentials(
            String catalogName, String schemaName, String tableName) {
        return getCredentials(catalogName, schemaName, tableName, DeltaCredentialOperation.READ);
    }

    DeltaCredentialsResponse getWriteCredentials(
            String catalogName, String schemaName, String tableName) {
        return getCredentials(catalogName, schemaName, tableName,
                DeltaCredentialOperation.READ_WRITE);
    }

    DeltaStagingTableResponse createStagingTable(
            String catalogName, String schemaName, String tableName) {
        negotiateDeltaProtocol(catalogName);
        try {
            return deltaTablesApi.createStagingTable(catalogName, schemaName,
                    new DeltaCreateStagingTableRequest().name(tableName));
        } catch (ApiException e) {
            throw requestFailure("create Unity Delta staging table '" + catalogName + "."
                    + schemaName + "." + tableName + "'", e);
        }
    }

    Configuration buildStagingHadoopConfiguration(String location, String stagingTableId,
            Configuration baseConfiguration) {
        Configuration configuration = new Configuration(baseConfiguration);
        String scheme = storageScheme(location);
        if ("file".equals(scheme)) {
            return configuration;
        }
        try {
            Map<String, String> credentialProperties = UCCredentialHadoopConfs
                    .builder(workspaceUri, scheme)
                    .tokenProvider(tokenProvider)
                    .apiClient(apiClient)
                    .enableCredentialRenewal(true)
                    .enableCredentialScopedFs(true)
                    .hadoopConf(configuration)
                    .addAppVersions(APP_VERSIONS)
                    .buildForStagingTable(stagingTableId, location);
            credentialProperties.forEach(configuration::set);
            return configuration;
        } catch (ApiException e) {
            throw requestFailure("vend staging credentials for Unity Delta table at '"
                    + location + "'", e);
        }
    }

    void createCatalogManagedTable(DeltaStagingTableResponse staging,
            String catalogName, String schemaName, String tableName,
            StructType schema, Map<String, String> requestedProperties,
            List<String> partitionColumns,
            Configuration baseConfiguration) {
        if (staging == null || staging.getTableId() == null
                || staging.getLocation() == null || staging.getLocation().isBlank()) {
            throw new DorisConnectorException(
                    "Unity Delta staging response is missing table ID or location");
        }
        if (staging.getTableType() != null && staging.getTableType() != DeltaTableType.MANAGED) {
            throw new DorisConnectorException(
                    "Unity Delta staging endpoint returned a non-managed table");
        }
        validateStagingProtocol(staging);
        Map<String, String> properties = new java.util.LinkedHashMap<>(requestedProperties);
        if (staging.getRequiredProperties() != null) {
            for (Map.Entry<String, String> entry : staging.getRequiredProperties().entrySet()) {
                if (entry.getValue() != null && properties.containsKey(entry.getKey())
                        && !entry.getValue().equals(properties.get(entry.getKey()))) {
                    throw new DorisConnectorException(
                            "Unity Delta required table property conflicts with CREATE request: "
                                    + entry.getKey());
                }
                if (entry.getValue() != null) {
                    properties.put(entry.getKey(), entry.getValue());
                }
            }
        }
        Configuration configuration = buildStagingHadoopConfiguration(
                staging.getLocation(), staging.getTableId().toString(), baseConfiguration);
        Engine engine = DefaultEngine.create(configuration);
        try (UCDeltaTokenBasedRestClient catalogClient = new NormalizingUCDeltaTokenBasedRestClient(
                workspaceUri, tokenProvider, APP_VERSIONS)) {
            UCTableIdentifier identifier = new UCTableIdentifier(
                    catalogName, schemaName, tableName);
            CreateTableTransactionBuilder builder = new NamedUCCatalogManagedClient(
                    catalogClient, identifier).buildCreateTableTransaction(
                            staging.getTableId().toString(), staging.getLocation(), schema,
                            APP_NAME + " native-delta", identifier)
                    .withTableProperties(properties);
            builder.withDataLayoutSpec(DeltaKernelWriter.dataLayoutSpec(schema, partitionColumns));
            new DeltaKernelWriter(engine).commitCreateTable(builder, staging.getLocation());
        } catch (IOException e) {
            throw new DorisConnectorException(
                    "Failed to close Unity Delta create client for '" + catalogName + "."
                            + schemaName + "." + tableName + "'", e);
        }
    }

    /**
     * Unity client 0.5.0 expects ColumnDef.typeJson to contain a complete struct field, while
     * Delta Kernel emits the valid type-only JSON form. Normalize only at this SDK boundary.
     */
    private static final class NormalizingUCDeltaTokenBasedRestClient
            extends UCDeltaTokenBasedRestClient {
        private NormalizingUCDeltaTokenBasedRestClient(String baseUri,
                TokenProvider tokenProvider, Map<String, String> appVersions) {
            super(baseUri, tokenProvider, appVersions);
        }

        @Override
        public void finalizeCreate(String tableName, String catalogName, String schemaName,
                String storageLocation, List<UCClient.ColumnDef> columns,
                Map<String, String> properties) throws CommitFailedException {
            List<UCClient.ColumnDef> normalized = columns.stream()
                    .map(NormalizingUCDeltaTokenBasedRestClient::normalizeColumn)
                    .collect(Collectors.toList());
            super.finalizeCreate(tableName, catalogName, schemaName, storageLocation,
                    normalized, properties);
        }

        private static UCClient.ColumnDef normalizeColumn(UCClient.ColumnDef column) {
            String typeJson = column.getTypeJson();
            if (typeJson == null || typeJson.isBlank()
                    || (typeJson.trim().startsWith("{")
                    && typeJson.contains("\"name\""))) {
                return column;
            }
            String escapedName = column.getName().replace("\\", "\\\\")
                    .replace("\"", "\\\"");
            String fieldJson = "{\"name\":\"" + escapedName + "\",\"type\":"
                    + typeJson + ",\"nullable\":" + column.isNullable()
                    + ",\"metadata\":{}}";
            return new UCClient.ColumnDef(column.getName(), column.getTypeName(),
                    column.getTypeText(), fieldJson, column.isNullable(), column.getPosition());
        }
    }

    private static void validateStagingProtocol(DeltaStagingTableResponse staging) {
        if (staging.getRequiredProtocol() == null) {
            throw new DorisConnectorException(
                    "Unity Delta staging response has no required protocol");
        }
        Integer reader = staging.getRequiredProtocol().getMinReaderVersion();
        Integer writer = staging.getRequiredProtocol().getMinWriterVersion();
        if (reader == null || writer == null || reader > 3 || writer > 7) {
            throw new DorisConnectorException(
                    "Unity Delta staging protocol exceeds Doris native create support");
        }
        Set<String> unsupportedWriterFeatures = new HashSet<>();
        if (staging.getRequiredProtocol().getWriterFeatures() != null) {
            unsupportedWriterFeatures.addAll(staging.getRequiredProtocol().getWriterFeatures());
        }
        unsupportedWriterFeatures.remove("catalogManaged");
        unsupportedWriterFeatures.remove("vacuumProtocolCheck");
        if (!unsupportedWriterFeatures.isEmpty()) {
            throw new DorisConnectorException(
                    "Unity Delta staging requires unsupported writer features: "
                            + unsupportedWriterFeatures);
        }
    }

    private DeltaCredentialsResponse getCredentials(
            String catalogName, String schemaName, String tableName,
            DeltaCredentialOperation operation) {
        negotiateDeltaProtocol(catalogName);
        try {
            return credentialsApi.getTableCredentials(
                    operation, catalogName, schemaName, tableName);
        } catch (ApiException e) {
            throw requestFailure(
                    "vend backend " + operation.getValue() + " credentials for Delta table '"
                            + catalogName + "."
                            + schemaName + "." + tableName + "'", e);
        }
    }

    synchronized void negotiateDeltaProtocol(String catalogName) {
        if (catalogName.equals(negotiatedCatalog)) {
            return;
        }
        DeltaCatalogConfig config;
        try {
            config = configurationApi.getConfig(catalogName, DELTA_PROTOCOL_VERSION);
        } catch (ApiException e) {
            throw requestFailure(
                    "negotiate Unity Delta API protocol for catalog '" + catalogName + "'", e);
        }
        if (config == null || !DELTA_PROTOCOL_VERSION.equals(config.getProtocolVersion())) {
            throw new DorisConnectorException(
                    "Unity Catalog negotiated unsupported Delta API protocol '"
                            + (config == null ? null : config.getProtocolVersion())
                            + "'; Doris requires " + DELTA_PROTOCOL_VERSION);
        }
        if (config.getEndpoints() == null || config.getEndpoints().isEmpty()) {
            throw new DorisConnectorException(
                    "Unity Catalog Delta API configuration contains no endpoints");
        }
        negotiatedCatalog = catalogName;
    }

    Snapshot loadCatalogManagedSnapshot(Engine engine, String tableId, String tablePath,
            String catalogName, String schemaName, String tableName,
            Optional<Long> version) throws IOException {
        return loadCatalogManagedSnapshot(engine, tableId, tablePath, catalogName,
                schemaName, tableName, version, Optional.empty());
    }

    Snapshot loadCatalogManagedSnapshot(Engine engine, String tableId, String tablePath,
            String catalogName, String schemaName, String tableName,
            Optional<Long> version, Optional<Long> timestampMillis) throws IOException {
        UCTableIdentifier tableIdentifier =
                new UCTableIdentifier(catalogName, schemaName, tableName);
        try (UCDeltaTokenBasedRestClient catalogClient =
                new UCDeltaTokenBasedRestClient(
                        workspaceUri, tokenProvider, APP_VERSIONS)) {
            return new NamedUCCatalogManagedClient(catalogClient, tableIdentifier).loadSnapshot(
                    engine, tableId, tablePath, tableIdentifier,
                    version, timestampMillis);
        }
    }

    CatalogManagedSnapshot openCatalogManagedSnapshot(Engine engine, String tableId,
            String tablePath, String catalogName, String schemaName, String tableName,
            Optional<Long> version) throws IOException {
        UCDeltaTokenBasedRestClient catalogClient = new UCDeltaTokenBasedRestClient(
                workspaceUri, tokenProvider, APP_VERSIONS);
        UCTableIdentifier tableIdentifier =
                new UCTableIdentifier(catalogName, schemaName, tableName);
        try {
            Snapshot snapshot = new NamedUCCatalogManagedClient(
                    catalogClient, tableIdentifier).loadSnapshot(
                    engine, tableId, tablePath, tableIdentifier, version, Optional.empty());
            return new CatalogManagedSnapshot(snapshot, catalogClient);
        } catch (RuntimeException e) {
            try {
                catalogClient.close();
            } catch (IOException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }

    static final class CatalogManagedSnapshot implements AutoCloseable {
        private final Snapshot snapshot;
        private final UCDeltaTokenBasedRestClient catalogClient;

        private CatalogManagedSnapshot(Snapshot snapshot,
                UCDeltaTokenBasedRestClient catalogClient) {
            this.snapshot = snapshot;
            this.catalogClient = catalogClient;
        }

        Snapshot getSnapshot() {
            return snapshot;
        }

        @Override
        public void close() throws IOException {
            catalogClient.close();
        }
    }

    /** Keeps the table identifier on the committer used by name-based Unity REST updates. */
    private static final class NamedUCCatalogManagedClient extends UCCatalogManagedClient {
        private final UCTableIdentifier tableIdentifier;

        private NamedUCCatalogManagedClient(
                UCClient catalogClient, UCTableIdentifier tableIdentifier) {
            super(catalogClient);
            this.tableIdentifier = tableIdentifier;
        }

        @Override
        protected Committer createUCCommitter(
                UCClient catalogClient, String tableId, String tablePath) {
            return new UCCatalogManagedCommitter(
                    catalogClient, tableId, tablePath, tableIdentifier);
        }
    }

    private static boolean isReadableDeltaTable(JsonNode table) {
        String format = textValue(table, "data_source_format");
        String tableType = textValue(table, "table_type");
        if (!"DELTA".equalsIgnoreCase(format)
                || (!"MANAGED".equalsIgnoreCase(tableType)
                && !"EXTERNAL".equalsIgnoreCase(tableType))) {
            return false;
        }
        // The native Parquet scan does not evaluate Unity cross-engine ABAC policies yet.
        // Keep policy-bearing tables out of discovery instead of exposing unfiltered data.
        if (hasPolicy(table, "row_filter", "row-filter", "column_masks", "column-masks")
                || hasColumnMask(table)) {
            return false;
        }
        return tableCapabilities(table).contains("HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT");
    }

    private static Set<String> tableCapabilities(JsonNode table) {
        JsonNode capabilities = table.get("manifest_capabilities");
        if (capabilities == null) {
            capabilities = table.get("manifest-capabilities");
        }
        if (capabilities == null) {
            JsonNode manifest = table.get("securable_kind_manifest");
            capabilities = manifest == null ? null : manifest.get("capabilities");
        }
        return capabilityNames(capabilities);
    }

    private static boolean hasPolicy(JsonNode table, String... fieldNames) {
        for (String fieldName : fieldNames) {
            JsonNode policy = table.get(fieldName);
            if (policy != null && !policy.isNull()
                    && !(policy.isObject() && policy.isEmpty())
                    && !(policy.isArray() && policy.isEmpty())) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasColumnMask(JsonNode table) {
        JsonNode columns = table.get("columns");
        if (columns == null || !columns.isArray()) {
            return false;
        }
        for (JsonNode column : columns) {
            if (hasPolicy(column, "mask")) {
                return true;
            }
        }
        return false;
    }

    private static String textValue(JsonNode node, String fieldName) {
        JsonNode value = node.get(fieldName);
        return value == null || value.isNull() ? null : value.asText();
    }

    private static Set<String> capabilityNames(JsonNode capabilities) {
        if (capabilities == null || capabilities.isNull()) {
            return Set.of();
        }
        Set<String> names = new HashSet<>();
        collectCapabilityNames(capabilities, names);
        return names;
    }

    private static void collectCapabilityNames(JsonNode node, Set<String> names) {
        if (node.isTextual()) {
            names.add(node.asText());
            return;
        }
        if (node.isArray()) {
            for (JsonNode child : node) {
                collectCapabilityNames(child, names);
            }
            return;
        }
        if (!node.isObject()) {
            return;
        }
        node.fields().forEachRemaining(entry -> {
            if (entry.getValue().isBoolean() && entry.getValue().asBoolean()) {
                names.add(entry.getKey());
            }
            collectCapabilityNames(entry.getValue(), names);
        });
    }

    static String credentialRegion(DeltaStorageCredentialConfig config) {
        if (config instanceof DeltaStorageCredentialConfigWithRegion) {
            return ((DeltaStorageCredentialConfigWithRegion) config).getRegion();
        }
        return null;
    }

    private static void registerCredentialConfigDeserializer(ApiClient apiClient) {
        ObjectMapper mapper = apiClient.getObjectMapper();
        SimpleModule module = new SimpleModule("doris-delta-credential-config");
        module.addDeserializer(DeltaStorageCredentialConfig.class,
                new DeltaStorageCredentialConfigDeserializer());
        mapper.registerModule(module);
        apiClient.setObjectMapper(mapper);
    }

    private static final class DeltaStorageCredentialConfigDeserializer
            extends StdDeserializer<DeltaStorageCredentialConfig> {
        private DeltaStorageCredentialConfigDeserializer() {
            super(DeltaStorageCredentialConfig.class);
        }

        @Override
        public DeltaStorageCredentialConfig deserialize(JsonParser parser,
                com.fasterxml.jackson.databind.DeserializationContext context) throws IOException {
            JsonNode node = parser.getCodec().readTree(parser);
            DeltaStorageCredentialConfig config = new DeltaStorageCredentialConfig()
                    .s3AccessKeyId(text(node, "s3.access-key-id"))
                    .s3SecretAccessKey(text(node, "s3.secret-access-key"))
                    .s3SessionToken(text(node, "s3.session-token"))
                    .azureSasToken(text(node, "azure.sas-token"))
                    .gcsOauthToken(text(node, "gcs.oauth-token"));
            String region = firstNonBlank(node, "client.region", "s3.region", "aws.region");
            if (region == null) {
                return config;
            }
            return new DeltaStorageCredentialConfigWithRegion(config, region);
        }

        private static String text(JsonNode node, String key) {
            JsonNode value = node.get(key);
            return value == null || value.isNull() ? null : value.asText();
        }

        private static String firstNonBlank(JsonNode node, String... keys) {
            for (String key : keys) {
                String value = text(node, key);
                if (value != null && !value.trim().isEmpty()) {
                    return value.trim();
                }
            }
            return null;
        }
    }

    private static final class DeltaStorageCredentialConfigWithRegion
            extends DeltaStorageCredentialConfig {
        private final String region;

        private DeltaStorageCredentialConfigWithRegion(
                DeltaStorageCredentialConfig config, String region) {
            s3AccessKeyId(config.getS3AccessKeyId());
            s3SecretAccessKey(config.getS3SecretAccessKey());
            s3SessionToken(config.getS3SessionToken());
            azureSasToken(config.getAzureSasToken());
            gcsOauthToken(config.getGcsOauthToken());
            this.region = region;
        }

        private String getRegion() {
            return region;
        }
    }

    private static String storageScheme(String location) {
        String scheme = URI.create(location).getScheme();
        if (scheme == null) {
            throw new IllegalArgumentException(
                    "Unity Catalog returned a Delta location without a URI scheme");
        }
        if ("s3a".equalsIgnoreCase(scheme)) {
            return "s3";
        }
        switch (scheme.toLowerCase(java.util.Locale.ROOT)) {
            case "file":
            case "s3":
            case "gs":
            case "abfs":
            case "abfss":
                return scheme.toLowerCase(java.util.Locale.ROOT);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Unity Delta storage scheme: " + scheme);
        }
    }

    private static DorisConnectorException requestFailure(String operation, ApiException cause) {
        return new DorisConnectorException(
                "Unity Catalog request failed while attempting to " + operation
                        + " (HTTP " + cause.getCode() + ")", cause);
    }

    private static String stripTrailingSlash(String value) {
        String normalized = value;
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }
}
