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

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.delta.model.DeltaStorageCredentialConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class UnityDeltaCatalogAdapterTest {
    private static final String TEST_TOKEN = "test-unity-token-never-log";

    private HttpServer server;
    private String workspaceUri;
    private String tableLocation;
    private String catalogManagedLocation;
    private String catalogManagedTableId;
    private String deltaProtocolVersion;
    private String createStagingLocation;
    private int deleteStatus;
    private final List<String> requestPaths = new ArrayList<>();
    private final List<String> requestQueries = new ArrayList<>();
    private final List<String> updateRequestPaths = new ArrayList<>();
    private final Set<String> deletedTables = new HashSet<>();

    @TempDir
    private Path tempDirectory;

    @BeforeEach
    public void startServer() throws Exception {
        URL fixture = Objects.requireNonNull(
                getClass().getClassLoader().getResource("delta/path_table"));
        tableLocation = Paths.get(fixture.toURI()).toUri().toString();
        URL catalogManagedFixture = Objects.requireNonNull(
                getClass().getClassLoader().getResource("delta/catalog_managed_table"));
        catalogManagedLocation = Paths.get(catalogManagedFixture.toURI()).toUri().toString();
        catalogManagedTableId = "c79de738-d13c-44a5-8e75-8435123d60c7";
        deltaProtocolVersion = "1.0";
        deleteStatus = 200;
        createStagingLocation = tempDirectory.resolve("unity-created-table").toUri().toString();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handleRequest);
        server.start();
        workspaceUri = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    public void stopServer() {
        server.stop(0);
    }

    @Test
    public void testUnityCatalogDiscoveryAndPinnedPathSnapshot() {
        Map<String, String> properties = Map.of(
                "type", "delta",
                DeltaConnectorProperties.CATALOG_TYPE, DeltaConnectorProperties.CATALOG_TYPE_UNITY,
                DeltaConnectorProperties.UNITY_URI, workspaceUri,
                DeltaConnectorProperties.UNITY_CATALOG, "main",
                DeltaConnectorProperties.UNITY_TOKEN, TEST_TOKEN);

        DeltaConnectorProvider provider = new DeltaConnectorProvider();
        provider.validateProperties(properties);
        DeltaConnector connector = provider.create(properties, connectorContext());

        Assertions.assertTrue(connector.testConnection(null).isSuccess());
        Assertions.assertTrue(connector.defaultTestConnection());
        Assertions.assertInstanceOf(UnityDeltaCatalogAdapter.class, connector.getCatalogAdapter());
        Assertions.assertEquals(List.of("default"),
                connector.getMetadata(null).listDatabaseNames(null));
        Assertions.assertEquals(List.of("events", "catalog_managed", "propertyless",
                "missing_type", "official_manifest"),
                connector.getMetadata(null).listTableNames(null, "default"));
        Assertions.assertTrue(connector.getMetadata(null)
                .getTableHandle(null, "default", "policy_table").isEmpty());
        Assertions.assertTrue(connector.getMetadata(null)
                .getTableHandle(null, "default", "masked_table").isEmpty());
        Assertions.assertTrue(connector.getMetadata(null)
                .getTableHandle(null, "default", "blocked").isEmpty());
        Assertions.assertTrue(connector.getMetadata(null)
                .getTableHandle(null, "default", "missing_capabilities").isEmpty());
        ConnectorTableHandle handle = connector.getMetadata(null)
                .getTableHandle(null, "default", "events").orElseThrow();
        Assertions.assertFalse(((DeltaTableHandle) handle).supportsExternalWrite());
        Assertions.assertTrue(((DeltaTableHandle) connector.getMetadata(null)
                .getTableHandle(null, "default", "catalog_managed").orElseThrow())
                .supportsExternalWrite());
        Assertions.assertEquals(1, ((DeltaTableHandle) handle).getSnapshotVersion());
        Assertions.assertNotNull(((DeltaTableHandle) handle).getPinnedSnapshot());
        Assertions.assertEquals(2, connector.getScanPlanProvider()
                .planScan(null, handle, List.of(), java.util.Optional.empty()).size());
        Map<String, String> scanProperties = connector.getScanPlanProvider()
                .getScanNodeProperties(null, handle, List.of(), java.util.Optional.empty());
        Assertions.assertEquals("parquet", scanProperties.get("file_format_type"));
        Assertions.assertFalse(scanProperties.containsValue(TEST_TOKEN));
        Assertions.assertTrue(requestPaths.stream().anyMatch(
                path -> path.endsWith("/delta/v1/catalogs/main/schemas/default/tables/events")));
        Assertions.assertTrue(requestPaths.stream().anyMatch(
                path -> path.endsWith("/delta/v1/config")));
        Assertions.assertTrue(requestQueries.stream().anyMatch(query -> query != null
                && query.contains("protocol-versions=1.0")));
        Assertions.assertTrue(requestQueries.stream().anyMatch(query -> query != null
                && query.contains("include_manifest_capabilities=true")));
        Assertions.assertTrue(requestQueries.stream().anyMatch(query -> query != null
                && query.contains("max_results=50")));
        Assertions.assertTrue(requestPaths.stream().noneMatch(path -> path.endsWith("/credentials")));
    }

    @Test
    public void testUnityOAuthClientCredentialsAuthentication() {
        Map<String, String> properties = Map.of(
                "type", "delta",
                DeltaConnectorProperties.CATALOG_TYPE, DeltaConnectorProperties.CATALOG_TYPE_UNITY,
                DeltaConnectorProperties.UNITY_URI, workspaceUri,
                DeltaConnectorProperties.UNITY_AUTH_TYPE, "oauth",
                DeltaConnectorProperties.UNITY_OAUTH_URI, workspaceUri + "/oauth/token",
                DeltaConnectorProperties.UNITY_OAUTH_CLIENT_ID, "client-id",
                DeltaConnectorProperties.UNITY_OAUTH_CLIENT_SECRET, "client-secret",
                DeltaConnectorProperties.UNITY_CATALOG, "main");

        DeltaConnectorProvider provider = new DeltaConnectorProvider();
        provider.validateProperties(properties);
        DeltaConnector connector = provider.create(properties, connectorContext());

        Assertions.assertTrue(connector.testConnection(null).isSuccess());
        Assertions.assertEquals(List.of("default"),
                connector.getMetadata(null).listDatabaseNames(null));
        Assertions.assertTrue(requestPaths.stream().anyMatch(
                path -> path.equals("/oauth/token")));
    }

    @Test
    public void testRejectsIncompatibleDeltaApiProtocol() {
        deltaProtocolVersion = "2.0";
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);

        DorisConnectorException exception = Assertions.assertThrows(
                DorisConnectorException.class,
                () -> client.loadTable("main", "default", "events"));
        Assertions.assertTrue(exception.getMessage().contains("requires 1.0"));
    }

    @Test
    public void testUnityOAuthPropertiesRequireCompleteCredentials() {
        DeltaConnectorProvider provider = new DeltaConnectorProvider();
        Map<String, String> properties = new java.util.HashMap<>(Map.of(
                "type", "delta",
                DeltaConnectorProperties.CATALOG_TYPE, DeltaConnectorProperties.CATALOG_TYPE_UNITY,
                DeltaConnectorProperties.UNITY_URI, workspaceUri,
                DeltaConnectorProperties.UNITY_AUTH_TYPE, "oauth",
                DeltaConnectorProperties.UNITY_OAUTH_URI, workspaceUri + "/oauth/token",
                DeltaConnectorProperties.UNITY_OAUTH_CLIENT_ID, "client-id",
                DeltaConnectorProperties.UNITY_CATALOG, "main"));

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> provider.validateProperties(properties));
    }

    @Test
    public void testOfficialDeltaCredentialEndpointAndBackendMappings() {
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);
        DeltaCredentialsResponse response = client.getReadCredentials("main", "default", "events");

        Map<String, String> aws = UnityDeltaStorageProperties.toBackendProperties(
                "s3://delta-bucket/tables/events", response, Map.of("s3.region", "us-west-2"));
        Assertions.assertEquals("temporary-ak", aws.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("temporary-sk", aws.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("temporary-session", aws.get("AWS_TOKEN"));
        long awsExpiration = Long.parseLong(aws.get("AWS_TOKEN_EXPIRATION_TIME_MS"));
        Assertions.assertTrue(awsExpiration > System.currentTimeMillis());
        Assertions.assertTrue(awsExpiration <= System.currentTimeMillis() + 3600000L);
        Assertions.assertEquals("s3.us-west-2.amazonaws.com", aws.get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-west-2", aws.get("AWS_REGION"));
        Assertions.assertFalse(aws.containsValue(TEST_TOKEN));
        Map<String, String> responseRegion = UnityDeltaStorageProperties.toBackendProperties(
                "s3://delta-bucket/tables/events", response, Map.of());
        Assertions.assertEquals("us-east-2", responseRegion.get("AWS_REGION"));
        Assertions.assertEquals("s3.us-east-2.amazonaws.com", responseRegion.get("AWS_ENDPOINT"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> UnityDeltaStorageProperties.toBackendProperties(
                        "s3://delta-bucket/tables/events", response, Map.of(),
                        DeltaCredentialOperation.READ_WRITE));
        Assertions.assertTrue(requestPaths.stream().anyMatch(path -> path.endsWith(
                "/delta/v1/catalogs/main/schemas/default/tables/events/credentials")));

        DeltaCredentialsResponse writeResponse = client.getWriteCredentials(
                "main", "default", "events");
        Map<String, String> writeProperties = UnityDeltaStorageProperties.toBackendProperties(
                "s3://delta-bucket/tables/events", writeResponse,
                Map.of("s3.region", "us-west-2"), DeltaCredentialOperation.READ_WRITE);
        Assertions.assertEquals("temporary-ak", writeProperties.get("AWS_ACCESS_KEY"));
        Assertions.assertTrue(requestQueries.stream().anyMatch(
                query -> query != null && query.contains("operation=READ_WRITE")));

        DeltaCredentialsResponse azureResponse = credentials(
                "abfss://container@account.dfs.core.windows.net/tables/events",
                new DeltaStorageCredentialConfig().azureSasToken("azure-sas"));
        Map<String, String> azure = UnityDeltaStorageProperties.toBackendProperties(
                "abfss://container@account.dfs.core.windows.net/tables/events",
                azureResponse, Map.of());
        Assertions.assertEquals("AZURE", azure.get("provider"));
        Assertions.assertEquals("account.blob.core.windows.net", azure.get("AWS_ENDPOINT"));
        Assertions.assertEquals("azure", azure.get("AWS_REGION"));
        Assertions.assertEquals("azure-sas", azure.get("AWS_TOKEN"));
        long azureExpiration = Long.parseLong(azure.get("AWS_TOKEN_EXPIRATION_TIME_MS"));
        Assertions.assertTrue(azureExpiration > System.currentTimeMillis());
        Assertions.assertTrue(azureExpiration <= System.currentTimeMillis() + 3600000L);
        Assertions.assertFalse(azure.containsKey("fs.azure.sas.fixed.token.account.dfs.core.windows.net"));

        DeltaCredentialsResponse gcsResponse = credentials(
                "gs://delta-bucket/tables/events",
                new DeltaStorageCredentialConfig().gcsOauthToken("gcs-oauth"));
        Map<String, String> gcs = UnityDeltaStorageProperties.toBackendProperties(
                "gs://delta-bucket/tables/events", gcsResponse, Map.of());
        Assertions.assertEquals("GCP", gcs.get("provider"));
        Assertions.assertEquals("https://storage.googleapis.com", gcs.get("uri"));
        Assertions.assertEquals("Bearer gcs-oauth", gcs.get("http.header.Authorization"));
        Assertions.assertTrue(Long.parseLong(gcs.get("AWS_TOKEN_EXPIRATION_TIME_MS"))
                > System.currentTimeMillis());
        Assertions.assertEquals("https://storage.googleapis.com/delta-bucket/tables/events",
                UnityDeltaStorageProperties.toBackendPath(
                        "gs://delta-bucket/tables/events", gcs));
        Assertions.assertEquals("https://gcs.example.test/base/delta-bucket/tables/events",
                UnityDeltaStorageProperties.toBackendPath(
                        "gs://delta-bucket/tables/events",
                        Map.of("gcs.endpoint", "https://gcs.example.test/base/")));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> UnityDeltaStorageProperties.toBackendPath(
                        "gs://delta-bucket/tables/events",
                        Map.of("gcs.endpoint", "http://gcs.example.test")));
        Map<String, String> gcsWrite = UnityDeltaStorageProperties.toBackendProperties(
                "gs://delta-bucket/tables/events", credentials(
                        "gs://delta-bucket/tables/events",
                        new DeltaStorageCredentialConfig().gcsOauthToken("gcs-oauth"),
                        DeltaCredentialOperation.READ_WRITE), Map.of(),
                DeltaCredentialOperation.READ_WRITE);
        Assertions.assertEquals("GCP", gcsWrite.get("provider"));
        Assertions.assertEquals("Bearer gcs-oauth",
                gcsWrite.get("http.header.Authorization"));
    }

    @Test
    public void testOfficialGcsCredentialScopedFilesystemConfiguration() {
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);
        org.apache.hadoop.conf.Configuration configuration = client.buildReadHadoopConfiguration(
                "main", "default", "gcs_events",
                "gs://delta-bucket/tables/events",
                new org.apache.hadoop.conf.Configuration(false));

        Assertions.assertEquals("io.unitycatalog.hadoop.internal.fs.CredScopedFileSystem",
                configuration.get("fs.gs.impl"));
        Assertions.assertEquals("com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
                configuration.get("fs.gs.impl.original"));
        Assertions.assertEquals("ACCESS_TOKEN_PROVIDER", configuration.get("fs.gs.auth.type"));
        Assertions.assertEquals("io.unitycatalog.hadoop.internal.auth.GcsVendedTokenProvider",
                configuration.get("fs.gs.auth.access.token.provider"));
        Assertions.assertEquals("gcs-oauth", configuration.get("fs.gs.init.oauth.token"));
        Assertions.assertEquals("true",
                configuration.get("fs.unitycatalog.delta.credentials.api.enabled"));
        Assertions.assertEquals("gcs_events",
                configuration.get("fs.unitycatalog.delta.table.name"));
        Assertions.assertTrue(requestPaths.stream().anyMatch(path -> path.endsWith(
                "/delta/v1/catalogs/main/schemas/default/tables/gcs_events/credentials")));
    }

    @Test
    public void testRejectIncompleteVendedCredentials() {
        DeltaCredentialsResponse incompleteAws = credentials(
                "s3://delta-bucket/tables/events",
                new DeltaStorageCredentialConfig()
                        .s3AccessKeyId("temporary-ak")
                        .s3SecretAccessKey("temporary-sk"));
        IllegalArgumentException awsException = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> UnityDeltaStorageProperties.toBackendProperties(
                        "s3://delta-bucket/tables/events", incompleteAws, Map.of()));
        Assertions.assertTrue(awsException.getMessage().contains("session token"));

        DeltaCredentialsResponse incompleteAzure = credentials(
                "abfss://container@account.dfs.core.windows.net/tables/events",
                new DeltaStorageCredentialConfig().azureSasToken(""));
        IllegalArgumentException azureException = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> UnityDeltaStorageProperties.toBackendProperties(
                        "abfss://container@account.dfs.core.windows.net/tables/events",
                        incompleteAzure, Map.of()));
        Assertions.assertTrue(azureException.getMessage().contains("Azure SAS token"));

        DeltaCredentialsResponse incompleteGcs = credentials(
                "gs://delta-bucket/tables/events",
                new DeltaStorageCredentialConfig().gcsOauthToken(""));
        IllegalArgumentException gcsException = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> UnityDeltaStorageProperties.toBackendProperties(
                        "gs://delta-bucket/tables/events", incompleteGcs, Map.of()));
        Assertions.assertTrue(gcsException.getMessage().contains("GCS OAuth token"));
    }

    @Test
    public void testGcsScanFileAndDeletionVectorPathsUseHttps() {
        Map<String, String> properties = Map.of(
                "gcs.endpoint", "https://gcs.example.test/base");
        DeltaDeletionVector absoluteDv = new DeltaDeletionVector(
                "p", "gs://delta-bucket/tables/events/dv.bin",
                java.util.Optional.of(4), 16, 2);
        DeltaScanFile absolute = UnityDeltaStorageProperties.toBackendScanFile(
                new DeltaScanFile("gs://delta-bucket/tables/events/part.parquet", 100, 10,
                        Map.of(), absoluteDv, "gs://delta-bucket/tables/events"), properties);

        Assertions.assertEquals(
                "https://gcs.example.test/base/delta-bucket/tables/events/part.parquet",
                absolute.getPath());
        Assertions.assertEquals(
                "https://gcs.example.test/base/delta-bucket/tables/events",
                absolute.getTablePath());
        Assertions.assertEquals(
                "https://gcs.example.test/base/delta-bucket/tables/events/dv.bin",
                absolute.getDeletionVector().getPathOrInlineDv());

        DeltaDeletionVector uuidDv = new DeltaDeletionVector(
                "u", "prefix-and-encoded-uuid", java.util.Optional.of(0), 16, 2);
        DeltaScanFile uuid = UnityDeltaStorageProperties.toBackendScanFile(
                new DeltaScanFile("gs://delta-bucket/tables/events/part.parquet", 100, 10,
                        Map.of(), uuidDv, "gs://delta-bucket/tables/events"), properties);
        Assertions.assertEquals("prefix-and-encoded-uuid",
                uuid.getDeletionVector().getPathOrInlineDv());
        Assertions.assertEquals(
                "https://gcs.example.test/base/delta-bucket/tables/events",
                uuid.getTablePath());
    }

    @Test
    public void testRejectMissingVendedCredentials() {
        DeltaCredentialsResponse empty = new DeltaCredentialsResponse();
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> UnityDeltaStorageProperties.toBackendProperties(
                        "s3://delta-bucket/tables/events", empty, Map.of()));
        Assertions.assertTrue(exception.getMessage().contains("no storage credentials"));

        DeltaCredentialsResponse nullList = new DeltaCredentialsResponse()
                .storageCredentials(null);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> UnityDeltaStorageProperties.toBackendProperties(
                        "s3://delta-bucket/tables/events", nullList, Map.of()));
    }

    @Test
    public void testRejectVendedCredentialWithoutOperation() {
        DeltaCredentialsResponse response = new DeltaCredentialsResponse()
                .addStorageCredentialsItem(new DeltaStorageCredential()
                        .prefix("s3://delta-bucket/tables/events")
                        .config(new DeltaStorageCredentialConfig()
                                .s3AccessKeyId("temporary-ak")
                                .s3SecretAccessKey("temporary-sk")
                                .s3SessionToken("temporary-session")));
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> UnityDeltaStorageProperties.toBackendProperties(
                        "s3://delta-bucket/tables/events", response, Map.of()));
        Assertions.assertTrue(exception.getMessage().contains("without an operation"));
    }

    @Test
    public void testRejectCredentialsThatAreAboutToExpire() {
        DeltaCredentialsResponse response = new DeltaCredentialsResponse()
                .addStorageCredentialsItem(new DeltaStorageCredential()
                        .prefix("s3://delta-bucket/tables/events")
                        .operation(DeltaCredentialOperation.READ)
                        .config(new DeltaStorageCredentialConfig()
                                .s3AccessKeyId("temporary-ak")
                                .s3SecretAccessKey("temporary-sk")
                                .s3SessionToken("temporary-session"))
                        .expirationTimeMs(System.currentTimeMillis() + 1000));
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> UnityDeltaStorageProperties.toBackendProperties(
                        "s3://delta-bucket/tables/events", response,
                        Map.of("client.region", "us-east-1")));
        Assertions.assertTrue(exception.getMessage().contains("less than"));
    }

    @Test
    public void testRejectCredentialsWithoutExpiration() {
        DeltaCredentialsResponse response = new DeltaCredentialsResponse()
                .addStorageCredentialsItem(new DeltaStorageCredential()
                        .prefix("s3://delta-bucket/tables/events")
                        .operation(DeltaCredentialOperation.READ)
                        .config(new DeltaStorageCredentialConfig()
                                .s3AccessKeyId("temporary-ak")
                                .s3SecretAccessKey("temporary-sk")
                                .s3SessionToken("temporary-session")));
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> UnityDeltaStorageProperties.toBackendProperties(
                        "s3://delta-bucket/tables/events", response,
                        Map.of("client.region", "us-east-1")));
        Assertions.assertTrue(exception.getMessage().contains("without an expiration time"));
    }

    @Test
    public void testUnityTableMetadataWithoutPropertiesIsOrdinaryExternalTable() {
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);
        UnityDeltaCatalogAdapter adapter = new UnityDeltaCatalogAdapter(
                "main", client, new org.apache.hadoop.conf.Configuration(), Map.of());

        DeltaTableHandle handle = adapter.getTableHandle("default", "propertyless")
                .orElseThrow();

        Assertions.assertFalse(handle.isCatalogManaged());
        Assertions.assertTrue(handle.isExternalTable());
    }

    @Test
    public void testUnityTableMetadataWithoutTableTypeFailsClosed() {
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);
        UnityDeltaCatalogAdapter adapter = new UnityDeltaCatalogAdapter(
                "main", client, new org.apache.hadoop.conf.Configuration(), Map.of());

        DorisConnectorException exception = Assertions.assertThrows(
                DorisConnectorException.class,
                () -> adapter.getTableHandle("default", "missing_type"));

        Assertions.assertTrue(exception.getMessage().contains("supported table type"));
    }

    @Test
    public void testCatalogManagedSnapshotIncludesRatifiedLogTail() {
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);
        UnityDeltaCatalogAdapter adapter = new UnityDeltaCatalogAdapter(
                "main", client, new org.apache.hadoop.conf.Configuration(), Map.of());

        DeltaTableHandle handle = adapter.getTableHandle("default", "catalog_managed")
                .orElseThrow();
        DeltaKernelSnapshot snapshot = adapter.loadSnapshot(handle);

        Assertions.assertTrue(handle.isCatalogManaged());
        Assertions.assertEquals("c79de738-d13c-44a5-8e75-8435123d60c7",
                handle.getCatalogTableId());
        Assertions.assertEquals(2, handle.getSnapshotVersion());
        Assertions.assertEquals(2, snapshot.getVersion());
        Assertions.assertEquals(
                List.of("part-00001.parquet", "part-00002.parquet"),
                snapshot.getActiveFiles().stream()
                        .map(file -> Paths.get(URI.create(file.getPath()))
                                .getFileName().toString())
                        .sorted()
                .collect(java.util.stream.Collectors.toList()));
    }

    @Test
    public void testUnityExternalAndCatalogManagedTimeTravel() {
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);
        UnityDeltaCatalogAdapter adapter = new UnityDeltaCatalogAdapter(
                "main", client, new org.apache.hadoop.conf.Configuration(), Map.of());

        DeltaTableHandle external = adapter.getTableHandle("default", "events").orElseThrow();
        DeltaTableHandle externalVersionZero = adapter.applyTableSnapshot(
                external, DeltaTableSnapshot.version(0));
        Assertions.assertEquals(0, externalVersionZero.getSnapshotVersion());
        Assertions.assertNotNull(externalVersionZero.getPinnedSnapshot());
        Assertions.assertEquals(2, adapter.loadSnapshot(externalVersionZero)
                .getActiveFiles().size());

        DeltaTableHandle managed = adapter.getTableHandle("default", "catalog_managed")
                .orElseThrow();
        DeltaTableHandle managedVersionOne = adapter.applyTableSnapshot(
                managed, DeltaTableSnapshot.version(1));
        Assertions.assertEquals(1, managedVersionOne.getSnapshotVersion());
        Assertions.assertNotNull(managedVersionOne.getPinnedSnapshot());
        Assertions.assertEquals(List.of("part-00001.parquet"),
                adapter.loadSnapshot(managedVersionOne).getActiveFiles().stream()
                        .map(file -> Paths.get(URI.create(file.getPath())).getFileName().toString())
                        .collect(java.util.stream.Collectors.toList()));

        DeltaTableHandle managedAtTimestamp = adapter.applyTableSnapshot(
                managed, DeltaTableSnapshot.timestampMillis(1_700_000_000_001L));
        Assertions.assertEquals(1, managedAtTimestamp.getSnapshotVersion());
    }

    @Test
    public void testUnityManagedCreateUsesStagingAndFinalize() {
        Map<String, String> properties = Map.of(
                DeltaConnectorProperties.WRITE_ENABLED, "true",
                DeltaConnectorProperties.CREATE_ENABLED, "true");
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);
        UnityDeltaCatalogAdapter adapter = new UnityDeltaCatalogAdapter(
                "main", client, new org.apache.hadoop.conf.Configuration(), properties);
        ConnectorCreateTableRequest request = DeltaTestUtils.createRequest(
                "default", new ConnectorTableSchema("created_events", List.of(
                        new ConnectorColumn("id", ConnectorType.of("BIGINT"), "", false, null)),
                        "DELTA", Map.of()), List.of(), Map.of(), "created events", false);

        Assertions.assertTrue(adapter.supportsCreateTable());
        Assertions.assertFalse(adapter.createTable(request));
        Assertions.assertTrue(Files.exists(Paths.get(URI.create(createStagingLocation))
                .resolve("_delta_log/00000000000000000000.json")));
        Assertions.assertTrue(requestPaths.stream().anyMatch(path -> path.endsWith(
                "/delta/v1/catalogs/main/schemas/default/staging-tables")));
        Assertions.assertTrue(updateRequestPaths.stream().anyMatch(path -> path.endsWith(
                "/delta/v1/catalogs/main/schemas/default/tables")));
    }

    @Test
    public void testUnityDropUsesDeltaApiWithoutDeletingExternalData() {
        Map<String, String> properties = Map.of(
                "type", "delta",
                DeltaConnectorProperties.CATALOG_TYPE,
                        DeltaConnectorProperties.CATALOG_TYPE_UNITY,
                DeltaConnectorProperties.UNITY_URI, workspaceUri,
                DeltaConnectorProperties.UNITY_CATALOG, "main",
                DeltaConnectorProperties.UNITY_TOKEN, TEST_TOKEN,
                DeltaConnectorProperties.WRITE_ENABLED, "true",
                DeltaConnectorProperties.DROP_ENABLED, "true");
        DeltaConnector connector = new DeltaConnectorProvider().create(
                properties, connectorContext());
        ConnectorTableHandle handle = connector.getMetadata(null)
                .getTableHandle(null, "default", "events").orElseThrow();
        Path deltaLog = Paths.get(URI.create(tableLocation)).resolve("_delta_log");

        Assertions.assertTrue(connector.getCatalogAdapter().supportsDropTable());
        connector.getMetadata(null).dropTable(null, handle);

        Assertions.assertTrue(Files.isDirectory(deltaLog));
        Assertions.assertTrue(connector.getMetadata(null)
                .getTableHandle(null, "default", "events").isEmpty());
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);
        Assertions.assertDoesNotThrow(
                () -> client.deleteTable("main", "default", "events"));
        Assertions.assertTrue(updateRequestPaths.stream().anyMatch(path -> path.endsWith(
                "/delta/v1/catalogs/main/schemas/default/tables/events")));
    }

    @Test
    public void testUnityDropRequiresExplicitOptIn() {
        Map<String, String> properties = Map.of(
                "type", "delta",
                DeltaConnectorProperties.CATALOG_TYPE,
                        DeltaConnectorProperties.CATALOG_TYPE_UNITY,
                DeltaConnectorProperties.UNITY_URI, workspaceUri,
                DeltaConnectorProperties.UNITY_CATALOG, "main",
                DeltaConnectorProperties.UNITY_TOKEN, TEST_TOKEN,
                DeltaConnectorProperties.WRITE_ENABLED, "true");
        DeltaConnector connector = new DeltaConnectorProvider().create(
                properties, connectorContext());
        ConnectorTableHandle handle = connector.getMetadata(null)
                .getTableHandle(null, "default", "events").orElseThrow();

        Assertions.assertFalse(connector.getCatalogAdapter().supportsDropTable());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> connector.getMetadata(null).dropTable(null, handle));
    }

    @Test
    public void testUnityReadOnlyCapabilityBlocksWriteBeforeCredentialVending() {
        Map<String, String> properties = Map.of(
                "type", "delta",
                DeltaConnectorProperties.CATALOG_TYPE,
                        DeltaConnectorProperties.CATALOG_TYPE_UNITY,
                DeltaConnectorProperties.UNITY_URI, workspaceUri,
                DeltaConnectorProperties.UNITY_CATALOG, "main",
                DeltaConnectorProperties.UNITY_TOKEN, TEST_TOKEN,
                DeltaConnectorProperties.WRITE_ENABLED, "true");
        DeltaConnector connector = new DeltaConnectorProvider().create(
                properties, connectorContext());
        ConnectorTableHandle handle = connector.getMetadata(null)
                .getTableHandle(null, "default", "events").orElseThrow();
        List<ConnectorColumn> columns = connector.getMetadata(null)
                .getTableSchema(null, handle).getColumns();

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> connector.getMetadata(null).getWriteConfig(null, handle, columns));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> connector.getMetadata(null).beginInsert(null, handle, columns));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> connector.getMetadata(null).beginInsertOverwrite(null, handle, columns));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> connector.getMetadata(null).truncateTable(null, handle));
        Assertions.assertTrue(requestPaths.stream().noneMatch(path -> path.endsWith(
                "/tables/events/credentials")));
        Assertions.assertTrue(updateRequestPaths.isEmpty());
    }

    @Test
    public void testUnityDropAuthorizationFailurePreservesTableAndTokenSecrecy() {
        Map<String, String> properties = Map.of(
                "type", "delta",
                DeltaConnectorProperties.CATALOG_TYPE,
                        DeltaConnectorProperties.CATALOG_TYPE_UNITY,
                DeltaConnectorProperties.UNITY_URI, workspaceUri,
                DeltaConnectorProperties.UNITY_CATALOG, "main",
                DeltaConnectorProperties.UNITY_TOKEN, TEST_TOKEN,
                DeltaConnectorProperties.WRITE_ENABLED, "true",
                DeltaConnectorProperties.DROP_ENABLED, "true");
        DeltaConnector connector = new DeltaConnectorProvider().create(
                properties, connectorContext());
        ConnectorTableHandle handle = connector.getMetadata(null)
                .getTableHandle(null, "default", "events").orElseThrow();
        deleteStatus = 403;

        DorisConnectorException failure = Assertions.assertThrows(
                DorisConnectorException.class,
                () -> connector.getMetadata(null).dropTable(null, handle));

        Assertions.assertTrue(failure.getMessage().contains("delete Delta table"));
        Assertions.assertFalse(failure.getMessage().contains(TEST_TOKEN));
        Assertions.assertTrue(connector.getMetadata(null)
                .getTableHandle(null, "default", "events").isPresent());
    }

    @Test
    public void testCatalogManagedWriterAcceptsInCommitTimestamp() throws Exception {
        Path table = copyCatalogManagedFixture();
        catalogManagedLocation = table.toUri().toString();
        Path initialCommit = table.resolve("_delta_log/00000000000000000000.json");
        Files.writeString(initialCommit, Files.readString(initialCommit).replace(
                "\"writerFeatures\":[\"catalogManaged\"]",
                "\"writerFeatures\":[\"catalogManaged\",\"inCommitTimestamp\"]"));
        DeltaConnector connector = new DeltaConnectorProvider().create(Map.of(
                "type", "delta",
                DeltaConnectorProperties.CATALOG_TYPE, DeltaConnectorProperties.CATALOG_TYPE_UNITY,
                DeltaConnectorProperties.UNITY_URI, workspaceUri,
                DeltaConnectorProperties.UNITY_CATALOG, "main",
                DeltaConnectorProperties.UNITY_TOKEN, TEST_TOKEN,
                DeltaConnectorProperties.WRITE_ENABLED, "true"), connectorContext());
        ConnectorTableHandle handle = connector.getMetadata(null)
                .getTableHandle(null, "default", "catalog_managed").orElseThrow();
        List<ConnectorColumn> columns = connector.getMetadata(null)
                .getTableSchema(null, handle).getColumns();

        Assertions.assertEquals(catalogManagedLocation, connector.getMetadata(null)
                .getWriteConfig(null, handle, columns).getWriteLocation());
        DeltaInsertHandle insert = connector.getMetadata(null).beginInsert(null, handle, columns);
        connector.getMetadata(null).abortInsert(null, insert);
    }

    @Test
    public void testCatalogManagedInsertUsesUnityCatalogCommitter() throws Exception {
        catalogManagedLocation = copyCatalogManagedFixture().toUri().toString();
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);
        UnityDeltaCatalogAdapter adapter = new UnityDeltaCatalogAdapter(
                "main", client, new org.apache.hadoop.conf.Configuration(), Map.of(
                        DeltaConnectorProperties.WRITE_ENABLED, "true"));
        DeltaTableHandle handle = adapter.getTableHandle("default", "catalog_managed")
                .orElseThrow();

        Assertions.assertFalse(handle.isExternalTable());
        DeltaInsertHandle insert = adapter.beginInsert(handle, "doris-catalog-managed-query");
        Path dataFile = Paths.get(URI.create(catalogManagedLocation)).resolve("part-doris.parquet");
        Files.write(dataFile, new byte[] {1, 2, 3, 4});

        DeltaInsertHandle deltaInsert = (DeltaInsertHandle) insert;
        deltaInsert.getWriter().finishInsert(deltaInsert, List.of(new DeltaFileCommitInfo(
                dataFile.toUri().toString(), 2, Files.size(dataFile),
                Files.getLastModifiedTime(dataFile).toMillis(), Map.of())));

        Assertions.assertTrue(updateRequestPaths.stream().anyMatch(
                path -> path.endsWith("/tables/catalog_managed")));
        try (java.util.stream.Stream<Path> stagedCommits = Files.list(
                Paths.get(URI.create(catalogManagedLocation)).resolve("_delta_log/_staged_commits"))) {
            Assertions.assertTrue(stagedCommits.anyMatch(
                    path -> path.getFileName().toString().startsWith("00000000000000000003.")));
        }
    }

    @Test
    public void testCatalogManagedOverwriteUsesUnityCatalogCommitter() throws Exception {
        catalogManagedLocation = copyCatalogManagedFixture().toUri().toString();
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);
        UnityDeltaCatalogAdapter adapter = new UnityDeltaCatalogAdapter(
                "main", client, new org.apache.hadoop.conf.Configuration(), Map.of(
                        DeltaConnectorProperties.WRITE_ENABLED, "true"));
        DeltaTableHandle handle = adapter.getTableHandle("default", "catalog_managed")
                .orElseThrow();
        Path dataFile = Paths.get(URI.create(catalogManagedLocation))
                .resolve("part-overwrite.parquet");
        Files.write(dataFile, new byte[] {9, 8, 7});

        DeltaInsertHandle overwrite = (DeltaInsertHandle) adapter.beginOverwrite(
                handle, "doris-catalog-managed-overwrite");
        overwrite.getWriter().finishInsert(overwrite, List.of(new DeltaFileCommitInfo(
                dataFile.toUri().toString(), 1, Files.size(dataFile),
                Files.getLastModifiedTime(dataFile).toMillis(), Map.of())));

        Path stagedCommit;
        try (java.util.stream.Stream<Path> stagedCommits = Files.list(
                Paths.get(URI.create(catalogManagedLocation)).resolve("_delta_log/_staged_commits"))) {
            stagedCommit = stagedCommits.filter(
                    path -> path.getFileName().toString().startsWith("00000000000000000003."))
                    .findFirst().orElseThrow();
        }
        String commit = Files.readString(stagedCommit);
        Assertions.assertEquals(2, countOccurrences(commit, "\"remove\""));
        Assertions.assertEquals(1, countOccurrences(commit, "\"add\""));
        Assertions.assertTrue(commit.contains("part-overwrite.parquet"));
    }

    @Test
    public void testRejectRecreatedUnityTableWhilePlanningPinnedHandle() {
        UnityDeltaClient client = UnityDeltaClient.create(workspaceUri, TEST_TOKEN);
        UnityDeltaCatalogAdapter adapter = new UnityDeltaCatalogAdapter(
                "main", client, new org.apache.hadoop.conf.Configuration(), Map.of());
        DeltaTableHandle handle = adapter.getTableHandle("default", "catalog_managed")
                .orElseThrow();

        catalogManagedTableId = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";

        DorisConnectorException exception = Assertions.assertThrows(
                DorisConnectorException.class, () -> adapter.loadSnapshot(handle));
        Assertions.assertTrue(exception.getMessage().contains("identity changed"));
    }

    @Test
    public void testUnityCatalogPropertiesRejectNonWorkspaceUris() {
        DeltaConnectorProvider provider = new DeltaConnectorProvider();
        Map<String, String> base = Map.of(
                "type", "delta",
                DeltaConnectorProperties.CATALOG_TYPE, DeltaConnectorProperties.CATALOG_TYPE_UNITY,
                DeltaConnectorProperties.UNITY_CATALOG, "main",
                DeltaConnectorProperties.UNITY_TOKEN, TEST_TOKEN);

        for (String invalidUri : List.of(
                "ftp://workspace.example.test",
                "http://workspace.example.test",
                "https:missing-host",
                "https://workspace.example.test/api/2.1/unity-catalog",
                "https://user@workspace.example.test",
                "https://workspace.example.test?token=unsafe",
                "https://workspace.example.test#fragment")) {
            Map<String, String> properties = new java.util.HashMap<>(base);
            properties.put(DeltaConnectorProperties.UNITY_URI, invalidUri);
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> provider.validateProperties(properties), invalidUri);
        }

        Map<String, String> insecureOauth = new java.util.HashMap<>(base);
        insecureOauth.put(DeltaConnectorProperties.UNITY_URI, "https://workspace.example.test");
        insecureOauth.put(DeltaConnectorProperties.UNITY_AUTH_TYPE, "oauth");
        insecureOauth.remove(DeltaConnectorProperties.UNITY_TOKEN);
        insecureOauth.put(DeltaConnectorProperties.UNITY_OAUTH_URI,
                "http://login.example.test/oauth/token");
        insecureOauth.put(DeltaConnectorProperties.UNITY_OAUTH_CLIENT_ID, "client-id");
        insecureOauth.put(DeltaConnectorProperties.UNITY_OAUTH_CLIENT_SECRET, "client-secret");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> provider.validateProperties(insecureOauth));
    }

    private void handleRequest(HttpExchange exchange) throws IOException {
        if (exchange.getRequestURI().getPath().equals("/oauth/token")) {
            requestPaths.add(exchange.getRequestURI().getPath());
            requestQueries.add(exchange.getRequestURI().getRawQuery());
            respond(exchange, 200,
                    "{\"access_token\":\"" + TEST_TOKEN + "\",\"expires_in\":3600}");
            return;
        }
        String authorization = exchange.getRequestHeaders().getFirst("Authorization");
        if (!("Bearer " + TEST_TOKEN).equals(authorization)) {
            respond(exchange, 401, "{\"error_code\":\"UNAUTHENTICATED\"}");
            return;
        }
        String path = exchange.getRequestURI().getPath();
        requestPaths.add(path);
        requestQueries.add(exchange.getRequestURI().getRawQuery());
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            updateRequestPaths.add(path);
        }
        if (path.equals("/api/2.1/unity-catalog/schemas")) {
            respond(exchange, 200,
                    "{\"schemas\":[{\"name\":\"default\",\"catalog_name\":\"main\"}]}");
            return;
        }
        if (path.equals("/api/2.1/unity-catalog/delta/v1/config")) {
            respond(exchange, 200, "{\"endpoints\":["
                    + "\"GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}\","
                    + "\"GET /v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/credentials\"],"
                    + "\"protocol-version\":\"" + deltaProtocolVersion + "\"}");
            return;
        }
        if (path.equals("/api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/staging-tables")) {
            respond(exchange, 200, "{\"table-id\":\"4ae3f0d4-7d7f-43d6-9d2f-1f6e6c8e14aa\","
                    + "\"table-type\":\"MANAGED\",\"location\":\""
                    + createStagingLocation + "\",\"storage-credentials\":[],"
                    + "\"required-protocol\":{\"min-reader-version\":3,"
                    + "\"min-writer-version\":7,\"writer-features\":["
                    + "\"catalogManaged\",\"vacuumProtocolCheck\"]},"
                    + "\"required-properties\":{\"delta.enableInCommitTimestamps\":\"true\"}}");
            return;
        }
        if (path.equals("/api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/tables")
                && "POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.getRequestBody().readAllBytes();
            updateRequestPaths.add(path);
            respond(exchange, 200, loadTableResponse(Map.of()));
            return;
        }
        String deltaTablePrefix =
                "/api/2.1/unity-catalog/delta/v1/catalogs/main/schemas/default/tables/";
        if (path.startsWith(deltaTablePrefix)
                && "DELETE".equalsIgnoreCase(exchange.getRequestMethod())) {
            if (deleteStatus != 200) {
                respond(exchange, deleteStatus,
                        "{\"error_code\":\"PERMISSION_DENIED\"}");
                return;
            }
            String tableName = path.substring(deltaTablePrefix.length());
            if (!deletedTables.add(tableName)) {
                respond(exchange, 404, "{\"error_code\":\"NOT_FOUND\"}");
                return;
            }
            respond(exchange, 200, "{}");
            return;
        }
        if (path.equals("/api/2.1/unity-catalog/tables")) {
            respond(exchange, 200, "{\"tables\":["
                    + "{\"name\":\"events\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"EXTERNAL\","
                    + "\"data_source_format\":\"DELTA\","
                    + "\"manifest_capabilities\":[\"HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT\"]},"
                    + "{\"name\":\"catalog_managed\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"MANAGED\","
                    + "\"data_source_format\":\"DELTA\","
                    + "\"manifest_capabilities\":[\"HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT\","
                    + "\"HAS_DIRECT_EXTERNAL_ENGINE_WRITE_SUPPORT\"]},"
                    + "{\"name\":\"blocked\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"MANAGED\","
                    + "\"data_source_format\":\"DELTA\","
                    + "\"manifest_capabilities\":[\"OTHER_CAPABILITY\"]},"
                    + "{\"name\":\"empty_capabilities\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"EXTERNAL\","
                    + "\"data_source_format\":\"DELTA\","
                    + "\"manifest_capabilities\":[]},"
                    + "{\"name\":\"missing_capabilities\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"EXTERNAL\","
                    + "\"data_source_format\":\"DELTA\"},"
                    + "{\"name\":\"propertyless\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"EXTERNAL\","
                    + "\"data_source_format\":\"DELTA\","
                    + "\"manifest_capabilities\":[\"HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT\"]},"
                    + "{\"name\":\"missing_type\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"EXTERNAL\","
                    + "\"data_source_format\":\"DELTA\","
                    + "\"manifest_capabilities\":[\"HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT\"]},"
                    + "{\"name\":\"policy_table\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"EXTERNAL\","
                    + "\"data_source_format\":\"DELTA\","
                    + "\"row_filter\":{\"expression\":\"id > 0\"},"
                    + "\"manifest_capabilities\":[\"HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT\"]},"
                    + "{\"name\":\"masked_table\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"EXTERNAL\","
                    + "\"data_source_format\":\"DELTA\",\"columns\":[{"
                    + "\"name\":\"id\",\"mask\":{\"function_name\":\"redact\"}}],"
                    + "\"manifest_capabilities\":[\"HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT\"]},"
                    + "{\"name\":\"official_manifest\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"EXTERNAL\","
                    + "\"data_source_format\":\"DELTA\","
                    + "\"securable_kind_manifest\":{\"capabilities\":["
                    + "\"HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT\"]}},"
                    + "{\"name\":\"raw\",\"catalog_name\":\"main\","
                    + "\"schema_name\":\"default\",\"table_type\":\"EXTERNAL\","
                    + "\"data_source_format\":\"PARQUET\"}]}");
            return;
        }
        if (path.endsWith("/tables/catalog_managed")
                && !"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.getRequestBody().readAllBytes();
            respond(exchange, 200, catalogManagedLoadTableResponse());
            return;
        }
        if (path.endsWith("/tables/catalog_managed")) {
            respond(exchange, 200, catalogManagedLoadTableResponse());
            return;
        }
        if (path.endsWith("/tables/events/credentials")) {
            String operation = exchange.getRequestURI().getRawQuery() != null
                    && exchange.getRequestURI().getRawQuery().contains("READ_WRITE")
                    ? "READ_WRITE" : "READ";
            respond(exchange, 200, "{\"storage-credentials\":[{"
                    + "\"prefix\":\"s3://delta-bucket/tables/events\","
                    + "\"operation\":\"" + operation + "\",\"config\":{"
                    + "\"s3.access-key-id\":\"temporary-ak\","
                    + "\"s3.secret-access-key\":\"temporary-sk\","
                    + "\"s3.session-token\":\"temporary-session\","
                    + "\"client.region\":\"us-east-2\"},"
                    + "\"expiration-time-ms\":" + (System.currentTimeMillis() + 3600000) + "}]}");
            return;
        }
        if (path.endsWith("/tables/gcs_events/credentials")) {
            respond(exchange, 200, "{\"storage-credentials\":[{"
                    + "\"prefix\":\"gs://delta-bucket/tables/events\","
                    + "\"operation\":\"READ\",\"config\":{"
                    + "\"gcs.oauth-token\":\"gcs-oauth\"},"
                    + "\"expiration-time-ms\":"
                    + (System.currentTimeMillis() + 3600000) + "}]}");
            return;
        }
        if (path.endsWith("/tables/events")) {
            if (deletedTables.contains("events")) {
                respond(exchange, 404, "{\"error_code\":\"NOT_FOUND\"}");
                return;
            }
            respond(exchange, 200, loadTableResponse(Map.of()));
            return;
        }
        if (path.endsWith("/tables/propertyless")) {
            respond(exchange, 200, loadTableResponseWithoutProperties(true));
            return;
        }
        if (path.endsWith("/tables/missing_type")) {
            respond(exchange, 200, loadTableResponseWithoutProperties(false));
            return;
        }
        respond(exchange, 404, "{\"error_code\":\"NOT_FOUND\"}");
    }

    private String loadTableResponse(Map<String, String> tableProperties) {
        String propertiesJson = tableProperties.isEmpty()
                ? "{}"
                : "{\"delta.feature.catalogManaged\":\"supported\"}";
        return "{\"metadata\":{\"etag\":\"test-etag\","
                + "\"table-type\":\"EXTERNAL\","
                + "\"table-uuid\":\"2ae93418-45d7-4f06-a899-d0379b3067d6\","
                + "\"location\":\"" + tableLocation + "\","
                + "\"partition-columns\":[],\"properties\":" + propertiesJson + ","
                + "\"last-commit-version\":1},\"commits\":[],\"latest-table-version\":1}";
    }

    private String loadTableResponseWithoutProperties(boolean includeTableType) {
        String tableType = includeTableType ? "\"table-type\":\"EXTERNAL\"," : "";
        return "{\"metadata\":{\"etag\":\"test-etag\"," + tableType
                + "\"table-uuid\":\"2ae93418-45d7-4f06-a899-d0379b3067d6\","
                + "\"location\":\"" + tableLocation + "\","
                + "\"partition-columns\":[],\"last-commit-version\":1},"
                + "\"commits\":[],\"latest-table-version\":1}";
    }

    private String catalogManagedLoadTableResponse() throws IOException {
        String firstCommit =
                "00000000000000000001.11111111-1111-1111-1111-111111111111.json";
        String secondCommit =
                "00000000000000000002.22222222-2222-2222-2222-222222222222.json";
        Path commitDirectory = Paths.get(URI.create(catalogManagedLocation))
                .resolve("_delta_log/_staged_commits");
        return "{\"metadata\":{\"etag\":\"catalog-managed-etag\","
                + "\"table-type\":\"MANAGED\","
                + "\"table-uuid\":\"" + catalogManagedTableId + "\","
                + "\"location\":\"" + catalogManagedLocation + "\","
                + "\"partition-columns\":[],\"properties\":{"
                + "\"delta.feature.catalogManaged\":\"supported\","
                + "\"delta.enableInCommitTimestamps\":\"true\"},"
                + "\"last-commit-version\":0},\"commits\":["
                + commitJson(1, firstCommit, Files.size(commitDirectory.resolve(firstCommit))) + ","
                + commitJson(2, secondCommit, Files.size(commitDirectory.resolve(secondCommit)))
                + "],\"latest-table-version\":2}";
    }

    private Path copyCatalogManagedFixture() throws Exception {
        URL fixture = Objects.requireNonNull(
                getClass().getClassLoader().getResource("delta/catalog_managed_table"));
        Path source = Paths.get(fixture.toURI());
        Path target = tempDirectory.resolve("catalog-managed-table");
        try (java.util.stream.Stream<Path> paths = Files.walk(source)) {
            paths.forEach(path -> {
                try {
                    Path relative = source.relativize(path);
                    Path destination = target.resolve(relative);
                    if (Files.isDirectory(path)) {
                        Files.createDirectories(destination);
                    } else {
                        Files.createDirectories(destination.getParent());
                        Files.copy(path, destination, StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
        }
        return target;
    }

    private static String commitJson(long version, String fileName, long fileSize) {
        return "{\"version\":" + version + ",\"timestamp\":"
                + (1700000000000L + version) + ",\"file-name\":\"" + fileName + "\","
                + "\"file-size\":" + fileSize + ",\"file-modification-timestamp\":"
                + (1700000000000L + version) + "}";
    }

    private static int countOccurrences(String value, String needle) {
        int count = 0;
        int offset = 0;
        while ((offset = value.indexOf(needle, offset)) >= 0) {
            count++;
            offset += needle.length();
        }
        return count;
    }

    private static DeltaCredentialsResponse credentials(
            String prefix, DeltaStorageCredentialConfig config) {
        return credentials(prefix, config, DeltaCredentialOperation.READ);
    }

    private static DeltaCredentialsResponse credentials(
            String prefix, DeltaStorageCredentialConfig config, DeltaCredentialOperation operation) {
        return new DeltaCredentialsResponse().addStorageCredentialsItem(
                new DeltaStorageCredential()
                        .prefix(prefix)
                        .operation(operation)
                        .config(config)
                        .expirationTimeMs(System.currentTimeMillis() + 3600000));
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private static ConnectorContext connectorContext() {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "unity_delta_test";
            }

            @Override
            public long getCatalogId() {
                return 2;
            }
        };
    }
}
