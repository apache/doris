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

package org.apache.doris.connector;

import org.apache.doris.connector.iceberg.IcebergCatalogOps;
import org.apache.doris.connector.iceberg.IcebergCatalogProperties;
import org.apache.doris.connector.iceberg.IcebergColumnHandle;
import org.apache.doris.connector.iceberg.IcebergScanPlanProvider;
import org.apache.doris.connector.iceberg.IcebergTableHandle;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.datasource.storage.StorageTypeId;
import org.apache.doris.filesystem.azure.AzureFileSystemProperties;
import org.apache.doris.filesystem.azure.AzureFileSystemProvider;
import org.apache.doris.foundation.property.StoragePropertiesException;
import org.apache.doris.kerberos.ExecutionAuthenticator;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Real scan planning and engine/provider binding; only the remote catalog response is substituted. */
class IcebergNativeStorageAccessIntegrationTest {
    private static final String TABLE_ROOT = "abfss://container@account.dfs.core.windows.net/table";
    private static final String DATA_PATH = TABLE_ROOT + "/data/p=a%2Fb/file.parquet";
    private static final String TOKEN = "sig=fresh-test-signature&se=2100-01-01T00:00:00Z";
    private static final Map<String, String> VENDED = Map.of(
            "adls.sas-token.account.dfs.core.windows.net", TOKEN,
            "adls.sas-token-expires-at-ms.account.dfs.core.windows.net", "4102444800000");
    private static final Map<String, String> CATALOG_PROPERTIES = Map.of(
            "iceberg.catalog.type", "rest", "iceberg.rest.vended-credentials-enabled", "true",
            "azure.account_name", "account", "azure.sas_token", "sig=expired-static-test-signature",
            "azure.sas_expiry_ms", "1");
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db", "t");
    private static final IcebergTableHandle HANDLE = new IcebergTableHandle("db", "t");
    private static final List<ConnectorColumnHandle> COLUMNS = List.of(new IcebergColumnHandle("id", 1));

    private final InMemoryCatalog catalog = new InMemoryCatalog();
    private final ConnectorStatementScopeImpl statementScope = new ConnectorStatementScopeImpl();
    private final ConnectorSession session = new TestSession(statementScope);

    @BeforeEach
    void initializeInMemoryMetadata() {
        // InMemoryCatalog uses InMemoryFileIO even for ABFSS-shaped locations. Commits write only
        // metadata/manifest bytes in memory; neither data files nor an Azure client are opened.
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db"));
    }

    @AfterEach
    void closeInMemoryMetadata() throws IOException {
        statementScope.closeAll();
        catalog.close();
    }

    @Test
    void scanEntryReplacesExpiredStaticSasAndSelectsNativeAzureForData() {
        Table table = tableWithData(2);
        IcebergScanPlanProvider provider = provider(table);

        Map<String, String> nodeProperties = provider.getScanNodeProperties(
                session, HANDLE, COLUMNS, Optional.empty());
        Map<String, String> backend = backendProperties(nodeProperties);
        assertFreshNativeAzure(backend);
        Assertions.assertEquals("parquet", nodeProperties.get(ScanNodePropertyKeys.FILE_FORMAT_TYPE));

        List<ConnectorScanRange> ranges = provider.planScan(session,
                ConnectorScanRequest.builder(HANDLE, COLUMNS).build());

        Assertions.assertEquals(1, ranges.size());
        ConnectorScanRange range = ranges.get(0);
        Assertions.assertEquals(Optional.of(DATA_PATH), range.getPath());
        Assertions.assertEquals(Optional.of("FILE_S3"), range.getBackendFileType());
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, populate(range).getFormatType());
        Assertions.assertTrue(range.getProperties().isEmpty(), "credentials belong to the shared scan binding");
    }

    @Test
    void scanRejectsDataOutsideItsSingleAzureCredentialPrefix() {
        IcebergScanPlanProvider provider = provider(tableWithData(2), CATALOG_PROPERTIES, VENDED,
                List.of(StorageCredential.create(TABLE_ROOT + "/other/", VENDED)));

        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.planScan(session, ConnectorScanRequest.builder(HANDLE, COLUMNS).build()));

        Assertions.assertTrue(error.getMessage().contains("outside the Azure storage credential prefix"));
        Assertions.assertFalse(error.getMessage().contains(TOKEN));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            TABLE_ROOT + "/data/",
            "wasbs://container@account.blob.core.windows.net/table/data/",
            "https://account.blob.core.windows.net/container/table/data/"
    })
    void dataPrefixDoesNotNeedToCoverTheMetadataRoot(String prefix) {
        IcebergScanPlanProvider provider = provider(tableWithData(2), CATALOG_PROPERTIES, VENDED,
                List.of(StorageCredential.create(prefix, VENDED)));

        Map<String, String> before = backendProperties(provider.getScanNodeProperties(
                session, HANDLE, COLUMNS, Optional.empty()));
        List<ConnectorScanRange> ranges = provider.planScan(session,
                ConnectorScanRequest.builder(HANDLE, COLUMNS).build());

        assertFreshNativeAzure(before);
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals(Optional.of(DATA_PATH), ranges.get(0).getPath());
        Assertions.assertEquals(Optional.of("FILE_S3"), ranges.get(0).getBackendFileType());
        Assertions.assertEquals(before, backendProperties(provider.getScanNodeProperties(
                session, HANDLE, COLUMNS, Optional.empty())),
                "revalidating the cached binding must not treat the metadata root as an opened object");
    }

    @Test
    void changingTheFutureWriteLocationDoesNotBypassOldAzureFileScopes() {
        Table table = tableWithData(2);
        table.updateProperties().set(TableProperties.WRITE_DATA_LOCATION, "s3://bucket/new-data").commit();
        IcebergScanPlanProvider provider = provider(table, mixedCatalogProperties(), VENDED,
                List.of(StorageCredential.create(TABLE_ROOT + "/other/", VENDED)));

        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.planScan(session, ConnectorScanRequest.builder(HANDLE, COLUMNS).build()));

        Assertions.assertTrue(error.getMessage().contains("outside the Azure storage credential prefix"));
    }

    @Test
    void scopedHistoricalAzureFilesKeepLegacyPlanningAfterTheWriteLocationChanges() throws IOException {
        Table table = tableWithData(2);
        table.updateProperties().set(TableProperties.WRITE_DATA_LOCATION, "s3://bucket/new-data").commit();

        assertMatchesUnscopedPlanning(table, VENDED, TABLE_ROOT + "/data/", DATA_PATH);
    }

    @Test
    void actualS3FilesIgnoreTheAzureCredentialPrefixAndKeepLegacyPlanning() throws IOException {
        String dataPath = "s3://bucket/new-data/file.parquet";
        Table table = tableWithData(2, dataPath);
        table.updateProperties().set(TableProperties.WRITE_DATA_LOCATION, "s3://bucket/new-data").commit();
        Map<String, String> fileIoProperties = new LinkedHashMap<>(VENDED);
        // The legacy normalizer consumes FileIO's vended map, not unrelated static bindings.
        // Keep both providers present without changing that existing planner contract.
        fileIoProperties.putAll(Map.of("s3.endpoint", "https://s3.us-west-2.amazonaws.com",
                "s3.region", "us-west-2", "s3.access-key-id", "s3-test-access",
                "s3.secret-access-key", "s3-test-secret"));

        assertMatchesUnscopedPlanning(table, fileIoProperties, TABLE_ROOT + "/other/", dataPath);
    }

    @Test
    void eachStatementKeepsThePrefixFromItsOwnCredentialGeneration() {
        List<StorageCredential> credentials = new ArrayList<>(List.of(
                StorageCredential.create(TABLE_ROOT + "/data/", VENDED)));
        IcebergScanPlanProvider provider = provider(tableWithData(2), CATALOG_PROPERTIES, VENDED, credentials);
        assertFreshNativeAzure(backendProperties(provider.getScanNodeProperties(
                session, HANDLE, COLUMNS, Optional.empty())));
        credentials.set(0, StorageCredential.create(TABLE_ROOT + "/other/", VENDED));
        ConnectorScanRequest request = ConnectorScanRequest.builder(HANDLE, COLUMNS).build();

        Assertions.assertEquals(1, provider.planScan(session, request).size());
        ConnectorStatementScopeImpl nextScope = new ConnectorStatementScopeImpl();
        try {
            ConnectorSession nextSession = new TestSession(nextScope);
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                    () -> provider.planScan(nextSession, request));
            Assertions.assertTrue(error.getMessage().contains("outside the Azure storage credential prefix"));
            Assertions.assertEquals(1, provider.planScan(session, request).size(),
                    "a new request must not replace a previously captured prefix");
        } finally {
            nextScope.closeAll();
        }
    }

    @Test
    void mixedStorageScansKeepCredentialsAndPrefixInTheSameStatementGeneration() {
        Table table = tableWithData(2);
        table.updateProperties().set(TableProperties.WRITE_DATA_LOCATION, "s3://bucket/new-data").commit();
        Map<String, String> fileIoProperties = new LinkedHashMap<>(VENDED);
        fileIoProperties.putAll(Map.of("s3.endpoint", "https://s3.us-west-2.amazonaws.com",
                "s3.region", "us-west-2", "s3.access_key", "s3-test-access", "s3.secret_key", "s3-test-secret"));
        List<StorageCredential> credentials = new ArrayList<>(List.of(
                StorageCredential.create(TABLE_ROOT + "/data/", VENDED)));
        IcebergScanPlanProvider provider = provider(table, mixedCatalogProperties(), fileIoProperties, credentials);
        Map<String, String> originalBackend = backendProperties(provider.getScanNodeProperties(
                session, HANDLE, COLUMNS, Optional.empty()));
        Assertions.assertEquals(TOKEN, originalBackend.get("AZURE_SAS_TOKEN"));
        String nextToken = "sig=next-scope-signature&se=2100-01-01T00:00:00Z";
        Map<String, String> nextCredentials = new LinkedHashMap<>(VENDED);
        nextCredentials.put("adls.sas-token.account.dfs.core.windows.net", nextToken);
        credentials.set(0, StorageCredential.create(TABLE_ROOT + "/other/", nextCredentials));

        Assertions.assertEquals(originalBackend, backendProperties(provider.getScanNodeProperties(
                session, HANDLE, COLUMNS, Optional.empty())), "the old prefix must not be paired with a new token");
        ConnectorScanRequest request = ConnectorScanRequest.builder(HANDLE, COLUMNS).build();
        Assertions.assertEquals(1, provider.planScan(session, request).size());
        ConnectorStatementScopeImpl nextScope = new ConnectorStatementScopeImpl();
        try {
            ConnectorSession nextSession = new TestSession(nextScope);
            Assertions.assertEquals(nextToken, backendProperties(provider.getScanNodeProperties(
                    nextSession, HANDLE, COLUMNS, Optional.empty())).get("AZURE_SAS_TOKEN"));
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                    () -> provider.planScan(nextSession, request));
            Assertions.assertTrue(error.getMessage().contains("outside the Azure storage credential prefix"));
            Assertions.assertEquals(originalBackend, backendProperties(provider.getScanNodeProperties(
                    session, HANDLE, COLUMNS, Optional.empty())));
        } finally {
            nextScope.closeAll();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"dfs", "blob"})
    void scanEntryBindsScopedSasAfterProviderFileIoExpiryNormalization(String service) {
        String host = "account." + service + ".core.windows.net";
        Map<String, String> rawScoped = Map.of(
                "adls.sas-token." + host, TOKEN,
                "adls.sas-token-expires-at-ms." + host, "4102444800500");
        AzureFileSystemProperties binding = new AzureFileSystemProvider()
                .bindVended(rawScoped, Collections.emptyMap()).orElseThrow();
        Map<String, String> fileIoProperties = binding.toIcebergFileIOProperties();
        Assertions.assertEquals("4102444800000",
                fileIoProperties.get("adls.sas-token-expires-at-ms.account.dfs.core.windows.net"));
        Assertions.assertEquals("4102444800000",
                fileIoProperties.get("adls.sas-token-expires-at-ms.account.blob.core.windows.net"));
        // REST retains the original scoped credential alongside the provider-transformed FileIO
        // properties. A per-key overlay would restore only one host to the later raw expiry.
        IcebergScanPlanProvider provider = provider(tableWithData(2), CATALOG_PROPERTIES, fileIoProperties,
                List.of(StorageCredential.create(TABLE_ROOT, rawScoped)));

        Map<String, String> backend = backendProperties(provider.getScanNodeProperties(
                session, HANDLE, COLUMNS, Optional.empty()));
        assertFreshNativeAzure(backend);
        List<ConnectorScanRange> ranges = provider.planScan(session,
                ConnectorScanRequest.builder(HANDLE, COLUMNS).build());

        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals(Optional.of("FILE_S3"), ranges.get(0).getBackendFileType());
        Assertions.assertEquals(Optional.of(DATA_PATH), ranges.get(0).getPath());
    }

    @ParameterizedTest
    @ValueSource(strings = {"dfs", "blob"})
    void scanEntryDoesNotInheritFileIoExpiryAfterScopedSasRotation(String service) {
        AzureFileSystemProperties previous = new AzureFileSystemProvider()
                .bindVended(VENDED, Collections.emptyMap()).orElseThrow();
        Map<String, String> fileIoProperties = previous.toIcebergFileIOProperties();
        String rotatedToken = "si=stored-policy&sig=rotated-test-signature";
        Map<String, String> rawScoped = Map.of(
                "adls.sas-token.account." + service + ".core.windows.net", rotatedToken);
        IcebergScanPlanProvider provider = provider(tableWithData(2), CATALOG_PROPERTIES, fileIoProperties,
                List.of(StorageCredential.create(TABLE_ROOT, rawScoped)));

        Map<String, String> backend = backendProperties(provider.getScanNodeProperties(
                session, HANDLE, COLUMNS, Optional.empty()));

        Assertions.assertEquals("azure", backend.get("provider"));
        Assertions.assertEquals("SAS", backend.get("AZURE_AUTH_TYPE"));
        Assertions.assertEquals(rotatedToken, backend.get("AZURE_SAS_TOKEN"));
        Assertions.assertFalse(backend.containsKey("AZURE_SAS_EXPIRY_MS"),
                "unknown expiry on the selected SAS must not inherit the previous FileIO's known expiry");
        Assertions.assertEquals("https://account.blob.core.windows.net", backend.get("AZURE_ENDPOINT"));
        Assertions.assertEquals("account", backend.get("AZURE_ACCOUNT_NAME"));
        List<ConnectorScanRange> ranges = provider.planScan(session,
                ConnectorScanRequest.builder(HANDLE, COLUMNS).build());
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals(Optional.of("FILE_S3"), ranges.get(0).getBackendFileType());
    }

    @Test
    void metadataTaskDoesNotSendNativeAzureCredentialsAlongsideItsFileIo() {
        IcebergScanPlanProvider provider = provider(tableWithData(2));
        IcebergTableHandle allManifests = IcebergTableHandle.forSystemTable(
                "db", "t", "all_manifests", -1L, null, -1L);

        Map<String, String> nodeProperties = provider.getScanNodeProperties(
                session, allManifests, Collections.emptyList(), Optional.empty());

        Assertions.assertEquals("jni", nodeProperties.get(ScanNodePropertyKeys.FILE_FORMAT_TYPE));
        Map<String, String> backend = backendProperties(nodeProperties);
        Assertions.assertFalse(backend.containsKey("provider"), "the serialized FileIO owns metadata storage");
        Assertions.assertTrue(backend.keySet().stream().noneMatch(key -> key.startsWith("AZURE_")),
                "native credentials must not be copied into JNI's Hadoop parameter channel");
    }

    @Test
    void allManifestsEntryDoesNotAccessExpiredNativeCredentials() {
        IcebergScanPlanProvider provider = provider(tableWithData(2));
        IcebergTableHandle allManifests = IcebergTableHandle.forSystemTable(
                "db", "t", "all_manifests", -1L, null, -1L);

        Map<String, String> nodeProperties = provider.getScanNodeProperties(
                session, allManifests, Collections.emptyList(), Optional.empty());

        Assertions.assertEquals("jni", nodeProperties.get(ScanNodePropertyKeys.FILE_FORMAT_TYPE));
        Assertions.assertTrue(backendProperties(nodeProperties).isEmpty(),
                "metadata must not access an expired native binding when its FileIO already owns authentication");
    }

    @Test
    void allManifestsRetainsTheIndependentHdfsBindingWithoutAzureNativeCredentials() {
        Map<String, String> catalogProperties = new LinkedHashMap<>(CATALOG_PROPERTIES);
        catalogProperties.put("fs.defaultFS", "hdfs://namenode:8020");
        catalogProperties.put("hadoop.username", "metadata-reader");
        IcebergScanPlanProvider provider = provider(tableWithData(2), catalogProperties);
        IcebergTableHandle allManifests = IcebergTableHandle.forSystemTable(
                "db", "t", "all_manifests", -1L, null, -1L);

        Map<String, String> backend = backendProperties(provider.getScanNodeProperties(
                session, allManifests, Collections.emptyList(), Optional.empty()));

        Assertions.assertEquals("hdfs://namenode:8020", backend.get("fs.defaultFS"));
        Assertions.assertEquals("metadata-reader", backend.get("hadoop.username"));
        Assertions.assertFalse(backend.containsKey("provider"));
        Assertions.assertTrue(backend.keySet().stream().noneMatch(key -> key.startsWith("AZURE_")));
    }

    @ParameterizedTest
    @CsvSource({"PARQUET,2,1", "PUFFIN,3,3"})
    void dataAndPositionDeleteScansShareNativeCredentialsAndReader(
            FileFormat deleteFormat, int formatVersion, int content) {
        Table table = tableWithData(formatVersion);
        String deletePath = TABLE_ROOT + "/delete/p=a%2Fb/"
                + (deleteFormat == FileFormat.PUFFIN ? "vector.puffin" : "positions.parquet");
        FileMetadata.Builder delete = FileMetadata.deleteFileBuilder(table.spec())
                .ofPositionDeletes()
                .withPath(deletePath)
                .withFormat(deleteFormat)
                .withFileSizeInBytes(256L)
                .withRecordCount(4L);
        if (deleteFormat == FileFormat.PUFFIN) {
            delete.withReferencedDataFile(DATA_PATH).withContentOffset(16L).withContentSizeInBytes(64L);
        }
        table.newRowDelta().addDeletes(delete.build()).commit();
        IcebergScanPlanProvider provider = provider(table);

        Map<String, String> dataBackend = backendProperties(provider.getScanNodeProperties(
                session, HANDLE, COLUMNS, Optional.empty()));
        assertFreshNativeAzure(dataBackend);
        List<ConnectorScanRange> dataRanges = provider.planScan(session,
                ConnectorScanRequest.builder(HANDLE, COLUMNS).build());
        Assertions.assertEquals(1, dataRanges.size());
        ConnectorScanRange dataRange = dataRanges.get(0);
        Assertions.assertEquals(Optional.of(DATA_PATH), dataRange.getPath());
        Assertions.assertEquals(Optional.of("FILE_S3"), dataRange.getBackendFileType());
        TIcebergFileDesc dataFile = populate(dataRange).getTableFormatParams().getIcebergParams();
        Assertions.assertEquals(1, dataFile.getDeleteFilesSize());
        TIcebergDeleteFileDesc dataDelete = dataFile.getDeleteFiles().get(0);
        Assertions.assertEquals(deletePath, dataDelete.getPath());
        Assertions.assertEquals(content, dataDelete.getContent());

        IcebergTableHandle positionDeletes = IcebergTableHandle.forSystemTable(
                "db", "t", "position_deletes", -1L, null, -1L);
        Map<String, String> deleteBackend = backendProperties(provider.getScanNodeProperties(
                session, positionDeletes, Collections.emptyList(), Optional.empty()));
        List<ConnectorScanRange> deleteRanges = provider.planScan(session,
                ConnectorScanRequest.builder(positionDeletes, Collections.emptyList()).build());

        Assertions.assertEquals(dataBackend, deleteBackend,
                "ordinary data and native position_deletes must use the same resolved Azure credentials");
        Assertions.assertEquals(1, deleteRanges.size());
        ConnectorScanRange deleteRange = deleteRanges.get(0);
        Assertions.assertEquals(Optional.of(deletePath), deleteRange.getPath());
        Assertions.assertEquals(dataRange.getBackendFileType(), deleteRange.getBackendFileType());
        TFileRangeDesc descriptor = populate(deleteRange);
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, descriptor.getFormatType());
        TIcebergFileDesc deleteFile = descriptor.getTableFormatParams().getIcebergParams();
        Assertions.assertEquals(content, deleteFile.getContent());
        Assertions.assertFalse(deleteFile.isSetSerializedSplit(), "position_deletes is not a JNI metadata task");
        Assertions.assertEquals(1, deleteFile.getDeleteFilesSize());
        TIcebergDeleteFileDesc deleteEntry = deleteFile.getDeleteFiles().get(0);
        Assertions.assertEquals(deletePath, deleteEntry.getOriginalPath());
        Assertions.assertEquals(content, deleteEntry.getContent());
        if (deleteFormat == FileFormat.PUFFIN) {
            Assertions.assertEquals(DATA_PATH, deleteEntry.getReferencedDataFilePath());
            Assertions.assertEquals(16L, dataDelete.getContentOffset());
            Assertions.assertEquals(64L, dataDelete.getContentSizeInBytes());
            Assertions.assertEquals(dataDelete.getContentOffset(), deleteEntry.getContentOffset());
            Assertions.assertEquals(dataDelete.getContentSizeInBytes(), deleteEntry.getContentSizeInBytes());
        }
    }

    @ParameterizedTest
    @CsvSource({"PARQUET,2,false", "PARQUET,2,true", "PUFFIN,3,false", "PUFFIN,3,true"})
    void dataAndPositionDeleteScansRejectDeleteFilesOutsideTheCredentialPrefix(
            FileFormat format, int version, boolean positionDeletes) {
        Table table = tableWithData(version);
        FileMetadata.Builder delete = FileMetadata.deleteFileBuilder(table.spec())
                .ofPositionDeletes().withPath(TABLE_ROOT + "/delete/outside." + format.name().toLowerCase(Locale.ROOT))
                .withFormat(format).withFileSizeInBytes(256L).withRecordCount(4L);
        if (format == FileFormat.PUFFIN) {
            delete.withReferencedDataFile(DATA_PATH).withContentOffset(16L).withContentSizeInBytes(64L);
        }
        table.newRowDelta().addDeletes(delete.build()).commit();
        IcebergScanPlanProvider provider = provider(table, CATALOG_PROPERTIES, VENDED,
                List.of(StorageCredential.create(TABLE_ROOT + "/data/", VENDED)));
        IcebergTableHandle handle = positionDeletes
                ? IcebergTableHandle.forSystemTable("db", "t", "position_deletes", -1L, null, -1L) : HANDLE;
        List<ConnectorColumnHandle> columns = positionDeletes ? Collections.emptyList() : COLUMNS;

        assertFreshNativeAzure(backendProperties(provider.getScanNodeProperties(
                session, handle, columns, Optional.empty())));
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.planScan(session, ConnectorScanRequest.builder(handle, columns).build()));

        Assertions.assertTrue(error.getMessage().contains("outside the Azure storage credential prefix"));
        Assertions.assertFalse(error.getMessage().contains(TOKEN));
    }

    @Test
    void hdfsDataCanBePlannedWhenOnlyIcebergFileIoCanAccessTheMetadataRoot() {
        String dataPath = "hdfs://namenode:8020/table/data.parquet";
        Table table = catalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned(),
                "s3://metadata-bucket/table", Map.of(TableProperties.FORMAT_VERSION, "2"));
        table.newAppend().appendFile(DataFiles.builder(table.spec())
                .withPath(dataPath).withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(512L).withRecordCount(10L).build()).commit();
        Map<String, String> properties = Map.of("iceberg.catalog.type", "rest",
                "fs.defaultFS", "hdfs://namenode:8020", "hadoop.username", "reader");
        StorageAdapter hdfs = StorageAdapter.ofProvider("HDFS", properties);
        DefaultConnectorContext context = new DefaultConnectorContext("hdfs_test", 1L,
                () -> new ExecutionAuthenticator() {}, () -> Map.of(StorageTypeId.HDFS, hdfs), () -> properties);
        Assertions.assertThrows(StoragePropertiesException.class,
                () -> context.newStorageAccessResolver(Collections.emptyMap()).apply(table.location()),
                "only the Iceberg FileIO, not Doris data storage, can resolve this metadata root");
        Catalog remoteCatalog = Mockito.mock(Catalog.class);
        Mockito.when(remoteCatalog.loadTable(TABLE_ID)).thenReturn(table);
        IcebergScanPlanProvider provider = new IcebergScanPlanProvider(IcebergCatalogProperties.of(properties),
                new IcebergCatalogOps.CatalogBackedIcebergCatalogOps(remoteCatalog), context);

        Map<String, String> backend = backendProperties(provider.getScanNodeProperties(
                session, HANDLE, COLUMNS, Optional.empty()));
        List<ConnectorScanRange> ranges = provider.planScan(session,
                ConnectorScanRequest.builder(HANDLE, COLUMNS).build());

        Assertions.assertEquals("hdfs://namenode:8020", backend.get("fs.defaultFS"));
        Assertions.assertFalse(backend.containsKey("AZURE_AUTH_TYPE"));
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals(Optional.of(dataPath), ranges.get(0).getPath());
        Assertions.assertTrue(ranges.get(0).getBackendFileType().isEmpty(), "keep legacy HDFS range routing");
    }

    private Table tableWithData(int formatVersion) {
        return tableWithData(formatVersion, DATA_PATH);
    }

    private Table tableWithData(int formatVersion, String dataPath) {
        Table table = catalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned(), TABLE_ROOT,
                Map.of(TableProperties.FORMAT_VERSION, Integer.toString(formatVersion),
                        TableProperties.DEFAULT_FILE_FORMAT, "parquet"));
        table.newAppend().appendFile(DataFiles.builder(table.spec())
                .withPath(dataPath)
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(512L)
                .withRecordCount(10L)
                .build()).commit();
        return table;
    }

    private static Map<String, String> mixedCatalogProperties() {
        Map<String, String> properties = new LinkedHashMap<>(CATALOG_PROPERTIES);
        properties.remove("azure.sas_token");
        properties.remove("azure.sas_expiry_ms");
        properties.put("azure.account_key", "azure-test-key");
        properties.put("azure.endpoint", "https://account.blob.core.windows.net");
        properties.putAll(Map.of("s3.endpoint", "https://s3.us-west-2.amazonaws.com",
                "s3.region", "us-west-2", "s3.access_key", "s3-test-access", "s3.secret_key", "s3-test-secret"));
        return properties;
    }

    private void assertMatchesUnscopedPlanning(Table table, Map<String, String> fileIoProperties,
            String azurePrefix, String expectedPath) throws IOException {
        Map<String, String> catalogProperties = mixedCatalogProperties();
        IcebergScanPlanProvider baseline = provider(table, catalogProperties, fileIoProperties, Collections.emptyList());
        IcebergScanPlanProvider scoped = provider(table, catalogProperties, fileIoProperties,
                List.of(StorageCredential.create(azurePrefix, VENDED)));
        ConnectorStatementScopeImpl baselineScope = new ConnectorStatementScopeImpl();
        try {
            ConnectorSession baselineSession = new TestSession(baselineScope);
            Map<String, String> baselineBackend = backendProperties(baseline.getScanNodeProperties(
                    baselineSession, HANDLE, COLUMNS, Optional.empty()));
            Map<String, String> scopedBackend = backendProperties(scoped.getScanNodeProperties(
                    session, HANDLE, COLUMNS, Optional.empty()));
            ConnectorScanRequest request = ConnectorScanRequest.builder(HANDLE, COLUMNS).build();
            List<ConnectorScanRange> baselineRanges = baseline.planScan(baselineSession, request);
            List<ConnectorScanRange> scopedRanges = scoped.planScan(session, request);

            Assertions.assertEquals("s3-test-access", baselineBackend.get("AWS_ACCESS_KEY"));
            Assertions.assertEquals("https://account.blob.core.windows.net", baselineBackend.get("AZURE_ENDPOINT"));
            Assertions.assertEquals(TOKEN, baselineBackend.get("AZURE_SAS_TOKEN"));
            Assertions.assertEquals(baselineBackend, scopedBackend);
            Assertions.assertEquals(1, baselineRanges.size());
            Assertions.assertEquals(1, scopedRanges.size());
            ConnectorScanRange baselineRange = baselineRanges.get(0);
            ConnectorScanRange scopedRange = scopedRanges.get(0);
            Assertions.assertEquals(Optional.of(expectedPath), baselineRange.getPath());
            Assertions.assertEquals(baselineRange.getPath(), scopedRange.getPath());
            Assertions.assertTrue(baselineRange.getBackendFileType().isEmpty(), "keep legacy per-file routing");
            Assertions.assertEquals(baselineRange.getBackendFileType(), scopedRange.getBackendFileType());
            Assertions.assertEquals(baselineRange.getStart(), scopedRange.getStart());
            Assertions.assertEquals(baselineRange.getLength(), scopedRange.getLength());
            Assertions.assertEquals(baselineRange.getFileSize(), scopedRange.getFileSize());
            Assertions.assertEquals(baselineRange.getProperties(), scopedRange.getProperties());
            Assertions.assertEquals(populate(baselineRange), populate(scopedRange),
                    "scope protection must not change the legacy range payload; no cloud IO runs in this fixture");
        } finally {
            baselineScope.closeAll();
        }
    }

    private IcebergScanPlanProvider provider(Table table) {
        return provider(table, CATALOG_PROPERTIES);
    }

    private IcebergScanPlanProvider provider(Table table, Map<String, String> catalogProperties) {
        return provider(table, catalogProperties, VENDED, Collections.emptyList());
    }

    private IcebergScanPlanProvider provider(Table table, Map<String, String> catalogProperties,
            Map<String, String> fileIoProperties, List<StorageCredential> credentials) {
        StorageAdapter staticAzure = StorageAdapter.ofProvider("AZURE", catalogProperties);
        if (catalogProperties.containsKey("azure.sas_expiry_ms")) {
            Assertions.assertThrows(StoragePropertiesException.class, staticAzure::getBackendConfigProperties,
                    "the static credential must actually be expired, not a prebuilt fake backend map");
        }
        DefaultConnectorContext context = new DefaultConnectorContext("azure_test", 1L,
                () -> new ExecutionAuthenticator() {}, () -> Map.of(StorageTypeId.AZURE, staticAzure),
                () -> catalogProperties);

        // A real BaseTable is frozen through TableOperations by the statement scope. Supplying the
        // vended FileIO there, rather than only overriding Table.io(), keeps that actual path intact.
        FileIO vendedIo = Mockito.mock(FileIO.class, Mockito.withSettings()
                .extraInterfaces(SupportsStorageCredentials.class)
                .defaultAnswer(AdditionalAnswers.delegatesTo(table.io())));
        Mockito.doReturn(fileIoProperties).when(vendedIo).properties();
        Mockito.doReturn(credentials).when((SupportsStorageCredentials) vendedIo).credentials();
        TableOperations operations = Mockito.spy(((BaseTable) table).operations());
        Mockito.doReturn(vendedIo).when(operations).io();
        Table authorizedTable = new BaseTable(operations, table.name());
        Catalog remoteCatalog = Mockito.mock(Catalog.class);
        Mockito.when(remoteCatalog.loadTable(TABLE_ID)).thenReturn(authorizedTable);

        return new IcebergScanPlanProvider(IcebergCatalogProperties.of(catalogProperties),
                new IcebergCatalogOps.CatalogBackedIcebergCatalogOps(remoteCatalog), context);
    }

    private static Map<String, String> backendProperties(Map<String, String> nodeProperties) {
        Map<String, String> backend = new LinkedHashMap<>();
        nodeProperties.forEach((key, value) -> {
            if (key.startsWith(ScanNodePropertyKeys.LOCATION_PREFIX)) {
                backend.put(key.substring(ScanNodePropertyKeys.LOCATION_PREFIX.length()), value);
            }
        });
        return backend;
    }

    private static void assertFreshNativeAzure(Map<String, String> backend) {
        Assertions.assertEquals("azure", backend.get("provider"));
        Assertions.assertEquals("SAS", backend.get("AZURE_AUTH_TYPE"));
        Assertions.assertEquals("account", backend.get("AZURE_ACCOUNT_NAME"));
        Assertions.assertEquals("https://account.blob.core.windows.net", backend.get("AZURE_ENDPOINT"));
        Assertions.assertEquals(TOKEN, backend.get("AZURE_SAS_TOKEN"));
        Assertions.assertEquals("4102444800000", backend.get("AZURE_SAS_EXPIRY_MS"));
        Assertions.assertEquals(Set.of("provider", "AZURE_AUTH_TYPE", "AZURE_ACCOUNT_NAME", "AZURE_ENDPOINT",
                "AZURE_SAS_TOKEN", "AZURE_SAS_EXPIRY_MS"), backend.keySet(),
                "credential scope stays in the FE request, outside the native backend dialect");
        Assertions.assertTrue(backend.keySet().stream()
                .allMatch(key -> key.equals("provider") || key.startsWith("AZURE_")));
        Assertions.assertFalse(backend.containsKey("AZURE_CONTAINER"));
        Assertions.assertFalse(backend.containsKey("AZURE_ACCOUNT_KEY"));
        Assertions.assertFalse(backend.containsKey("AZURE_CLIENT_SECRET"));
    }

    private static TFileRangeDesc populate(ConnectorScanRange range) {
        TTableFormatFileDesc format = new TTableFormatFileDesc();
        format.setTableFormatType(range.getTableFormatType());
        TFileRangeDesc descriptor = new TFileRangeDesc();
        descriptor.setPath(range.getPath().orElseThrow());
        range.populateRangeParams(format, descriptor);
        descriptor.setTableFormatParams(format);
        return descriptor;
    }

    private static final class TestSession implements ConnectorSession {
        private final ConnectorStatementScope scope;

        private TestSession(ConnectorStatementScope scope) {
            this.scope = scope;
        }

        @Override
        public String getQueryId() {
            return "azure-native-storage-test";
        }

        @Override
        public String getUser() {
            return "test-user";
        }

        @Override
        public String getTimeZone() {
            return "UTC";
        }

        @Override
        public String getLocale() {
            return "en_US";
        }

        @Override
        public long getCatalogId() {
            return 1L;
        }

        @Override
        public String getCatalogName() {
            return "azure_test";
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return CATALOG_PROPERTIES;
        }

        @Override
        public ConnectorStatementScope getStatementScope() {
            return scope;
        }
    }
}
