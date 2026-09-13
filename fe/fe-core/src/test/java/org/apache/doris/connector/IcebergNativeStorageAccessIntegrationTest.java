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
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
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
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
        Table table = tableWithData();
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
        IcebergScanPlanProvider provider = provider(tableWithData(), VENDED,
                List.of(StorageCredential.create(TABLE_ROOT + "/other/", VENDED)));

        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider.planScan(session, ConnectorScanRequest.builder(HANDLE, COLUMNS).build()));

        Assertions.assertTrue(error.getMessage().contains("outside the Azure storage credential prefix"));
        Assertions.assertFalse(error.getMessage().contains(TOKEN));
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
        IcebergScanPlanProvider provider = provider(tableWithData(), fileIoProperties,
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

    @Test
    void metadataTaskDoesNotSendNativeAzureCredentialsAlongsideItsFileIo() {
        IcebergScanPlanProvider provider = provider(tableWithData());
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

    private Table tableWithData() {
        Table table = catalog.createTable(TABLE_ID, SCHEMA, PartitionSpec.unpartitioned(), TABLE_ROOT,
                Map.of(TableProperties.FORMAT_VERSION, "2",
                        TableProperties.DEFAULT_FILE_FORMAT, "parquet"));
        table.newAppend().appendFile(DataFiles.builder(table.spec())
                .withPath(DATA_PATH)
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(512L)
                .withRecordCount(10L)
                .build()).commit();
        return table;
    }

    private IcebergScanPlanProvider provider(Table table) {
        return provider(table, VENDED, Collections.emptyList());
    }

    private IcebergScanPlanProvider provider(Table table, Map<String, String> fileIoProperties,
            List<StorageCredential> credentials) {
        StorageAdapter staticAzure = StorageAdapter.ofProvider("AZURE", CATALOG_PROPERTIES);
        Assertions.assertThrows(StoragePropertiesException.class, staticAzure::getBackendConfigProperties,
                "the static credential must actually be expired, not a prebuilt fake backend map");
        DefaultConnectorContext context = new DefaultConnectorContext("azure_test", 1L,
                () -> new ExecutionAuthenticator() {}, () -> Map.of(StorageTypeId.AZURE, staticAzure),
                () -> CATALOG_PROPERTIES);

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

        return new IcebergScanPlanProvider(IcebergCatalogProperties.of(CATALOG_PROPERTIES),
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
        Assertions.assertEquals(Map.of("provider", "azure", "AZURE_AUTH_TYPE", "SAS",
                "AZURE_ACCOUNT_NAME", "account", "AZURE_ENDPOINT", "https://account.blob.core.windows.net",
                "AZURE_SAS_TOKEN", TOKEN, "AZURE_SAS_EXPIRY_MS", "4102444800000"), backend,
                "credential scope stays in the FE request, outside the native backend dialect");
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
