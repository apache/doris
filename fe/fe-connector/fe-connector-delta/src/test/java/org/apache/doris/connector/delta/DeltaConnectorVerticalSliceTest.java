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

import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TTableFormatFileDesc;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.VariantType;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

public class DeltaConnectorVerticalSliceTest {

    @TempDir
    Path tempDirectory;

    @Test
    public void testProviderIsDiscoverableAndValidatesSingleTableProperties() throws Exception {
        DeltaConnectorProvider provider = ServiceLoader.load(ConnectorProvider.class).stream()
                .map(ServiceLoader.Provider::get)
                .filter(candidate -> "delta".equals(candidate.getType()))
                .map(DeltaConnectorProvider.class::cast)
                .findFirst()
                .orElseThrow();

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> provider.validateProperties(Map.of()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> provider.validateProperties(Map.of(
                        DeltaConnectorProperties.WRITE_ENABLED, "yes")));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> provider.validateProperties(Map.of(
                        DeltaConnectorProperties.DROP_ENABLED, "yes")));
        Map<String, String> invalidTimeouts = new LinkedHashMap<>(deltaProperties("delta/path_table"));
        invalidTimeouts.put(DeltaConnectorProperties.UNITY_READ_TIMEOUT_MS, "0");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> provider.validateProperties(invalidTimeouts));
        Map<String, String> destructivePath = new LinkedHashMap<>(
                deltaProperties("delta/path_table"));
        destructivePath.put(DeltaConnectorProperties.WRITE_ENABLED, "true");
        destructivePath.put(DeltaConnectorProperties.DROP_ENABLED, "true");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> provider.validateProperties(destructivePath));

        Map<String, String> properties = deltaProperties("delta/path_table");
        provider.validateProperties(properties);
        DeltaConnector connector = provider.create(properties, connectorContext());
        Assertions.assertTrue(connector.testConnection(null).isSuccess());
        Assertions.assertTrue(connector.defaultTestConnection());
        Assertions.assertTrue(connector.getScanPlanProvider().ignorePartitionPruneShortCircuit());
        Assertions.assertTrue(connector.getCapabilities().contains(
                ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT));
        Assertions.assertNotSame(connector.getMetadata(null), connector.getMetadata(null));
        Assertions.assertNotNull(connector.getScanPlanProvider());
        ConnectorTableHandle handle = connector.getMetadata(null)
                .getTableHandle(null, "default", "events").orElseThrow();
        Assertions.assertEquals(2, connector.getScanPlanProvider()
                .planScan(null, handle, List.of(), java.util.Optional.empty()).size());
    }

    @Test
    public void testPathCatalogCreatesVersionZeroTable() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("type", "delta");
        properties.put(DeltaConnectorProperties.CATALOG_TYPE,
                DeltaConnectorProperties.CATALOG_TYPE_PATH);
        properties.put(DeltaConnectorProperties.DATABASE, "default");
        properties.put(DeltaConnectorProperties.TABLE, "created_events");
        properties.put(DeltaConnectorProperties.TABLE_PATH,
                tempDirectory.resolve("created-events").toUri().toString());
        properties.put(DeltaConnectorProperties.WRITE_ENABLED, "true");
        DeltaConnector connector = new DeltaConnectorProvider().create(
                properties, connectorContext());
        ConnectorCreateTableRequest request = DeltaTestUtils.createRequest(
                "default", new ConnectorTableSchema("created_events", List.of(
                        new ConnectorColumn("id", ConnectorType.of("BIGINT"), "", false, null),
                        new ConnectorColumn("payload", ConnectorType.of("STRING"), "", true, null)),
                        "DELTA", Map.of()), List.of(), Map.of("owner", "doris"),
                "created by test", false);

        Assertions.assertEquals(java.util.EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE,
                WriteOperation.DELETE, WriteOperation.UPDATE, WriteOperation.MERGE),
                connector.getWritePlanProvider().supportedOperations());
        Assertions.assertTrue(connector.getWritePlanProvider().supportsCopyOnWriteDml());
        Assertions.assertFalse(connector.getCatalogAdapter().supportsDropTable());
        Assertions.assertTrue(connector.testConnection(null).isSuccess());
        Assertions.assertTrue(connector.getMetadata(null)
                .listTableNames(null, "default").isEmpty());
        ConnectorCreateTableRequest partitioned = DeltaTestUtils.createRequest(
                request.getDbName(), new ConnectorTableSchema(request.getTableName(), request.getColumns(),
                        "DELTA", Map.of()), List.of("missing"),
                request.getProperties(), request.getComment(), false);
        Assertions.assertThrows(DorisConnectorException.class,
                () -> connector.getMetadata(null).createTable(null, partitioned));
        ConnectorCreateTableRequest withDefault = DeltaTestUtils.createRequest(
                "default", new ConnectorTableSchema("created_events", List.of(
                        new ConnectorColumn("id", ConnectorType.of("BIGINT"), "", false, "1")),
                        "DELTA", Map.of()), List.of(), Map.of(), "", false);
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> connector.getMetadata(null).createTable(null, withDefault));
        connector.getMetadata(null).createTable(null, request);
        Assertions.assertEquals(List.of("created_events"), connector.getMetadata(null)
                .listTableNames(null, "default"));
        DeltaTableHandle handle = (DeltaTableHandle) connector.getMetadata(null)
                .getTableHandle(null, "default", "created_events").orElseThrow();
        Assertions.assertEquals(0, handle.getSnapshotVersion());

        ConnectorCreateTableRequest ifNotExists = DeltaTestUtils.createRequest(
                request.getDbName(), new ConnectorTableSchema(request.getTableName(), request.getColumns(),
                        "DELTA", Map.of()), List.of(),
                request.getProperties(), request.getComment(), true);
        connector.getMetadata(null).createTable(null, ifNotExists);
        Assertions.assertEquals(0, ((DeltaTableHandle) connector.getMetadata(null)
                .getTableHandle(null, "default", "created_events").orElseThrow()).getSnapshotVersion());
        Assertions.assertThrows(DorisConnectorException.class,
                () -> connector.getMetadata(null).createTable(null, request));

        Map<String, String> readOnlyProperties = new LinkedHashMap<>(properties);
        readOnlyProperties.remove(DeltaConnectorProperties.WRITE_ENABLED);
        readOnlyProperties.put(DeltaConnectorProperties.TABLE_PATH,
                tempDirectory.resolve("read-only-missing").toUri().toString());
        DeltaConnector readOnly = new DeltaConnectorProvider().create(
                readOnlyProperties, connectorContext());
        Assertions.assertNull(readOnly.getWritePlanProvider());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> readOnly.getMetadata(null).createTable(null, request));
        Assertions.assertFalse(readOnly.testConnection(null).isSuccess());
    }

    @Test
    public void testMetadataAndScanPlanUseOnePinnedSnapshot() throws Exception {
        Map<String, String> properties = new LinkedHashMap<>(deltaProperties("delta/path_table"));
        properties.put("s3.endpoint", "https://storage.example.test");
        properties.put("s3.region", "us-west-2");
        DeltaPathCatalogAdapter adapter = pathAdapter(properties);
        DeltaConnectorMetadata metadata = new DeltaConnectorMetadata(adapter, properties);

        ConnectorTableHandle handle = metadata.getTableHandle(null, "default", "events")
                .orElseThrow();
        Assertions.assertEquals(1, ((DeltaTableHandle) handle).getSnapshotVersion());
        Assertions.assertTrue(metadata.getTableHandle(null, "other", "events").isEmpty());

        ConnectorTableSchema schema = metadata.getTableSchema(null, handle);
        DeltaKernelSnapshot snapshot = adapter.loadSnapshot((DeltaTableHandle) handle);
        Assertions.assertSame(((DeltaTableHandle) handle).getPinnedSnapshot(), snapshot);
        Assertions.assertEquals(2, snapshot.getMinWriterVersion());
        Assertions.assertTrue(snapshot.getWriterFeatures().isEmpty());
        Assertions.assertEquals(List.of("id", "name"), schema.getColumns().stream()
                .map(ConnectorColumn::getName).collect(Collectors.toList()));
        Assertions.assertEquals(List.of("BIGINT", "STRING"), schema.getColumns().stream()
                .map(column -> column.getType().getTypeName()).collect(Collectors.toList()));
        Assertions.assertEquals(List.of("id", "name"),
                new ArrayList<>(metadata.getColumnHandles(null, handle).keySet()));

        DeltaScanPlanProvider scanProvider = new DeltaScanPlanProvider(adapter, properties);
        List<ConnectorScanRange> ranges = scanProvider.planScan(
                null, handle, List.of(), java.util.Optional.empty());
        Assertions.assertEquals(2, ranges.size());
        Assertions.assertEquals(List.of(200L, 300L), ranges.stream()
                .map(ConnectorScanRange::getLength).collect(Collectors.toList()));
        Assertions.assertTrue(ranges.stream()
                .allMatch(range -> "parquet".equals(range.getFileFormat())
                        && "delta".equals(range.getTableFormatType())));

        Map<String, String> scanProperties = scanProvider.getScanNodeProperties(
                null, handle, List.of(), java.util.Optional.empty());
        Assertions.assertEquals("parquet", scanProperties.get("file_format_type"));
        Assertions.assertEquals("https://storage.example.test",
                scanProperties.get("location.s3.endpoint"));
        Assertions.assertEquals("https://storage.example.test",
                scanProperties.get("location.AWS_ENDPOINT"));
        Assertions.assertEquals("us-west-2", scanProperties.get("location.AWS_REGION"));
        Assertions.assertFalse(scanProperties.containsKey("path_partition_keys"));

        TFileRangeDesc thriftRange = new TFileRangeDesc();
        ranges.get(0).populateRangeParams(new TTableFormatFileDesc(), thriftRange);
        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, thriftRange.getFormatType());

        DeltaTableHandle versionZero = (DeltaTableHandle) metadata.applyTableSnapshot(
                null, handle, DeltaTableSnapshot.version(0));
        Assertions.assertEquals(0, versionZero.getSnapshotVersion());
        Assertions.assertSame(versionZero.getPinnedSnapshot(),
                adapter.loadSnapshot(versionZero));
        Assertions.assertEquals(
                List.of("part-00000.parquet", "part-00001.parquet"),
                scanProvider.planScan(null, versionZero, List.of(), java.util.Optional.empty())
                        .stream().map(range -> Paths.get(URI.create(range.getPath().orElseThrow()))
                                .getFileName().toString())
                        .collect(Collectors.toList()));
    }

    @Test
    public void testPinnedSnapshotIsQueryLocalAndNotSerialized() throws Exception {
        Map<String, String> properties = deltaProperties("delta/path_table");
        DeltaTableHandle handle = pathAdapter(properties)
                .getTableHandle("default", "events").orElseThrow();
        Assertions.assertNotNull(handle.getPinnedSnapshot());

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(handle);
        }
        DeltaTableHandle restored;
        try (ObjectInputStream input = new ObjectInputStream(
                new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (DeltaTableHandle) input.readObject();
        }

        Assertions.assertEquals(handle, restored);
        Assertions.assertNull(restored.getPinnedSnapshot());
    }

    @Test
    public void testTimeTravelRejectsHistoricalSchemaEvolution() throws Exception {
        Map<String, String> properties = deltaProperties("delta/schema_evolution_table");
        DeltaPathCatalogAdapter adapter = pathAdapter(properties);
        DeltaConnectorMetadata metadata = new DeltaConnectorMetadata(adapter, properties);
        ConnectorTableHandle latest = metadata.getTableHandle(null, "default", "events")
                .orElseThrow();

        UnsupportedOperationException exception = Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> metadata.applyTableSnapshot(
                        null, latest, DeltaTableSnapshot.version(0)));
        Assertions.assertTrue(exception.getMessage().contains("schema or partition evolution"));
    }

    @Test
    public void testStoragePropertiesFeedKernelAndNativeBackend() {
        Assertions.assertDoesNotThrow(() -> Class.forName(
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"));

        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("AWS_ENDPOINT", "https://storage.example.test");
        properties.put("AWS_REGION", "us-west-2");
        properties.put("AWS_ACCESS_KEY", "temporary-ak");
        properties.put("AWS_SECRET_KEY", "temporary-sk");
        properties.put("AWS_TOKEN", "temporary-session");
        properties.put("use_path_style", "true");

        Configuration configuration = DeltaConnector.buildHadoopConfiguration(properties);
        Assertions.assertEquals("https://storage.example.test",
                configuration.get("fs.s3a.endpoint"));
        Assertions.assertEquals("us-west-2", configuration.get("fs.s3a.endpoint.region"));
        Assertions.assertEquals("temporary-ak", configuration.get("fs.s3a.access.key"));
        Assertions.assertEquals("temporary-sk", configuration.get("fs.s3a.secret.key"));
        Assertions.assertEquals("temporary-session", configuration.get("fs.s3a.session.token"));
        Assertions.assertEquals("org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
                configuration.get("fs.s3a.aws.credentials.provider"));
        Assertions.assertEquals("true", configuration.get("fs.s3a.path.style.access"));

        Map<String, String> backend = DeltaStorageProperties.toBackendProperties(properties);
        Assertions.assertEquals("https://storage.example.test", backend.get("AWS_ENDPOINT"));
        Assertions.assertEquals("us-west-2", backend.get("AWS_REGION"));
        Assertions.assertEquals("temporary-ak", backend.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("temporary-sk", backend.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("temporary-session", backend.get("AWS_TOKEN"));
        Assertions.assertEquals("true", backend.get("use_path_style"));

        Map<String, String> regionOnly = DeltaStorageProperties.toBackendProperties(
                Map.of("s3.region", "eu-west-1"));
        Assertions.assertEquals("s3.eu-west-1.amazonaws.com", regionOnly.get("AWS_ENDPOINT"));
        Assertions.assertEquals("eu-west-1", regionOnly.get("AWS_REGION"));

        Map<String, String> aliases = DeltaStorageProperties.toBackendProperties(Map.of(
                "aws.endpoint", "https://alias.example.test",
                "region", "local",
                "access_key", "alias-ak",
                "secret_key", "alias-sk"));
        Assertions.assertEquals("https://alias.example.test", aliases.get("AWS_ENDPOINT"));
        Assertions.assertEquals("local", aliases.get("AWS_REGION"));
        Assertions.assertEquals("alias-ak", aliases.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("alias-sk", aliases.get("AWS_SECRET_KEY"));

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> DeltaStorageProperties.toBackendProperties(
                        Map.of("AWS_ACCESS_KEY", "incomplete-ak")));
        Assertions.assertDoesNotThrow(() -> DeltaStorageProperties.toBackendProperties(Map.of(
                "provider", "AZURE", "AWS_TOKEN", "azure-sas")));
    }

    @Test
    public void testVendedCredentialMustCoverQueryTimeout() {
        long nowMs = 1_700_000_000_000L;
        ConnectorSession session = connectorSessionWithTimeouts("120", "240");
        Map<String, String> insufficient = Map.of(
                "location.AWS_TOKEN_EXPIRATION_TIME_MS",
                String.valueOf(nowMs + 179_999L));

        DorisConnectorException exception = Assertions.assertThrows(
                DorisConnectorException.class,
                () -> DeltaVendedCredentialLifetime.validate(session, insufficient,
                        "location.AWS_TOKEN_EXPIRATION_TIME_MS", "query_timeout",
                        60_000L, nowMs));
        Assertions.assertTrue(exception.getMessage().contains("query_timeout"));

        Map<String, String> sufficient = Map.of(
                "location.AWS_TOKEN_EXPIRATION_TIME_MS",
                String.valueOf(nowMs + 180_001L));
        Assertions.assertDoesNotThrow(
                () -> DeltaVendedCredentialLifetime.validate(session, sufficient,
                        "location.AWS_TOKEN_EXPIRATION_TIME_MS", "query_timeout",
                        60_000L, nowMs));

        Map<String, String> insufficientWrite = Map.of(
                "AWS_TOKEN_EXPIRATION_TIME_MS", String.valueOf(nowMs + 299_999L));
        DorisConnectorException writeException = Assertions.assertThrows(
                DorisConnectorException.class,
                () -> DeltaVendedCredentialLifetime.validate(session, insufficientWrite,
                        "AWS_TOKEN_EXPIRATION_TIME_MS", "insert_timeout", 60_000L, nowMs));
        Assertions.assertTrue(writeException.getMessage().contains("insert_timeout"));
    }

    @Test
    public void testScanPlanRejectsCredentialThatExpiresBeforeQueryTimeout() throws Exception {
        Map<String, String> properties = deltaProperties("delta/path_table");
        DeltaPathCatalogAdapter baseAdapter = pathAdapter(properties);
        DeltaPathCatalogAdapter credentialAdapter = new DeltaPathCatalogAdapter(
                baseAdapter.getDatabaseName(), baseAdapter.getTableName(),
                baseAdapter.getTablePath(), new DeltaKernelSnapshotLoader(
                        DefaultEngine.create(new Configuration()))) {
            @Override
            public Map<String, String> getBackendStorageProperties(DeltaTableHandle tableHandle) {
                return Map.of(
                        DeltaStorageProperties.S3_TOKEN, "temporary-session",
                        DeltaStorageProperties.S3_TOKEN_EXPIRATION_TIME_MS,
                        String.valueOf(System.currentTimeMillis() + 1_000L));
            }
        };
        DeltaTableHandle handle = credentialAdapter.getTableHandle("default", "events")
                .orElseThrow();
        DeltaScanPlanProvider scanProvider = new DeltaScanPlanProvider(
                credentialAdapter, properties);

        Assertions.assertThrows(DorisConnectorException.class,
                () -> scanProvider.getScanNodeProperties(
                        connectorSessionWithTimeouts("120", "240"), handle, List.of(),
                        java.util.Optional.empty()));
    }

    @Test
    public void testRepositoryDeltaFixturePlansRealParquetFiles() throws Exception {
        Path tablePath = repositoryCustomerTablePath();
        DeltaKernelSnapshotLoader loader = new DeltaKernelSnapshotLoader(
                DefaultEngine.create(new Configuration()));

        DeltaKernelSnapshot snapshot = loader.loadLatest(tablePath.toUri().toString());

        Assertions.assertEquals(0, snapshot.getVersion());
        Assertions.assertEquals(List.of("c_custkey", "c_name", "c_address", "c_nationkey",
                "c_phone", "c_acctbal", "c_mktsegment", "c_comment"),
                snapshot.getSchema().fields().stream()
                        .map(io.delta.kernel.types.StructField::getName)
                        .collect(Collectors.toList()));
        Assertions.assertEquals(4, snapshot.getActiveFiles().size());
        Assertions.assertTrue(snapshot.getActiveFiles().stream()
                .allMatch(file -> file.getPath().endsWith(".parquet") && file.getSize() > 0));
        Assertions.assertEquals(1_564_827L, snapshot.getActiveFiles().stream()
                .mapToLong(DeltaScanFile::getSize).sum());
    }

    @Test
    public void testPartitionValuesFollowDeltaPartitionColumnOrder() throws Exception {
        Map<String, String> properties = deltaProperties("delta/partitioned_table");
        properties.put(DeltaConnectorProperties.WRITE_ENABLED, "true");
        DeltaPathCatalogAdapter adapter = pathAdapter(properties);
        DeltaTableHandle handle = adapter.getTableHandle("default", "events").orElseThrow();
        DeltaScanPlanProvider scanProvider = new DeltaScanPlanProvider(adapter, properties);

        List<ConnectorScanRange> ranges = scanProvider.planScan(
                null, handle, List.of(), java.util.Optional.empty());

        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals(List.of("p2", "p1"),
                new ArrayList<>(ranges.get(0).getPartitionValues().keySet()));
        Assertions.assertEquals(List.of("two", "one"),
                new ArrayList<>(ranges.get(0).getPartitionValues().values()));
        Assertions.assertEquals("p2,p1", scanProvider.getScanNodeProperties(
                null, handle, List.of(), java.util.Optional.empty())
                .get("path_partition_keys"));

        TFileRangeDesc thriftRange = new TFileRangeDesc();
        ranges.get(0).populateRangeParams(new TTableFormatFileDesc(), thriftRange);
        Assertions.assertEquals(List.of("p2", "p1"), thriftRange.getColumnsFromPathKeys());
        Assertions.assertEquals(List.of("two", "one"), thriftRange.getColumnsFromPath());
        Assertions.assertEquals(List.of(false, false), thriftRange.getColumnsFromPathIsNull());

        DeltaConnectorMetadata metadata = new DeltaConnectorMetadata(adapter, properties,
                new DeltaKernelWriter(DefaultEngine.create(new Configuration())));
        List<ConnectorColumn> columns = metadata.getTableSchema(null, handle).getColumns();
        Assertions.assertEquals(List.of("p2", "p1"),
                metadata.getWriteConfig(null, handle, columns).getPartitionColumns());
    }

    @Test
    public void testColumnMappingSchemaIsPassedToBackendScanParams() throws Exception {
        Map<String, String> properties = deltaProperties("delta/column_mapping_table");
        DeltaPathCatalogAdapter adapter = pathAdapter(properties);
        DeltaTableHandle handle = adapter.getTableHandle("default", "events").orElseThrow();
        DeltaScanPlanProvider scanProvider = new DeltaScanPlanProvider(adapter, properties);

        Map<String, String> scanProperties = scanProvider.getScanNodeProperties(
                null, handle, List.of(), java.util.Optional.empty());
        Assertions.assertNotNull(scanProperties.get(DeltaSchemaInfo.SERIALIZED_SCHEMA_PROPERTY));
        Assertions.assertEquals("0", scanProperties.get(DeltaSchemaInfo.SCHEMA_VERSION_PROPERTY));

        TFileScanRangeParams params = new TFileScanRangeParams();
        scanProvider.populateScanLevelParams(params, scanProperties);
        Assertions.assertEquals(1, params.getHistorySchemaInfoSize());
        Assertions.assertEquals(0, params.getCurrentSchemaId());
        Assertions.assertEquals(1, params.getExternalScanSemanticsVersion());
        Assertions.assertEquals(List.of("col-91e40a2f-1b63-42a0-a044-35764a3b259a"),
                params.getHistorySchemaInfo().get(0).getRootField().getFields().get(0)
                        .getFieldPtr().getNameMapping());
    }

    @Test
    public void testDeltaPartitionLiteralIsNotConfusedWithNull() {
        Map<String, String> partitionValues = new LinkedHashMap<>();
        partitionValues.put("p1", "__HIVE_DEFAULT_PARTITION__");
        partitionValues.put("p2", null);
        DeltaScanRange range = new DeltaScanRange(
                new DeltaScanFile("file:///tmp/part.parquet", 1, 0, partitionValues));

        TFileRangeDesc thriftRange = new TFileRangeDesc();
        range.populateRangeParams(new TTableFormatFileDesc(), thriftRange);

        Assertions.assertEquals(List.of("p1", "p2"), thriftRange.getColumnsFromPathKeys());
        Assertions.assertEquals(List.of("__HIVE_DEFAULT_PARTITION__", "\\N"),
                thriftRange.getColumnsFromPath());
        Assertions.assertEquals(List.of(false, true), thriftRange.getColumnsFromPathIsNull());
    }

    @Test
    public void testDeltaTypeMappingIsRecursiveAndFailsClosed() {
        ConnectorType decimal = DeltaTypeMapping.fromDeltaType(new DecimalType(18, 4));
        Assertions.assertEquals(ConnectorType.of("DECIMALV3", 18, 4), decimal);

        StructType nested = new StructType()
                .add("items", new ArrayType(IntegerType.INTEGER, true))
                .add("attributes", new MapType(StringType.STRING, StringType.STRING, true));
        ConnectorType mapped = DeltaTypeMapping.fromDeltaType(nested);
        Assertions.assertEquals("STRUCT", mapped.getTypeName());
        Assertions.assertEquals(List.of("items", "attributes"), mapped.getFieldNames());
        Assertions.assertEquals("ARRAY", mapped.getChildren().get(0).getTypeName());
        Assertions.assertEquals("MAP", mapped.getChildren().get(1).getTypeName());

        Assertions.assertThrows(DorisConnectorException.class,
                () -> DeltaTypeMapping.fromDeltaType(VariantType.VARIANT));
    }

    @Test
    public void testInitialWriterRequiresCompleteSchemaOrder() throws Exception {
        Map<String, String> properties = deltaProperties("delta/path_table");
        properties.put(DeltaConnectorProperties.WRITE_ENABLED, "true");
        properties.put("fs.s3a.access.key", "test-key");
        properties.put("fs.s3a.secret.key", "test-secret");
        properties.put(DeltaConnectorProperties.UNITY_TOKEN, "control-plane-secret");
        DeltaPathCatalogAdapter adapter = pathAdapter(properties);
        Engine engine = DefaultEngine.create(new Configuration());
        DeltaConnectorMetadata metadata = new DeltaConnectorMetadata(
                adapter, properties, new DeltaKernelWriter(engine));
        DeltaTableHandle handle = adapter.getTableHandle("default", "events").orElseThrow();
        List<ConnectorColumn> columns = metadata.getTableSchema(null, handle).getColumns();

        DeltaWriteConfig writeConfig = metadata.getWriteConfig(null, handle, columns);
        Assertions.assertEquals("parquet", writeConfig.getFileFormat());
        Assertions.assertEquals("test-key",
                writeConfig.getProperties().get("fs.s3a.access.key"));
        Assertions.assertFalse(writeConfig.getProperties().containsKey(
                DeltaConnectorProperties.UNITY_TOKEN));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> metadata.getWriteConfig(null, handle, List.of(columns.get(0))));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> metadata.getWriteConfig(null, handle,
                        List.of(columns.get(1), columns.get(0))));
        ConnectorColumn wrongType = new ConnectorColumn(columns.get(0).getName(),
                ConnectorType.of("STRING"), "", columns.get(0).isNullable(), null);
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> metadata.getWriteConfig(null, handle,
                        List.of(wrongType, columns.get(1))));
        DeltaInsertHandle overwrite = metadata.beginInsertOverwrite(
                null, handle, columns);
        Assertions.assertTrue(((DeltaInsertHandle) overwrite).isOverwrite());
        metadata.abortInsert(null, overwrite);
    }

    @Test
    public void testWriteTypeCompatibilityNormalizesDorisDecimalStorageWidth() {
        ConnectorType deltaDecimal = ConnectorType.of("DECIMALV3", 12, 2);

        Assertions.assertTrue(DeltaConnectorMetadata.hasCompatibleWriteType(
                ConnectorType.of("INT"), ConnectorType.of("INT", 0, 0)));
        Assertions.assertTrue(DeltaConnectorMetadata.hasCompatibleWriteType(
                deltaDecimal, ConnectorType.of("DECIMAL64")));
        Assertions.assertTrue(DeltaConnectorMetadata.hasCompatibleWriteType(
                deltaDecimal, ConnectorType.of("DECIMAL64", 12, 2)));
        Assertions.assertFalse(DeltaConnectorMetadata.hasCompatibleWriteType(
                deltaDecimal, ConnectorType.of("DECIMAL32")));
        Assertions.assertFalse(DeltaConnectorMetadata.hasCompatibleWriteType(
                deltaDecimal, ConnectorType.of("DECIMAL64", 12, 3)));
    }

    private DeltaPathCatalogAdapter pathAdapter(Map<String, String> properties) {
        DeltaKernelSnapshotLoader loader = new DeltaKernelSnapshotLoader(
                DefaultEngine.create(new Configuration()));
        return new DeltaPathCatalogAdapter(
                properties.get(DeltaConnectorProperties.DATABASE),
                properties.get(DeltaConnectorProperties.TABLE),
                properties.get(DeltaConnectorProperties.TABLE_PATH), loader);
    }

    private Map<String, String> deltaProperties(String resourceName) throws Exception {
        URL resource = Objects.requireNonNull(getClass().getClassLoader().getResource(resourceName));
        URI tableUri = Paths.get(resource.toURI()).toUri();
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("type", "delta");
        properties.put(DeltaConnectorProperties.DATABASE, "default");
        properties.put(DeltaConnectorProperties.TABLE, "events");
        properties.put(DeltaConnectorProperties.TABLE_PATH, tableUri.toString());
        return properties;
    }

    private static ConnectorContext connectorContext() {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "delta_test";
            }

            @Override
            public long getCatalogId() {
                return 1;
            }
        };
    }

    private static ConnectorSession connectorSessionWithTimeouts(
            String queryTimeoutSeconds, String insertTimeoutSeconds) {
        return new ConnectorSession() {
            @Override
            public String getQueryId() {
                return "delta-timeout-test";
            }

            @Override
            public String getUser() {
                return "root";
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
                return "delta_test";
            }

            @Override
            public <T> T getProperty(String name, Class<T> type) {
                return null;
            }

            @Override
            public Map<String, String> getCatalogProperties() {
                return Map.of();
            }

            @Override
            public Map<String, String> getSessionProperties() {
                return Map.of("query_timeout", queryTimeoutSeconds,
                        "insert_timeout", insertTimeoutSeconds);
            }
        };
    }

    private static Path repositoryCustomerTablePath() {
        Path relativePath = Paths.get("samples", "datalake", "deltalake_and_kudu", "data",
                "customer");
        Path current = Paths.get("").toAbsolutePath().normalize();
        List<Path> candidates = List.of(current.resolve(relativePath),
                current.getParent().resolve(relativePath),
                current.getParent().getParent().resolve(relativePath),
                current.getParent().getParent().getParent().resolve(relativePath));
        return candidates.stream()
                .filter(Files::isDirectory)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "Repository Delta fixture is not available: " + relativePath));
    }
}
