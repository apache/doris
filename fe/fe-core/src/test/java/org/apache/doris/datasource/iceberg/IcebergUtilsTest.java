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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.iceberg.source.IcebergTableQueryInfo;
import org.apache.doris.datasource.property.storage.OSSProperties;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.GenericPartitionFieldSummary;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFile.PartitionFieldSummary;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StructType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class IcebergUtilsTest {
    @Test
    public void testSelectEffectiveStoragePropertiesPrefersOssOverGenericS3() throws UserException {
        Map<String, String> properties = new HashMap<>();
        properties.put("iceberg.rest.signing-name", "osstables");
        properties.put("iceberg.rest.signing-region", "cn-beijing");
        properties.put("oss.endpoint", "https://oss-cn-beijing.aliyuncs.com");
        properties.put("oss.region", "cn-beijing");
        properties.put("oss.access_key", "ak");
        properties.put("oss.secret_key", "sk");

        Map<StorageProperties.Type, StorageProperties> detected = new HashMap<>();
        for (StorageProperties storageProperties : StorageProperties.createAll(properties)) {
            detected.put(storageProperties.getType(), storageProperties);
        }
        Assert.assertTrue(detected.get(StorageProperties.Type.S3) instanceof S3Properties);
        Assert.assertTrue(detected.get(StorageProperties.Type.OSS) instanceof OSSProperties);

        Map<StorageProperties.Type, StorageProperties> selected =
                IcebergUtils.selectEffectiveStorageProperties(detected);

        Assert.assertFalse(selected.containsKey(StorageProperties.Type.S3));
        Assert.assertTrue(selected.get(StorageProperties.Type.OSS) instanceof OSSProperties);
        Assert.assertSame(selected.get(StorageProperties.Type.OSS),
                LocationPath.of("s3://bucket/data.parquet", selected).getStorageProperties());
    }

    @Test
    public void testSnapshotCacheFreezesSharedTableOperations() {
        Schema originalSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Schema evolvedSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "payload", Types.StringType.get()));
        AtomicReference<TableMetadata> currentMetadata = new AtomicReference<>(
                TableMetadata.newTableMetadata(originalSchema, PartitionSpec.unpartitioned(),
                        "file:/tmp/iceberg-cache-table", Collections.emptyMap()));
        TableOperations operations = Mockito.mock(TableOperations.class);
        Mockito.when(operations.current()).thenAnswer(invocation -> currentMetadata.get());
        Mockito.when(operations.io()).thenReturn(Mockito.mock(org.apache.iceberg.io.FileIO.class));
        Mockito.when(operations.locationProvider())
                .thenReturn(Mockito.mock(org.apache.iceberg.io.LocationProvider.class));
        Table sharedTable = new BaseTable(operations, "table");

        IcebergSnapshotCacheValue cacheValue = new IcebergSnapshotCacheValue(
                IcebergPartitionInfo.empty(), new IcebergSnapshot(-1L, originalSchema.schemaId()),
                Optional.empty(), sharedTable);
        currentMetadata.set(TableMetadata.newTableMetadata(evolvedSchema, PartitionSpec.unpartitioned(),
                "file:/tmp/iceberg-cache-table", Collections.emptyMap()));

        Assert.assertEquals(2, sharedTable.schema().columns().size());
        Assert.assertEquals(1, cacheValue.getIcebergTable().get().schema().columns().size());
    }

    @Test
    public void testRetainedGenerationKeepsProjectionAtomic() {
        Schema originalSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Schema evolvedSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "payload", Types.StringType.get()));
        TableMetadata originalMetadata = TableMetadata.newTableMetadata(
                originalSchema, PartitionSpec.unpartitioned(),
                "file:/tmp/iceberg-atomic-projection", Collections.emptyMap());
        TableMetadata evolvedMetadata = TableMetadata.newTableMetadata(
                evolvedSchema, PartitionSpec.unpartitioned(),
                "file:/tmp/iceberg-atomic-projection",
                Collections.singletonMap(TableProperties.DEFAULT_NAME_MAPPING,
                        "{\"type\":\"struct\",\"fields\":[{\"field-id\":2,\"names\":[\"payload\"]}]}"));
        TableOperations operations = Mockito.mock(TableOperations.class);
        Mockito.when(operations.current()).thenReturn(originalMetadata, evolvedMetadata);
        Mockito.when(operations.io()).thenReturn(Mockito.mock(org.apache.iceberg.io.FileIO.class));
        Mockito.when(operations.locationProvider())
                .thenReturn(Mockito.mock(org.apache.iceberg.io.LocationProvider.class));
        Table sharedTable = new BaseTable(operations, "table");

        Table retainedTable = IcebergSnapshotCacheValue.retainTableGeneration(sharedTable);
        IcebergSnapshot snapshot = IcebergUtils.getLatestIcebergSnapshot(retainedTable);
        Optional<Map<Integer, List<String>>> nameMapping = IcebergUtils.getNameMapping(retainedTable);
        IcebergSnapshotCacheValue cacheValue = new IcebergSnapshotCacheValue(
                IcebergPartitionInfo.empty(), snapshot, nameMapping, retainedTable);

        Assert.assertEquals(originalSchema.schemaId(), snapshot.getSchemaId());
        Assert.assertFalse(nameMapping.isPresent());
        Assert.assertEquals(1, cacheValue.getIcebergTable().get().schema().columns().size());
        Mockito.verify(operations, Mockito.times(1)).current();
    }

    @Test
    public void testGetFileFormatUsesPropertiesWithoutPlanningDataFiles() {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.properties()).thenReturn(Collections.emptyMap());
        Mockito.when(table.currentSnapshot()).thenReturn(Mockito.mock(Snapshot.class));

        Assert.assertEquals(org.apache.iceberg.FileFormat.PARQUET, IcebergUtils.getFileFormat(table));
        // Do not call newScan planFiles()
        Mockito.verify(table, Mockito.never()).newScan();
    }

    @Test
    public void testGetFileFormatUsesConfiguredTableFormat() {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.properties()).thenReturn(
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, "orc"));

        Assert.assertEquals(org.apache.iceberg.FileFormat.ORC, IcebergUtils.getFileFormat(table));
        // Do not call newScan planFiles()
        Mockito.verify(table, Mockito.never()).newScan();
    }

    @Test
    public void testPartitionColumnsUseFrozenTableSpec() {
        Schema historicalSchema = new Schema(17, Arrays.asList(
                Types.NestedField.required(1, "p", Types.IntegerType.get()),
                Types.NestedField.optional(2, "q", Types.IntegerType.get())));
        Schema currentSchema = new Schema(18, Arrays.asList(
                Types.NestedField.required(1, "p_renamed", Types.IntegerType.get()),
                Types.NestedField.optional(2, "q", Types.IntegerType.get())));
        Table frozenTable = Mockito.mock(Table.class);
        Mockito.when(frozenTable.schema()).thenReturn(currentSchema);
        Mockito.when(frozenTable.schemas()).thenReturn(ImmutableMap.of(
                historicalSchema.schemaId(), historicalSchema,
                currentSchema.schemaId(), currentSchema));
        Mockito.when(frozenTable.spec()).thenReturn(
                PartitionSpec.builderFor(currentSchema).identity("p_renamed").build());
        Mockito.when(frozenTable.currentSnapshot()).thenReturn(Mockito.mock(Snapshot.class));

        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(dorisTable.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("catalog");
        IcebergSchemaCacheValue cacheValue = IcebergUtils.buildTableSchemaCacheValue(
                dorisTable, historicalSchema.schemaId(), frozenTable,
                new ExecutionAuthenticator() { }, false, false);

        Assert.assertEquals(Collections.singletonList("p"), cacheValue.getPartitionColumns().stream()
                .map(Column::getName).collect(java.util.stream.Collectors.toList()));
    }

    @Test
    public void testPartitionColumnsProjectInsideSnapshotLease() {
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheManager = Mockito.mock(ExternalMetaCacheMgr.class);
        IcebergExternalMetaCache cache = Mockito.mock(IcebergExternalMetaCache.class);
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        ExternalTable dorisTable = Mockito.mock(ExternalTable.class);
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        Table frozenTable = Mockito.mock(Table.class);
        IcebergSnapshotCacheValue snapshotValue = new IcebergSnapshotCacheValue(
                IcebergPartitionInfo.empty(), new IcebergSnapshot(11L, 17L),
                Optional.empty(), frozenTable);
        List<Column> partitionColumns = Collections.singletonList(new Column("p", Type.INT));
        IcebergSchemaCacheValue schemaValue = new IcebergSchemaCacheValue(
                partitionColumns, partitionColumns);
        AtomicBoolean leaseActive = new AtomicBoolean();
        Mockito.when(dorisTable.getCatalog()).thenReturn(catalog);
        Mockito.when(dorisTable.getOrBuildNameMapping()).thenReturn(mapping);
        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheManager);
        Mockito.when(cacheManager.iceberg(1L)).thenReturn(cache);
        Mockito.when(cache.withSnapshotCacheValue(Mockito.eq(dorisTable), Mockito.any()))
                .thenAnswer(invocation -> {
                    leaseActive.set(true);
                    try {
                        Function<IcebergSnapshotCacheValue, List<Column>> projection = invocation.getArgument(1);
                        return projection.apply(snapshotValue);
                    } finally {
                        leaseActive.set(false);
                    }
                });
        Mockito.when(cache.getIcebergSchemaCacheValue(mapping, 17L, frozenTable))
                .thenAnswer(invocation -> {
                    Assert.assertTrue("schema/spec projection must remain inside the snapshot lease", leaseActive.get());
                    return schemaValue;
                });

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);

            Assert.assertSame(partitionColumns,
                    IcebergUtils.getIcebergPartitionColumns(Optional.empty(), dorisTable));
        }
        Mockito.verify(cache, Mockito.never()).getSnapshotCache(dorisTable);
    }

    @Test
    public void testParseTableName() {
        try {
            IcebergHMSExternalCatalog c1 =
                    new IcebergHMSExternalCatalog(1, "name", null, new HashMap<>(), "");
            HiveCatalog i1 = IcebergUtils.createIcebergHiveCatalog(c1, "i1");
            Assert.assertTrue(i1 instanceof DorisHiveCatalog);
            Assert.assertTrue(getListAllTables(i1));

            IcebergHMSExternalCatalog c2 =
                    new IcebergHMSExternalCatalog(1, "name", null,
                            new HashMap<String, String>() {{
                                    put("list-all-tables", "true");
                                    put("type", "hms");
                                    put("hive.metastore.uris", "http://127.1.1.0:9000");
                                }},
                            "");
            HiveCatalog i2 = IcebergUtils.createIcebergHiveCatalog(c2, "i1");
            Assert.assertTrue(getListAllTables(i2));

            IcebergHMSExternalCatalog c3 =
                    new IcebergHMSExternalCatalog(1, "name", null,
                            new HashMap<String, String>() {{
                                    put("list-all-tables", "false");
                                    put("type", "hms");
                                    put("hive.metastore.uris", "http://127.1.1.0:9000");
                                }},
                        "");
            HiveCatalog i3 = IcebergUtils.createIcebergHiveCatalog(c3, "i1");
            Assert.assertFalse(getListAllTables(i3));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    private boolean getListAllTables(HiveCatalog hiveCatalog) throws IllegalAccessException, NoSuchFieldException {
        Field declaredField = HiveCatalog.class.getDeclaredField("listAllTables");
        declaredField.setAccessible(true);
        return declaredField.getBoolean(hiveCatalog);
    }

    @Test
    public void testDataLocationUsesLegacyObjectStorePath() {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.properties()).thenReturn(ImmutableMap.of(
                TableProperties.OBJECT_STORE_ENABLED, "true",
                TableProperties.OBJECT_STORE_PATH, "s3://bucket/legacy-object-store",
                TableProperties.WRITE_FOLDER_STORAGE_LOCATION, "s3://bucket/folder-storage"));

        Assert.assertEquals("s3://bucket/legacy-object-store", IcebergUtils.dataLocation(table));
    }

    @Test
    public void testDataLocationPrefersWriteDataPathOverLegacyObjectStorePath() {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.properties()).thenReturn(ImmutableMap.of(
                TableProperties.WRITE_DATA_LOCATION, "s3://bucket/data-path",
                TableProperties.OBJECT_STORE_PATH, "s3://bucket/legacy-object-store"));

        Assert.assertEquals("s3://bucket/data-path", IcebergUtils.dataLocation(table));
    }

    @Test
    public void testDataLocationIgnoresObjectStorePathWhenObjectStoreDisabled() {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.properties()).thenReturn(ImmutableMap.of(
                TableProperties.OBJECT_STORE_ENABLED, "false",
                TableProperties.OBJECT_STORE_PATH, "s3://bucket/legacy-object-store",
                TableProperties.WRITE_FOLDER_STORAGE_LOCATION, "s3://bucket/folder-storage"));

        Assert.assertEquals("s3://bucket/folder-storage", IcebergUtils.dataLocation(table));
    }

    @Test
    public void testDataLocationIgnoresObjectStorePathWhenObjectStoreUnset() {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.properties()).thenReturn(ImmutableMap.of(
                TableProperties.OBJECT_STORE_PATH, "s3://bucket/legacy-object-store",
                TableProperties.WRITE_FOLDER_STORAGE_LOCATION, "s3://bucket/folder-storage"));

        Assert.assertEquals("s3://bucket/folder-storage", IcebergUtils.dataLocation(table));
    }

    @Test
    public void testIsIcebergRowLineageColumn() {
        Column rowIdColumn = new Column(IcebergUtils.ICEBERG_ROW_ID_COL, Type.BIGINT, true);
        Column sequenceColumn = new Column(IcebergUtils.ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL, Type.BIGINT, true);
        Column normalColumn = new Column("id", Type.INT, true);

        Assert.assertTrue(IcebergUtils.isIcebergRowLineageColumn(rowIdColumn));
        Assert.assertTrue(IcebergUtils.isIcebergRowLineageColumn(sequenceColumn));
        Assert.assertTrue(IcebergUtils.isIcebergRowLineageColumn("_ROW_ID"));
        Assert.assertFalse(IcebergUtils.isIcebergRowLineageColumn(normalColumn));
        Assert.assertFalse(IcebergUtils.isIcebergRowLineageColumn("id"));
    }

    @Test
    public void testAppendRowLineageColumnsForV3AddsInvisibleColumns() {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", Type.INT, true));
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.properties()).thenReturn(ImmutableMap.of("format-version", "3"));

        List<Column> schemaWithRowLineage = IcebergUtils.appendRowLineageColumnsForV3(schema, table);

        Assert.assertEquals(3, schemaWithRowLineage.size());
        Assert.assertEquals(IcebergUtils.ICEBERG_ROW_ID_COL, schemaWithRowLineage.get(1).getName());
        Assert.assertEquals(IcebergUtils.ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL,
                schemaWithRowLineage.get(2).getName());
        Assert.assertFalse(schemaWithRowLineage.get(1).isVisible());
        Assert.assertFalse(schemaWithRowLineage.get(2).isVisible());
    }

    @Test
    public void testAppendRowLineageColumnsForV2ReturnsOriginalSchema() {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", Type.INT, true));
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.properties()).thenReturn(ImmutableMap.of("format-version", "2"));

        List<Column> schemaWithRowLineage = IcebergUtils.appendRowLineageColumnsForV3(schema, table);

        Assert.assertSame(schema, schemaWithRowLineage);
        Assert.assertEquals(1, schemaWithRowLineage.size());
    }

    @Test
    public void testAppendRowLineageFieldsForV3AddsMetadataFields() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

        Schema schemaWithRowLineage = IcebergUtils.appendRowLineageFieldsForV3(schema);

        Assert.assertNotNull(schemaWithRowLineage.findField(MetadataColumns.ROW_ID.fieldId()));
        Assert.assertNotNull(schemaWithRowLineage.findField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId()));
    }

    @Test
    public void testParseSchemaPreservesNonLowercaseColumnNames() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "mIxEd_COL", Types.IntegerType.get()),
                Types.NestedField.required(2, "PART", Types.StringType.get()));

        List<Column> columns = IcebergUtils.parseSchema(schema, false, false);

        Assert.assertEquals("mIxEd_COL", columns.get(0).getName());
        Assert.assertEquals("PART", columns.get(1).getName());
    }

    @Test
    public void testParseSchemaPreservesTopLevelAndNestedComments() {
        Schema schema = new Schema(Types.NestedField.optional(
                1, "info", Types.StructType.of(
                        Types.NestedField.optional(2, "value", Types.IntegerType.get(), "nested-comment")),
                "top-level-comment"));

        List<Column> columns = IcebergUtils.parseSchema(schema, false, false);

        Assert.assertEquals("top-level-comment", columns.get(0).getComment());
        Assert.assertTrue(columns.get(0).getType().toSql().contains("comment 'nested-comment'"));
    }

    @Test
    public void testIcebergVariantUsesComputeV2Representation() {
        Type type = IcebergUtils.icebergTypeToDorisType(
                Types.VariantType.get(), false, false);

        Assert.assertTrue(type instanceof org.apache.doris.catalog.VariantType);
        Assert.assertTrue(((org.apache.doris.catalog.VariantType) type).isComputeV2());
        Assert.assertEquals(Types.VariantType.get(), IcebergUtils.dorisTypeToIcebergType(type));
    }

    @Test
    public void testIcebergVariantWriteCapabilityMatrix() {
        Type variant = IcebergUtils.icebergTypeToDorisType(Types.VariantType.get(), false, false);
        Column column = new Column("payload", variant);
        IcebergUtils.validateWriteSchema(ImmutableList.of(column), 3, FileFormat.PARQUET);
        try {
            IcebergUtils.validateWriteSchema(ImmutableList.of(column), 2, FileFormat.PARQUET);
            Assert.fail("Iceberg VARIANT writes must require format-version 3");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("format-version 3"));
        }
        try {
            IcebergUtils.validateWriteSchema(ImmutableList.of(column), 3, FileFormat.ORC);
            Assert.fail("Iceberg VARIANT writes must require Parquet");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Parquet"));
        }

        Column nestedColumn = new Column("nested", new org.apache.doris.catalog.StructType(
                new ArrayList<>(ImmutableList.of(new StructField("payload", variant)))));
        IcebergUtils.validateWriteSchema(
                ImmutableList.of(nestedColumn), 3, FileFormat.PARQUET);
    }

    @Test
    public void testRejectVariantWritesWhenParquetShreddingIsEnabled() {
        String shredVariantsProperty = "write.parquet.shred-variants";
        Type variant = IcebergUtils.icebergTypeToDorisType(Types.VariantType.get(), false, false);
        List<Column> variantColumns = ImmutableList.of(new Column("payload", variant));

        IcebergUtils.validateVariantWriteProperties(variantColumns, Collections.emptyMap());
        IcebergUtils.validateVariantWriteProperties(variantColumns,
                ImmutableMap.of(shredVariantsProperty, "false"));

        AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                () -> IcebergUtils.validateVariantWriteProperties(variantColumns,
                        ImmutableMap.of(shredVariantsProperty, "true")));
        Assert.assertTrue(exception.getMessage().contains("only unshredded Iceberg VARIANT writes"));
        Assert.assertTrue(exception.getMessage().contains(shredVariantsProperty + "=false"));

        IcebergUtils.validateVariantWriteProperties(
                ImmutableList.of(new Column("id", Type.INT)),
                ImmutableMap.of(shredVariantsProperty, "true"));
    }

    @Test
    public void testEffectiveFileFormatPrecedenceForVariantDdl() {
        Assert.assertEquals(FileFormat.PARQUET,
                IcebergUtils.getEffectiveFileFormat(Collections.emptyMap(), Collections.emptyMap()));
        Assert.assertEquals(FileFormat.ORC, IcebergUtils.getEffectiveFileFormat(
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, "orc"), Collections.emptyMap()));
        Assert.assertEquals(FileFormat.ORC, IcebergUtils.getEffectiveFileFormat(
                Collections.emptyMap(), ImmutableMap.of(
                        CatalogProperties.TABLE_DEFAULT_PREFIX + TableProperties.DEFAULT_FILE_FORMAT, "orc")));
        Assert.assertEquals(FileFormat.PARQUET, IcebergUtils.getEffectiveFileFormat(
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, "orc"), ImmutableMap.of(
                        CatalogProperties.TABLE_OVERRIDE_PREFIX + TableProperties.DEFAULT_FILE_FORMAT,
                        "parquet")));
        // Iceberg persists catalog overrides under the standard key without removing the
        // write-format alias. Match getFileFormat(Table): the alias still wins at runtime.
        Assert.assertEquals(FileFormat.ORC, IcebergUtils.getEffectiveFileFormat(
                ImmutableMap.of(IcebergUtils.WRITE_FORMAT, "orc"), ImmutableMap.of(
                        CatalogProperties.TABLE_OVERRIDE_PREFIX + TableProperties.DEFAULT_FILE_FORMAT,
                        "parquet")));
        Assert.assertEquals(FileFormat.PARQUET, IcebergUtils.getEffectiveFileFormat(
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, "orc"), ImmutableMap.of(
                        CatalogProperties.TABLE_OVERRIDE_PREFIX + IcebergUtils.WRITE_FORMAT,
                        "parquet")));
        Assert.assertEquals(FileFormat.ORC, IcebergUtils.getEffectiveFileFormat(
                Collections.emptyMap(), ImmutableMap.of(
                        CatalogProperties.TABLE_DEFAULT_PREFIX + IcebergUtils.WRITE_FORMAT, "orc",
                        CatalogProperties.TABLE_DEFAULT_PREFIX + TableProperties.DEFAULT_FILE_FORMAT,
                        "parquet")));
    }

    @Test
    public void testRejectSmoothUpgradeSourceBackendForVariantWrite() {
        Type variant = IcebergUtils.icebergTypeToDorisType(Types.VariantType.get(), false, false);
        List<Column> variantColumns = ImmutableList.of(new Column("payload", variant));

        Backend currentBackend = Mockito.mock(Backend.class);
        Mockito.when(currentBackend.isQueryAvailable()).thenReturn(true);
        Backend smoothUpgradeSource = Mockito.mock(Backend.class);
        Mockito.when(smoothUpgradeSource.isQueryAvailable()).thenReturn(true);
        Mockito.when(smoothUpgradeSource.isSmoothUpgradeSrc()).thenReturn(true);
        Mockito.when(smoothUpgradeSource.getId()).thenReturn(10004L);

        IcebergUtils.validateVariantWriteBackendCompatibility(
                variantColumns, ImmutableList.of(currentBackend));
        try {
            IcebergUtils.validateVariantWriteBackendCompatibility(
                    variantColumns, ImmutableList.of(currentBackend, smoothUpgradeSource));
            Assert.fail("Variant writes must not be scheduled while an old backend is eligible");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("backend 10004 is a smooth upgrade source"));
        }

        // Unavailable old backends cannot receive the sink fragment and must not block writes.
        Mockito.when(smoothUpgradeSource.isQueryAvailable()).thenReturn(false);
        IcebergUtils.validateVariantWriteBackendCompatibility(
                variantColumns, ImmutableList.of(currentBackend, smoothUpgradeSource));
    }

    @Test
    public void testRejectSmoothUpgradeSourceBackendForOrcBinaryWrite() {
        Schema binarySchema = new Schema(Types.NestedField.optional(1, "payload",
                Types.StructType.of(
                        Types.NestedField.optional(2, "uuid", Types.UUIDType.get()),
                        Types.NestedField.optional(3, "fixed", Types.FixedType.ofLength(4)),
                        Types.NestedField.optional(4, "binary", Types.BinaryType.get()))));
        Backend currentBackend = Mockito.mock(Backend.class);
        Mockito.when(currentBackend.isQueryAvailable()).thenReturn(true);
        Backend smoothUpgradeSource = Mockito.mock(Backend.class);
        Mockito.when(smoothUpgradeSource.isQueryAvailable()).thenReturn(true);
        Mockito.when(smoothUpgradeSource.isSmoothUpgradeSrc()).thenReturn(true);
        Mockito.when(smoothUpgradeSource.getId()).thenReturn(10006L);

        IcebergUtils.validateOrcBinaryWriteBackendCompatibility(
                binarySchema, FileFormat.ORC, ImmutableList.of(currentBackend));
        IcebergUtils.validateOrcBinaryWriteBackendCompatibility(
                binarySchema, FileFormat.PARQUET,
                ImmutableList.of(currentBackend, smoothUpgradeSource));
        AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                () -> IcebergUtils.validateOrcBinaryWriteBackendCompatibility(
                        binarySchema, FileFormat.ORC,
                        ImmutableList.of(currentBackend, smoothUpgradeSource)));
        Assert.assertTrue(exception.getMessage().contains(
                "backend 10006 is a smooth upgrade source"));

        Mockito.when(smoothUpgradeSource.isQueryAvailable()).thenReturn(false);
        IcebergUtils.validateOrcBinaryWriteBackendCompatibility(
                binarySchema, FileFormat.ORC,
                ImmutableList.of(currentBackend, smoothUpgradeSource));
    }

    @Test
    public void testIcebergVariantEnablesParquetMetricsCollection() {
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.properties()).thenReturn(ImmutableMap.of(
                TableProperties.DEFAULT_FILE_FORMAT, "parquet"));
        Schema schema = new Schema(
                Types.NestedField.optional(1, "payload", Types.VariantType.get()));

        Assert.assertTrue(IcebergUtils.shouldCollectColumnStats(table, schema));
    }

    @Test
    public void testIcebergDefaultsStaySeparateFromDorisColumnDefault() {
        Schema schema = new Schema(
                Types.NestedField.optional("added_column")
                        .withId(1)
                        .ofType(Types.IntegerType.get())
                        .withInitialDefault(7)
                        .withWriteDefault(9)
                        .build(),
                Types.NestedField.optional("added_timestamp")
                        .withId(2)
                        .ofType(Types.TimestampType.withoutZone())
                        .withInitialDefault(1_704_067_200_123_456L)
                        .build(),
                Types.NestedField.optional("added_timestamptz")
                        .withId(6)
                        .ofType(Types.TimestampType.withZone())
                        .withInitialDefault(1_704_067_200_123_456L)
                        .build(),
                Types.NestedField.optional("added_uuid")
                        .withId(3)
                        .ofType(Types.UUIDType.get())
                        .withInitialDefault(UUID.fromString("00000000-0000-0000-0000-000000000000"))
                        .build(),
                Types.NestedField.optional("added_binary")
                        .withId(4)
                        .ofType(Types.BinaryType.get())
                        .withInitialDefault(ByteBuffer.wrap(new byte[] {0, 1, 2, (byte) 0xFF}))
                        .build(),
                Types.NestedField.optional("added_fixed")
                        .withId(5)
                        .ofType(Types.FixedType.ofLength(4))
                        .withInitialDefault(ByteBuffer.wrap(new byte[] {3, 2, 1, 0}))
                        .build());

        List<Column> columns = IcebergUtils.parseSchema(schema, true, false);

        for (Column column : columns) {
            Assert.assertNull(column.getDefaultValue());
        }

        Map<Integer, String> serializedDefaults =
                IcebergUtils.getSerializedInitialDefaults(schema, false);
        Assert.assertEquals("7", serializedDefaults.get(1));
        Assert.assertEquals("2024-01-01 00:00:00.123456", serializedDefaults.get(2));
        Assert.assertEquals("2024-01-01 00:00:00.123456+00:00", serializedDefaults.get(6));
        Assert.assertEquals("AAAAAAAAAAAAAAAAAAAAAA==", serializedDefaults.get(3));
        Assert.assertEquals("AAEC/w==", serializedDefaults.get(4));
        Assert.assertEquals("AwIBAA==", serializedDefaults.get(5));

        Map<Integer, String> base64Defaults = IcebergUtils.getBase64EncodedInitialDefaults(schema);
        Assert.assertEquals("AAAAAAAAAAAAAAAAAAAAAA==", base64Defaults.get(3));
        Assert.assertEquals("AAEC/w==", base64Defaults.get(4));
        Assert.assertEquals("AwIBAA==", base64Defaults.get(5));
    }

    @Test
    public void testLegacyTimestamptzMissingColumnExpressionUsesSessionTimeZone() {
        Types.NestedField field = Types.NestedField.optional("event_time")
                .withId(1)
                .ofType(Types.TimestampType.withZone())
                .withInitialDefault(1_737_162_123_654_321L)
                .build();
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setTimeZone("Asia/Shanghai");
        context.setThreadLocalInfo();
        try {
            Assert.assertEquals("2025-01-18 09:02:03.654321",
                    IcebergUtils.getSerializedInitialDefaultForDorisExpression(field, false));
            Assert.assertEquals("2025-01-18 01:02:03.654321+00:00",
                    IcebergUtils.getSerializedInitialDefaultForDorisExpression(field, true));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testParseSchemaPreservesNestedInitialDefaultsAndRequiredness() {
        Types.NestedField nestedInt = Types.NestedField.required("nested_int")
                .withId(2)
                .ofType(Types.IntegerType.get())
                .withInitialDefault(17)
                .build();
        Types.NestedField nestedBinary = Types.NestedField.optional("nested_binary")
                .withId(3)
                .ofType(Types.BinaryType.get())
                .withInitialDefault(ByteBuffer.wrap(new byte[] {0, 1, 2, (byte) 0xFF}))
                .build();
        Types.NestedField nestedUuidWithoutDefault = Types.NestedField.optional("nested_uuid")
                .withId(4)
                .ofType(Types.UUIDType.get())
                .build();
        Schema schema = new Schema(Types.NestedField.required("payload")
                .withId(1)
                .ofType(Types.StructType.of(nestedInt, nestedBinary, nestedUuidWithoutDefault))
                .build());

        List<Column> columns = IcebergUtils.parseSchema(schema, true, false);
        Assert.assertTrue(columns.get(0).isAllowNull());
        Assert.assertTrue(columns.get(0).getChildren().get(0).isAllowNull());
        Assert.assertTrue(columns.get(0).getChildren().get(1).isAllowNull());
        Assert.assertTrue(columns.get(0).getChildren().get(2).isAllowNull());
        Assert.assertEquals(ImmutableSet.of(1, 2), IcebergUtils.getRequiredFieldIds(schema.columns()));

        Map<Integer, String> defaults = IcebergUtils.getSerializedInitialDefaults(schema, false);
        Assert.assertEquals("17", defaults.get(2));
        Assert.assertEquals("AAEC/w==", defaults.get(3));
        Assert.assertFalse(defaults.containsKey(4));
        Assert.assertEquals(Collections.singleton(3),
                IcebergUtils.getBase64EncodedInitialDefaults(schema).keySet());
        Assert.assertEquals(ImmutableSet.of(3, 4), IcebergUtils.getBinaryLikeFieldIds(schema));
    }

    @Test
    public void testParseSchemaPreservesNestedNonBinaryInitialDefault() {
        Schema schema = new Schema(Types.NestedField.optional(10, "s", Types.StructType.of(
                Types.NestedField.optional("added_int")
                        .withId(11)
                        .ofType(Types.IntegerType.get())
                        .withInitialDefault(7)
                        .build())));

        List<Column> columns = IcebergUtils.parseSchema(schema, true, false);

        Assert.assertNull(columns.get(0).getChildren().get(0).getDefaultValue());
        Assert.assertEquals("7", IcebergUtils.getSerializedInitialDefaults(schema, false).get(11));
    }

    @Test
    public void testGetPartitionInfoMapSkipBinaryIdentityPartition() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "partition_bin", Types.BinaryType.get()));
        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema).identity("partition_bin").build();
        PartitionData partitionData = new PartitionData(partitionSpec.partitionType());
        partitionData.set(0, ByteBuffer.wrap(new byte[] {0x0F, (byte) 0xF1, 0x02, (byte) 0xFD, (byte) 0xFE,
                (byte) 0xFF}));

        Map<String, String> partitionInfoMap = IcebergUtils.getPartitionInfoMap(partitionData, partitionSpec, "UTC");
        Assert.assertNull(partitionInfoMap);
    }

    @Test
    public void testGetIdentityPartitionColumnsIgnoresTransformPartitions() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "Dt", Types.StringType.get()),
                Types.NestedField.required(3, "ts", Types.TimestampType.withoutZone()));
        PartitionSpec specWithTransform = PartitionSpec.builderFor(schema)
                .withSpecId(1)
                .identity("Dt")
                .day("ts")
                .build();
        PartitionSpec identityOnlySpec = PartitionSpec.builderFor(schema)
                .withSpecId(2)
                .identity("id")
                .build();
        Map<Integer, PartitionSpec> specs = new LinkedHashMap<>();
        specs.put(specWithTransform.specId(), specWithTransform);
        specs.put(identityOnlySpec.specId(), identityOnlySpec);

        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.specs()).thenReturn(specs);

        Assert.assertEquals(Arrays.asList("Dt", "id"), IcebergUtils.getIdentityPartitionColumns(table));
    }

    @Test
    public void testGetCommonIdentityPartitionColumnsUsesSafeIntersection() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "Dt", Types.StringType.get()),
                Types.NestedField.required(3, "ts", Types.TimestampType.withoutZone()));
        PartitionSpec oldSpec = PartitionSpec.builderFor(schema)
                .withSpecId(1)
                .identity("id")
                .identity("Dt")
                .build();
        PartitionSpec currentSpec = PartitionSpec.builderFor(schema)
                .withSpecId(2)
                .identity("Dt")
                .day("ts")
                .build();
        Map<Integer, PartitionSpec> specs = new LinkedHashMap<>();
        specs.put(oldSpec.specId(), oldSpec);
        specs.put(currentSpec.specId(), currentSpec);

        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(currentSpec);
        Mockito.when(table.specs()).thenReturn(specs);

        Assert.assertEquals(Collections.singletonList("Dt"),
                IcebergUtils.getCommonIdentityPartitionColumns(table));
    }

    @Test
    public void testGetIdentityPartitionInfoMapReturnsIdentityColumnsOnly() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "Dt", Types.StringType.get()),
                Types.NestedField.required(2, "ts", Types.TimestampType.withoutZone()));
        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
                .identity("Dt")
                .day("ts")
                .build();
        PartitionData partitionData = new PartitionData(partitionSpec.partitionType());
        partitionData.set(0, "2025-01-01");
        partitionData.set(1, 20000);

        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);

        Map<String, String> partitionInfoMap = IcebergUtils.getIdentityPartitionInfoMap(
                partitionData, partitionSpec, table, "UTC");
        Assert.assertEquals(Collections.singletonMap("Dt", "2025-01-01"), partitionInfoMap);
    }

    @Test
    public void testMappedTypesAreExcludedFromPartitionMetadata() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "Dt", Types.StringType.get()),
                Types.NestedField.required(2, "uuid_col", Types.UUIDType.get()),
                Types.NestedField.required(3, "ts_tz", Types.TimestampType.withZone()));
        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
                .identity("Dt")
                .identity("uuid_col")
                .identity("ts_tz")
                .build();
        PartitionData partitionData = new PartitionData(partitionSpec.partitionType());
        partitionData.set(0, "2026-08-03");
        partitionData.set(1, UUID.fromString("123e4567-e89b-12d3-a456-426614174000"));
        partitionData.set(2, 0L);

        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.spec()).thenReturn(partitionSpec);
        Mockito.when(table.specs()).thenReturn(Collections.singletonMap(partitionSpec.specId(), partitionSpec));

        Assert.assertEquals(Collections.singletonList("Dt"),
                IcebergUtils.getIdentityPartitionColumns(table, true, true));
        Assert.assertEquals(Collections.singletonList("Dt"),
                IcebergUtils.getCommonIdentityPartitionColumns(table, true, true));
        Assert.assertEquals(Collections.singletonMap("Dt", "2026-08-03"),
                IcebergUtils.getIdentityPartitionInfoMap(
                        partitionData, partitionSpec, table, "Asia/Shanghai", true, true));
    }

    @Test
    public void testGetIdentityPartitionInfoMapSupportsFloatingPointPartitions() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "float_partition", Types.FloatType.get()),
                Types.NestedField.required(2, "double_partition", Types.DoubleType.get()));
        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
                .identity("float_partition")
                .identity("double_partition")
                .build();
        float floatValue = Math.nextUp(0.1F);
        double doubleValue = Math.nextUp(0.1D);
        PartitionData partitionData = new PartitionData(partitionSpec.partitionType());
        partitionData.set(0, floatValue);
        partitionData.set(1, doubleValue);

        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);

        Map<String, String> partitionInfoMap = IcebergUtils.getIdentityPartitionInfoMap(
                partitionData, partitionSpec, table, "UTC");

        String serializedFloat = partitionInfoMap.get("float_partition");
        String serializedDouble = partitionInfoMap.get("double_partition");
        Assert.assertEquals(Float.toString(floatValue), serializedFloat);
        Assert.assertEquals(Double.toString(doubleValue), serializedDouble);
        Assert.assertEquals(Float.floatToIntBits(floatValue),
                Float.floatToIntBits(Float.parseFloat(serializedFloat)));
        Assert.assertEquals(Double.doubleToLongBits(doubleValue),
                Double.doubleToLongBits(Double.parseDouble(serializedDouble)));
    }

    @Test
    public void testParseFloatingPointPartitionValueSupportsSpecialValues() {
        Assert.assertTrue(Float.isNaN(
                (Float) IcebergUtils.parsePartitionValueFromString("NaN", Types.FloatType.get())));
        Assert.assertTrue(Float.isNaN(
                (Float) IcebergUtils.parsePartitionValueFromString("nan", Types.FloatType.get())));
        Assert.assertEquals(Float.POSITIVE_INFINITY,
                (Float) IcebergUtils.parsePartitionValueFromString("Infinity", Types.FloatType.get()), 0.0F);
        Assert.assertEquals(Float.NEGATIVE_INFINITY,
                (Float) IcebergUtils.parsePartitionValueFromString("-inf", Types.FloatType.get()), 0.0F);
        Assert.assertTrue(Double.isNaN(
                (Double) IcebergUtils.parsePartitionValueFromString("NaN", Types.DoubleType.get())));
        Assert.assertTrue(Double.isNaN(
                (Double) IcebergUtils.parsePartitionValueFromString("nan", Types.DoubleType.get())));
        Assert.assertEquals(Double.POSITIVE_INFINITY,
                (Double) IcebergUtils.parsePartitionValueFromString("Infinity", Types.DoubleType.get()), 0.0D);
        Assert.assertEquals(Double.NEGATIVE_INFINITY,
                (Double) IcebergUtils.parsePartitionValueFromString("-inf", Types.DoubleType.get()), 0.0D);
    }

    @Test
    public void testGetMatchingManifest() {

        // partition : 100 - 200
        ManifestFile f1 = getManifestFileForDataTypeWithPartitionSummary(
                "manifest_f1.avro",
                Collections.singletonList(new GenericPartitionFieldSummary(
                    false, false, getByteBufferForLong(100), getByteBufferForLong(200))));

        // partition : 300 - 400
        ManifestFile f2 = getManifestFileForDataTypeWithPartitionSummary(
                "manifest_f2.avro",
                Collections.singletonList(new GenericPartitionFieldSummary(
                    false, false, getByteBufferForLong(300), getByteBufferForLong(400))));

        // partition : 500 - 600
        ManifestFile f3 = getManifestFileForDataTypeWithPartitionSummary(
                "manifest_f3.avro",
                    Collections.singletonList(new GenericPartitionFieldSummary(
                        false, false, getByteBufferForLong(500), getByteBufferForLong(600))));

        List<ManifestFile> manifestFiles = new ArrayList<ManifestFile>() {{
                add(f1);
                add(f2);
                add(f3);
            }};

        Schema schema = new Schema(
                StructType.of(
                        Types.NestedField.required(1, "id", LongType.get()),
                        Types.NestedField.required(2, "data", LongType.get()),
                        Types.NestedField.required(3, "par", LongType.get()))
                    .fields());

        // test empty partition spec
        HashMap<Integer, PartitionSpec> emptyPartitionSpecsById = new HashMap<Integer, PartitionSpec>() {{
                put(0, PartitionSpec.builderFor(schema).build());
            }};
        assertManifest(manifestFiles, emptyPartitionSpecsById, Expressions.alwaysTrue(), manifestFiles);

        // test long partition spec
        HashMap<Integer, PartitionSpec> longPartitionSpecsById = new HashMap<Integer, PartitionSpec>() {{
                put(0, PartitionSpec.builderFor(schema).identity("par").build());
            }};
        // 1. par > 10
        UnboundPredicate<Long> e1 = Expressions.greaterThan("par", 10L);
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(Expressions.alwaysTrue(), e1), manifestFiles);

        // 2. 10 < par < 90
        UnboundPredicate<Long> e2 = Expressions.greaterThan("par", 90L);
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(e1, e2), manifestFiles);

        // 3. 10 < par < 300
        UnboundPredicate<Long> e3 = Expressions.lessThan("par", 300L);
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(e1, e3), Collections.singletonList(f1));

        // 4. 10 < par < 400
        UnboundPredicate<Long> e4 = Expressions.lessThan("par", 400L);
        ArrayList<ManifestFile> expect1 = new ArrayList<ManifestFile>() {{
                add(f1);
                add(f2);
            }};
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(e1, e4), expect1);

        // 5. 10 < par < 501
        UnboundPredicate<Long> e5 = Expressions.lessThan("par", 501L);
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(e1, e5), manifestFiles);

        // 6. 200 < par < 501
        UnboundPredicate<Long> e6 = Expressions.greaterThan("par", 200L);
        ArrayList<ManifestFile> expect2 = new ArrayList<ManifestFile>() {{
                add(f2);
                add(f3);
            }};
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(e6, e5), expect2);

        // 7. par > 600
        UnboundPredicate<Long> e7 = Expressions.greaterThan("par", 600L);
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(Expressions.alwaysTrue(), e7), Collections.emptyList());

        // 8. par < 100
        UnboundPredicate<Long> e8 = Expressions.lessThan("par", 100L);
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(Expressions.alwaysTrue(), e8), Collections.emptyList());
    }

    private void assertManifest(List<ManifestFile> dataManifests,
                                Map<Integer, PartitionSpec> specsById,
                                Expression dataFilter,
                                List<ManifestFile> expected) {
        CloseableIterable<ManifestFile> matchingManifest =
                IcebergUtils.getMatchingManifest(dataManifests, specsById, dataFilter);
        List<ManifestFile> ret = new ArrayList<>();
        matchingManifest.forEach(ret::add);
        ret.sort(Comparator.comparing(ManifestFile::path));
        Assert.assertEquals(expected, ret);
    }

    private ByteBuffer getByteBufferForLong(long num) {
        return Conversions.toByteBuffer(Types.LongType.get(), num);
    }

    private ManifestFile getManifestFileForDataTypeWithPartitionSummary(
            String path,
            List<PartitionFieldSummary> partitionFieldSummaries) {
        ManifestFile file = Mockito.mock(ManifestFile.class);
        Mockito.when(file.path()).thenReturn(path);
        Mockito.when(file.length()).thenReturn(1024L);
        Mockito.when(file.partitionSpecId()).thenReturn(0);
        Mockito.when(file.content()).thenReturn(ManifestContent.DATA);
        Mockito.when(file.sequenceNumber()).thenReturn(1L);
        Mockito.when(file.minSequenceNumber()).thenReturn(1L);
        Mockito.when(file.snapshotId()).thenReturn(123456789L);
        Mockito.when(file.partitions()).thenReturn(partitionFieldSummaries);
        Mockito.when(file.addedFilesCount()).thenReturn(1);
        Mockito.when(file.addedRowsCount()).thenReturn(100L);
        Mockito.when(file.existingFilesCount()).thenReturn(0);
        Mockito.when(file.existingRowsCount()).thenReturn(0L);
        Mockito.when(file.deletedFilesCount()).thenReturn(0);
        Mockito.when(file.deletedRowsCount()).thenReturn(0L);
        Mockito.when(file.hasAddedFiles()).thenReturn(true);
        Mockito.when(file.hasExistingFiles()).thenReturn(false);
        Mockito.when(file.copy()).thenReturn(file);
        return file;
    }

    @Test
    public void testGetQuerySpecSnapshot() throws UserException {
        Table table = Mockito.mock(Table.class);

        // init schemas 0,1,2
        HashMap<Integer, Schema> schemas = new HashMap<>();
        schemas.put(0, mockSchemaWithId(0));
        schemas.put(1, mockSchemaWithId(1));
        schemas.put(2, mockSchemaWithId(2));
        Mockito.when(table.schemas()).thenReturn(schemas);
        // init current schema
        Mockito.when(table.schema()).thenReturn(schemas.get(2));

        // init snapshot 1,2,3,4
        Snapshot s1 = mockSnapshot(1, 0);
        Mockito.when(table.snapshot(1)).thenReturn(s1);
        Snapshot s2 = mockSnapshot(2, 0);
        Mockito.when(table.snapshot(2)).thenReturn(s2);
        Snapshot s3 = mockSnapshot(3, 1);
        Mockito.when(table.snapshot(3)).thenReturn(s3);
        Snapshot s4 = mockSnapshot(4, 1);
        Mockito.when(table.snapshot(4)).thenReturn(s4);

        // init history for snapshots
        List<HistoryEntry> history = new ArrayList<>();
        history.add(mockHistory(1, "2025-05-01 12:34:56"));
        history.add(mockHistory(2, "2025-05-01 22:34:56"));
        history.add(mockHistory(3, "2025-05-02 12:34:56"));
        history.add(mockHistory(4, "2025-05-03 12:34:56"));
        Mockito.when(table.history()).thenReturn(history);

        // create some refs
        HashMap<String, SnapshotRef> refs = new HashMap<>();
        String tag1 = "tag1";
        refs.put(tag1, SnapshotRef.tagBuilder(1).build());
        String branch1 = "branch1";
        refs.put(branch1, SnapshotRef.branchBuilder(1).build());
        String branch2 = "branch2";
        refs.put(branch2, SnapshotRef.branchBuilder(3).build());
        Mockito.when(table.refs()).thenReturn(refs);

        // query tag1
        assertQuerySpecSnapshotByVersionOf(table, tag1, 1, 0, tag1);
        assertQuerySpecSnapshotByAtTagMap(table, tag1, 1, 0, tag1);
        assertQuerySpecSnapshotByAtTagList(table, tag1, 1, 0, tag1);

        // query branch1
        assertQuerySpecSnapshotByVersionOf(table, branch1, 1, 2, branch1);
        assertQuerySpecSnapshotByAtBranchMap(table, branch1, 1, 2, branch1);
        assertQuerySpecSnapshotByAtBranchList(table, branch1, 1, 2, branch1);

        // query branch2
        assertQuerySpecSnapshotByVersionOf(table, branch2, 3, 2, branch2);
        assertQuerySpecSnapshotByAtBranchMap(table, branch2, 3, 2, branch2);
        assertQuerySpecSnapshotByAtBranchList(table, branch2, 3, 2, branch2);

        // query snapshotId 1
        assertQuerySpecSnapshotByVersionOf(table, "1", 1, 0, null);

        // query snapshotId 2
        assertQuerySpecSnapshotByVersionOf(table, "2", 2, 0, null);

        // query snapshotId 3
        assertQuerySpecSnapshotByVersionOf(table, "3", 3, 1, null);

        // query ref not exists
        Assert.assertThrows(
                UserException.class,
                () -> assertQuerySpecSnapshotByVersionOf(table, "ref_not_exists", -1, -1, null));

        // query snapshotId not exists
        Assert.assertThrows(
                UserException.class,
                () -> assertQuerySpecSnapshotByVersionOf(table, "99", -3, -1, null));

        // query branch not exists
        Assert.assertThrows(
                UserException.class,
                () -> assertQuerySpecSnapshotByAtBranchMap(table, "branch_not_exists", -3, -1, null));

        // query tag not exists
        Assert.assertThrows(
                UserException.class,
                () -> assertQuerySpecSnapshotByAtTagMap(table, "tag_not_exists", -3, -1, null));

        // query tag with @branch
        Assert.assertThrows(
                UserException.class,
                () -> assertQuerySpecSnapshotByAtBranchMap(table, tag1, -3, -1, null));

        // query branch with @tag
        Assert.assertThrows(
                UserException.class,
                () -> assertQuerySpecSnapshotByAtTagMap(table, branch1, -3, -1, null));
        Assert.assertThrows(
                UserException.class,
                () -> assertQuerySpecSnapshotByAtTagMap(table, branch2, -3, -1, null));

        // query version with tag
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> IcebergUtils.getQuerySpecSnapshot(
                    table,
                    Optional.of(TableSnapshot.timeOf("v1")),
                    Optional.of(new TableScanParams("tag", null,
                            new ArrayList<String>() {{
                                    add("v1");
                                }
                            }))
                ));

        // query version with branch
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> IcebergUtils.getQuerySpecSnapshot(
                    table,
                    Optional.of(TableSnapshot.timeOf("v1")),
                    Optional.of(new TableScanParams("branch", null,
                            new ArrayList<String>() {{
                                    add("v1");
                                }
                            }))
                ));

        // query branch with invalid param
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> IcebergUtils.getQuerySpecSnapshot(
                        table,
                        Optional.empty(),
                        Optional.of(new TableScanParams("branch",
                                ImmutableMap.of(
                                        "k1", "k2"),
                                null))
                ));

        // query time
        assertQuerySpecSnapshotByTimeOf(table, "2025-05-01 12:34:56", 1, 0, null);
        assertQuerySpecSnapshotByTimeOf(table, "2025-05-01 14:34:56", 1, 0, null);
        assertQuerySpecSnapshotByTimeOf(table, "2025-05-02 11:34:56", 2, 0, null);
        assertQuerySpecSnapshotByTimeOf(table, "2025-05-02 12:34:56", 3, 1, null);
        assertQuerySpecSnapshotByTimeOf(table, "2025-05-03 12:34:56", 4, 1, null);

        // query invalid time format
        Assert.assertThrows(
                DateTimeException.class,
                () -> assertQuerySpecSnapshotByTimeOf(table, "1212-240", 3, 1, null)
        );

        // query invalid time
        Assert.assertThrows(
                IllegalArgumentException.class,
                () -> assertQuerySpecSnapshotByTimeOf(table, "2025-05-01 12:34:55", 3, 1, null)
        );
    }

    private Snapshot mockSnapshot(long snapshotId, int schemaId) {
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.when(snapshot.snapshotId()).thenReturn(snapshotId);
        Mockito.when(snapshot.schemaId()).thenReturn(schemaId);
        return snapshot;
    }

    private HistoryEntry mockHistory(long snapshotId, String time) {
        HistoryEntry historyEntry = Mockito.mock(HistoryEntry.class);
        Mockito.when(historyEntry.snapshotId()).thenReturn(snapshotId);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dateTime = LocalDateTime.parse(time, formatter);
        long millis = dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

        Mockito.when(historyEntry.timestampMillis()).thenReturn(millis);
        return historyEntry;
    }

    private Schema mockSchemaWithId(int id) {
        Schema schema = Mockito.mock(Schema.class);
        Mockito.when(schema.schemaId()).thenReturn(id);
        return schema;
    }

    // select * from tb for version as of ...
    private void assertQuerySpecSnapshotByVersionOf(
            Table table,
            String version,
            long expectSnapshotId,
            int expectSchemaId,
            String expectRef) throws UserException {
        Optional<TableSnapshot> tableSnapshot = Optional.of(TableSnapshot.versionOf(version));
        IcebergTableQueryInfo queryInfo = IcebergUtils.getQuerySpecSnapshot(table, tableSnapshot, Optional.empty());
        assertQueryInfo(queryInfo, expectSnapshotId, expectSchemaId, expectRef);
    }

    // select * from tb for time as of ...
    private void assertQuerySpecSnapshotByTimeOf(
            Table table,
            String version,
            long expectSnapshotId,
            int expectSchemaId,
            String expectRef) throws UserException {
        Optional<TableSnapshot> tableSnapshot = Optional.of(TableSnapshot.timeOf(version));
        IcebergTableQueryInfo queryInfo = IcebergUtils.getQuerySpecSnapshot(table, tableSnapshot, Optional.empty());
        assertQueryInfo(queryInfo, expectSnapshotId, expectSchemaId, expectRef);
    }

    // select * from abc@tag('name'='tag_name')
    private void assertQuerySpecSnapshotByAtTagMap(
            Table table,
            String version,
            long expectSnapshotId,
            int expectSchemaId,
            String expectRef) throws UserException {
        HashMap<String, String> map = new HashMap<>();
        map.put("name", version);
        TableScanParams tsp = new TableScanParams("tag", map, null);
        IcebergTableQueryInfo queryInfo = IcebergUtils.getQuerySpecSnapshot(table, Optional.empty(), Optional.of(tsp));
        assertQueryInfo(queryInfo, expectSnapshotId, expectSchemaId, expectRef);
    }

    // select * from abc@tag(tag_name)
    private void assertQuerySpecSnapshotByAtTagList(
            Table table,
            String version,
            long expectSnapshotId,
            int expectSchemaId,
            String expectRef) throws UserException {
        List<String> list = new ArrayList<>();
        list.add(version);
        TableScanParams tsp = new TableScanParams("tag", null, list);
        IcebergTableQueryInfo queryInfo = IcebergUtils.getQuerySpecSnapshot(table, Optional.empty(), Optional.of(tsp));
        assertQueryInfo(queryInfo, expectSnapshotId, expectSchemaId, expectRef);
    }

    // select * from abc@branch('name'='branch_name')
    private void assertQuerySpecSnapshotByAtBranchMap(
            Table table,
            String version,
            long expectSnapshotId,
            int expectSchemaId,
            String expectRef) throws UserException {
        HashMap<String, String> map = new HashMap<>();
        map.put("name", version);
        TableScanParams tsp = new TableScanParams("branch", map, null);
        IcebergTableQueryInfo queryInfo = IcebergUtils.getQuerySpecSnapshot(table, Optional.empty(), Optional.of(tsp));
        assertQueryInfo(queryInfo, expectSnapshotId, expectSchemaId, expectRef);
    }

    // select * from abc@branch(branch_name)
    private void assertQuerySpecSnapshotByAtBranchList(
            Table table,
            String version,
            long expectSnapshotId,
            int expectSchemaId,
            String expectRef) throws UserException {
        List<String> list = new ArrayList<>();
        list.add(version);
        TableScanParams tsp = new TableScanParams("branch", null, list);
        IcebergTableQueryInfo queryInfo = IcebergUtils.getQuerySpecSnapshot(table, Optional.empty(), Optional.of(tsp));
        assertQueryInfo(queryInfo, expectSnapshotId, expectSchemaId, expectRef);
    }

    private void assertQueryInfo(
            IcebergTableQueryInfo queryInfo,
            long expectSnapshotId,
            int expectSchemaId,
            String expectRef) {
        Assert.assertEquals(expectSnapshotId, queryInfo.getSnapshotId());
        Assert.assertEquals(expectSchemaId, queryInfo.getSchemaId());
        Assert.assertEquals(expectRef, queryInfo.getRef());
    }
}
