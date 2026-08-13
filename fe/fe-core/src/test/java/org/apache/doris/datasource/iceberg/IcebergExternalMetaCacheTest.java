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

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.iceberg.cache.ManifestCacheValue;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.MetaCacheEntryStats;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class IcebergExternalMetaCacheTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testSnapshotAndManifestWeightsScaleLinearlyToOneHundredThousandItems() {
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        IcebergSnapshotEntryKey snapshotKey = IcebergSnapshotEntryKey.tryCreate(
                mapping, tableWithMetadataLocation("/metadata/linear-v1.json")).get();
        long snapshotBase = snapshotWeight(snapshotKey, 0);
        long snapshotOneThousand = snapshotWeight(snapshotKey, 1_000);
        assertLinearScale(snapshotBase, snapshotOneThousand,
                snapshotWeight(snapshotKey, 10_000), snapshotWeight(snapshotKey, 100_000));

        IcebergManifestEntryKey manifestKey = new IcebergManifestEntryKey(
                "/manifest/linear.avro", ManifestContent.DATA);
        long manifestBase = manifestWeight(manifestKey, 0);
        long manifestOneThousand = manifestWeight(manifestKey, 1_000);
        assertLinearScale(manifestBase, manifestOneThousand,
                manifestWeight(manifestKey, 10_000), manifestWeight(manifestKey, 100_000));
    }

    @Test
    public void testWeightedEntriesAreRegistered() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            Map<String, String> properties = com.google.common.collect.Maps.newHashMap();
            properties.put("meta.cache.iceberg.table.max-weight", "4MB");
            properties.put("meta.cache.iceberg.snapshot.max-weight", "8MB");
            properties.put("meta.cache.iceberg.manifest.enable", "true");
            properties.put("meta.cache.iceberg.manifest.max-weight", "16MB");
            cache.initCatalog(1L, properties);

            Map<String, MetaCacheEntryStats> stats = cache.stats(1L);
            Assert.assertTrue(stats.get(IcebergExternalMetaCache.ENTRY_TABLE).isWeightBounded());
            Assert.assertTrue(stats.get(IcebergExternalMetaCache.ENTRY_SNAPSHOT).isWeightBounded());
            Assert.assertTrue(stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST).isWeightBounded());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSnapshotInheritsLegacyTableCountSettingsButNotWeight() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            Map<String, String> properties = com.google.common.collect.Maps.newHashMap();
            properties.put("meta.cache.iceberg.table.enable", "false");
            properties.put("meta.cache.iceberg.table.ttl-second", "17");
            properties.put("meta.cache.iceberg.table.capacity", "23");
            cache.initCatalog(1L, properties);

            MetaCacheEntryStats snapshot = cache.stats(1L).get(IcebergExternalMetaCache.ENTRY_SNAPSHOT);
            Assert.assertFalse(snapshot.isConfigEnabled());
            Assert.assertEquals(17L, snapshot.getTtlSecond());
            Assert.assertEquals(23L, snapshot.getCapacity());
            Assert.assertFalse(snapshot.isWeightBounded());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSnapshotKeyIncludesMetadataGeneration() {
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        Table first = tableWithMetadataLocation("/metadata/v1.json");
        Table second = tableWithMetadataLocation("/metadata/v2.json");
        Table recreated = tableWithMetadataLocation("/metadata/v1.json");

        IcebergSnapshotEntryKey firstKey = IcebergSnapshotEntryKey.tryCreate(mapping, first).get();
        IcebergSnapshotEntryKey sameKey = IcebergSnapshotEntryKey.tryCreate(mapping, first).get();
        IcebergSnapshotEntryKey secondKey = IcebergSnapshotEntryKey.tryCreate(mapping, second).get();
        IcebergSnapshotEntryKey recreatedKey = IcebergSnapshotEntryKey.tryCreate(mapping, recreated).get();

        Assert.assertEquals(firstKey, sameKey);
        Assert.assertNotEquals(firstKey, secondKey);
        Assert.assertNotEquals("drop/recreate may reuse HadoopCatalog's v1 path", firstKey, recreatedKey);
        Assert.assertEquals("/metadata/v1.json", firstKey.getMetadataFileLocation());
        Assert.assertNotEquals(firstKey.getTableUuid(), recreatedKey.getTableUuid());
        Assert.assertFalse(IcebergSnapshotEntryKey.tryCreate(mapping,
                newInterfaceProxy(Table.class)).isPresent());
    }

    @Test
    public void testTableSnapshotAndManifestEstimatesArePrecomputed() {
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        Table table = tableWithMetadataLocation("/metadata/v1.json");
        IcebergTableCacheValue tableValue = new IcebergTableCacheValue(table);
        tableValue.prepareForCachePublication(mapping);
        Assert.assertTrue(tableValue.getSizeEstimate().isComplete());
        Assert.assertTrue(tableValue.getSizeEstimate().getBytes() > 0L);

        IcebergSnapshotEntryKey snapshotKey = IcebergSnapshotEntryKey.tryCreate(mapping, table).get();
        IcebergSnapshotCacheValue snapshotValue = new IcebergSnapshotCacheValue(
                IcebergPartitionInfo.empty(), new IcebergSnapshot(-1L, 0L), Optional.empty(), table);
        snapshotValue.prepareForCachePublication(snapshotKey);
        Assert.assertTrue(snapshotValue.getSizeEstimate().getIncompleteReason(),
                snapshotValue.getSizeEstimate().isComplete());
        Assert.assertTrue(snapshotValue.getSizeEstimate().getBytes() > 0L);

        IcebergManifestEntryKey manifestKey = new IcebergManifestEntryKey("/manifest/a.avro", ManifestContent.DATA);
        ManifestCacheValue manifestValue = ManifestCacheValue.forDataFiles(Collections.singletonList(
                DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/data/a.parquet").withFileSizeInBytes(10L).withRecordCount(1L).build()));
        MetaCacheSizeEstimate manifestEstimate =
                IcebergCacheSizeEstimator.estimateManifestEntry(manifestKey, manifestValue);
        Assert.assertTrue(manifestEstimate.getIncompleteReason(), manifestEstimate.isComplete());
        Assert.assertTrue(manifestEstimate.getBytes() > 0L);

        Table unsupportedTable = newInterfaceProxy(Table.class);
        MetaCacheSizeEstimate unsupported = IcebergCacheSizeEstimator.estimateTableEntry(
                mapping, new IcebergTableCacheValue(unsupportedTable));
        Assert.assertFalse(unsupported.isComplete());
        Assert.assertTrue(unsupported.getIncompleteReason().startsWith("unsupported_iceberg_table:"));
    }

    @Test
    public void testTableEstimateAccountsForNestedSchemaAndPropertyPayload() {
        String largePayload = repeatedCharacter('x', 64 * 1024);
        Table smallTable = tableWithNestedSchemaAndProperty("x", "x");
        Table largeTable = tableWithNestedSchemaAndProperty(largePayload, largePayload);
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        IcebergTableCacheValue smallValue = new IcebergTableCacheValue(smallTable);
        IcebergTableCacheValue largeValue = new IcebergTableCacheValue(largeTable);

        smallValue.prepareForCachePublication(mapping);
        largeValue.prepareForCachePublication(mapping);

        long expectedPayloadDelta = (largePayload.length() - 1L) * 4L;
        Assert.assertTrue(largeValue.getSizeEstimate().getBytes()
                - smallValue.getSizeEstimate().getBytes() >= expectedPayloadDelta);
    }

    @Test
    public void testTablePayloadCountsHistoricalSchemaSpecAndSortFields() {
        List<Types.NestedField> fields = IntStream.range(0, 100)
                .mapToObj(index -> Types.NestedField.optional(
                        index + 1, "field_" + index, Types.StringType.get()))
                .collect(Collectors.toList());
        Schema largeSchema = new Schema(0, fields);
        Schema smallSchema = new Schema(1, fields.get(0));
        TableMetadata schemaHistory = TableMetadata.newTableMetadata(
                largeSchema, PartitionSpec.unpartitioned(),
                "file:/warehouse/schema-history", Collections.emptyMap());
        schemaHistory = TableMetadata.buildFrom(schemaHistory)
                .addSchema(smallSchema)
                .setCurrentSchema(smallSchema.schemaId())
                .discardChanges()
                .build();
        TableMetadata smallSchemaOnly = TableMetadata.newTableMetadata(
                smallSchema, PartitionSpec.unpartitioned(),
                "file:/warehouse/schema-history", Collections.emptyMap());

        PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(largeSchema).withSpecId(0);
        SortOrder.Builder sortBuilder = SortOrder.builderFor(largeSchema).withOrderId(1);
        for (Types.NestedField field : fields) {
            specBuilder.identity(field.name());
            sortBuilder.asc(field.name());
        }
        TableMetadata fieldHistory = TableMetadata.newTableMetadata(
                largeSchema, specBuilder.build(), sortBuilder.build(),
                "file:/warehouse/field-history", Collections.emptyMap());
        fieldHistory = TableMetadata.buildFrom(fieldHistory)
                .setDefaultPartitionSpec(
                        PartitionSpec.builderFor(largeSchema).withSpecId(1).build())
                .setDefaultSortOrder(SortOrder.unsorted())
                .discardChanges()
                .build();
        TableMetadata emptyFields = TableMetadata.newTableMetadata(
                largeSchema, PartitionSpec.unpartitioned(), SortOrder.unsorted(),
                "file:/warehouse/field-history", Collections.emptyMap());

        long schemaDelta = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                tableWithMetadata(schemaHistory))
                - IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                        tableWithMetadata(smallSchemaOnly));
        long specAndSortDelta = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                tableWithMetadata(fieldHistory))
                - IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                        tableWithMetadata(emptyFields));

        Assert.assertTrue(schemaDelta >= 99L * 512L);
        Assert.assertTrue(specAndSortDelta >= 100L * (384L + 256L));
    }

    @Test
    public void testTablePayloadExcludesQueryLocalHistoricalMetadata() {
        String largePayload = repeatedCharacter('x', 64 * 1024);
        long smallBytes = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                tableWithMetadata(metadataWithMaterializedPayload("x", 32)));
        long largeBytes = IcebergCacheSizeEstimator.retainedTablePayloadBytes(
                tableWithMetadata(metadataWithMaterializedPayload(largePayload, 64 * 1024)));

        Assert.assertEquals(smallBytes, largeBytes);
    }

    @Test
    public void testTableEstimateExcludesQueryLocalBranchHistory() {
        TableMetadata oneCommit = metadataWithSnapshotSequence(1L);
        TableMetadata tenThousandCommits = metadataWithSnapshotSequence(10_000L);
        IcebergTableCacheValue smallValue = new IcebergTableCacheValue(tableWithMetadata(oneCommit));
        IcebergTableCacheValue largeValue = new IcebergTableCacheValue(
                tableWithMetadata(tenThousandCommits));
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");

        smallValue.prepareForCachePublication(mapping);
        largeValue.prepareForCachePublication(mapping);

        Assert.assertTrue(smallValue.getSizeEstimate().getIncompleteReason(),
                smallValue.getSizeEstimate().isComplete());
        Assert.assertTrue(largeValue.getSizeEstimate().getIncompleteReason(),
                largeValue.getSizeEstimate().isComplete());
        Assert.assertEquals(smallValue.getSizeEstimate().getBytes(),
                largeValue.getSizeEstimate().getBytes());
        Mockito.verify(oneCommit, Mockito.never()).snapshots();
        Mockito.verify(tenThousandCommits, Mockito.never()).snapshots();
        Mockito.verify(oneCommit, Mockito.never()).lastSequenceNumber();
        Mockito.verify(tenThousandCommits, Mockito.never()).lastSequenceNumber();
        Mockito.verify(oneCommit, Mockito.never()).snapshotLog();
        Mockito.verify(tenThousandCommits, Mockito.never()).snapshotLog();
        Mockito.verify(oneCommit, Mockito.never()).refs();
        Mockito.verify(tenThousandCommits, Mockito.never()).refs();
    }

    @Test
    public void testWeightedTablePreparationRunsInsideCatalogAuthenticator() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicBoolean authenticated = new AtomicBoolean();
        AtomicBoolean firstPreparation = new AtomicBoolean(true);
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        IcebergMetadataOps metadataOps = Mockito.mock(IcebergMetadataOps.class);
        Table table = tableWithMetadataLocation("/metadata/authenticated-v1.json");
        Mockito.when(catalog.getMetadataOps()).thenReturn(metadataOps);
        Mockito.when(metadataOps.loadTable("remote_db", "remote_tbl")).thenReturn(table);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
            @Override
            public <T> T execute(Callable<T> task) throws Exception {
                Assert.assertTrue(authenticated.compareAndSet(false, true));
                try {
                    return task.call();
                } finally {
                    authenticated.set(false);
                }
            }
        });
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor) {
            @Override
            protected CatalogIf<?> getCatalog(long catalogId) {
                return catalog;
            }

            @Override
            MetaCacheSizeEstimate prepareTableForCachePublication(
                    NameMapping nameMapping, IcebergTableCacheValue value) {
                if (firstPreparation.compareAndSet(true, false)) {
                    Assert.assertTrue("publication preparation must retain Kerberos scope",
                            authenticated.get());
                }
                return super.prepareTableForCachePublication(nameMapping, value);
            }
        };
        try {
            cache.initCatalog(1L, Collections.singletonMap(
                    "meta.cache.iceberg.table.max-weight", "4MB"));
            NameMapping mapping = new NameMapping(
                    1L, "db", "tbl", "remote_db", "remote_tbl");

            IcebergTableCacheValue value = cache.entry(1L, IcebergExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, IcebergTableCacheValue.class).get(mapping);
            IcebergTableCacheValue cached = cache.entry(1L, IcebergExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, IcebergTableCacheValue.class).get(mapping);

            Assert.assertTrue(value.getSizeEstimate().getIncompleteReason(),
                    value.getSizeEstimate().isComplete());
            Assert.assertSame(value, cached);
            Mockito.verify(metadataOps, Mockito.times(1)).loadTable("remote_db", "remote_tbl");
            Assert.assertFalse(authenticated.get());
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testExactMetadataReadsRunInsideCatalogAuthenticator() throws Exception {
        String tableLocation = temporaryFolder.newFolder("authenticated-metadata").toURI().toString();
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Table liveTable = new HadoopTables(new Configuration()).create(
                schema, PartitionSpec.unpartitioned(), tableLocation);
        AtomicBoolean authenticated = new AtomicBoolean();
        AtomicInteger metadataReads = new AtomicInteger();
        ExecutionAuthenticator authenticator = new ExecutionAuthenticator() {
            @Override
            public <T> T execute(Callable<T> task) throws Exception {
                Assert.assertTrue(authenticated.compareAndSet(false, true));
                try {
                    return task.call();
                } finally {
                    authenticated.set(false);
                }
            }
        };
        FileIO trackingFileIO = Mockito.mock(FileIO.class);
        Mockito.when(trackingFileIO.newInputFile(Mockito.anyString())).thenAnswer(invocation -> {
            Assert.assertTrue("metadata FileIO must retain catalog authentication", authenticated.get());
            metadataReads.incrementAndGet();
            return liveTable.io().newInputFile((String) invocation.getArgument(0));
        });
        TableMetadata metadata = ((HasTableOperations) liveTable).operations().current();
        Table trackedTable = new BaseTable(
                new StaticTableOperations(metadata, trackingFileIO), liveTable.name());
        IcebergTableCacheValue countValue = new IcebergTableCacheValue(trackedTable, authenticator);
        countValue.getWritableIcebergTable(liveTable);
        Assert.assertEquals("count-based writes must not add metadata FileIO", 0, metadataReads.get());
        IcebergTableCacheValue value = new IcebergTableCacheValue(trackedTable, authenticator);
        value.prepareForCachePublication(NameMapping.createForTest(1L, "db", "tbl"));

        Table statementTable = value.newQueryScopedTable();
        IcebergSnapshotCacheValue.loadQueryMetadataForStatement(statementTable);
        IcebergSnapshotCacheValue statementValue = new IcebergSnapshotCacheValue(
                IcebergPartitionInfo.empty(), new IcebergSnapshot(-1L, 0L),
                Optional.empty(), statementTable);
        Assert.assertSame(statementTable, statementValue.getIcebergTable().get());
        com.google.common.collect.Lists.newArrayList(
                statementValue.getIcebergTable().get().snapshots());
        Assert.assertEquals("statement handoff must reuse parsed metadata", 1, metadataReads.get());
        value.getWritableIcebergTable(liveTable);

        Assert.assertEquals(2, metadataReads.get());
        Assert.assertFalse(authenticated.get());
    }

    @Test
    public void testCountModeTimeTravelDoesNotEnableQueryIsolation() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            cache.initCatalog(1L, Collections.emptyMap());
            NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
            IcebergTableCacheValue value = new IcebergTableCacheValue(
                    tableWithMetadataLocation("/metadata/count-v1.json"));
            cache.entry(1L, IcebergExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, IcebergTableCacheValue.class).put(mapping, value);
            ExternalTable table = Mockito.mock(IcebergExternalTable.class);
            Mockito.when(table.getOrBuildNameMapping()).thenReturn(mapping);

            Table queryTable = cache.getQueryScopedIcebergTable(table);

            Assert.assertSame(value.getRetainedIcebergTable(), queryTable);
            Assert.assertFalse(value.isQueryIsolationPrepared());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testTimeTravelGenerationBundleDoesNotMixReplacedTableValue() throws Exception {
        String firstLocation = temporaryFolder.newFolder("bundle-first").toURI().toString();
        String secondLocation = temporaryFolder.newFolder("bundle-second").toURI().toString();
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Table firstTable = new HadoopTables(new Configuration()).create(
                schema, PartitionSpec.unpartitioned(), firstLocation);
        firstTable.newAppend().appendFile(DataFiles.builder(firstTable.spec())
                .withPath(firstLocation + "/data/a.parquet")
                .withFileSizeInBytes(10L).withRecordCount(1L).build()).commit();
        Table secondTable = new HadoopTables(new Configuration()).create(
                schema, PartitionSpec.unpartitioned(), secondLocation);
        secondTable.newAppend().appendFile(DataFiles.builder(secondTable.spec())
                .withPath(secondLocation + "/data/b.parquet")
                .withFileSizeInBytes(20L).withRecordCount(2L).build()).commit();
        long firstSnapshotId = firstTable.currentSnapshot().snapshotId();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            cache.initCatalog(1L, Collections.singletonMap(
                    "meta.cache.iceberg.table.max-weight", "4MB"));
            NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
            MetaCacheEntry<NameMapping, IcebergTableCacheValue> entry = cache.entry(
                    1L, IcebergExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, IcebergTableCacheValue.class);
            entry.put(mapping, new IcebergTableCacheValue(firstTable));
            ExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
            Mockito.when(dorisTable.getOrBuildNameMapping()).thenReturn(mapping);

            Table queryTable = cache.getQueryScopedIcebergTable(dorisTable);
            entry.put(mapping, new IcebergTableCacheValue(secondTable));

            Assert.assertEquals(firstSnapshotId,
                    queryTable.currentSnapshot().snapshotId());
            Assert.assertEquals(firstTable.location(),
                    queryTable.location());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testMissingPinnedMetadataRefreshesBeforeStatementFence() throws Exception {
        String staleLocation = temporaryFolder.newFolder("stale-metadata").toURI().toString();
        String freshLocation = temporaryFolder.newFolder("fresh-metadata").toURI().toString();
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        Table staleTable = new HadoopTables(new Configuration()).create(
                schema, PartitionSpec.unpartitioned(), staleLocation);
        Table freshTable = new HadoopTables(new Configuration()).create(
                schema, PartitionSpec.unpartitioned(), freshLocation);
        String staleMetadataLocation = ((HasTableOperations) staleTable)
                .operations().current().metadataFileLocation();
        IcebergTableCacheValue staleValue = new IcebergTableCacheValue(staleTable);
        staleValue.prepareForCachePublication(NameMapping.createForTest(1L, "db", "tbl"));
        staleTable.io().deleteFile(staleMetadataLocation);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        IcebergExternalCatalog catalog = Mockito.mock(IcebergExternalCatalog.class);
        IcebergMetadataOps metadataOps = Mockito.mock(IcebergMetadataOps.class);
        Mockito.when(catalog.getMetadataOps()).thenReturn(metadataOps);
        Mockito.when(catalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() { });
        Mockito.when(metadataOps.loadTable("db", "tbl")).thenReturn(freshTable);
        IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor) {
            @Override
            protected CatalogIf<?> getCatalog(long catalogId) {
                return catalog;
            }
        };
        try {
            cache.initCatalog(1L, Collections.singletonMap(
                    "meta.cache.iceberg.table.max-weight", "4MB"));
            NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
            cache.entry(1L, IcebergExternalMetaCache.ENTRY_TABLE,
                    NameMapping.class, IcebergTableCacheValue.class).put(mapping, staleValue);
            ExternalTable table = Mockito.mock(IcebergExternalTable.class);
            Mockito.when(table.getOrBuildNameMapping()).thenReturn(mapping);

            Table queryTable = cache.getQueryScopedIcebergTable(table);

            Assert.assertEquals(freshTable.schema().asStruct(), queryTable.schema().asStruct());
            Mockito.verify(metadataOps, Mockito.times(1)).loadTable("db", "tbl");
        } finally {
            cache.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void testWeightedTablePublicationRetainsNonGrowingGeneration() {
        NameMapping mapping = NameMapping.createForTest(1L, "db", "tbl");
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        TableMetadata metadata = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(),
                "file:/warehouse/db/tbl", Collections.emptyMap());
        Snapshot currentSnapshot = SnapshotParser.fromJson("{\"snapshot-id\":7,\"timestamp-ms\":2,"
                + "\"summary\":{\"operation\":\"append\"},"
                + "\"manifests\":[\"/manifest/current-a.avro\","
                + "\"/manifest/current-b.avro\"],\"schema-id\":0}");
        metadata = TableMetadata.buildFrom(metadata)
                .setBranchSnapshot(currentSnapshot, SnapshotRef.MAIN_BRANCH)
                .discardChanges().withMetadataLocation("/metadata/v1.json").build();
        FileIO fileIO = Mockito.mock(FileIO.class);
        Mockito.when(fileIO.newInputFile(Mockito.anyString())).thenAnswer(invocation -> {
            InputFile inputFile = Mockito.mock(InputFile.class);
            Mockito.when(inputFile.location()).thenReturn(invocation.getArgument(0));
            return inputFile;
        });
        Table liveTable = new BaseTable(new StaticTableOperations(metadata, fileIO), "db.tbl");
        IcebergTableCacheValue value = new IcebergTableCacheValue(liveTable);

        value.prepareForCachePublication(mapping);

        Assert.assertTrue(value.getSizeEstimate().getIncompleteReason(), value.getSizeEstimate().isComplete());
        Table retained = value.getRetainedIcebergTable();
        Assert.assertTrue(IcebergSnapshotCacheValue.isFrozenGeneration(retained));
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> retained.snapshot(7L).dataManifests(retained.io()));
        Table firstUse = value.getIcebergTable();
        Table secondUse = value.getIcebergTable();
        Assert.assertNotSame(retained, firstUse);
        Assert.assertNotSame(firstUse, secondUse);
        Assert.assertNotSame(retained.currentSnapshot(), firstUse.currentSnapshot());
        Assert.assertNotSame(firstUse.currentSnapshot(), secondUse.currentSnapshot());
        Assert.assertEquals(2, firstUse.snapshot(7L).dataManifests(firstUse.io()).size());
        Assert.assertEquals(2, secondUse.snapshot(7L).dataManifests(secondUse.io()).size());
        Assert.assertTrue(IcebergSnapshotCacheValue.isFrozenGeneration(firstUse));

        IcebergSnapshotEntryKey snapshotKey =
                IcebergSnapshotEntryKey.tryCreate(mapping, retained).get();
        IcebergSnapshotCacheValue snapshotValue = new IcebergSnapshotCacheValue(
                IcebergPartitionInfo.empty(), new IcebergSnapshot(7L, 0L),
                Optional.empty(), retained, value.getRetainedCurrentSnapshotJson());
        snapshotValue.prepareForCachePublication(snapshotKey);
        Assert.assertTrue(snapshotValue.getSizeEstimate().getIncompleteReason(),
                snapshotValue.getSizeEstimate().isComplete());
        Table snapshotQuery = snapshotValue.getIcebergTable().get();
        Assert.assertEquals(2,
                snapshotQuery.currentSnapshot().dataManifests(snapshotQuery.io()).size());
    }

    @Test
    public void testWeightedV2ManifestListMaterializesOnlyInQueryView() throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        String tableLocation = temporaryFolder.newFolder("v2-table").toURI().toString();
        Table liveTable = new HadoopTables(new Configuration()).create(
                schema, PartitionSpec.unpartitioned(), tableLocation);
        liveTable.newAppend().appendFile(
                DataFiles.builder(liveTable.spec())
                        .withPath(tableLocation + "/data/a.parquet")
                        .withFileSizeInBytes(10L)
                        .withRecordCount(1L)
                        .build()).commit();
        Assert.assertNotNull(liveTable.currentSnapshot().manifestListLocation());
        IcebergTableCacheValue value = new IcebergTableCacheValue(liveTable);

        value.prepareForCachePublication(NameMapping.createForTest(1L, "db", "tbl"));

        Assert.assertTrue(value.getSizeEstimate().getIncompleteReason(),
                value.getSizeEstimate().isComplete());
        Table retained = value.getRetainedIcebergTable();
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> retained.currentSnapshot().dataManifests(retained.io()));
        Table firstQuery = value.getIcebergTable();
        Table secondQuery = value.getIcebergTable();
        List<ManifestFile> firstManifests =
                firstQuery.currentSnapshot().dataManifests(firstQuery.io());
        List<ManifestFile> secondManifests =
                secondQuery.currentSnapshot().dataManifests(secondQuery.io());
        Assert.assertEquals(1, firstManifests.size());
        Assert.assertEquals(1, secondManifests.size());
        Assert.assertNotSame(firstQuery.currentSnapshot(), secondQuery.currentSnapshot());
        Assert.assertNotSame(firstManifests, secondManifests);
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> retained.currentSnapshot().dataManifests(retained.io()));
    }

    @Test
    public void testTablePublicationDoesNotReadHistoricalManifestLists() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        TableMetadata metadata = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(),
                "file:/warehouse/db/tbl", Collections.emptyMap());
        Snapshot historical = SnapshotParser.fromJson("{\"snapshot-id\":6,\"timestamp-ms\":1,"
                + "\"summary\":{\"operation\":\"append\"},"
                + "\"manifest-list\":\"/manifest-list/history.avro\",\"schema-id\":0}");
        Snapshot current = SnapshotParser.fromJson("{\"snapshot-id\":7,\"timestamp-ms\":2,"
                + "\"summary\":{\"operation\":\"append\"},\"manifests\":[],\"schema-id\":0}");
        metadata = TableMetadata.buildFrom(metadata)
                .addSnapshot(historical)
                .setBranchSnapshot(current, SnapshotRef.MAIN_BRANCH)
                .discardChanges().withMetadataLocation("/metadata/v2.json").build();
        FileIO fileIO = Mockito.mock(FileIO.class);
        IcebergTableCacheValue value = new IcebergTableCacheValue(
                new BaseTable(new StaticTableOperations(metadata, fileIO), "db.tbl"));

        value.prepareForCachePublication(NameMapping.createForTest(1L, "db", "tbl"));

        Assert.assertTrue(value.getSizeEstimate().getIncompleteReason(), value.getSizeEstimate().isComplete());
        Mockito.verify(fileIO, Mockito.never()).newInputFile("/manifest-list/history.avro");
    }

    @Test
    public void testManifestEstimateScalesWithFileCount() {
        ManifestCacheValue oneFile = ManifestCacheValue.forDataFiles(Collections.singletonList(
                DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/data/one.parquet").withFileSizeInBytes(10L).withRecordCount(1L).build()));
        ManifestCacheValue twoFiles = ManifestCacheValue.forDataFiles(java.util.Arrays.asList(
                oneFile.getDataFiles().get(0), oneFile.getDataFiles().get(0)));
        IcebergManifestEntryKey key = new IcebergManifestEntryKey("/manifest/data.avro", ManifestContent.DATA);

        long oneFileBytes = IcebergCacheSizeEstimator.estimateManifestEntry(key, oneFile).getBytes();
        long twoFileBytes = IcebergCacheSizeEstimator.estimateManifestEntry(key, twoFiles).getBytes();

        Assert.assertTrue(twoFileBytes > oneFileBytes);
    }

    @Test
    public void testManifestEstimateAccountsForSkewedFilePaths() {
        String largePath = "/data/" + repeatedCharacter('x', 64 * 1024) + ".parquet";
        ManifestCacheValue smallValue = ManifestCacheValue.forDataFiles(Collections.singletonList(
                DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/data/x.parquet")
                        .withFileSizeInBytes(10L)
                        .withRecordCount(1L)
                        .build()));
        ManifestCacheValue largeValue = ManifestCacheValue.forDataFiles(Collections.singletonList(
                DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath(largePath)
                        .withFileSizeInBytes(10L)
                        .withRecordCount(1L)
                        .build()));
        IcebergManifestEntryKey key = new IcebergManifestEntryKey(
                "/manifest/path-skew.avro", ManifestContent.DATA);

        long smallBytes = IcebergCacheSizeEstimator.estimateManifestEntry(key, smallValue).getBytes();
        long largeBytes = IcebergCacheSizeEstimator.estimateManifestEntry(key, largeValue).getBytes();

        Assert.assertTrue(largeBytes - smallBytes >= (largePath.length() - "/data/x.parquet".length()) * 2L);
    }

    @Test
    public void testManifestEstimateAccountsForSkewedBufferPayload() {
        Metrics smallMetrics = new Metrics(1L, Collections.emptyMap(), Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(),
                Collections.singletonMap(1, ByteBuffer.allocateDirect(32)), Collections.emptyMap());
        Metrics largeMetrics = new Metrics(1L, Collections.emptyMap(), Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(),
                Collections.singletonMap(1, ByteBuffer.allocateDirect(64 * 1024)), Collections.emptyMap());
        ManifestCacheValue smallValue = ManifestCacheValue.forDataFiles(Collections.singletonList(
                DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/data/encrypted.parquet")
                        .withFileSizeInBytes(10L)
                        .withMetrics(smallMetrics)
                        .build()));
        ManifestCacheValue largeValue = ManifestCacheValue.forDataFiles(Collections.singletonList(
                DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/data/encrypted.parquet")
                        .withFileSizeInBytes(10L)
                        .withMetrics(largeMetrics)
                        .build()));

        IcebergManifestEntryKey key = new IcebergManifestEntryKey(
                "/manifest/encrypted.avro", ManifestContent.DATA);
        MetaCacheSizeEstimate smallEstimate = IcebergCacheSizeEstimator.estimateManifestEntry(key, smallValue);
        MetaCacheSizeEstimate largeEstimate = IcebergCacheSizeEstimator.estimateManifestEntry(key, largeValue);

        Assert.assertTrue(smallEstimate.getIncompleteReason(), smallEstimate.isComplete());
        Assert.assertTrue(largeEstimate.getIncompleteReason(), largeEstimate.isComplete());
        Assert.assertEquals(1L, smallValue.getDataFileMetricEntryCount());
        Assert.assertEquals(1L, largeValue.getDataFileMetricEntryCount());
        Assert.assertTrue(largeEstimate.getBytes() - smallEstimate.getBytes() >= 64 * 1024 - 32);
    }

    @Test
    public void testManifestEstimateAccountsForDeleteFileAuxiliaryPayload() {
        String largeReference = "/data/" + repeatedCharacter('x', 64 * 1024) + ".parquet";
        List<Long> largeOffsets = IntStream.range(0, 4096)
                .mapToObj(index -> (long) index).collect(Collectors.toList());
        DeleteFile smallPositionDelete = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofPositionDeletes()
                .withPath("/delete/position.parquet")
                .withFileSizeInBytes(10L)
                .withRecordCount(1L)
                .withReferencedDataFile("/data/x.parquet")
                .withSplitOffsets(Collections.singletonList(0L))
                .build();
        DeleteFile largePositionDelete = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofPositionDeletes()
                .withPath("/delete/position.parquet")
                .withFileSizeInBytes(10L)
                .withRecordCount(1L)
                .withReferencedDataFile(largeReference)
                .withSplitOffsets(largeOffsets)
                .build();
        int[] largeEqualityIds = IntStream.range(0, 4096).toArray();
        DeleteFile smallEqualityDelete = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofEqualityDeletes(1)
                .withPath("/delete/equality.parquet")
                .withFileSizeInBytes(10L)
                .withRecordCount(1L)
                .build();
        DeleteFile largeEqualityDelete = FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofEqualityDeletes(largeEqualityIds)
                .withPath("/delete/equality.parquet")
                .withFileSizeInBytes(10L)
                .withRecordCount(1L)
                .build();
        IcebergManifestEntryKey key = new IcebergManifestEntryKey(
                "/manifest/delete.avro", ManifestContent.DELETES);

        long smallPositionBytes = IcebergCacheSizeEstimator.estimateManifestEntry(key,
                ManifestCacheValue.forDeleteFiles(Collections.singletonList(smallPositionDelete))).getBytes();
        long largePositionBytes = IcebergCacheSizeEstimator.estimateManifestEntry(key,
                ManifestCacheValue.forDeleteFiles(Collections.singletonList(largePositionDelete))).getBytes();
        long smallEqualityBytes = IcebergCacheSizeEstimator.estimateManifestEntry(key,
                ManifestCacheValue.forDeleteFiles(Collections.singletonList(smallEqualityDelete))).getBytes();
        long largeEqualityBytes = IcebergCacheSizeEstimator.estimateManifestEntry(key,
                ManifestCacheValue.forDeleteFiles(Collections.singletonList(largeEqualityDelete))).getBytes();

        Assert.assertTrue(largePositionBytes > smallPositionBytes);
        Assert.assertTrue(largeEqualityBytes > smallEqualityBytes);
    }

    @Test
    public void testSnapshotPublicationDoesNotMaterializeManifestLists() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        TableMetadata metadata = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(),
                "file:/warehouse/db/tbl", Collections.emptyMap());
        Snapshot snapshot = SnapshotParser.fromJson("{\"snapshot-id\":7,\"timestamp-ms\":1,"
                + "\"summary\":{\"operation\":\"append\"},"
                + "\"manifests\":[\"/manifest/a.avro\",\"/manifest/b.avro\"],\"schema-id\":0}");
        metadata = TableMetadata.buildFrom(metadata).setBranchSnapshot(snapshot, SnapshotRef.MAIN_BRANCH)
                .discardChanges().withMetadataLocation("/metadata/v1.json").build();
        FileIO fileIO = Mockito.mock(FileIO.class);
        Table table = new BaseTable(new StaticTableOperations(metadata, fileIO), "db.tbl");
        IcebergSnapshotCacheValue value = new IcebergSnapshotCacheValue(
                IcebergPartitionInfo.empty(), new IcebergSnapshot(7L, 0L), Optional.empty(), table);
        IcebergSnapshotEntryKey key = IcebergSnapshotEntryKey.tryCreate(
                NameMapping.createForTest(1L, "db", "tbl"), table).get();

        value.prepareForCachePublication(key);

        Assert.assertTrue(value.getSizeEstimate().getIncompleteReason(),
                value.getSizeEstimate().isComplete());
        Table queryTable = value.getIcebergTable().get();
        Assert.assertNotSame(table.currentSnapshot(), queryTable.currentSnapshot());
        Assert.assertTrue(IcebergSnapshotCacheValue.isFrozenGeneration(queryTable));
        Mockito.verifyNoInteractions(fileIO);
    }

    @Test
    public void testInvalidateTableKeepsManifestCache() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, manifestCacheEnabledProperties());
            NameMapping t1 = new NameMapping(catalogId, "db1", "tbl1", "rdb1", "rtbl1");
            NameMapping t2 = new NameMapping(catalogId, "db1", "tbl2", "rdb1", "rtbl2");

            MetaCacheEntry<NameMapping, IcebergTableCacheValue> tableEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_TABLE, NameMapping.class, IcebergTableCacheValue.class);
            tableEntry.put(t1, new IcebergTableCacheValue(newInterfaceProxy(Table.class)));
            tableEntry.put(t2, new IcebergTableCacheValue(newInterfaceProxy(Table.class)));

            Table snapshotTable = tableWithMetadataLocation("/metadata/invalidate-v1.json");
            IcebergSnapshotEntryKey snapshotKey = IcebergSnapshotEntryKey.tryCreate(t1, snapshotTable).get();
            MetaCacheEntry<IcebergSnapshotEntryKey, IcebergSnapshotCacheValue> snapshotEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_SNAPSHOT,
                    IcebergSnapshotEntryKey.class, IcebergSnapshotCacheValue.class);
            snapshotEntry.put(snapshotKey,
                    new IcebergSnapshotCacheValue(IcebergPartitionInfo.empty(), new IcebergSnapshot(-1L, 0L)));

            MetaCacheEntry<NameMapping, org.apache.iceberg.view.View> viewEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_VIEW, NameMapping.class, org.apache.iceberg.view.View.class);
            viewEntry.put(t1, newInterfaceProxy(org.apache.iceberg.view.View.class));
            viewEntry.put(t2, newInterfaceProxy(org.apache.iceberg.view.View.class));

            String sharedManifestPath = "/tmp/shared.avro";
            IcebergManifestEntryKey m1 = mockManifestKey(sharedManifestPath);
            IcebergManifestEntryKey m2 = mockManifestKey(sharedManifestPath);
            MetaCacheEntry<IcebergManifestEntryKey, ManifestCacheValue> manifestEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_MANIFEST, IcebergManifestEntryKey.class, ManifestCacheValue.class);
            Assert.assertEquals(m1, m2);
            manifestEntry.put(m1, ManifestCacheValue.forDataFiles(com.google.common.collect.Lists.newArrayList()));
            manifestEntry.put(m2, ManifestCacheValue.forDataFiles(com.google.common.collect.Lists.newArrayList()));

            Assert.assertNotNull(manifestEntry.getIfPresent(m1));
            Assert.assertNotNull(manifestEntry.getIfPresent(m2));
            cache.invalidateTable(catalogId, "db1", "tbl1");

            Assert.assertNull(tableEntry.getIfPresent(t1));
            Assert.assertNotNull(tableEntry.getIfPresent(t2));
            Assert.assertNull(snapshotEntry.getIfPresent(snapshotKey));
            Assert.assertNull(viewEntry.getIfPresent(t1));
            Assert.assertNotNull(viewEntry.getIfPresent(t2));
            Assert.assertNotNull(manifestEntry.getIfPresent(m1));
            Assert.assertNotNull(manifestEntry.getIfPresent(m2));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testInvalidateDbAndStats() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, manifestCacheEnabledProperties());
            NameMapping db1Table = new NameMapping(catalogId, "db1", "tbl1", "rdb1", "rtbl1");
            NameMapping db2Table = new NameMapping(catalogId, "db2", "tbl1", "rdb2", "rtbl1");

            MetaCacheEntry<NameMapping, IcebergTableCacheValue> tableEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_TABLE, NameMapping.class, IcebergTableCacheValue.class);
            tableEntry.put(db1Table, new IcebergTableCacheValue(newInterfaceProxy(Table.class)));
            tableEntry.put(db2Table, new IcebergTableCacheValue(newInterfaceProxy(Table.class)));

            MetaCacheEntry<IcebergSchemaCacheKey, SchemaCacheValue> schemaEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_SCHEMA, IcebergSchemaCacheKey.class, SchemaCacheValue.class);
            IcebergSchemaCacheKey db1Schema = new IcebergSchemaCacheKey(db1Table, 1L);
            IcebergSchemaCacheKey db2Schema = new IcebergSchemaCacheKey(db2Table, 2L);
            schemaEntry.put(db1Schema, new SchemaCacheValue(Collections.emptyList()));
            schemaEntry.put(db2Schema, new SchemaCacheValue(Collections.emptyList()));
            MetaCacheEntry<IcebergManifestEntryKey, ManifestCacheValue> manifestEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_MANIFEST, IcebergManifestEntryKey.class, ManifestCacheValue.class);
            IcebergManifestEntryKey manifestKey = mockManifestKey("/tmp/db-invalidate.avro");
            manifestEntry.put(manifestKey,
                    ManifestCacheValue.forDataFiles(com.google.common.collect.Lists.newArrayList()));

            cache.invalidateDb(catalogId, "db1");

            Assert.assertNull(tableEntry.getIfPresent(db1Table));
            Assert.assertNotNull(tableEntry.getIfPresent(db2Table));
            Assert.assertNull(schemaEntry.getIfPresent(db1Schema));
            Assert.assertNotNull(schemaEntry.getIfPresent(db2Schema));
            Assert.assertNotNull(manifestEntry.getIfPresent(manifestKey));

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            Assert.assertTrue(stats.containsKey(IcebergExternalMetaCache.ENTRY_TABLE));
            Assert.assertTrue(stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST).isConfigEnabled());
            Assert.assertTrue(stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST).isEffectiveEnabled());
            Assert.assertFalse(stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST).isAutoRefresh());
            Assert.assertEquals(-1L, stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST).getTtlSecond());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSchemaStatsWhenSchemaCacheDisabled() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            long catalogId = 1L;
            Map<String, String> properties = com.google.common.collect.Maps.newHashMap();
            properties.put(ExternalCatalog.SCHEMA_CACHE_TTL_SECOND, "0");
            cache.initCatalog(catalogId, properties);

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            MetaCacheEntryStats schemaStats = stats.get(IcebergExternalMetaCache.ENTRY_SCHEMA);
            Assert.assertNotNull(schemaStats);
            Assert.assertEquals(0L, schemaStats.getTtlSecond());
            Assert.assertTrue(schemaStats.isConfigEnabled());
            Assert.assertFalse(schemaStats.isEffectiveEnabled());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testManifestStatsDisabledByDefault() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, Collections.emptyMap());

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            MetaCacheEntryStats manifestStats = stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST);
            Assert.assertNotNull(manifestStats);
            Assert.assertFalse(manifestStats.isConfigEnabled());
            Assert.assertFalse(manifestStats.isEffectiveEnabled());
            Assert.assertFalse(manifestStats.isAutoRefresh());
            Assert.assertEquals(-1L, manifestStats.getTtlSecond());
            Assert.assertEquals(100000L, manifestStats.getCapacity());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testManifestEntryRequiresContextualLoader() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            long catalogId = 1L;
            cache.initCatalog(catalogId, manifestCacheEnabledProperties());
            MetaCacheEntry<IcebergManifestEntryKey, ManifestCacheValue> manifestEntry = cache.entry(catalogId,
                    IcebergExternalMetaCache.ENTRY_MANIFEST, IcebergManifestEntryKey.class, ManifestCacheValue.class);
            IcebergManifestEntryKey manifestKey = mockManifestKey("/tmp/contextual-only.avro");

            UnsupportedOperationException exception = Assert.assertThrows(UnsupportedOperationException.class,
                    () -> manifestEntry.get(manifestKey));
            Assert.assertTrue(exception.getMessage().contains("contextual miss loader"));

            ManifestCacheValue value = manifestEntry.get(manifestKey,
                    ignored -> ManifestCacheValue.forDataFiles(com.google.common.collect.Lists.newArrayList()));
            Assert.assertNotNull(value);
            Assert.assertSame(value, manifestEntry.getIfPresent(manifestKey));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testManifestEnableUsesDefaultCapacity() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            IcebergExternalMetaCache cache = new IcebergExternalMetaCache(executor);
            long catalogId = 1L;
            Map<String, String> properties = com.google.common.collect.Maps.newHashMap();
            properties.put("meta.cache.iceberg.manifest.enable", "true");
            cache.initCatalog(catalogId, properties);

            Map<String, MetaCacheEntryStats> stats = cache.stats(catalogId);
            MetaCacheEntryStats manifestStats = stats.get(IcebergExternalMetaCache.ENTRY_MANIFEST);
            Assert.assertNotNull(manifestStats);
            Assert.assertTrue(manifestStats.isConfigEnabled());
            Assert.assertTrue(manifestStats.isEffectiveEnabled());
            Assert.assertEquals(-1L, manifestStats.getTtlSecond());
            Assert.assertEquals(100000L, manifestStats.getCapacity());
        } finally {
            executor.shutdownNow();
        }
    }

    private Map<String, String> manifestCacheEnabledProperties() {
        Map<String, String> properties = com.google.common.collect.Maps.newHashMap();
        properties.put("meta.cache.iceberg.manifest.enable", "true");
        return properties;
    }

    private long snapshotWeight(IcebergSnapshotEntryKey key, int partitionCount) {
        IcebergPartitionInfo partitionInfo = Mockito.mock(IcebergPartitionInfo.class);
        Map<String, org.apache.doris.catalog.PartitionItem> partitionItems = sizeOnlyMap(partitionCount);
        Map<String, IcebergPartition> partitions = sizeOnlyMap(partitionCount);
        Map<String, java.util.Set<String>> aliases = sizeOnlyMap(partitionCount);
        Mockito.when(partitionInfo.getNameToPartitionItem()).thenReturn(partitionItems);
        Mockito.when(partitionInfo.getNameToIcebergPartition()).thenReturn(partitions);
        Mockito.when(partitionInfo.getNameToIcebergPartitionNames()).thenReturn(aliases);
        IcebergSnapshotCacheValue value = new IcebergSnapshotCacheValue(
                partitionInfo, new IcebergSnapshot(key.getSnapshotId(), key.getSchemaId()));
        MetaCacheSizeEstimate estimate = IcebergCacheSizeEstimator.estimateSnapshotEntry(key, value);
        Assert.assertTrue(estimate.getIncompleteReason(), estimate.isComplete());
        return estimate.getBytes();
    }

    private long manifestWeight(IcebergManifestEntryKey key, int fileCount) {
        ManifestCacheValue value = Mockito.mock(ManifestCacheValue.class);
        List<org.apache.iceberg.DataFile> dataFiles = sizeOnlyList(fileCount);
        Mockito.when(value.getDataFiles()).thenReturn(dataFiles);
        Mockito.when(value.getDeleteFiles()).thenReturn(Collections.emptyList());
        MetaCacheSizeEstimate estimate = IcebergCacheSizeEstimator.estimateManifestEntry(key, value);
        Assert.assertTrue(estimate.getIncompleteReason(), estimate.isComplete());
        return estimate.getBytes();
    }

    private void assertLinearScale(long base, long oneThousand, long tenThousand, long oneHundredThousand) {
        long oneThousandPayload = oneThousand - base;
        Assert.assertTrue(oneThousandPayload > 0L);
        Assert.assertEquals(oneThousandPayload * 10L, tenThousand - base);
        Assert.assertEquals(oneThousandPayload * 100L, oneHundredThousand - base);
    }

    @SuppressWarnings("unchecked")
    private <K, V> Map<K, V> sizeOnlyMap(int size) {
        Map<K, V> map = Mockito.mock(Map.class);
        Mockito.when(map.size()).thenReturn(size);
        return map;
    }

    @SuppressWarnings("unchecked")
    private <V> List<V> sizeOnlyList(int size) {
        List<V> list = Mockito.mock(List.class);
        Mockito.when(list.size()).thenReturn(size);
        return list;
    }

    private static String repeatedCharacter(char character, int count) {
        char[] characters = new char[count];
        java.util.Arrays.fill(characters, character);
        return new String(characters);
    }

    private Table tableWithMetadataLocation(String metadataLocation) {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        TableMetadata metadata = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(),
                "file:/warehouse/db/tbl", Collections.emptyMap());
        metadata = TableMetadata.buildFrom(metadata).discardChanges()
                .withMetadataLocation(metadataLocation).build();
        return new BaseTable(new StaticTableOperations(metadata, null), "db.tbl");
    }

    private Table tableWithMetadata(TableMetadata metadata) {
        TableOperations operations = Mockito.mock(TableOperations.class);
        Mockito.when(operations.current()).thenReturn(metadata);
        return new BaseTable(operations, "db.tbl");
    }

    private TableMetadata metadataWithMaterializedPayload(String payload, int bufferBytes) {
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.specs()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.sortOrders()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.properties()).thenReturn(Collections.emptyMap());
        Mockito.when(metadata.uuid()).thenReturn("stable-uuid");
        Mockito.when(metadata.refs()).thenReturn(Collections.singletonMap(
                payload, Mockito.mock(SnapshotRef.class)));
        TableMetadata.MetadataLogEntry metadataLogEntry =
                Mockito.mock(TableMetadata.MetadataLogEntry.class);
        Mockito.when(metadataLogEntry.file()).thenReturn(payload);
        Mockito.when(metadata.previousFiles()).thenReturn(
                Collections.singletonList(metadataLogEntry));
        GenericBlobMetadata blob = new GenericBlobMetadata(
                payload, 1L, 1L, Collections.singletonList(1),
                Collections.singletonMap(payload, payload));
        Mockito.when(metadata.statisticsFiles()).thenReturn(Collections.singletonList(
                new GenericStatisticsFile(1L, payload, 1L, 1L,
                        Collections.singletonList(blob))));
        org.apache.iceberg.PartitionStatisticsFile partitionStatistics =
                Mockito.mock(org.apache.iceberg.PartitionStatisticsFile.class);
        Mockito.when(partitionStatistics.path()).thenReturn(payload);
        Mockito.when(metadata.partitionStatisticsFiles()).thenReturn(
                Collections.singletonList(partitionStatistics));
        EncryptedKey encryptedKey = Mockito.mock(EncryptedKey.class);
        Mockito.when(encryptedKey.keyId()).thenReturn(payload);
        Mockito.when(encryptedKey.encryptedById()).thenReturn(payload);
        Mockito.when(encryptedKey.encryptedKeyMetadata()).thenReturn(
                ByteBuffer.allocateDirect(bufferBytes));
        Mockito.when(encryptedKey.properties()).thenReturn(
                Collections.singletonMap(payload, payload));
        Mockito.when(metadata.encryptionKeys()).thenReturn(Collections.singletonList(encryptedKey));
        return metadata;
    }

    private TableMetadata metadataWithSnapshotSequence(long lastSequenceNumber) {
        TableMetadata metadata = Mockito.mock(TableMetadata.class);
        Mockito.when(metadata.schemas()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.specs()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.sortOrders()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.properties()).thenReturn(Collections.emptyMap());
        Mockito.when(metadata.snapshotLog()).thenReturn(Collections.singletonList(
                Mockito.mock(org.apache.iceberg.HistoryEntry.class)));
        Mockito.when(metadata.refs()).thenReturn(Collections.singletonMap(
                "branch-tip", Mockito.mock(SnapshotRef.class)));
        Mockito.when(metadata.previousFiles()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.statisticsFiles()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.partitionStatisticsFiles()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.encryptionKeys()).thenReturn(Collections.emptyList());
        Mockito.when(metadata.lastSequenceNumber()).thenReturn(lastSequenceNumber);
        Mockito.when(metadata.metadataFileLocation()).thenReturn("/metadata/sequence.json");
        return metadata;
    }

    private Table tableWithNestedSchemaAndProperty(String nestedFieldName, String propertyValue) {
        Schema schema = new Schema(Types.NestedField.optional(1, "payload",
                Types.StructType.of(Types.NestedField.optional(
                        2, nestedFieldName, Types.StringType.get()))));
        TableMetadata metadata = TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(),
                "file:/warehouse/db/tbl", Collections.singletonMap("payload", propertyValue));
        metadata = TableMetadata.buildFrom(metadata).discardChanges()
                .withMetadataLocation("/metadata/nested.json").build();
        return new BaseTable(new StaticTableOperations(metadata, null), "db.tbl");
    }

    private IcebergManifestEntryKey mockManifestKey(String path) {
        return IcebergManifestEntryKey.of(new TestingManifestFile(path, ManifestContent.DATA));
    }

    private <T> T newInterfaceProxy(Class<T> type) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
            if (method.getDeclaringClass() == Object.class) {
                switch (method.getName()) {
                    case "equals":
                        return proxy == args[0];
                    case "hashCode":
                        return System.identityHashCode(proxy);
                    case "toString":
                        return type.getSimpleName() + "Proxy";
                    default:
                        return null;
                }
            }
            return defaultValue(method.getReturnType());
        }));
    }

    private Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0F;
        }
        if (type == double.class) {
            return 0D;
        }
        if (type == char.class) {
            return '\0';
        }
        throw new IllegalArgumentException("unsupported primitive type: " + type);
    }

    private static final class TestingManifestFile implements ManifestFile {
        private final String path;
        private final ManifestContent content;

        private TestingManifestFile(String path, ManifestContent content) {
            this.path = path;
            this.content = content;
        }

        @Override
        public String path() {
            return path;
        }

        @Override
        public ManifestContent content() {
            return content;
        }

        @Override
        public long length() {
            return 0;
        }

        @Override
        public int partitionSpecId() {
            return 0;
        }

        @Override
        public long sequenceNumber() {
            return 0;
        }

        @Override
        public long minSequenceNumber() {
            return 0;
        }

        @Override
        public Long snapshotId() {
            return null;
        }

        @Override
        public Integer addedFilesCount() {
            return null;
        }

        @Override
        public Long addedRowsCount() {
            return null;
        }

        @Override
        public Integer existingFilesCount() {
            return null;
        }

        @Override
        public Long existingRowsCount() {
            return null;
        }

        @Override
        public Integer deletedFilesCount() {
            return null;
        }

        @Override
        public Long deletedRowsCount() {
            return null;
        }

        @Override
        public List<PartitionFieldSummary> partitions() {
            return null;
        }

        @Override
        public ByteBuffer keyMetadata() {
            return null;
        }

        @Override
        public ManifestFile copy() {
            return new TestingManifestFile(path, content);
        }
    }
}
