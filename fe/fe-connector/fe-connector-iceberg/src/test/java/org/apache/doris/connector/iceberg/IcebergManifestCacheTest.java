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

import org.apache.doris.connector.cache.CatalogMetaCache;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFileIndex;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link IcebergManifestCache} (T08). Uses a real {@link InMemoryCatalog} table so the cache is
 * exercised against genuine iceberg {@link ManifestFile}s (no I/O — InMemoryCatalog serves manifests in-memory).
 */
public class IcebergManifestCacheTest {

    @Test
    public void stablePartitionTypesOpenOneStreamAndWarmCacheOpensNone() throws Exception {
        assertManifestStreamCount(Types.IntegerType.get(), 7, 1);
        assertManifestStreamCount(Types.StringType.get(), "north", 1);
        assertManifestStreamCount(Types.DateType.get(), 2, 1);
        assertManifestStreamCount(Types.TimestampType.withoutZone(), 123456L, 1);
        assertManifestStreamCount(Types.FloatType.get(), 1.25F, 1);
        assertManifestStreamCount(Types.BooleanType.get(), true, 1);
        assertManifestStreamCount(Types.BinaryType.get(), ByteBuffer.wrap(new byte[] {1, 2}), 1);
    }

    @Test
    public void promotablePartitionTypesAlsoOpenOneStream() throws Exception {
        assertManifestStreamCount(Types.LongType.get(), 7L, 1);
        assertManifestStreamCount(Types.DoubleType.get(), 1.25D, 1);
        assertManifestStreamCount(Types.DecimalType.of(9, 2), new BigDecimal("1.25"), 1);
    }

    @Test
    public void unpartitionedManifestOpensOneStream() throws Exception {
        Table table = tableWithTwoDataFiles();
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        CountingFileIO io = new CountingFileIO(table.io());
        try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, io, null)) {
            Assertions.assertEquals(2, countFiles(reader));
        }
        Assertions.assertEquals(2, io.opens.get(), "the SDK header and entry readers open separate streams");
        Table wrapped = tableWithIo(table, io);
        IcebergManifestCache cache = new IcebergManifestCache();
        ManifestCacheValue value = cache.getManifestCacheValue(manifest, wrapped);
        Assertions.assertEquals(2, value.getDataFiles().size());
        Assertions.assertEquals(3, io.opens.get(), "the cache loader must add only one stream");
        Assertions.assertSame(value, cache.getManifestCacheValue(manifest, wrapped));
        Assertions.assertEquals(3, io.opens.get());
    }

    @Test
    public void bucketPartitionRemainsSingleReadAfterSourceTypePromotion() throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("id", 16).build();
        Table table = tableWithPartitionValue(schema, spec, 7);
        long snapshotId = table.currentSnapshot().snapshotId();
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        IcebergManifestCache cache = new IcebergManifestCache();
        ManifestCacheValue before = cache.getManifestCacheValue(manifest, table);
        table.updateSchema().updateColumn("id", Types.LongType.get()).commit();
        Assertions.assertEquals(snapshotId, table.currentSnapshot().snapshotId());
        Assertions.assertSame(before, cache.getManifestCacheValue(manifest, table));
        CountingFileIO io = new CountingFileIO(table.io());
        DataFile cold = new IcebergManifestCache().getManifestCacheValue(manifest, tableWithIo(table, io))
                .getDataFiles().get(0);
        Assertions.assertEquals(1, io.opens.get());
        Assertions.assertEquals(before.getDataFiles().get(0).partition().get(0, Integer.class),
                cold.partition().get(0, Integer.class));
    }

    @Test
    public void partitionSpecEvolutionUsesEachManifestsSpecId() throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").build();
        Table table = tableWithPartitionValue(schema, spec, "north");
        ManifestFile oldManifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        table.updateSpec().renameField("id", "region").commit();
        Assertions.assertNotEquals(oldManifest.partitionSpecId(), table.spec().specId());
        CountingFileIO io = new CountingFileIO(table.io());
        DataFile file = new IcebergManifestCache().getManifestCacheValue(oldManifest, tableWithIo(table, io))
                .getDataFiles().get(0);
        Assertions.assertEquals(1, io.opens.get());
        Assertions.assertEquals(oldManifest.partitionSpecId(), file.specId());
        Assertions.assertEquals("north", file.partition().get(0, String.class));
    }

    @Test
    public void droppingPartitionSourceRetainsValuesWithOneRead() throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.StringType.get()),
                Types.NestedField.optional(2, "payload", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").build();
        Table table = tableWithPartitionValue(schema, spec, "north");
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        table.updateSpec().removeField("id").commit();
        table.updateSchema().deleteColumn("id").commit();
        CountingFileIO io = new CountingFileIO(table.io());
        DataFile file = new IcebergManifestCache().getManifestCacheValue(manifest, tableWithIo(table, io))
                .getDataFiles().get(0);
        Assertions.assertEquals(1, io.opens.get(), "recover the dropped source type from retained table schemas");
        Assertions.assertEquals("north", file.partition().get(0, String.class));
        try (ManifestReader<DataFile> nativeReader = ManifestFiles.read(manifest, table.io(), table.specs())) {
            DataFile nativeFile = nativeReader.iterator().next();
            ManifestCacheValue current = ManifestCacheValue.forDataFiles(Collections.singletonList(file))
                    .forScan(table.specs().get(manifest.partitionSpecId()));
            Assertions.assertEquals(nativeFile.partition().get(0, Object.class),
                    current.getDataFiles().get(0).partition().get(0, Object.class));
            Assertions.assertEquals("north", file.partition().get(0, String.class), "cached value remains intact");
        }
    }

    @Test
    public void droppedDeletePartitionSourceMatchesNativeProjectionWithoutChangingCache() throws Exception {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.StringType.get()),
                Types.NestedField.optional(2, "payload", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").build();
        Table table = tableWithPartitionValue(schema, spec, "north");
        PartitionData partition = new PartitionData(spec.partitionType());
        partition.set(0, "north");
        DeleteFile deleted = FileMetadata.deleteFileBuilder(spec).ofEqualityDeletes(2)
                .withPath("/data/delete.parquet").withFileSizeInBytes(100).withRecordCount(1)
                .withPartition(partition).build();
        table.newRowDelta().addDeletes(deleted).commit();
        ManifestFile manifest = table.currentSnapshot().deleteManifests(table.io()).get(0);
        table.updateSpec().removeField("id").commit();
        table.updateSchema().deleteColumn("id").commit();
        ManifestCacheValue cached = new IcebergManifestCache().getManifestCacheValue(manifest, table);
        try (ManifestReader<DeleteFile> reader = ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
            Object nativeValue = reader.iterator().next().partition().get(0, Object.class);
            ManifestCacheValue projected = cached.forScan(table.specs().get(manifest.partitionSpecId()));
            Assertions.assertEquals("north", nativeValue);
            Assertions.assertEquals(nativeValue, projected.getDeleteFiles().get(0).partition().get(0, Object.class));
            Assertions.assertEquals("north", cached.getDeleteFiles().get(0).partition().get(0, String.class));
            Assertions.assertSame(cached, cached.forScan(spec), "historical schema keeps the cached physical values");
        }
    }

    @Test
    public void retainedBucketManifestSurvivesSourceSchemaExpiration() throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "payload", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("id", 16).build();
        Table table = tableWithPartitionValue(schema, spec, 7);
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        table.updateSpec().removeField("id_bucket").commit();
        table.updateSchema().deleteColumn("id").commit();
        table.newFastAppend().appendFile(DataFiles.builder(table.spec()).withPath("/data/new.parquet")
                .withFileSizeInBytes(100).withRecordCount(1).build()).commit();
        table.expireSnapshots().expireOlderThan(System.currentTimeMillis() + 1000)
                .retainLast(1).cleanExpiredMetadata(true).commit();
        Assertions.assertTrue(table.schemas().values().stream().noneMatch(value -> value.findField(1) != null));
        CountingFileIO io = new CountingFileIO(table.io());
        DataFile file = new IcebergManifestCache().getManifestCacheValue(manifest, tableWithIo(table, io))
                .getDataFiles().get(0);
        Assertions.assertEquals(7, file.partition().get(0, Integer.class));
        Assertions.assertEquals(1, io.opens.get());
    }

    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    void retainedVariableTypeManifestSurvivesSourceSchemaExpiration(boolean truncate, boolean deletes)
            throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.StringType.get()),
                Types.NestedField.optional(2, "payload", Types.StringType.get()));
        PartitionSpec spec = truncate ? PartitionSpec.builderFor(schema).truncate("id", 5).build()
                : PartitionSpec.builderFor(schema).identity("id").build();
        Table table = tableWithPartitionValue(schema, spec, "north");
        ManifestFile manifest;
        if (deletes) {
            PartitionData partition = new PartitionData(spec.partitionType());
            partition.set(0, "north");
            table.newRowDelta().addDeletes(FileMetadata.deleteFileBuilder(spec).ofPositionDeletes()
                    .withPath("/data/delete.parquet").withPartition(partition)
                    .withFileSizeInBytes(100).withRecordCount(1).build()).commit();
            manifest = table.currentSnapshot().deleteManifests(table.io()).get(0);
        } else {
            manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        }
        table.updateSpec().removeField(spec.fields().get(0).name()).commit();
        table.updateSchema().deleteColumn("id").commit();
        table.newFastAppend().appendFile(DataFiles.builder(table.spec()).withPath("/data/new.parquet")
                .withFileSizeInBytes(100).withRecordCount(1).build()).commit();
        table.expireSnapshots().expireOlderThan(System.currentTimeMillis() + 1000)
                .retainLast(1).cleanExpiredMetadata(true).commit();
        Assertions.assertTrue(table.schemas().values().stream().noneMatch(value -> value.findField(1) != null));
        CountingFileIO io = new CountingFileIO(table.io());
        IcebergManifestCache cache = new IcebergManifestCache();
        Table wrapped = tableWithIo(table, io);
        ManifestCacheValue value = cache.getManifestCacheValue(manifest, wrapped);
        Object physical = deletes ? value.getDeleteFiles().get(0).partition().get(0, Object.class)
                : value.getDataFiles().get(0).partition().get(0, Object.class);
        Assertions.assertEquals("north", physical.toString());
        Assertions.assertEquals(2, io.opens.get(), "only expired source schemas require the SDK header recovery");
        Assertions.assertSame(value, cache.getManifestCacheValue(manifest, wrapped));
        Assertions.assertEquals(2, io.opens.get(), "warm reads do not reopen the header or entries");
        ManifestCacheValue projected = value.forScan(table.specs().get(spec.specId()));
        Object preserved = deletes ? projected.getDeleteFiles().get(0).partition().get(0, Object.class)
                : projected.getDataFiles().get(0).partition().get(0, Object.class);
        Assertions.assertEquals("north", preserved.toString());
        if (deletes) {
            ManifestFile dataManifest = table.currentSnapshot().dataManifests(table.io()).stream()
                    .filter(file -> file.partitionSpecId() == spec.specId()).findFirst().orElseThrow();
            DataFile data = cache.getManifestCacheValue(dataManifest, wrapped)
                    .forScan(table.specs().get(spec.specId())).getDataFiles().get(0);
            DeleteFileIndex index = DeleteFileIndex.builderFor(projected.getDeleteFiles())
                    .schemasById(table.schemas()).specsById(table.specs()).build();
            Assertions.assertEquals(1, index.forDataFile(data.dataSequenceNumber(), data).length,
                    "physical partition recovery must preserve position-delete matching");
        }
    }

    @Test
    public void recoveredSourceNamesCannotCollideWithPartitionNames() throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "p", Types.StringType.get()),
                Types.NestedField.optional(2, "q", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").bucket("q", 16, "source_1").build();
        Table table = tableWithPartitionValue(schema, spec, "north");
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        table.updateSpec().removeField("p").commit();
        table.updateSchema().deleteColumn("p").commit();
        CountingFileIO io = new CountingFileIO(table.io());
        DataFile file = new IcebergManifestCache().getManifestCacheValue(manifest, tableWithIo(table, io))
                .getDataFiles().get(0);
        Assertions.assertEquals("north", file.partition().get(0, String.class));
        Assertions.assertEquals(1, io.opens.get());
    }

    @Test
    public void floatingPointAndDecimalPromotionsAdaptBothCacheOrders() throws Exception {
        assertPromotionAdaptsPartitionType(Types.FloatType.get(), Types.DoubleType.get(), 1.25F);
        assertPromotionAdaptsPartitionType(Types.DecimalType.of(9, 2), Types.DecimalType.of(18, 2),
                new BigDecimal("1.25"));
    }

    private static void assertPromotionAdaptsPartitionType(Type.PrimitiveType from, Type.PrimitiveType to,
            Object partitionValue) {
        Schema schema = new Schema(Types.NestedField.required(1, "id", from));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").build();
        Table table = tableWithPartitionValue(schema, spec, partitionValue);
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        IcebergManifestCache cache = new IcebergManifestCache();
        ManifestCacheValue warm = cache.getManifestCacheValue(manifest, table);
        table.updateSchema().updateColumn("id", to).commit();
        Assertions.assertSame(warm, cache.getManifestCacheValue(manifest, table));
        CountingFileIO io = new CountingFileIO(table.io());
        DataFile cold = new IcebergManifestCache().getManifestCacheValue(manifest, tableWithIo(table, io))
                .getDataFiles().get(0);
        Assertions.assertEquals(1, io.opens.get());
        for (ManifestCacheValue payload : Arrays.asList(warm,
                ManifestCacheValue.forDataFiles(Collections.singletonList(cold)))) {
            for (PartitionSpec target : Arrays.asList(spec, table.spec())) {
                DataFile adapted = payload.forScan(target).getDataFiles().get(0);
                Assertions.assertEquals(target.rawPartitionType(),
                        ((PartitionData) adapted.partition()).getPartitionType());
                Assertions.assertEquals(Expressions.alwaysTrue(),
                        ResidualEvaluator.of(target,
                                Expressions.equal("id", partitionValue), true)
                                .residualFor(adapted.partition()));
            }
        }
        Assertions.assertEquals(partitionValue, warm.getDataFiles().get(0).partition().get(0, Object.class));
    }

    private static void assertManifestStreamCount(Type type, Object partitionValue, int expectedOpens)
            throws IOException {
        Schema schema = new Schema(Types.NestedField.required(1, "id", type));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").build();
        Table table = tableWithPartitionValue(schema, spec, partitionValue);
        CountingFileIO io = new CountingFileIO(table.io());
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        IcebergManifestCache cache = new IcebergManifestCache();
        Table wrapped = tableWithIo(table, io);
        ManifestCacheValue value = cache.getManifestCacheValue(manifest, wrapped);
        Assertions.assertEquals(partitionValue, value.getDataFiles().get(0).partition().get(0, Object.class));
        try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, table.io(), null)) {
            Assertions.assertEquals(((PartitionData) reader.iterator().next().partition()).getPartitionType(),
                    ((PartitionData) value.getDataFiles().get(0).partition()).getPartitionType());
        }
        Assertions.assertEquals(expectedOpens, io.opens.get(), type.toString());
        Assertions.assertSame(value, cache.getManifestCacheValue(manifest, wrapped));
        Assertions.assertEquals(expectedOpens, io.opens.get(), "a parsed-manifest cache hit must not open a stream");
    }

    private static Table tableWithPartitionValue(Schema schema, PartitionSpec spec, Object partitionValue) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Table table = catalog.createTable(TableIdentifier.of("db1", "physical"), schema, spec);
        PartitionData partition = new PartitionData(spec.partitionType());
        partition.set(0, partitionValue);
        table.newFastAppend().appendFile(DataFiles.builder(spec).withPath("/data/physical.parquet")
                .withFileSizeInBytes(100).withRecordCount(1).withPartition(partition).build()).commit();
        return table;
    }

    private static int countFiles(ManifestReader<DataFile> reader) {
        int count = 0;
        Iterator<DataFile> files = reader.iterator();
        while (files.hasNext()) {
            files.next();
            count++;
        }
        return count;
    }

    private static Table tableWithIo(Table table, FileIO io) {
        return (Table) Proxy.newProxyInstance(Table.class.getClassLoader(), new Class<?>[] {Table.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("io")) {
                        return io;
                    }
                    try {
                        return method.invoke(table, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static final class CountingFileIO implements FileIO {
        private final FileIO delegate;
        private final AtomicInteger opens = new AtomicInteger();

        private CountingFileIO(FileIO delegate) {
            this.delegate = delegate;
        }

        @Override
        public Map<String, String> properties() {
            Map<String, String> properties = new HashMap<>(delegate.properties());
            properties.put("io.manifest.cache-enabled", "false");
            return properties;
        }

        @Override
        public InputFile newInputFile(String path) {
            InputFile input = delegate.newInputFile(path);
            return new InputFile() {
                @Override
                public long getLength() {
                    return input.getLength();
                }

                @Override
                public SeekableInputStream newStream() {
                    opens.incrementAndGet();
                    return input.newStream();
                }

                @Override
                public String location() {
                    return input.location();
                }

                @Override
                public boolean exists() {
                    return input.exists();
                }
            };
        }

        @Override
        public OutputFile newOutputFile(String path) {
            return delegate.newOutputFile(path);
        }

        @Override
        public void deleteFile(String path) {
            delegate.deleteFile(path);
        }
    }

    @Test
    public void schemaPromotionAdaptsCachedPartitionsWithoutMutation() throws Exception {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").build();
        Table table = catalog.createTable(TableIdentifier.of("db1", "promoted"), schema, spec);
        table.newFastAppend().appendFile(DataFiles.builder(spec).withPath("/data/old.parquet")
                .withFileSizeInBytes(100).withRecordCount(1).withPartitionPath("id=7").build()).commit();
        long snapshotId = table.currentSnapshot().snapshotId();
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        IcebergManifestCache cache = new IcebergManifestCache();
        ManifestCacheValue before = cache.getManifestCacheValue(manifest, table);

        table.updateSchema().updateColumn("id", Types.LongType.get()).commit();
        Assertions.assertEquals(snapshotId, table.currentSnapshot().snapshotId(),
                "a metadata-only schema change keeps the snapshot and manifest identities");
        Assertions.assertSame(before, cache.getManifestCacheValue(manifest, table));
        DataFile warm = before.getDataFiles().get(0);
        DataFile cold = new IcebergManifestCache().getManifestCacheValue(manifest, table).getDataFiles().get(0);
        Assertions.assertEquals(7, warm.partition().get(0, Integer.class));
        Assertions.assertEquals(7L, cold.partition().get(0, Long.class));
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            List<Callable<Void>> reads = new ArrayList<>();
            for (ManifestCacheValue payload : Arrays.asList(before,
                    ManifestCacheValue.forDataFiles(Collections.singletonList(cold)))) {
                for (PartitionSpec target : Arrays.asList(spec, table.spec())) {
                    reads.add(() -> {
                        for (int i = 0; i < 100; i++) {
                            DataFile adapted = payload.forScan(target).getDataFiles().get(0);
                            Assertions.assertEquals(Expressions.alwaysTrue(),
                                    ResidualEvaluator.of(target,
                                            Expressions.equal("id", 7), true)
                                            .residualFor(adapted.partition()));
                            Assertions.assertEquals(warm.dataSequenceNumber(), adapted.dataSequenceNumber());
                            Assertions.assertEquals(warm.fileSequenceNumber(), adapted.fileSequenceNumber());
                            Assertions.assertEquals(target.rawPartitionType(),
                                    ((PartitionData) adapted.copy().partition()).getPartitionType());
                        }
                        return null;
                    });
                }
            }
            for (Future<Void> future : pool.invokeAll(reads)) {
                future.get();
            }
        } finally {
            pool.shutdownNow();
        }
        Assertions.assertEquals(7, warm.partition().get(0, Integer.class));
        Assertions.assertEquals(7L, cold.partition().get(0, Long.class));
    }

    @Test
    public void promotedPartitionsStillMatchEqualityDeletes() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        PartitionSpec oldSpec = PartitionSpec.builderFor(schema).identity("id").build();
        Table table = tableWithPartitionValue(schema, oldSpec, 7);
        ManifestFile dataManifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        DeleteFile delete = FileMetadata.deleteFileBuilder(oldSpec)
                .ofEqualityDeletes(1).withPath("/delete.parquet").withRecordCount(1).withFileSizeInBytes(100)
                .withPartitionPath("id=7").build();
        table.newRowDelta().addDeletes(delete).commit();
        ManifestFile deleteManifest = table.currentSnapshot().deleteManifests(table.io()).get(0);
        IcebergManifestCache cache = new IcebergManifestCache();
        ManifestCacheValue data = cache.getManifestCacheValue(dataManifest, table);
        ManifestCacheValue deletes = cache.getManifestCacheValue(deleteManifest, table);
        table.updateSchema().updateColumn("id", Types.LongType.get()).commit();
        for (PartitionSpec target : Arrays.asList(oldSpec, table.spec())) {
            DeleteFileIndex index = DeleteFileIndex
                    .builderFor(deletes.forScan(target).getDeleteFiles())
                    .schemasById(table.schemas()).specsById(Collections.singletonMap(target.specId(), target)).build();
            DataFile adapted = data.forScan(target).getDataFiles().get(0);
            DeleteFile[] matches = index.forDataFile(adapted.dataSequenceNumber(), adapted);
            Assertions.assertEquals(1, matches.length);
            Assertions.assertEquals(delete.location(), matches[0].location());
            Assertions.assertEquals(deletes.getDeleteFiles().get(0).dataSequenceNumber(),
                    matches[0].dataSequenceNumber());
        }
        Assertions.assertEquals(7, deletes.getDeleteFiles().get(0).partition().get(0, Integer.class));
    }

    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    void promotedPartitionsMatchDeletesAfterSourceCleanup(boolean warmDeletes, boolean truncate) throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "p", Types.IntegerType.get()),
                Types.NestedField.required(2, "id", Types.IntegerType.get()));
        PartitionSpec original = truncate ? PartitionSpec.builderFor(schema).truncate("p", 4).build()
                : PartitionSpec.builderFor(schema).identity("p").build();
        int value = truncate ? 4 : 7;
        Table table = tableWithPartitionValue(schema, original, value);
        ManifestFile dataManifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        PartitionData partition = new PartitionData(original.rawPartitionType());
        partition.set(0, value);
        DeleteFile delete = FileMetadata.deleteFileBuilder(original).ofEqualityDeletes(2)
                .withPath("/data/delete.parquet").withFileSizeInBytes(100).withRecordCount(1)
                .withPartition(partition).build();
        table.newRowDelta().addDeletes(delete).commit();
        ManifestFile deleteManifest = table.currentSnapshot().deleteManifests(table.io()).get(0);
        table.updateSchema().updateColumn("p", Types.LongType.get()).commit();
        IcebergManifestCache cache = new IcebergManifestCache();
        ManifestCacheValue warm = cache.getManifestCacheValue(warmDeletes ? deleteManifest : dataManifest, table);
        table.updateSpec().removeField(original.fields().get(0).name()).commit();
        table.updateSchema().deleteColumn("p").commit();
        table.newFastAppend().appendFile(DataFiles.builder(table.spec()).withPath("/data/new.parquet")
                .withFileSizeInBytes(100).withRecordCount(1).build()).commit();
        table.expireSnapshots().expireOlderThan(System.currentTimeMillis() + 1000)
                .retainLast(1).cleanExpiredMetadata(true).commit();
        Assertions.assertTrue(table.schemas().values().stream().noneMatch(retained -> retained.findField(1) != null));
        ManifestCacheValue data = cache.getManifestCacheValue(dataManifest, table);
        ManifestCacheValue deletes = cache.getManifestCacheValue(deleteManifest, table);
        Assertions.assertSame(warm, warmDeletes ? deletes : data);
        Assertions.assertEquals((long) value, (warmDeletes ? deletes.getDeleteFiles().get(0)
                : data.getDataFiles().get(0)).partition().get(0, Object.class));
        Assertions.assertEquals(value, (warmDeletes ? data.getDataFiles().get(0)
                : deletes.getDeleteFiles().get(0)).partition().get(0, Object.class));
        PartitionSpec current = table.specs().get(original.specId());
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            List<Callable<Void>> reads = new ArrayList<>();
            for (PartitionSpec target : Arrays.asList(original, current)) {
                reads.add(() -> {
                    for (int i = 0; i < 20; i++) {
                        DataFile adapted = data.forScan(target).getDataFiles().get(0);
                        DeleteFileIndex index = DeleteFileIndex.builderFor(deletes.forScan(target).getDeleteFiles())
                                .schemasById(table.schemas())
                                .specsById(Collections.singletonMap(target.specId(), target)).build();
                        Assertions.assertEquals(1, index.forDataFile(adapted.dataSequenceNumber(), adapted).length,
                                "mixed warm/cold numeric values must match after source-schema cleanup");
                        PartitionData otherPartition = new PartitionData(original.rawPartitionType());
                        otherPartition.set(0, value + 4);
                        DataFile other = DataFiles.builder(original).withPath("/data/other.parquet")
                                .withPartition(otherPartition).withRecordCount(1).withFileSizeInBytes(100).build();
                        DataFile projected = ManifestCacheValue.forDataFiles(Collections.singletonList(other))
                                .forScan(target).getDataFiles().get(0);
                        Assertions.assertEquals(0, index.forDataFile(adapted.dataSequenceNumber(), projected).length,
                                "normalization must not attach deletes from another partition");
                    }
                    return null;
                });
            }
            for (Future<Void> result : pool.invokeAll(reads)) {
                result.get();
            }
        } finally {
            pool.shutdownNow();
        }
        Assertions.assertEquals((long) value, (warmDeletes ? deletes.getDeleteFiles().get(0)
                : data.getDataFiles().get(0)).partition().get(0, Object.class), "shared cache must remain unchanged");
    }

    @Test
    public void concurrentPrefetchCountsEveryCacheAccess() throws Exception {
        Table table = tableWithTwoDataFiles();
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        IcebergManifestCache cache = new IcebergManifestCache();
        cache.getManifestCacheValue(manifest, table);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> jobs = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                jobs.add(pool.submit(() -> {
                    for (int j = 0; j < 100; j++) {
                        Assertions.assertNotNull(cache.getCachedManifestValue(manifest, "parallel"));
                    }
                }));
            }
            for (Future<?> job : jobs) {
                job.get(5, java.util.concurrent.TimeUnit.SECONDS);
            }
            Assertions.assertArrayEquals(new long[] {800, 0, 0}, cache.takeStats("parallel"));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void manifestEstimationCountsBackingArraysAndRejectsHiddenHeapBuffers() {
        byte[] backing = new byte[1024 * 1024];
        ByteBuffer slice = ByteBuffer.wrap(backing, 0, 1).slice();
        ManifestCacheValue writable = manifestWithLowerBound(slice);
        Assertions.assertTrue(writable.getSizeEstimate().isComplete());
        Assertions.assertTrue(writable.getSizeEstimate().getBytes() >= backing.length);
        ManifestCacheValue readOnly = manifestWithLowerBound(slice.asReadOnlyBuffer());
        Assertions.assertFalse(readOnly.getSizeEstimate().isComplete());
        Assertions.assertEquals(1, readOnly.getDataFiles().size(), "rejection must not discard the query result");
        Assertions.assertFalse(manifestWithLowerBound(ByteBuffer.wrap(backing).asReadOnlyBuffer())
                .getSizeEstimate().isComplete());
    }

    private static ManifestCacheValue manifestWithLowerBound(ByteBuffer lowerBound) {
        DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/data/buffer.parquet").withFileSizeInBytes(100)
                .withMetrics(new Metrics(1L, null, null, null, null,
                        Collections.singletonMap(1, lowerBound), null)).build();
        return ManifestCacheValue.forDataFiles(Collections.singletonList(file), true);
    }

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));

    private static Table tableWithTwoDataFiles() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        Table table = catalog.createTable(TableIdentifier.of("db1", "t1"), SCHEMA, PartitionSpec.unpartitioned());
        table.newAppend()
                .appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/data/f1.parquet").withFileSizeInBytes(100).withRecordCount(1).build())
                .appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath("/data/f2.parquet").withFileSizeInBytes(200).withRecordCount(2).build())
                .commit();
        return table;
    }

    @Test
    public void loadsDataFilesAndCachesByManifestPath() {
        Table table = tableWithTwoDataFiles();
        List<ManifestFile> manifests = table.currentSnapshot().dataManifests(table.io());
        Assertions.assertEquals(1, manifests.size(), "the single append produced one data manifest");
        ManifestFile manifest = manifests.get(0);

        IcebergManifestCache cache = new IcebergManifestCache();
        ManifestCacheValue first = cache.getManifestCacheValue(manifest, table);
        // WHY: a DATA manifest yields the two appended data files. MUTATION: returning the delete-file branch
        // (empty) -> 0 -> red.
        Assertions.assertEquals(2, first.getDataFiles().size());
        Assertions.assertTrue(first.getDeleteFiles().isEmpty());
        Assertions.assertEquals(1, cache.size());

        // A second read of the SAME manifest path returns the SAME cached value (no re-parse). MUTATION: not
        // caching (re-loading) -> a different instance -> red.
        ManifestCacheValue second = cache.getManifestCacheValue(manifest, table);
        Assertions.assertSame(first, second, "the manifest payload must be served from the cache on a repeat");
        Assertions.assertEquals(1, cache.size());
    }

    @Test
    public void weightBoundedManifestIsEstimatedAndCached() {
        Table table = tableWithTwoDataFiles();
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        try (CatalogMetaCache owner = CatalogMetaCache.unmanaged()) {
            IcebergManifestCache cache = new IcebergManifestCache(owner,
                    Collections.singletonMap("meta.cache.iceberg.manifest.max-weight", "1MB"));

            ManifestCacheValue first = cache.getManifestCacheValue(manifest, table);
            ManifestCacheValue second = cache.getManifestCacheValue(manifest, table);

            Assertions.assertSame(first, second);
            Assertions.assertEquals(2, first.getDataFiles().size());
        }
    }

    @Test
    public void equalityDeleteFieldIdsLoadOncePerSnapshot() {
        IcebergManifestCache cache = new IcebergManifestCache();
        AtomicInteger loads = new AtomicInteger();

        Set<Integer> first = cache.getOrLoadEqualityDeleteFieldIds("/warehouse/db/t", 10L, () -> {
            loads.incrementAndGet();
            return Collections.singleton(7);
        });
        Set<Integer> second = cache.getOrLoadEqualityDeleteFieldIds("/warehouse/db/t", 10L, () -> {
            loads.incrementAndGet();
            return Collections.singleton(8);
        });
        Set<Integer> nextSnapshot = cache.getOrLoadEqualityDeleteFieldIds("/warehouse/db/t", 11L, () -> {
            loads.incrementAndGet();
            return Collections.singleton(9);
        });

        Assertions.assertEquals(Collections.singleton(7), first);
        Assertions.assertEquals(first, second, "the same immutable snapshot must reuse its field-id set");
        Assertions.assertEquals(Collections.singleton(9), nextSnapshot);
        Assertions.assertEquals(2, loads.get(), "only one manifest walk is allowed per snapshot");
    }

    @Test
    public void equalityDeleteFieldIdFailureIsNotCached() {
        IcebergManifestCache cache = new IcebergManifestCache();
        AtomicInteger loads = new AtomicInteger();

        Assertions.assertThrows(IllegalStateException.class,
                () -> cache.getOrLoadEqualityDeleteFieldIds("/warehouse/db/t", 10L, () -> {
                    loads.incrementAndGet();
                    throw new IllegalStateException("transient manifest failure");
                }));
        Set<Integer> retry = cache.getOrLoadEqualityDeleteFieldIds("/warehouse/db/t", 10L, () -> {
            loads.incrementAndGet();
            return Collections.singleton(7);
        });

        Assertions.assertEquals(Collections.singleton(7), retry);
        Assertions.assertEquals(2, loads.get(), "a failed manifest walk must be retried, not memoized");
    }

    @Test
    public void capacityOverflowFlushesWholesale() {
        Table table = tableWithTwoDataFiles();
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        // maxSize 1: the first load fills it; a re-read still hits (same key). The wholesale-flush valve is the
        // legacy behavior — re-reads are harmless since the value is immutable. Smoke that size stays bounded.
        IcebergManifestCache cache = new IcebergManifestCache(1);
        cache.getManifestCacheValue(manifest, table);
        Assertions.assertEquals(1, cache.size());
        cache.getManifestCacheValue(manifest, table);
        Assertions.assertTrue(cache.size() <= 1, "a bounded cache must not grow past its max size");
    }

    @Test
    public void invalidateAllClearsEveryEntry() {
        Table table = tableWithTwoDataFiles();
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        IcebergManifestCache cache = new IcebergManifestCache();
        cache.getManifestCacheValue(manifest, table);
        AtomicInteger equalityLoads = new AtomicInteger();
        cache.getOrLoadEqualityDeleteFieldIds(table.location(), table.currentSnapshot().snapshotId(), () -> {
            equalityLoads.incrementAndGet();
            return Collections.singleton(1);
        });
        Assertions.assertEquals(1, cache.size());
        // REFRESH CATALOG hook (H-5): invalidateAll drops every cached manifest (legacy catalog-wide
        // group.invalidateAll parity). MUTATION: a no-op invalidateAll -> size stays 1 -> red.
        cache.invalidateAll();
        Assertions.assertEquals(0, cache.size());
        cache.getOrLoadEqualityDeleteFieldIds(table.location(), table.currentSnapshot().snapshotId(), () -> {
            equalityLoads.incrementAndGet();
            return Collections.singleton(1);
        });
        Assertions.assertEquals(2, equalityLoads.get(), "catalog refresh must also clear the snapshot projection");
    }

    @Test
    public void copiesDataFilesSoReaderReuseDoesNotAlias() {
        Table table = tableWithTwoDataFiles();
        ManifestFile manifest = table.currentSnapshot().dataManifests(table.io()).get(0);
        List<DataFile> files = new IcebergManifestCache().getManifestCacheValue(manifest, table).getDataFiles();
        // WHY: ManifestReader reuses one object across iterations, so the cache must .copy() each file. Two
        // distinct paths prove the entries were copied out (not the same reused instance). MUTATION: dropping
        // .copy() -> both entries alias the last file -> both paths equal -> red.
        Assertions.assertNotEquals(files.get(0).path().toString(), files.get(1).path().toString());
    }
}
