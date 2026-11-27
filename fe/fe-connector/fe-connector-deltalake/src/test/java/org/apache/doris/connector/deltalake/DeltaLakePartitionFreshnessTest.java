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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.deltalake.DeltaLakePartitionFreshness.State;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.scan.ConnectorPartitionValues;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

class DeltaLakePartitionFreshnessTest {
    private static final long EPOCH = 1_750_000_000_000L;
    private static final StructType SCHEMA = new StructType().add("id", IntegerType.INTEGER)
            .add("p", StringType.STRING);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<String> PARTITIONS = Set.of("p=10", "p=20");
    private final TestingConnectorSession session = new TestingConnectorSession("mtmv");
    @TempDir
    Path warehouse;
    private Path table;
    private DeltaLakeRuntime runtime;
    private CatalogMetaCache owner;
    private DeltaLakePartitionFreshness freshness;
    private DeltaLakeConnectorMetadata metadata;

    @BeforeEach
    void setUp() {
        table = warehouse.resolve("db/events");
        var properties = DeltaLakeCatalogProperties.of(Map.of("warehouse", warehouse.toString()));
        runtime = new DeltaLakeRuntime(properties, new RecordingConnectorContext());
        owner = CatalogMetaCache.unmanaged();
        freshness = new DeltaLakePartitionFreshness(owner, Map.of());
        metadata = new DeltaLakeConnectorMetadata(runtime, properties, new DeltaLakeSnapshotCache(owner, Map.of()),
                new DeltaLakeStatisticsCache(owner, Map.of()), freshness);
    }

    @AfterEach
    void tearDown() throws IOException {
        owner.close();
        runtime.close();
    }

    private void metadata(long version, StructType schema, List<String> columns) throws Exception {
        DeltaLakeTestTables.commit(table, version, "table-id", schema, columns);
    }

    private Path commit(long version, String... actions) throws Exception {
        Path path = table.resolve(String.format("_delta_log/%020d.json", version));
        if (actions.length > 0) {
            Files.writeString(path, String.join("\n", actions) + "\n", StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        }
        Files.setLastModifiedTime(path, FileTime.fromMillis(EPOCH + version));
        return path;
    }

    private static String file(String action, String path, Map<String, String> partition, boolean dataChange)
            throws Exception {
        Map<String, Object> fields = new HashMap<>(Map.of("path", path, "size", 20L, "modificationTime", EPOCH - 5000,
                "dataChange", dataChange));
        if (partition != null) {
            fields.put("partitionValues", partition);
            fields.put("extendedFileMetadata", true);
        }
        return JSON.writeValueAsString(Map.of(action, fields));
    }

    private DeltaLakeTableHandle pin(long version) {
        Snapshot snapshot = TableManager.loadSnapshot(table.toString()).atVersion(version).build(runtime.engine);
        return new DeltaLakeTableHandle("db", "events", snapshot);
    }

    private Map<String, Long> listed(DeltaLakeTableHandle handle) {
        return metadata.listPartitions(session, handle, Optional.empty()).stream().collect(Collectors.toMap(
                ConnectorPartitionInfo::getPartitionName, ConnectorPartitionInfo::getLastModifiedMillis));
    }

    private State state(DeltaLakeTableHandle handle) {
        return freshness.atSnapshot(session, handle, runtime, handle.snapshot(runtime), ZoneOffset.UTC,
                () -> new DeltaLakePartitionFreshness.Partitions(PARTITIONS.stream().collect(
                        Collectors.toMap(Function.identity(), ignored -> Map.of())), Set.of(), 2));
    }

    @Test
    void appendRemoveOverwriteAndCompactionUseCommitTimes() throws Exception {
        var ten = Map.of("p", "10");
        var twenty = Map.of("p", "20");
        metadata(0, SCHEMA, List.of("p"));
        commit(0, file("add", "old", ten, true), file("add", "other", twenty, true));
        commit(1, file("add", "new", ten, true));
        commit(2, file("remove", "old", ten, true));
        commit(3, file("remove", "new", ten, true), file("add", "rewrite", ten, true));
        commit(4, file("remove", "rewrite", ten, false), file("add", "compact", ten, false));
        for (int v = 0; v <= 4; v++) {
            Assertions.assertEquals(Map.of("p=10", EPOCH + Math.min(v, 3), "p=20", EPOCH), listed(pin(v)));
        }
        // The same cache is already at v4. A historical lookup must not change that head or leak its times.
        Assertions.assertEquals(Map.of("p=10", EPOCH + 1, "p=20", EPOCH), listed(pin(1)));
        Assertions.assertEquals(Map.of("p=10", EPOCH + 3, "p=20", EPOCH), listed(pin(4)));
        var sqlCache = metadata.getSqlCacheVersion(session, pin(4)).orElseThrow();
        Assertions.assertEquals(EPOCH + 4, sqlCache.getUpdateTimeMillis());
        Assertions.assertNotEquals(sqlCache.getVersion(),
                metadata.getSqlCacheVersion(session, pin(3)).orElseThrow().getVersion());
    }

    @Test
    void deletionVectorChangeKeepsTheOriginalParquetMtime() throws Exception {
        metadata(0, SCHEMA, List.of("p"));
        commit(0, file("add", "a", Map.of("p", "10"), true), file("add", "b", Map.of("p", "20"), true));
        var withDv = JSON.readTree(file("add", "a", Map.of("p", "10"), true));
        ((ObjectNode) withDv.get("add")).set("deletionVector",
                JSON.valueToTree(Map.of("storageType", "u", "pathOrInlineDv", "Zcq?bs*HKQWCB[Sjf[.o",
                        "offset", 1, "sizeInBytes", 36, "cardinality", 2)));
        commit(1, file("remove", "a", Map.of("p", "10"), true), withDv.toString());
        State state = state(pin(1));
        Assertions.assertEquals(EPOCH + 1, state.lastChanged("p=10"));
        Assertions.assertEquals(EPOCH, state.lastChanged("p=20"));
    }

    @Test
    void unassignedRemovesAndChangedSchemasAdvanceAllPartitions() throws Exception {
        metadata(0, SCHEMA, List.of("p"));
        commit(0, file("add", "a", Map.of("p", "10"), true), file("add", "b", Map.of("p", "20"), true));
        commit(1, file("remove", "unassigned", null, true));
        commit(2, file("remove", "old-layout", Map.of("old-key", "10"), true));
        StructType evolved = SCHEMA.add("label", StringType.STRING);
        metadata(3, evolved, List.of("p"));
        commit(3);
        DeltaLakeTestTables.commit(table, 4, "table-id", evolved, List.of("p"),
                Map.of("minReaderVersion", 1, "minWriterVersion", 2), Map.of("description", "new property"),
                null, null);
        commit(4);
        for (int v = 0; v <= 4; v++) {
            Assertions.assertEquals(Map.of("p=10", EPOCH + Math.min(v, 3), "p=20", EPOCH + Math.min(v, 3)),
                    listed(pin(v)));
        }
    }

    @Test
    void cachedSuffixDoesNotReadOldJsonAndCarriesMtimeAcrossCommits() throws Exception {
        metadata(0, SCHEMA, List.of("p"));
        Path first = commit(0, file("add", "a", Map.of("p", "10"), true));
        Path second = commit(1, file("add", "b", Map.of("p", "20"), true));
        Files.setLastModifiedTime(second, FileTime.fromMillis(EPOCH - 100));
        var v0 = pin(0);
        var v1 = pin(1);
        State initial = state(v0);
        String original = Files.readString(first);
        Files.writeString(first, "invalid JSON");
        Assertions.assertSame(initial, state(v0));
        State advanced = state(v1);
        Assertions.assertEquals(EPOCH + 1, advanced.lastChanged("p=20"));
        Assertions.assertEquals(EPOCH, advanced.lastChanged("p=10"));
        Files.writeString(first, original);
        Files.setLastModifiedTime(first, FileTime.fromMillis(EPOCH));
        Assertions.assertEquals(EPOCH, state(v0).lastChanged("p=20"));
        Assertions.assertSame(advanced, state(v1));

        owner.invalidateTable("db", "events");
        Files.setLastModifiedTime(first, FileTime.fromMillis(EPOCH + 100));
        Assertions.assertEquals(EPOCH + 101, state(v1).lastChanged("p=20"));
    }

    @Test
    void retainedSuffixAndCheckpointOnlyAreConservativeAtThePin() throws Exception {
        metadata(0, SCHEMA, List.of("p"));
        Path zero = commit(0, file("add", "old", Map.of("p", "10"), true));
        Path one = commit(1, file("add", "other", Map.of("p", "20"), true));
        Path two = commit(2, file("add", "old2", Map.of("p", "10"), true));
        Path three = commit(3, file("add", "old3", Map.of("p", "10"), true));
        Path four = commit(4, file("add", "recent", Map.of("p", "20"), true));
        var first = pin(0);
        var target = pin(4);
        state(first);
        for (Path path : List.of(zero, one, two)) {
            Files.delete(path);
        }
        for (int attempt = 0; attempt < 2; attempt++) {
            State truncated = state(target);
            Assertions.assertEquals(EPOCH + 3, truncated.lastChanged("p=10"));
            Assertions.assertEquals(EPOCH + 4, truncated.lastChanged("p=20"));
            owner.invalidateTable("db", "events");
        }
        Assertions.assertEquals(EPOCH, state(first).lastChanged("p=10")); // earliest > historical target
        Files.delete(three);
        Files.delete(four);
        State checkpointOnly = state(target);
        Assertions.assertEquals(EPOCH + 4, checkpointOnly.lastChanged("p=10"));
        Assertions.assertEquals(EPOCH + 4, checkpointOnly.lastChanged("p=20"));
    }

    @Test
    void schemaRestoredAfterLogGapAdvancesAllPartitions() throws Exception {
        metadata(0, SCHEMA, List.of("p"));
        Path v0 = commit(0, file("add", "a", Map.of("p", "10"), true), file("add", "b", Map.of("p", "20"), true));
        state(pin(0));
        metadata(1, SCHEMA.add("label", StringType.STRING), List.of("p"));
        Path v1 = commit(1);
        commit(2, file("add", "c", Map.of("p", "10"), true));
        metadata(3, SCHEMA, List.of("p"));
        commit(3);
        var target = pin(3);
        Files.delete(v0);
        Files.delete(v1);

        State warm = state(target);
        owner.invalidateTable("db", "events");
        State cold = state(target);
        for (String partition : PARTITIONS) {
            Assertions.assertEquals(EPOCH + 3, cold.lastChanged(partition), partition);
            Assertions.assertEquals(cold.lastChanged(partition), warm.lastChanged(partition), partition);
        }
    }

    @Test
    void renamedPhysicalPartitionAndNullRemainDistinct() throws Exception {
        var id = FieldMetadata.builder().putLong("delta.columnMapping.id", 1L)
                .putString("delta.columnMapping.physicalName", "key1").build();
        var data = new StructField("id", IntegerType.INTEGER, true,
                FieldMetadata.builder().putLong("delta.columnMapping.id", 2L)
                        .putString("delta.columnMapping.physicalName", "key2").build());
        var protocol = Map.<String, Object>of("minReaderVersion", 2, "minWriterVersion", 5);
        var config = Map.of("delta.columnMapping.mode", "name", "delta.columnMapping.maxColumnId", "2");
        for (int v = 0; v <= 1; v++) {
            String column = v == 0 ? "p" : "region";
            var schema = new StructType().add(data).add(new StructField(column, StringType.STRING, true, id));
            DeltaLakeTestTables.commit(table, v, "table-id", schema, List.of(column), protocol, config, null, null);
            if (v == 0) {
                commit(0, file("add", "null", Collections.singletonMap("key1", null), true),
                        file("add", "ten", Map.of("key1", "10"), true));
            } else {
                commit(1);
            }
        }
        commit(2, file("add", "next", Map.of("key1", "10"), true));
        String marker = ConnectorPartitionValues.NULL_PARTITION_NAME;
        Assertions.assertEquals(Map.of("p=" + marker, EPOCH, "p=10", EPOCH), listed(pin(0)));
        Assertions.assertEquals(Map.of("region=" + marker, EPOCH + 1, "region=10", EPOCH + 1), listed(pin(1)));
        Assertions.assertEquals(Map.of("region=" + marker, EPOCH + 1, "region=10", EPOCH + 2), listed(pin(2)));
    }

    @Test
    void ictOverridesFileTimeOnlyFromItsEnablementVersion() throws Exception {
        metadata(0, SCHEMA, List.of("p"));
        commit(0, "{\"commitInfo\":{\"inCommitTimestamp\":11}}",
                file("add", "a", Map.of("p", "10"), true), file("add", "b", Map.of("p", "20"), true));
        long ict = EPOCH + 60000;
        DeltaLakeTestTables.commit(table, 1, "table-id", SCHEMA, List.of("p"),
                Map.of("minReaderVersion", 1, "minWriterVersion", 7, "writerFeatures", List.of("inCommitTimestamp")),
                Map.of("delta.enableInCommitTimestamps", "true", "delta.inCommitTimestampEnablementVersion", "1",
                        "delta.inCommitTimestampEnablementTimestamp", Long.toString(ict)), ict, null);
        commit(1, file("add", "new", Map.of("p", "20"), true));
        Assertions.assertEquals(Map.of("p=10", EPOCH, "p=20", ict), listed(pin(1)));
        Assertions.assertEquals(ict, metadata.getSqlCacheVersion(session, pin(1)).orElseThrow().getUpdateTimeMillis());
    }

    @Test
    void realCheckpointStillListsActivePartitionsAfterPinnedJsonIsCleanedUp() throws Exception {
        Path source = Path.of(getClass().getResource("/deltalake/dv-partitioned-with-checkpoint/_delta_log").toURI());
        Path log = Files.createDirectories(table.resolve("_delta_log"));
        String json = "00000000000000000010.json";
        for (String name : List.of(json, "00000000000000000010.checkpoint.parquet", "_last_checkpoint")) {
            Files.copy(source.resolve(name), log.resolve(name));
        }
        Files.setLastModifiedTime(log.resolve(json), FileTime.fromMillis(EPOCH));
        DeltaLakeTableHandle pinned = pin(10);
        Files.delete(log.resolve(json));
        Map<String, Long> active = listed(pinned);
        Assertions.assertFalse(active.isEmpty());
        Assertions.assertEquals(Set.of(EPOCH), Set.copyOf(active.values()));
    }

    @Test
    void refreshDuringAnIncrementalReadFencesTheOldPublisher() throws Exception {
        metadata(0, SCHEMA, List.of("p"));
        commit(0, file("add", "a", Map.of("p", "10"), true));
        commit(1, file("add", "b", Map.of("p", "20"), true));
        state(pin(0));
        var second = pin(1);
        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            var old = pool.submit(() -> freshness.atSnapshot(session, second, runtime, second.snapshot(runtime),
                    ZoneOffset.UTC, () -> {
                        reading.countDown();
                        try {
                            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                        return new DeltaLakePartitionFreshness.Partitions(Map.of(), Set.of(), 0);
                    }));
            Assertions.assertTrue(reading.await(10, TimeUnit.SECONDS));
            owner.invalidateTable("db", "events");
            State replacement = state(second);
            release.countDown();
            Assertions.assertNotSame(replacement, old.get(10, TimeUnit.SECONDS));
            Assertions.assertSame(replacement, state(second));
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void cacheIsolatedByUserZoneRegistrationAndTableIdentity() throws Exception {
        metadata(0, SCHEMA, List.of("p"));
        commit(0, file("add", "first", Map.of("p", "10"), true));
        var original = pin(0);
        var snapshot = original.snapshot(runtime);
        State head = state(original);
        DeltaLakePartitionFreshness.PartitionLoader loader =
                () -> DeltaLakePartitionFreshness.Partitions.collect(snapshot, runtime.engine, ZoneOffset.UTC);
        var otherUser = new TestingConnectorSession("other") {
            @Override
            public String getUser() {
                return "another_user";
            }
        };
        Assertions.assertNotSame(head, freshness.atSnapshot(otherUser, original, runtime, snapshot, ZoneOffset.UTC,
                loader));
        Assertions.assertNotSame(head, freshness.atSnapshot(session, original, runtime, snapshot, ZoneOffset.ofHours(8),
                loader));
        var registered = new DeltaLakeTableHandle("db", "events",
                new DeltaLakeTableReference(original.getPath(), "new-registration", false), snapshot);
        Assertions.assertNotSame(head, state(registered));
        DeltaLakeTestTables.commit(table, 0, "recreated-id", SCHEMA, List.of("p"));
        commit(0, file("add", "second", Map.of("p", "20"), true));
        DeltaLakeTableHandle recreated = pin(0);
        Assertions.assertNotSame(head, state(recreated));
        Assertions.assertNotEquals(metadata.getSqlCacheVersion(session, original).orElseThrow().getVersion(),
                metadata.getSqlCacheVersion(session, recreated).orElseThrow().getVersion());
        Assertions.assertSame(head, state(original));
    }

    @Test
    void emptyAndUnpartitionedTablesHaveNoSyntheticListPartitions() throws Exception {
        metadata(0, SCHEMA, List.of("p"));
        commit(0);
        Assertions.assertTrue(listed(pin(0)).isEmpty());
        metadata(1, SCHEMA, List.of());
        commit(1, file("add", "plain", Map.of(), true));
        Assertions.assertTrue(listed(pin(1)).isEmpty());
    }
}
