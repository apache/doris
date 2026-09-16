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

package org.apache.doris.datasource.lance;

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.common.profile.RuntimeProfile;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.datasource.lance.metadata.LanceMetadataLoader;
import org.apache.doris.datasource.lance.metadata.LanceReadOptions;
import org.apache.doris.datasource.lance.metadata.LanceTableAccess;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;
import org.apache.doris.datasource.lance.profile.LanceMetadataMetrics;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentOperation;
import org.lance.Session;
import org.lance.WriteParams;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.ScalarIndexParams;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

public class LanceSharedSessionTest {
    @Test
    public void testCatalogMetadataProfileIncludesAnalyzeReadsAndTimeTravel(@TempDir Path directory) throws Exception {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        SummaryProfile summary = new SummaryProfile();
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        Mockito.when(executor.getSummaryProfile()).thenReturn(summary);
        context.setExecutor(executor);
        context.setThreadLocalInfo();
        Schema schema = new Schema(Collections.singletonList(Field.nullable("id", new ArrowType.Int(64, true))));
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put(LanceExternalCatalog.WAREHOUSE, directory.toString());
        LanceExternalCatalog catalog = new LanceExternalCatalog(901, "profile_lance", null, properties, "");
        try (BufferAllocator allocator = new RootAllocator();
                Dataset writer = Dataset.write().allocator(allocator).uri(directory.resolve("table.lance").toString())
                        .schema(schema).execute()) {
            catalog.loadTableSchema("default", "table");
            catalog.loadBasicTableMetadata("default", "table");
            catalog.loadTableMetadata("default", "table");
            catalog.loadTableMetadataForSearch("default", "table");
            Assertions.assertEquals(writer.version(), catalog.loadTableMetadata("default", "table",
                    Optional.of(TableSnapshot.timeOf("2100-01-01 00:00:00"))).getVersion());
            Assertions.assertThrows(RuntimeException.class, () -> catalog.loadTableMetadata("default", "missing"));
            RuntimeProfile group = summary.getExecutionSummary().getChildMap().get(LanceMetadataMetrics.GROUP_NAME);
            Assertions.assertEquals(6, group.getCounterMap().get("MetadataReadCalls").getValue());
            Assertions.assertEquals(1, group.getCounterMap().get("MetadataReadFailures").getValue());
            Assertions.assertEquals(6, group.getCounterMap().get("TableAccessResolveCalls").getValue());
            Assertions.assertEquals(6, group.getCounterMap().get("DatasetOpenCalls").getValue());
            Assertions.assertEquals(5, group.getCounterMap().get("SchemaReadCalls").getValue());
            Assertions.assertEquals(4, group.getCounterMap().get("FragmentMetadataReadCalls").getValue());
            Assertions.assertEquals(3, group.getCounterMap().get("FieldIdsReadCalls").getValue());
            Assertions.assertEquals(3, group.getCounterMap().get("IndexMetadataReadCalls").getValue());
            Assertions.assertEquals(1, group.getCounterMap().get("VersionResolveCalls").getValue());
            Assertions.assertFalse(group.getCounterMap().containsKey("SplitPlanningTime"));
        } finally {
            catalog.onClose();
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    public void testCatalogQueryAndInspectionReadSameLocalTable(@TempDir Path directory) throws Exception {
        String uri = directory.resolve("table.lance").toString();
        Schema schema = new Schema(Collections.singletonList(Field.nullable("id", new ArrowType.Int(64, true))));
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put(LanceExternalCatalog.WAREHOUSE, directory.toString());
        LanceExternalCatalog catalog = new LanceExternalCatalog(900, "local_lance", null, properties, "");
        try (BufferAllocator allocator = new RootAllocator();
                Dataset writer = Dataset.write().allocator(allocator).uri(uri).schema(schema).execute()) {
            long firstVersion = writer.version();
            Assertions.assertEquals(firstVersion, catalog.loadTableMetadata("default", "table").getVersion());
            Assertions.assertEquals(firstVersion,
                    catalog.loadTableMetadataForSearch("default", "table").getVersion());
            Assertions.assertEquals(schema, catalog.loadTableSchema("default", "table"));
            Assertions.assertTrue(catalog.loadTableIndexesForShow("default", "table").isEmpty());
            Assertions.assertTrue(catalog.loadTableIndexEntries("default", "table").isEmpty());
            java.lang.reflect.Field clientField = LanceExternalCatalog.class.getDeclaredField("client");
            clientField.setAccessible(true);
            Object firstClient = clientField.get(catalog);
            java.lang.reflect.Field sessionField = LanceCatalogClient.class.getDeclaredField("session");
            sessionField.setAccessible(true);
            Session firstSession = (Session) sessionField.get(firstClient);
            catalog.onRefreshCache(false);
            Assertions.assertSame(firstClient, clientField.get(catalog));
            Assertions.assertFalse(firstSession.isClosed());
            catalog.onRefreshCache(true);
            Assertions.assertNotSame(firstClient, clientField.get(catalog));
            Assertions.assertTrue(firstSession.isClosed());
            Assertions.assertEquals(firstVersion, catalog.loadTableMetadata("default", "table").getVersion());
            writer.updateConfig(Collections.singletonMap("test_key", "new_version"));
            Assertions.assertTrue(catalog.loadBasicTableMetadata("default", "table").getVersion() > firstVersion);
            Assertions.assertEquals(writer.version(),
                    LanceMetadataLoader.loadLatestForTvf(uri, Collections.emptyList()).getVersion());
        } finally {
            catalog.onClose();
        }
    }

    @Test
    public void testCatalogRefreshReplacesCachedIndexesAfterSameUriRecreation(@TempDir Path directory)
            throws Exception {
        Path datasetPath = directory.resolve("table.lance");
        String uri = datasetPath.toString();
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put(LanceExternalCatalog.WAREHOUSE, directory.toString());
        LanceExternalCatalog catalog = new LanceExternalCatalog(903, "recreated_lance", null, properties, "");
        try (BufferAllocator allocator = new RootAllocator()) {
            long oldVersion = writeIndexedTable(allocator, uri);
            LanceTableMetadata oldMetadata = catalog.loadTableMetadata("default", "table");
            UUID oldUuid = oldMetadata.getIndexes().get(0).getSegments().get(0).getUuid();
            FileUtils.deleteDirectory(datasetPath.toFile());
            Assertions.assertEquals(oldVersion, writeIndexedTable(allocator, uri));
            UUID freshUuid;
            try (Dataset independent = Dataset.open().allocator(allocator).uri(uri)
                    .readOptions(LanceReadOptions.forIndependentRead(Collections.emptyMap(), OptionalLong.empty()))
                    .build()) {
                LanceTableMetadata fresh = LanceMetadataLoader.read(independent,
                        new LanceTableAccess(uri, Collections.emptyMap()),
                        LanceMetadataLoader.MetadataScope.WITH_INDEXES);
                freshUuid = fresh.getIndexes().get(0).getSegments().get(0).getUuid();
            }
            Assertions.assertNotEquals(oldUuid, freshUuid);
            // The padded manifest prevents open() from opportunistically replacing the cached index list.
            Assertions.assertEquals(oldUuid,
                    catalog.loadTableMetadata("default", "table").getIndexes().get(0).getSegments().get(0).getUuid());
            catalog.onRefreshCache(true);
            Assertions.assertEquals(freshUuid,
                    catalog.loadTableMetadata("default", "table").getIndexes().get(0).getSegments().get(0).getUuid());
        } finally {
            catalog.onClose();
        }
    }

    private static long writeIndexedTable(BufferAllocator allocator, String uri) {
        Schema schema = new Schema(Collections.singletonList(Field.nullable("id", new ArrowType.Int(32, true))));
        try (Dataset empty = Dataset.write().allocator(allocator).uri(uri).schema(schema).execute();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            root.allocateNew();
            ((IntVector) root.getVector("id")).setSafe(0, 1);
            root.setRowCount(1);
            FragmentOperation.Append append = new FragmentOperation.Append(
                    Fragment.create(uri, allocator, root, new WriteParams.Builder().build()));
            try (Dataset dataset = Dataset.commit(allocator, uri, append, Optional.of(empty.version()))) {
                dataset.createIndex(Collections.singletonList("id"), IndexType.BTREE, Optional.of("id_idx"),
                        IndexParams.builder().setScalarIndexParams(ScalarIndexParams.create("btree")).build(), true);
                // Keep the index section outside the manifest's last read block.
                dataset.updateConfig(Collections.singletonMap("test_padding", StringUtils.repeat("x", 256 * 1024)));
                return dataset.version();
            }
        }
    }

    @Test
    public void testSharedCacheSurvivesReadCloseAndLatestStillAdvances(@TempDir Path directory) {
        String uri = directory.resolve("table.lance").toString();
        Schema schema = new Schema(Collections.singletonList(Field.nullable("id", new ArrowType.Int(64, true))));
        try (BufferAllocator allocator = new RootAllocator();
                Dataset writer = Dataset.write().allocator(allocator).uri(uri).schema(schema).execute();
                Session session = Session.builder().metadataCacheSizeBytes(1024 * 1024)
                        .indexCacheSizeBytes(1024 * 1024).build()) {
            long firstVersion;
            try (BufferAllocator readAllocator = new RootAllocator(LanceMetadataLoader.READ_ALLOCATOR_LIMIT);
                    Dataset first = Dataset.open().allocator(readAllocator).uri(uri)
                            .readOptions(LanceReadOptions.forSharedSession(Collections.emptyMap(), OptionalLong.empty(), session)).build()) {
                Assertions.assertTrue(session.isSameAs(first.session()));
                firstVersion = first.version();
                LanceTableMetadata metadata = LanceMetadataLoader.read(first,
                        new LanceTableAccess(uri, Collections.emptyMap()),
                        LanceMetadataLoader.MetadataScope.WITH_INDEXES);
                Assertions.assertEquals(firstVersion, metadata.getVersion());
                Assertions.assertTrue(metadata.getLanceFieldId("id").isPresent());
            }
            Assertions.assertFalse(session.isClosed());
            long hitsBefore = session.metadataCacheStats().getHits();
            try (BufferAllocator readAllocator = new RootAllocator(LanceMetadataLoader.READ_ALLOCATOR_LIMIT);
                    Dataset second = Dataset.open().allocator(readAllocator).uri(uri)
                            .readOptions(LanceReadOptions.forSharedSession(Collections.emptyMap(), OptionalLong.empty(), session)).build()) {
                Assertions.assertTrue(session.isSameAs(second.session()));
                Assertions.assertEquals(firstVersion, second.version());
                LanceTableMetadata metadata = LanceMetadataLoader.read(second,
                        new LanceTableAccess(uri, Collections.emptyMap()),
                        LanceMetadataLoader.MetadataScope.WITH_INDEXES);
                Assertions.assertEquals(firstVersion, metadata.getVersion());
                Assertions.assertTrue(metadata.getLanceFieldId("id").isPresent());
            }
            Assertions.assertTrue(session.metadataCacheStats().getHits() > hitsBefore);
            writer.updateConfig(Collections.singletonMap("test_key", "new_version"));
            try (BufferAllocator readAllocator = new RootAllocator(LanceMetadataLoader.READ_ALLOCATOR_LIMIT);
                    Dataset latest = Dataset.open().allocator(readAllocator).uri(uri)
                            .readOptions(LanceReadOptions.forSharedSession(Collections.emptyMap(), OptionalLong.empty(), session)).build();
                    Dataset historical = Dataset.open().allocator(readAllocator).uri(uri)
                            .readOptions(LanceReadOptions.forSharedSession(Collections.emptyMap(), OptionalLong.of(firstVersion), session)).build()) {
                Assertions.assertTrue(latest.version() > firstVersion);
                Assertions.assertEquals(firstVersion, historical.version());
            }
        }
    }
}
