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

package org.apache.doris.connector.lance;

import com.github.benmanes.caffeine.cache.Ticker;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.Session;
import org.lance.WriteParams;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.ScalarIndexParams;
import org.lance.namespace.model.DescribeTableResponse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A catalog's Lance Session on a real dataset: every metadata read of a generation opens its dataset with that one
 * Session, a later read of a version already read is served from its metadata cache, the newest version is still
 * found, a table described with vended options is read with a Session of its own, and every REFRESH closes the
 * Session and moves the next read to a new one, which reads afresh a dataset written again at the same URI.
 *
 * <p>Describes go to a recording namespace that names the copied dataset, so that the table-access cache can be
 * counted; the allocator, the Session and the dataset reads are the real ones. Skips, loudly, where the native
 * libraries cannot load; see {@link LanceNativeTestSupport}.
 */
class LanceSharedSessionNativeTest {

    @TempDir
    Path warehouse;

    private final FakeResourceFactory resources = new FakeResourceFactory();
    private Path dataset;
    private LanceCatalogProperties properties;
    private LanceCatalogClient client;

    @BeforeEach
    void openCatalogOverTheVersionedDataset() throws Exception {
        LanceNativeTestSupport.requireNativeLibraries();
        dataset = LanceNativeTestSupport.copyDataset(LanceNativeTestSupport.VERSIONED_ALL_TYPES, warehouse);
        String uri = dataset.toString();
        resources.nextNamespace = () -> new FakeLanceNamespace().describing(
                new DescribeTableResponse().tableUri(uri));
        resources.nextSession = () -> Session.builder()
                .metadataCacheSizeBytes(LanceCatalogClient.SESSION_METADATA_CACHE_BYTES)
                .indexCacheSizeBytes(LanceCatalogClient.SESSION_INDEX_CACHE_BYTES).build();
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("type", "lance");
        catalogProperties.put("warehouse", warehouse.toString());
        properties = LanceCatalogProperties.of(catalogProperties);
        client = new LanceCatalogClient(properties, Collections::emptyList,
                () -> 256L * 1024 * 1024, resources, Ticker.systemTicker());
    }

    @AfterEach
    void closeCatalog() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void readsShareOneSessionAndALaterReadOfAVersionHitsItsCache() {
        Assertions.assertEquals(3, read(OptionalLong.of(3)).getVersion());
        Session session = resources.sessions.get(0);
        long hits = session.metadataCacheStats().getHits();
        Assertions.assertEquals(3, read(OptionalLong.of(3)).getVersion());
        Assertions.assertTrue(session.metadataCacheStats().getHits() > hits,
                "the second read of version 3 must be served from the Session's metadata cache");
        Assertions.assertEquals(1, read(OptionalLong.of(1)).getFragments().size());
        Assertions.assertEquals(1, resources.sessions.size());
        // The access described by the first read served the other two.
        Assertions.assertEquals(1, resources.namespaces.get(0).describes.size());
    }

    @Test
    void newestVersionIsStillFoundThroughTheSharedSession() {
        Assertions.assertEquals(3, read(OptionalLong.empty()).getVersion());
        try (Dataset writer = Dataset.open(dataset.toString())) {
            writer.updateConfig(Collections.singletonMap("doris_test_key", "new_version"));
        }
        Assertions.assertEquals(4, read(OptionalLong.empty()).getVersion());
        Assertions.assertEquals(3, read(OptionalLong.of(3)).getVersion());
        Assertions.assertEquals(1, resources.sessions.size());
    }

    /**
     * A table described with vended options is read with a Session of its own. A namespace may vend a new token on
     * every describe, and the shared Session would keep an object store for each set of options it was opened
     * with until the generation is retired. A table described without them still reads through the shared
     * Session.
     */
    @Test
    void readsWithVendedOptionsKeepOutOfTheSharedSession() {
        String uri = dataset.toString();
        AtomicInteger tokens = new AtomicInteger();
        resources.nextNamespace = () -> {
            FakeLanceNamespace namespace = new FakeLanceNamespace();
            namespace.describe = request -> new DescribeTableResponse().tableUri(uri).storageOptions(
                    Collections.singletonMap("doris_test_token", "token-" + tokens.incrementAndGet()));
            return namespace;
        };
        for (int read = 0; read < 8; read++) {
            Assertions.assertEquals(3, read(OptionalLong.of(3)).getVersion());
        }
        Session shared = resources.sessions.get(0);
        Assertions.assertEquals(8, resources.namespaces.get(0).describes.size());
        Assertions.assertEquals(0, shared.metadataCacheStats().getHits() + shared.metadataCacheStats().getMisses(),
                "a read with vended options must not open its dataset with the shared Session");

        resources.namespaces.get(0).describing(new DescribeTableResponse().tableUri(uri));
        read(OptionalLong.of(3));
        long hits = shared.metadataCacheStats().getHits();
        read(OptionalLong.of(3));
        Assertions.assertTrue(shared.metadataCacheStats().getHits() > hits,
                "a read without vended options must still be served from the shared Session");
    }

    /** REFRESH TABLE, DATABASE and CATALOG each close the Session and move the next read to a new generation. */
    @ParameterizedTest
    @EnumSource(Refresh.class)
    void everyRefreshClosesTheSessionAndTheNextReadOpensANewGeneration(Refresh refresh) {
        read(OptionalLong.of(3));
        Session first = resources.sessions.get(0);
        refresh.run(new LanceConnector(properties, new LanceTestContext(), client));
        Assertions.assertTrue(first.isClosed());

        Assertions.assertEquals(3, read(OptionalLong.of(3)).getVersion());
        Assertions.assertEquals(2, resources.sessions.size());
        Assertions.assertFalse(resources.sessions.get(1).isClosed());
        Assertions.assertEquals(1, resources.namespaces.get(1).describes.size());
        client.close();
        Assertions.assertTrue(resources.sessions.get(1).isClosed());
    }

    /**
     * A dataset deleted and written again at the same URI starts its versions over. With V1 manifest names Lance
     * keys a version's index list by the URI and the version alone, so the old Session can serve the deleted
     * dataset's indexes for the new dataset's version 2, and a filtered scan would then be planned over index
     * segments the BE cannot find. Every REFRESH moves the next read to a new Session, which reads the new
     * dataset's.
     */
    @ParameterizedTest
    @EnumSource(Refresh.class)
    void everyRefreshReadsTheIndexesOfADatasetWrittenAgainAtTheSameUri(Refresh refresh) throws IOException {
        Path recreated = warehouse.resolve("recreated.lance");
        String uri = recreated.toString();
        resources.nextNamespace = () -> new FakeLanceNamespace().describing(
                new DescribeTableResponse().tableUri(uri));
        // Version 1 holds the rows, version 2 adds a BTREE index on them.
        commitRows(uri, 0);
        try (Dataset writer = Dataset.open(uri)) {
            writer.createIndex(Collections.singletonList("id"), IndexType.BTREE, Optional.of("id_idx"),
                    IndexParams.builder().setScalarIndexParams(ScalarIndexParams.create("btree")).build(), true);
        }
        Assertions.assertEquals(Collections.singletonList("id_idx"), indexesOfTheNewestVersion(2));

        // The same URI again: version 1 holds other rows, version 2 changes a setting and indexes nothing.
        deleteRecursively(recreated);
        commitRows(uri, 100);
        try (Dataset writer = Dataset.open(uri)) {
            writer.updateConfig(Collections.singletonMap("doris_test_key", "written_again"));
        }
        Assertions.assertTrue(Files.exists(recreated.resolve("_versions").resolve("2.manifest")),
                "V1 manifest names expected: with V2 names Lance keys the index list by the manifest's e-tag too");

        refresh.run(new LanceConnector(properties, new LanceTestContext(), client));
        Assertions.assertEquals(Collections.emptyList(), indexesOfTheNewestVersion(2));
        Assertions.assertEquals(2, resources.sessions.size());
    }

    /** The REFRESH statements, as the engine routes them to the connector. */
    enum Refresh {
        TABLE {
            @Override
            void run(LanceConnector connector) {
                connector.invalidateTable("default", "recreated");
            }
        },
        DATABASE {
            @Override
            void run(LanceConnector connector) {
                connector.invalidateDb("default");
            }
        },
        CATALOG {
            @Override
            void run(LanceConnector connector) {
                connector.invalidateAll();
            }
        };

        abstract void run(LanceConnector connector);
    }

    private LanceTableMetadata read(OptionalLong version) {
        return client.loadTableMetadata("default", "all_types", version, new LanceMetadataMetrics());
    }

    /** What planning a scan with a pushed filter reads: the newest version, then the indexes of that version. */
    private List<String> indexesOfTheNewestVersion(long expectedVersion) {
        LanceMetadataMetrics metrics = new LanceMetadataMetrics();
        long version = client.loadTableMetadata("default", "recreated", OptionalLong.empty(), metrics).getVersion();
        Assertions.assertEquals(expectedVersion, version);
        return client.loadIndexMetadata("default", "recreated", version, metrics).getIndexes().stream()
                .map(LanceIndexSegmentGroup::getName).collect(Collectors.toList());
    }

    /** Commits three rows from {@code first} on as version 1 of a new dataset at {@code uri}. */
    private static void commitRows(String uri, int first) {
        Schema schema = new Schema(Collections.singletonList(Field.nullable("id", new ArrowType.Int(32, true))));
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot rows = VectorSchemaRoot.create(schema, allocator)) {
            rows.allocateNew();
            IntVector ids = (IntVector) rows.getVector("id");
            for (int i = 0; i < 3; i++) {
                ids.setSafe(i, first + i);
            }
            rows.setRowCount(3);
            List<FragmentMetadata> fragments = Fragment.create(uri, allocator, rows, new WriteParams.Builder().build());
            new FragmentOperation.Overwrite(fragments, schema)
                    .commit(allocator, uri, Optional.empty(), Collections.emptyMap()).close();
        }
    }

    private static void deleteRecursively(Path directory) throws IOException {
        try (Stream<Path> paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                Files.delete(path);
            }
        }
    }
}
