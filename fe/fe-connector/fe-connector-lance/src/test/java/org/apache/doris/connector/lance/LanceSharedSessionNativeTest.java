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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;
import org.lance.Session;
import org.lance.namespace.model.DescribeTableResponse;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;

/**
 * A catalog's Lance Session on a real dataset: every metadata read of a generation opens its dataset with that one
 * Session, a later read of a version already read is served from its metadata cache, the newest version is still
 * found, and a refresh closes it and moves the next read to a new one.
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
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put("warehouse", warehouse.toString());
        client = new LanceCatalogClient(LanceCatalogProperties.of(properties), Collections::emptyList,
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

    @Test
    void tableAccessInvalidationDescribesAgainWithTheSameSession() {
        read(OptionalLong.of(3));
        client.invalidateTableAccess();
        read(OptionalLong.of(3));
        Assertions.assertEquals(2, resources.namespaces.get(0).describes.size());
        Assertions.assertEquals(1, resources.sessions.size());
        Assertions.assertFalse(resources.sessions.get(0).isClosed());
    }

    @Test
    void refreshClosesTheSessionAndTheNextReadOpensANewGeneration() {
        read(OptionalLong.of(3));
        Session first = resources.sessions.get(0);
        client.refresh();
        Assertions.assertTrue(first.isClosed());

        Assertions.assertEquals(3, read(OptionalLong.of(3)).getVersion());
        Assertions.assertEquals(2, resources.sessions.size());
        Assertions.assertFalse(resources.sessions.get(1).isClosed());
        Assertions.assertEquals(1, resources.namespaces.get(1).describes.size());
        client.close();
        Assertions.assertTrue(resources.sessions.get(1).isClosed());
    }

    private LanceTableMetadata read(OptionalLong version) {
        return client.loadTableMetadata("default", "all_types", version, new LanceMetadataMetrics());
    }
}
