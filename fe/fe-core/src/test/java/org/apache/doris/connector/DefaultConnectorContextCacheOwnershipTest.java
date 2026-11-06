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

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.MetaCacheGovernance;
import org.apache.doris.connector.cache.ScopePath;
import org.apache.doris.connector.spi.Connector;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class DefaultConnectorContextCacheOwnershipTest {
    @Test
    void tracksRootAndLaterSiblingWithoutAffectingAnotherContext() {
        DefaultConnectorContext first = new DefaultConnectorContext("first", 913L);
        DefaultConnectorContext second = new DefaultConnectorContext("second", 913L);
        List<CatalogMetaCache> created = new ArrayList<>();
        try (MockedStatic<ConnectorFactory> factory = Mockito.mockStatic(ConnectorFactory.class)) {
            factory.when(() -> ConnectorFactory.createStandaloneCatalogConnector(
                    Mockito.anyString(), Mockito.anyMap(), Mockito.any())).thenAnswer(invocation -> {
                        created.add(cache());
                        return Mockito.mock(Connector.class);
                    });
            factory.when(() -> ConnectorFactory.createConnector(
                    Mockito.anyString(), Mockito.anyMap(), Mockito.any())).thenAnswer(invocation -> {
                        created.add(cache());
                        return Mockito.mock(Connector.class);
                    });
            first.createCatalogConnector("test", Collections.emptyMap());
            second.createCatalogConnector("test", Collections.emptyMap());
            // Not nested in root construction: the real Hive/Hudi/Iceberg sibling is created lazily too.
            first.createSiblingConnector("sibling", Collections.emptyMap());
            List<MetaCache<String, String>> entries = new ArrayList<>();
            for (CatalogMetaCache owner : created) {
                MetaCache<String, String> entry = owner.create(MetaCacheDefinition.<String, String>builder(
                        "entry", CacheSpec.of(true, -1L, 100L), key -> ScopePath.catalog()).build());
                entry.put("key", "value");
                entries.add(entry);
            }
            first.retireMetadataCaches();
            Assertions.assertThrows(IllegalStateException.class, () -> entries.get(0).getIfPresent("key"));
            Assertions.assertEquals("value", entries.get(1).getIfPresent("key"));
            Assertions.assertThrows(IllegalStateException.class, () -> entries.get(2).getIfPresent("key"));
            Assertions.assertThrows(IllegalStateException.class,
                    () -> first.createSiblingConnector("too-late", Collections.emptyMap()));
            Assertions.assertEquals(3, created.size());
        } finally {
            first.retireMetadataCaches();
            second.retireMetadataCaches();
        }
    }

    @Test
    void failedRootConstructionRetiresItsCacheRegistrationsBeforeRetry() {
        DefaultConnectorContext context = new DefaultConnectorContext("failed", 914L);
        IllegalStateException expected = new IllegalStateException("provider construction failed");
        try (MockedStatic<ConnectorFactory> factory = Mockito.mockStatic(ConnectorFactory.class)) {
            factory.when(() -> ConnectorFactory.createStandaloneCatalogConnector(
                    Mockito.anyString(), Mockito.anyMap(), Mockito.any())).thenAnswer(invocation -> {
                        CatalogMetaCache.managed(914L, "failed-context-test", Collections.emptyMap());
                        throw expected;
                    });
            IllegalStateException actual = Assertions.assertThrows(IllegalStateException.class,
                    () -> context.createCatalogConnector("test", Collections.emptyMap()));
            Assertions.assertSame(expected, actual);
            Assertions.assertTrue(MetaCacheGovernance.catalogCaches(914L).isEmpty());
        } finally {
            context.retireMetadataCaches();
        }
    }

    private static CatalogMetaCache cache() {
        return CatalogMetaCache.managed(913L, "context-test", Collections.emptyMap());
    }
}
