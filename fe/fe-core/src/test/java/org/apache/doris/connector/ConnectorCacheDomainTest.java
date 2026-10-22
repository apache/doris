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
import org.apache.doris.connector.cache.ScopePath;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorProvider;
import org.apache.doris.connector.spi.ConnectorSession;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class ConnectorCacheDomainTest {
    @AfterEach
    void reset() {
        ConnectorFactory.clearPluginManager();
    }

    private void providers() {
        ConnectorPluginManager manager = new ConnectorPluginManager();
        for (String type : new String[] {"primary", "sibling"}) {
            manager.registerProvider(new ConnectorProvider() {
                @Override
                public String getType() {
                    return type;
                }

                @Override
                public Connector create(Map<String, String> properties, ConnectorContext context) {
                    CachedConnector connector = new CachedConnector(context, type, properties);
                    if (properties.containsKey("fail")) {
                        throw new IllegalArgumentException("partial construction");
                    }
                    return connector;
                }
            });
        }
        ConnectorFactory.initPluginManager(manager);
    }

    @Test
    void factoryInstancesShareRootAndResourceInvalidationButOwnTheirLifetimes() throws Exception {
        providers();
        try (CatalogMetaCache root = CatalogMetaCache.unmanaged();
                DefaultConnectorContext context = new DefaultConnectorContext("c", 7L)) {
            context.bindCacheRoot(() -> root);
            CachedConnector first = (CachedConnector) ConnectorFactory.createConnector(
                    "primary", Collections.emptyMap(), context);
            CachedConnector child = (CachedConnector) first.context.createSiblingConnector(
                    "sibling", Collections.emptyMap());
            CachedConnector replacement = (CachedConnector) ConnectorFactory.createConnector(
                    "primary", Collections.emptyMap(), context);
            child.cache.put("t", "child");
            replacement.cache.put("t", "new");
            first.close();
            Assertions.assertTrue(child.domain.isClosed());
            Assertions.assertEquals("new", replacement.cache.getIfPresent("t"));
            Assertions.assertFalse(root.isClosed());
            Assertions.assertEquals(1, root.allEntries().size());
            Assertions.assertThrows(IllegalStateException.class, () -> first.context.createSiblingConnector(
                    "sibling", Collections.emptyMap()));
            root.invalidateScope(ScopePath.table("db", "t").in(ScopePath.Coordinates.REMOTE));
            Assertions.assertNull(replacement.cache.getIfPresent("t"));
        }
    }

    @Test
    void failedFactoryConstructionRollsBackOnlyItsInstanceDomain() throws Exception {
        providers();
        try (CatalogMetaCache root = CatalogMetaCache.unmanaged();
                DefaultConnectorContext context = new DefaultConnectorContext("c", 7L)) {
            context.bindCacheRoot(() -> root);
            CachedConnector live = (CachedConnector) ConnectorFactory.createConnector(
                    "primary", Collections.emptyMap(), context);
            live.cache.put("t", "live");
            Assertions.assertThrows(IllegalArgumentException.class, () -> ConnectorFactory.createConnector(
                    "primary", Collections.singletonMap("fail", "true"), context));
            Assertions.assertEquals("live", live.cache.getIfPresent("t"));
            Assertions.assertEquals(1, root.allEntries().size());
            Assertions.assertFalse(root.isClosed());
        }
    }

    @Test
    void contextRetirementDoesNotWaitForTheCatalogMonitorAndRejectsLateDomains() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try (CatalogMetaCache root = CatalogMetaCache.unmanaged();
                DefaultConnectorContext context = new DefaultConnectorContext("c", 7L)) {
            context.bindCacheRoot(() -> {
                entered.countDown();
                try {
                    Assertions.assertTrue(finish.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                return root;
            });
            Future<?> construction = workers.submit(() -> Assertions.assertThrows(IllegalStateException.class,
                    () -> context.createCacheDomain("primary", Collections.emptyMap())));
            Assertions.assertTrue(entered.await(10, TimeUnit.SECONDS));
            workers.submit(() -> {
                context.close();
                return null;
            }).get(10, TimeUnit.SECONDS);
            finish.countDown();
            construction.get(10, TimeUnit.SECONDS);
            Assertions.assertFalse(root.isClosed());
            Assertions.assertTrue(root.allEntries().isEmpty());
        } finally {
            finish.countDown();
            workers.shutdownNow();
        }
    }

    private static final class CachedConnector implements Connector {
        private final ConnectorContext context;
        private final CatalogMetaCache domain;
        private final MetaCache<String, String> cache;

        private CachedConnector(ConnectorContext context, String engine, Map<String, String> properties) {
            this.context = context;
            domain = context.createCacheDomain(engine, properties);
            cache = domain.create(MetaCacheDefinition.<String, String>builder("table", CacheSpec.of(true, -1, 10),
                    key -> ScopePath.table("db", key)).build());
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return null;
        }

        @Override
        public void close() {
            domain.close();
        }
    }
}
