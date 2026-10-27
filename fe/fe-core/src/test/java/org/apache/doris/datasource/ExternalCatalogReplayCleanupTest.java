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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.DefaultConnectorContext;
import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.ConnectorCacheOwner;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheCleanup;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.MetaCacheGovernance;
import org.apache.doris.connector.cache.MetaCacheSizeEstimate;
import org.apache.doris.connector.cache.ScopePath;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.datasource.log.CatalogLog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Keep one serial replay caller moving while the old connector's close remains blocked. */
public class ExternalCatalogReplayCleanupTest extends TestWithFeService {
    @Test
    void alterPublishesNewPropertiesAndProcessesNextLogBeforeOldClose() throws Exception {
        exerciseReplay("alter", 72801L);
    }

    @Test
    void dropAllowsSameNameReplacementBeforeOldClose() throws Exception {
        exerciseReplay("drop", 72802L);
    }

    @Test
    void renamePublishesNewNameAndRuntimeBeforeOldClose() throws Exception {
        exerciseReplay("rename", 72803L);
    }

    @Test
    void childResourceCleanupCompletesBeforeConnectorAndContextClose() throws Exception {
        long id = 72804L;
        CatalogMgr mgr = new CatalogMgr();
        Connector connector = Mockito.mock(Connector.class);
        DefaultConnectorContext context = Mockito.mock(DefaultConnectorContext.class);
        CountDownLatch childStarted = new CountDownLatch(1);
        CountDownLatch releaseChild = new CountDownLatch(1);
        CountDownLatch contextClosed = new CountDownLatch(1);
        HarnessCatalog catalog = new HarnessCatalog(id, "cleanup_dependency", connector, null, null);
        Deencapsulation.setField(catalog, "connectorContext", context);
        register(mgr, catalog);
        Mockito.doAnswer(invocation -> {
            contextClosed.countDown();
            return null;
        }).when(context).close();
        ConnectorCacheOwner cacheOwner = new ConnectorCacheOwner();
        catalog.cacheRetirement = cacheOwner::retire;
        CatalogMetaCache owner = cacheOwner.create(
                () -> CatalogMetaCache.managed(id, "cleanup-test", Collections.emptyMap()));
        MetaCache<String, String> entry = owner.create(MetaCacheDefinition.<String, String>builder("child",
                        CacheSpec.of(true, -1L, 100L), key -> ScopePath.catalog())
                .removalListener((key, value, reason) -> MetaCacheCleanup.reclaimResource("old FileIO", () -> {
                    childStarted.countDown();
                    try {
                        if (!releaseChild.await(10L, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("child cleanup was never released");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                })).build());
        entry.put("key", "old");
        ExecutorService replayer = Executors.newSingleThreadExecutor();
        try {
            CatalogLog log = new CatalogLog();
            log.setCatalogId(id);
            replayer.submit(() -> mgr.replayDropCatalog(log)).get(5L, TimeUnit.SECONDS);
            Assertions.assertTrue(childStarted.await(5L, TimeUnit.SECONDS));
            Mockito.verify(connector, Mockito.never()).close();
            Mockito.verify(context, Mockito.never()).close();
            releaseChild.countDown();
            Assertions.assertTrue(contextClosed.await(10L, TimeUnit.SECONDS));
            InOrder order = Mockito.inOrder(connector, context);
            order.verify(connector).close();
            order.verify(context).close();
        } finally {
            releaseChild.countDown();
            replayer.shutdownNow();
            Assertions.assertTrue(replayer.awaitTermination(10L, TimeUnit.SECONDS));
            owner.close();
        }
    }

    @Test
    void coldCatalogReplayDoesNotRetireAnotherEnvsMatchingCatalogId() throws Exception {
        long id = 72805L;
        ConnectorCacheOwner servingOwner = new ConnectorCacheOwner();
        try (CatalogMetaCache servingCache = servingOwner.create(
                () -> CatalogMetaCache.managed(id, "cleanup-test", Collections.emptyMap()))) {
            MetaCache<String, String> entry = servingCache.create(MetaCacheDefinition.<String, String>builder(
                    "serving", CacheSpec.of(true, -1L, 100L), key -> ScopePath.catalog()).build());
            entry.put("key", "serving value");
            CatalogMgr checkpointMgr = new CatalogMgr();
            HarnessCatalog checkpointCatalog = new HarnessCatalog(id, "checkpoint_copy", null, null, null);
            register(checkpointMgr, checkpointCatalog);
            CatalogLog log = new CatalogLog();
            log.setCatalogId(id);
            log.setNewProps(Collections.singletonMap("endpoint", "checkpoint-source"));
            checkpointMgr.replayAlterCatalogProps(log, Collections.emptyMap(), true);
            Assertions.assertEquals("serving value", entry.getIfPresent("key"));
            Assertions.assertTrue(MetaCacheGovernance.catalogCaches(id).contains(servingCache));
        } finally {
            servingOwner.retire();
        }
    }

    private void exerciseReplay(String operation, long id) throws Exception {
        CatalogMgr mgr = new CatalogMgr();
        CountDownLatch closeStarted = new CountDownLatch(1);
        CountDownLatch releaseClose = new CountDownLatch(1);
        CountDownLatch contextClosed = new CountDownLatch(1);
        Connector oldConnector = Mockito.mock(Connector.class);
        Connector newConnector = Mockito.mock(Connector.class);
        DefaultConnectorContext oldContext = Mockito.mock(DefaultConnectorContext.class);
        DefaultConnectorContext newContext = Mockito.mock(DefaultConnectorContext.class);
        Mockito.doAnswer(invocation -> {
            closeStarted.countDown();
            if (!releaseClose.await(10L, TimeUnit.SECONDS)) {
                throw new IllegalStateException("old close was never released");
            }
            return null;
        }).when(oldConnector).close();
        Mockito.doAnswer(invocation -> {
            contextClosed.countDown();
            return null;
        }).when(oldContext).close();
        HarnessCatalog catalog = new HarnessCatalog(id, "cleanup_" + operation, oldConnector,
                newConnector, newContext);
        Deencapsulation.setField(catalog, "connectorContext", oldContext);
        register(mgr, catalog);
        ExternalCatalog unrelatedCatalog = Mockito.mock(ExternalCatalog.class);
        mgr.getIdToCatalog().put(id + 1000L, unrelatedCatalog);
        CatalogLog log = new CatalogLog();
        log.setCatalogId(id);
        Map<String, String> changes = new HashMap<>();
        changes.put("endpoint", "new-source");
        changes.put("meta.cache.max-weight", "2KB");
        log.setNewProps(changes);
        log.setNewCatalogName("renamed_" + operation);
        catalog.tryModifyCatalogProps(Collections.singletonMap("meta.cache.max-weight", "4KB"));
        ConnectorCacheOwner cacheOwner = new ConnectorCacheOwner();
        catalog.cacheRetirement = cacheOwner::retire;
        CatalogMetaCache oldCache = cacheOwner.create(
                () -> CatalogMetaCache.managed(id, "cleanup-test", catalog.getProperties()));
        createWeightedEntry(oldCache).put("old", "metadata");
        CatalogMetaCache replacementCache = null;
        ExecutorService replayer = Executors.newSingleThreadExecutor();
        try {
            Future<?> replay = replayer.submit(() -> {
                if ("alter".equals(operation)) {
                    mgr.replayAlterCatalogProps(log, Collections.emptyMap(), true);
                } else if ("drop".equals(operation)) {
                    mgr.replayDropCatalog(log);
                } else {
                    mgr.replayAlterCatalogName(log);
                }
                // This is the same serial caller, rather than an unrelated thread acquiring the catalog lock.
                CatalogLog nextLog = new CatalogLog();
                nextLog.setCatalogId(id + 1000L);
                nextLog.setComment("next journal applied");
                mgr.replayAlterCatalogComment(nextLog);
                return null;
            });
            replay.get(5L, TimeUnit.SECONDS);
            Assertions.assertTrue(closeStarted.await(5L, TimeUnit.SECONDS));
            Assertions.assertEquals(1L, releaseClose.getCount());
            Mockito.verify(unrelatedCatalog).setComment("next journal applied");
            Mockito.verify(oldContext, Mockito.never()).close();
            if ("drop".equals(operation)) {
                Assertions.assertNull(mgr.getCatalog(id));
                Assertions.assertNull(mgr.getCatalog(catalog.getName()));
                Assertions.assertThrows(IllegalStateException.class, catalog::makeSureInitialized);
                HarnessCatalog replacement = new HarnessCatalog(id + 100L, catalog.getName(), null,
                        newConnector, newContext);
                register(mgr, replacement);
                Assertions.assertSame(newConnector, replacement.getConnector());
            } else {
                Assertions.assertSame(catalog, mgr.getCatalog(id));
                if ("alter".equals(operation)) {
                    Assertions.assertEquals("new-source", catalog.getProperties().get("endpoint"));
                    Assertions.assertFalse(MetaCacheGovernance.catalogCaches(id).contains(oldCache));
                    replacementCache = CatalogMetaCache.managed(id, "cleanup-test", catalog.getProperties());
                    createWeightedEntry(replacementCache).put("new", "metadata");
                    Assertions.assertEquals(2048L, replacementCache.catalogMaxWeight().getAsLong());
                } else {
                    Assertions.assertNull(mgr.getCatalog("cleanup_" + operation));
                    Assertions.assertSame(catalog, mgr.getCatalog("renamed_" + operation));
                    Assertions.assertEquals(id, catalog.getId());
                }
                Assertions.assertSame(newConnector, catalog.getConnector());
            }
            releaseClose.countDown();
            Assertions.assertTrue(contextClosed.await(10L, TimeUnit.SECONDS));
            Mockito.verify(newConnector, Mockito.never()).close();
            Mockito.verify(newContext, Mockito.never()).close();
            Mockito.verify(oldConnector, Mockito.times(1)).close();
            if ("drop".equals(operation)) {
                Assertions.assertEquals(id + 100L, mgr.getCatalog(catalog.getName()).getId());
            } else {
                Assertions.assertSame(catalog, mgr.getCatalog(catalog.getName()));
            }
        } finally {
            releaseClose.countDown();
            replayer.shutdownNow();
            Assertions.assertTrue(replayer.awaitTermination(10L, TimeUnit.SECONDS));
            Env.getCurrentEnv().getExtMetaCacheMgr().removeCatalog(id);
            oldCache.close();
            if (replacementCache != null) {
                replacementCache.close();
            }
        }
    }

    private static MetaCache<String, String> createWeightedEntry(CatalogMetaCache owner) {
        return owner.create(MetaCacheDefinition.<String, String>builder("entry",
                        CacheSpec.of(true, -1L, 100L), key -> ScopePath.catalog())
                .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(512L)).build());
    }

    private static void register(CatalogMgr mgr, HarnessCatalog catalog) {
        mgr.getIdToCatalog().put(catalog.getId(), catalog);
        Map<String, CatalogIf> names = Deencapsulation.getField(mgr, "nameToCatalog");
        names.put(catalog.getName(), catalog);
    }

    private static final class HarnessCatalog extends PluginDrivenExternalCatalog {
        private final Connector replacement;
        private final DefaultConnectorContext replacementContext;
        private Runnable cacheRetirement = () -> { };

        private HarnessCatalog(long id, String name, Connector initial, Connector replacement,
                DefaultConnectorContext replacementContext) {
            super(id, name, null, new HashMap<>(), "", initial);
            this.replacement = replacement;
            this.replacementContext = replacementContext;
        }

        @Override
        protected Runnable captureConnectorCacheRetirement() {
            return cacheRetirement;
        }

        @Override
        protected Connector createConnectorFromProperties() {
            Deencapsulation.setField(this, "connectorContext", replacementContext);
            return replacement;
        }
    }
}
