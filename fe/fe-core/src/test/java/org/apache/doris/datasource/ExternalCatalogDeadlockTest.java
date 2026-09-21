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
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InitCatalogLog.Type;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalDatabase;
import org.apache.doris.datasource.hive.event.MetastoreEvent;
import org.apache.doris.datasource.hive.event.MetastoreEventFactory;
import org.apache.doris.datasource.metacache.MetaCache;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ExternalCatalogDeadlockTest {
    // The ThreadMXBean scan is the deadlock oracle; this window only guards thread liveness, so
    // it is intentionally generous for slow FE UT workers.
    private static final long LIVENESS_JOIN_TIMEOUT_SECONDS = 20;

    @BeforeAll
    public static void initializeEnvBeforeDeadlockChecks() {
        // Keep global Env cold-start work outside the bounded thread joins used to detect lock cycles.
        Env.getCurrentEnv();
    }

    @Test
    public void testCatalogEventUpdateShouldNotDeadlockWithSameKeyObjectLoad() throws Exception {
        DeadlockCatalog catalog = new DeadlockCatalog();
        assertEventUpdateDoesNotDeadlockWithSameKeyObjectLoad(
                "catalog-event-cache", new DeadlockDatabase(catalog));
    }

    @Test
    public void testTableEventUpdateShouldNotDeadlockWithSameKeyObjectLoad() throws Exception {
        assertEventUpdateDoesNotDeadlockWithSameKeyObjectLoad(
                "table-event-cache", Mockito.mock(ExternalTable.class));
    }

    @Test
    public void testExcludedDatabaseEventDoesNotPublishIntoWarmCaseInsensitiveCache() throws Exception {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(ExternalCatalog.INCLUDE_DATABASE_LIST, "AllowedDb");
        properties.put(ExternalCatalog.EXCLUDE_DATABASE_LIST, "ExcludedDb");
        properties.put(ExternalCatalog.LOWER_CASE_DATABASE_NAMES, "2");
        FilteredEventCatalog catalog = new FilteredEventCatalog(properties);
        Map<String, String> lowerCaseRoutes = Maps.newConcurrentMap();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        MetaCache<ExternalDatabase<? extends ExternalTable>> cache = new MetaCache<>(
                "filtered-event-cache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> Lists.newArrayList(Pair.of("AllowedDb", "AllowedDb")),
                names -> {
                    lowerCaseRoutes.clear();
                    names.forEach(pair -> lowerCaseRoutes.put(pair.key().toLowerCase(), pair.key()));
                },
                (remoteName, localName) -> lowerCaseRoutes.put(remoteName.toLowerCase(), remoteName),
                localName -> lowerCaseRoutes.remove(localName.toLowerCase()),
                key -> Optional.empty(),
                (key, value, cause) -> { });
        catalog.setMetaCache(cache);
        catalog.setLowerCaseRoutes(lowerCaseRoutes);

        try {
            Assertions.assertEquals(Lists.newArrayList("AllowedDb"), cache.listNames());
            NotificationEvent excludedNotification = new NotificationEvent(
                    1L, 1, "CREATE_DATABASE", "");
            excludedNotification.setDbName("ExcludedDb");
            MetastoreEvent excludedEvent = new MetastoreEventFactory()
                    .transferNotificationEventToMetastoreEvents(excludedNotification, catalog.getName()).get(0);
            Assertions.assertEquals("excludeddb", excludedEvent.getDbName());
            Assertions.assertTrue(catalog.registerDatabaseFromEvent(2L, excludedEvent.getDbName(), 0L));
            Assertions.assertFalse(cache.tryGetMetaObj("ExcludedDb").isPresent());
            Assertions.assertFalse(lowerCaseRoutes.containsKey("excludeddb"));
            Assertions.assertNull(catalog.getDbNullable("eXcLuDeDdB"));
            Assertions.assertEquals(0, catalog.getBuildCount());

            NotificationEvent allowedNotification = new NotificationEvent(
                    2L, 1, "CREATE_DATABASE", "");
            allowedNotification.setDbName("AllowedDb");
            MetastoreEvent allowedEvent = new MetastoreEventFactory()
                    .transferNotificationEventToMetastoreEvents(allowedNotification, catalog.getName()).get(0);
            Assertions.assertEquals("alloweddb", allowedEvent.getDbName());
            Assertions.assertTrue(catalog.registerDatabaseFromEvent(3L, allowedEvent.getDbName(), 0L));
            Assertions.assertTrue(cache.tryGetMetaObj("alloweddb").isPresent());
            Assertions.assertEquals("alloweddb", lowerCaseRoutes.get("alloweddb"));
            Assertions.assertNotNull(catalog.getDbNullable("aLlOwEdDb"));
            Assertions.assertEquals(1, catalog.getBuildCount());
        } finally {
            refreshExecutor.shutdownNow();
            Assertions.assertTrue(refreshExecutor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private <T> void assertEventUpdateDoesNotDeadlockWithSameKeyObjectLoad(String cacheName, T eventObject)
            throws Exception {
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch allowLoaderToListNames = new CountDownLatch(1);
        AtomicReference<MetaCache<T>> cacheReference = new AtomicReference<>();
        AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        MetaCache<T> cache = new MetaCache<>(
                cacheName,
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> Lists.newArrayList(Pair.of("remote-name", "local-name")),
                key -> {
                    loaderEntered.countDown();
                    awaitLatch(allowLoaderToListNames);
                    cacheReference.get().listNames();
                    return Optional.empty();
                },
                (key, value, cause) -> { });
        cacheReference.set(cache);

        Thread queryThread = new Thread(
                () -> runQuietly(backgroundFailure, () -> cache.getMetaObj("local-name", 1)),
                cacheName + "-loader");
        Thread eventThread = new Thread(
                () -> runQuietly(backgroundFailure,
                        () -> cache.updateCache("remote-name", "local-name", eventObject, 1)),
                cacheName + "-event");
        queryThread.setDaemon(true);
        eventThread.setDaemon(true);

        try {
            queryThread.start();
            Assertions.assertTrue(loaderEntered.await(5, TimeUnit.SECONDS));
            eventThread.start();
            Assertions.assertTrue(waitForBlocked(eventThread));
            allowLoaderToListNames.countDown();
            assertNoDeadlock(queryThread, eventThread, backgroundFailure);
            Assertions.assertSame(eventObject, cache.tryGetMetaObj("local-name").orElse(null));
        } finally {
            allowLoaderToListNames.countDown();
            refreshExecutor.shutdownNow();
            Assertions.assertTrue(refreshExecutor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testGsonPostProcessRestoresMetadataLoadEpoch() throws Exception {
        DeadlockCatalog catalog = new DeadlockCatalog();
        Field epochField = ExternalCatalog.class.getDeclaredField("metadataLoadEpoch");
        epochField.setAccessible(true);
        epochField.set(catalog, null);
        catalog.prepareForGsonPostProcess();

        catalog.gsonPostProcess();
        Assertions.assertTrue(catalog.isMetadataLoadEpochCurrent(0));
        catalog.resetToUninitialized(false);
        Assertions.assertTrue(catalog.isMetadataLoadEpochCurrent(1));
    }

    @Test
    public void testResetToUninitializedShouldNotDeadlockWithCacheLoader() throws Exception {
        DeadlockCatalog catalog = new DeadlockCatalog();
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch allowLoaderToTouchCatalog = new CountDownLatch(1);
        AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        MetaCache<ExternalDatabase<? extends ExternalTable>> cache = new MetaCache<>(
                "deadlock-cache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> Lists.newArrayList(),
                key -> {
                    loaderEntered.countDown();
                    awaitLatch(allowLoaderToTouchCatalog);
                    catalog.makeSureInitialized();
                    return Optional.empty();
                },
                (key, value, cause) -> { });
        catalog.setMetaCache(cache);
        catalog.setLoaderRelease(allowLoaderToTouchCatalog);

        Thread queryThread = new Thread(
                () -> runQuietly(backgroundFailure, () -> cache.getMetaObj("deadlock-key", 1)),
                "deadlock-cache-loader");
        queryThread.setDaemon(true);
        queryThread.start();
        Assertions.assertTrue(loaderEntered.await(5, TimeUnit.SECONDS));

        Thread refreshThread = new Thread(
                () -> runQuietly(backgroundFailure, () -> catalog.resetToUninitialized(false)),
                "deadlock-catalog-refresh");
        refreshThread.setDaemon(true);
        refreshThread.start();

        try {
            assertNoDeadlock(queryThread, refreshThread, backgroundFailure);
        } finally {
            allowLoaderToTouchCatalog.countDown();
            refreshExecutor.shutdownNow();
            Assertions.assertTrue(refreshExecutor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testEventAdmissionRejectsDatabaseEvictedAfterLookup() throws Exception {
        DeadlockCatalog catalog = new DeadlockCatalog();
        DeadlockDatabase evictedDatabase = new DeadlockDatabase(catalog);
        DeadlockDatabase replacementDatabase = new DeadlockDatabase(catalog);
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        MetaCache<ExternalDatabase<? extends ExternalTable>> cache = new MetaCache<>(
                "database-cache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> Lists.newArrayList(),
                key -> Optional.empty(),
                (key, value, cause) -> { });
        catalog.setMetaCache(cache);
        cache.updateCache("deadlock-db", "deadlock-db", evictedDatabase, 3L);
        ExternalDatabase<? extends ExternalTable> databaseFromEventLookup =
                cache.tryGetMetaObj("deadlock-db").orElseThrow(AssertionError::new);
        cache.invalidate("deadlock-db", 3L);
        cache.updateCache("deadlock-db", "deadlock-db", replacementDatabase, 3L);
        AtomicBoolean eventPublished = new AtomicBoolean();

        try {
            Assertions.assertFalse(catalog.executeIfDatabaseCurrent(
                    databaseFromEventLookup, () -> {
                        eventPublished.set(true);
                        return true;
                    }));
            Assertions.assertFalse(eventPublished.get());
            Assertions.assertSame(replacementDatabase,
                    cache.tryGetMetaObj("deadlock-db").orElse(null));
        } finally {
            refreshExecutor.shutdownNow();
            Assertions.assertTrue(refreshExecutor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testDatabaseResetShouldNotDeadlockWithTableCacheLoader() throws Exception {
        DeadlockCatalog catalog = new DeadlockCatalog();
        DeadlockDatabase database = new DeadlockDatabase(catalog);
        CountDownLatch loaderEntered = new CountDownLatch(1);
        CountDownLatch allowLoaderToTouchDatabase = new CountDownLatch(1);
        AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        MetaCache<ExternalTable> cache = new MetaCache<>(
                "deadlock-table-cache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> Lists.newArrayList(),
                ignored -> allowLoaderToTouchDatabase.countDown(),
                (remoteName, localName) -> { },
                ignored -> { },
                key -> {
                    loaderEntered.countDown();
                    awaitLatch(allowLoaderToTouchDatabase);
                    database.makeSureInitialized();
                    return Optional.empty();
                },
                (key, value, cause) -> { });
        database.setMetaCache(cache);

        Thread queryThread = new Thread(
                () -> runQuietly(backgroundFailure, () -> cache.getMetaObj("deadlock-table", 1)),
                "deadlock-table-cache-loader");
        Thread resetThread = new Thread(
                () -> runQuietly(backgroundFailure, database::resetMetaToUninitialized),
                "deadlock-database-reset");
        queryThread.setDaemon(true);
        resetThread.setDaemon(true);

        queryThread.start();
        Assertions.assertTrue(loaderEntered.await(5, TimeUnit.SECONDS));
        resetThread.start();

        try {
            assertNoDeadlock(queryThread, resetThread, backgroundFailure);
        } finally {
            allowLoaderToTouchDatabase.countDown();
            refreshExecutor.shutdownNow();
            Assertions.assertTrue(refreshExecutor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testNamesLoadRechecksLifecycleAfterInitializationGap() throws Exception {
        CountDownLatch queryPassedInitialization = new CountDownLatch(1);
        CountDownLatch resumeQuery = new CountDownLatch(1);
        CountDownLatch resetInsideCatalogMonitor = new CountDownLatch(1);
        CountDownLatch releaseReset = new CountDownLatch(1);
        CountDownLatch namesLoaderEntered = new CountDownLatch(1);
        AtomicReference<List<String>> names = new AtomicReference<>();
        AtomicInteger initializedClientVersion = new AtomicInteger();
        AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        LifecycleCatalog catalog = new LifecycleCatalog(resetInsideCatalogMonitor, releaseReset);
        MetaCache<ExternalDatabase<? extends ExternalTable>> cache = new MetaCache<>(
                "lifecycle-cache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> {
                    namesLoaderEntered.countDown();
                    int clientVersion = catalog.getClientVersion();
                    return Lists.newArrayList(Pair.of("remote-" + clientVersion, "local-" + clientVersion));
                },
                ignored -> { },
                (remoteName, localName) -> { },
                ignored -> { },
                key -> Optional.empty(),
                (key, value, cause) -> { },
                catalog::acquireMetadataLoadEpoch,
                catalog::isMetadataLoadEpochCurrent);
        catalog.setMetaCache(cache);

        Thread queryThread = new Thread(
                () -> runQuietly(backgroundFailure, () -> {
                    catalog.makeSureInitialized();
                    initializedClientVersion.set(catalog.getClientVersion());
                    queryPassedInitialization.countDown();
                    awaitLatch(resumeQuery);
                    names.set(cache.listNames());
                }),
                "catalog-names-query");
        Thread resetThread = new Thread(
                () -> runQuietly(backgroundFailure, () -> catalog.resetToUninitialized(false)),
                "catalog-reset");

        try {
            queryThread.start();
            Assertions.assertTrue(queryPassedInitialization.await(5, TimeUnit.SECONDS));
            resetThread.start();
            Assertions.assertTrue(resetInsideCatalogMonitor.await(5, TimeUnit.SECONDS));
            resumeQuery.countDown();
            Assertions.assertFalse(namesLoaderEntered.await(200, TimeUnit.MILLISECONDS));
            releaseReset.countDown();
            queryThread.join(TimeUnit.SECONDS.toMillis(LIVENESS_JOIN_TIMEOUT_SECONDS));
            resetThread.join(TimeUnit.SECONDS.toMillis(LIVENESS_JOIN_TIMEOUT_SECONDS));
            Assertions.assertNull(backgroundFailure.get());
            Assertions.assertEquals(
                    Lists.newArrayList("local-" + (initializedClientVersion.get() + 1)), names.get());
        } finally {
            resumeQuery.countDown();
            releaseReset.countDown();
            refreshExecutor.shutdownNow();
            Assertions.assertTrue(refreshExecutor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testResetKeepsInitializationFenceUntilInternalRefreshCompletes() throws Exception {
        CountDownLatch resetInsideCatalogMonitor = new CountDownLatch(1);
        CountDownLatch releaseReset = new CountDownLatch(1);
        CountDownLatch queryStarted = new CountDownLatch(1);
        AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();
        AtomicReference<Boolean> queryCompleted = new AtomicReference<>(false);
        ResetFenceCatalog catalog = new ResetFenceCatalog(resetInsideCatalogMonitor, releaseReset);

        Thread resetThread = new Thread(
                () -> runQuietly(backgroundFailure, () -> catalog.resetToUninitialized(false)),
                "catalog-reset");
        Thread queryThread = new Thread(
                () -> runQuietly(backgroundFailure, () -> {
                    queryStarted.countDown();
                    catalog.makeSureInitialized();
                    queryCompleted.set(true);
                }),
                "catalog-query");
        resetThread.setDaemon(true);
        queryThread.setDaemon(true);

        try {
            resetThread.start();
            Assertions.assertTrue(resetInsideCatalogMonitor.await(5, TimeUnit.SECONDS));
            queryThread.start();
            Assertions.assertTrue(queryStarted.await(5, TimeUnit.SECONDS));
            Assertions.assertTrue(waitForBlocked(queryThread));
            Assertions.assertFalse(queryCompleted.get());
            releaseReset.countDown();
            resetThread.join(TimeUnit.SECONDS.toMillis(LIVENESS_JOIN_TIMEOUT_SECONDS));
            queryThread.join(TimeUnit.SECONDS.toMillis(LIVENESS_JOIN_TIMEOUT_SECONDS));
            Assertions.assertNull(backgroundFailure.get());
            Assertions.assertTrue(queryCompleted.get());
        } finally {
            releaseReset.countDown();
        }
    }

    @Test
    public void testCatalogResetRetiresObjectGenerationInsideInitializationFence() throws Exception {
        DeadlockCatalog catalog = new DeadlockCatalog();
        DeadlockDatabase replacement = new DeadlockDatabase(catalog);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        MetaCache<ExternalDatabase<? extends ExternalTable>> cache = new MetaCache<>(
                "catalog-cache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> Lists.newArrayList(),
                key -> Optional.empty(),
                (key, value, cause) -> { });
        catalog.setMetaCache(cache);
        cache.updateCache("db", "db", new DeadlockDatabase(catalog), 3L);
        Lock lifecycleReadLock = getLifecycleReadLock(cache);
        CountDownLatch queryStarted = new CountDownLatch(1);
        CountDownLatch queryCompleted = new CountDownLatch(1);
        AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();
        AtomicReference<Thread> resetThread = new AtomicReference<>();

        lifecycleReadLock.lock();
        boolean lifecycleReadLocked = true;
        Future<?> reset = executor.submit(() -> {
            resetThread.set(Thread.currentThread());
            runQuietly(backgroundFailure, () -> catalog.resetToUninitialized(false));
        });
        Future<?> query = null;
        try {
            Assertions.assertTrue(waitForStackFrame(resetThread, "retireObjects"));
            query = executor.submit(() -> runQuietly(backgroundFailure, () -> {
                queryStarted.countDown();
                catalog.makeSureInitialized();
                cache.updateCache("db", "db", replacement, 3L);
                queryCompleted.countDown();
            }));
            Assertions.assertTrue(queryStarted.await(3, TimeUnit.SECONDS));
            Assertions.assertFalse(queryCompleted.await(200, TimeUnit.MILLISECONDS));

            lifecycleReadLock.unlock();
            lifecycleReadLocked = false;
            reset.get(3, TimeUnit.SECONDS);
            query.get(3, TimeUnit.SECONDS);
            Assertions.assertNull(backgroundFailure.get());
            Assertions.assertSame(replacement, cache.tryGetMetaObj("db").orElse(null));
        } finally {
            if (lifecycleReadLocked) {
                lifecycleReadLock.unlock();
            }
            if (query != null) {
                query.cancel(true);
            }
            reset.cancel(true);
            executor.shutdownNow();
            refreshExecutor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
            Assertions.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testDatabaseResetRetiresObjectGenerationInsideInitializationFence() throws Exception {
        DeadlockCatalog catalog = new DeadlockCatalog();
        DeadlockDatabase database = new DeadlockDatabase(catalog);
        ExternalTable replacement = Mockito.mock(ExternalTable.class);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        ExecutorService refreshExecutor = Executors.newSingleThreadExecutor();
        MetaCache<ExternalTable> cache = new MetaCache<>(
                "table-cache",
                refreshExecutor,
                OptionalLong.empty(),
                OptionalLong.empty(),
                10,
                key -> Lists.newArrayList(),
                key -> Optional.empty(),
                (key, value, cause) -> { });
        database.setMetaCache(cache);
        cache.updateCache("table", "table", Mockito.mock(ExternalTable.class), 4L);
        Lock lifecycleReadLock = getLifecycleReadLock(cache);
        CountDownLatch queryStarted = new CountDownLatch(1);
        CountDownLatch queryCompleted = new CountDownLatch(1);
        AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();
        AtomicReference<Thread> resetThread = new AtomicReference<>();

        lifecycleReadLock.lock();
        boolean lifecycleReadLocked = true;
        Future<?> reset = executor.submit(() -> {
            resetThread.set(Thread.currentThread());
            runQuietly(backgroundFailure, database::resetMetaToUninitialized);
        });
        Future<?> query = null;
        try {
            Assertions.assertTrue(waitForStackFrame(resetThread, "retireObjects"));
            query = executor.submit(() -> runQuietly(backgroundFailure, () -> {
                queryStarted.countDown();
                database.makeSureInitialized();
                cache.updateCache("table", "table", replacement, 4L);
                queryCompleted.countDown();
            }));
            Assertions.assertTrue(queryStarted.await(3, TimeUnit.SECONDS));
            Assertions.assertFalse(queryCompleted.await(200, TimeUnit.MILLISECONDS));

            lifecycleReadLock.unlock();
            lifecycleReadLocked = false;
            reset.get(3, TimeUnit.SECONDS);
            query.get(3, TimeUnit.SECONDS);
            Assertions.assertNull(backgroundFailure.get());
            Assertions.assertSame(replacement, cache.tryGetMetaObj("table").orElse(null));
        } finally {
            if (lifecycleReadLocked) {
                lifecycleReadLock.unlock();
            }
            if (query != null) {
                query.cancel(true);
            }
            reset.cancel(true);
            executor.shutdownNow();
            refreshExecutor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(3, TimeUnit.SECONDS));
            Assertions.assertTrue(refreshExecutor.awaitTermination(3, TimeUnit.SECONDS));
        }
    }

    private static Lock getLifecycleReadLock(MetaCache<?> cache) throws Exception {
        Field lifecycleLockField = MetaCache.class.getDeclaredField("metaObjLifecycleLock");
        lifecycleLockField.setAccessible(true);
        return ((ReentrantReadWriteLock) lifecycleLockField.get(cache)).readLock();
    }

    private static boolean waitForStackFrame(AtomicReference<Thread> threadReference, String methodName)
            throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            Thread thread = threadReference.get();
            if (thread != null && Arrays.stream(thread.getStackTrace())
                    .anyMatch(frame -> frame.getMethodName().equals(methodName))) {
                return true;
            }
            Thread.sleep(10);
        }
        return false;
    }

    private static void assertNoDeadlock(Thread queryThread, Thread refreshThread,
            AtomicReference<Throwable> backgroundFailure) throws Exception {
        long[] deadlockedThreads = waitForDeadlock(queryThread, refreshThread);
        queryThread.join(TimeUnit.SECONDS.toMillis(LIVENESS_JOIN_TIMEOUT_SECONDS));
        refreshThread.join(TimeUnit.SECONDS.toMillis(LIVENESS_JOIN_TIMEOUT_SECONDS));
        Assertions.assertNull(backgroundFailure.get(), "unexpected background failure: " + backgroundFailure.get());
        Assertions.assertNull(deadlockedThreads,
                String.format("detected deadlock between threads %s and %s",
                        queryThread.getName(), refreshThread.getName()));
        Assertions.assertFalse(queryThread.isAlive(), queryThread.getName() + " is still running");
        Assertions.assertFalse(refreshThread.isAlive(), refreshThread.getName() + " is still running");
    }

    private static void awaitLatch(CountDownLatch latch) throws InterruptedException {
        Assertions.assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    private static void runQuietly(AtomicReference<Throwable> failure, ThrowingRunnable task) {
        try {
            task.run();
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
        }
    }

    private static long[] waitForDeadlock(Thread queryThread, Thread refreshThread) throws InterruptedException {
        ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
        for (int i = 0; i < 100; i++) {
            long[] deadlockedThreads = threadMxBean.findDeadlockedThreads();
            if (deadlockedThreads != null
                    && contains(deadlockedThreads, queryThread.getId())
                    && contains(deadlockedThreads, refreshThread.getId())) {
                return deadlockedThreads;
            }
            Thread.sleep(50);
        }
        return null;
    }

    private static boolean waitForBlocked(Thread thread) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            if (thread.getState() == Thread.State.BLOCKED) {
                return true;
            }
            Thread.sleep(10);
        }
        return false;
    }

    private static boolean contains(long[] ids, long targetId) {
        return Arrays.stream(ids).anyMatch(id -> id == targetId);
    }

    private static class DeadlockCatalog extends ExternalCatalog {
        private CountDownLatch loaderRelease;

        DeadlockCatalog() {
            super(1L, "deadlock-catalog", Type.TEST, "");
            initialized = true;
        }

        void setMetaCache(MetaCache<ExternalDatabase<? extends ExternalTable>> cache) {
            this.metaCache = cache;
        }

        void setLoaderRelease(CountDownLatch loaderRelease) {
            this.loaderRelease = loaderRelease;
        }

        void prepareForGsonPostProcess() {
            catalogProperty = new CatalogProperty(null, null);
        }

        @Override
        protected void initLocalObjectsImpl() {
        }

        @Override
        public void onClose() {
            if (loaderRelease != null) {
                loaderRelease.countDown();
            }
        }

        @Override
        protected java.util.List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
            return java.util.Collections.emptyList();
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
            return false;
        }
    }

    private static class FilteredEventCatalog extends HMSExternalCatalog {
        private final AtomicInteger buildCount = new AtomicInteger();

        FilteredEventCatalog(Map<String, String> properties) {
            super(1L, "filtered-event-catalog", null, properties, "");
            setInitializedForTest(true);
        }

        void setMetaCache(MetaCache<ExternalDatabase<? extends ExternalTable>> cache) {
            metaCache = cache;
        }

        void setLowerCaseRoutes(Map<String, String> routes) throws Exception {
            Field routesField = ExternalCatalog.class.getDeclaredField("lowerCaseToDatabaseName");
            routesField.setAccessible(true);
            routesField.set(this, routes);
        }

        int getBuildCount() {
            return buildCount.get();
        }

        @Override
        protected void initLocalObjectsImpl() {
        }

        @Override
        protected ExternalDatabase<? extends ExternalTable> buildDbForInit(String remoteDbName, String localDbName,
                long dbId, InitCatalogLog.Type logType, boolean checkExists) {
            buildCount.incrementAndGet();
            return new HMSExternalDatabase(this, dbId, remoteDbName, remoteDbName);
        }
    }

    private static class LifecycleCatalog extends DeadlockCatalog {
        private final CountDownLatch resetInsideCatalogMonitor;
        private final CountDownLatch releaseReset;
        private final AtomicInteger clientVersion = new AtomicInteger(1);

        LifecycleCatalog(CountDownLatch resetInsideCatalogMonitor, CountDownLatch releaseReset) {
            this.resetInsideCatalogMonitor = resetInsideCatalogMonitor;
            this.releaseReset = releaseReset;
        }

        int getClientVersion() {
            return clientVersion.get();
        }

        @Override
        protected void initLocalObjectsImpl() {
            clientVersion.incrementAndGet();
        }

        @Override
        public void onClose() {
            resetInsideCatalogMonitor.countDown();
            try {
                awaitLatch(releaseReset);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    private static class DeadlockDatabase extends ExternalDatabase<ExternalTable> {
        DeadlockDatabase(ExternalCatalog catalog) {
            super(catalog, 3L, "deadlock-db", "deadlock-db", InitDatabaseLog.Type.TEST);
            initialized = true;
        }

        void setMetaCache(MetaCache<ExternalTable> cache) {
            try {
                Field metaCacheField = ExternalDatabase.class.getDeclaredField("metaCache");
                metaCacheField.setAccessible(true);
                metaCacheField.set(this, cache);
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public ExternalTable buildTableInternal(String remoteTableName, String localTableName, long tableId,
                ExternalCatalog catalog, ExternalDatabase database) {
            return null;
        }
    }

    private static class ResetFenceCatalog extends ExternalCatalog {
        private final CountDownLatch resetInsideCatalogMonitor;
        private final CountDownLatch releaseReset;

        ResetFenceCatalog(CountDownLatch resetInsideCatalogMonitor, CountDownLatch releaseReset) {
            super(2L, "reset-fence-catalog", Type.TEST, "");
            this.resetInsideCatalogMonitor = resetInsideCatalogMonitor;
            this.releaseReset = releaseReset;
            initialized = true;
        }

        @Override
        protected void initLocalObjectsImpl() {
        }

        @Override
        public void onClose() {
            resetInsideCatalogMonitor.countDown();
            try {
                awaitLatch(releaseReset);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        @Override
        protected java.util.List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
            return java.util.Collections.emptyList();
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
            return false;
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
