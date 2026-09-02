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

import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InitCatalogLog.Type;
import org.apache.doris.datasource.metacache.MetaCache;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ExternalCatalogDeadlockTest {

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
            queryThread.join(TimeUnit.SECONDS.toMillis(5));
            resetThread.join(TimeUnit.SECONDS.toMillis(5));
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
            resetThread.join(TimeUnit.SECONDS.toMillis(5));
            queryThread.join(TimeUnit.SECONDS.toMillis(5));
            Assertions.assertNull(backgroundFailure.get());
            Assertions.assertTrue(queryCompleted.get());
        } finally {
            releaseReset.countDown();
        }
    }

    private static void assertNoDeadlock(Thread queryThread, Thread refreshThread,
            AtomicReference<Throwable> backgroundFailure) throws Exception {
        long[] deadlockedThreads = waitForDeadlock(queryThread, refreshThread);
        queryThread.join(TimeUnit.SECONDS.toMillis(5));
        refreshThread.join(TimeUnit.SECONDS.toMillis(5));
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
