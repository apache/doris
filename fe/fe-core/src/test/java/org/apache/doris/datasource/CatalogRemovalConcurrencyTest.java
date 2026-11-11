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

import org.apache.doris.authorization.spi.AuthorizationPlugin;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.datasource.log.CatalogLog;
import org.apache.doris.datasource.log.InitCatalogLog;
import org.apache.doris.mysql.privilege.AccessControllerFactory;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.CatalogAccessController;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class CatalogRemovalConcurrencyTest {
    @ParameterizedTest(name = "rename={0}, replay={1}")
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void removalUnpublishesBeforeControllerDetachment(boolean rename, boolean replay) throws Exception {
        CatalogMgr mgr = new CatalogMgr();
        Env env = mockEnv(mgr);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch retirementStarted = new CountDownLatch(1);
        CountDownLatch allowRetirement = new CountDownLatch(1);
        CountDownLatch oldCloseStarted = new CountDownLatch(1);
        CountDownLatch allowOldClose = new CountDownLatch(1);
        CountDownLatch oldClosed = new CountDownLatch(1);
        LatchingCatalog catalog = new LatchingCatalog(73001L, "retiring");
        catalog.retireCaches = () -> {
            retirementStarted.countDown();
            await(allowRetirement);
        };
        register(mgr, catalog);
        try {
            inEnv(env, () -> {
                AccessControllerManager access = new AccessControllerManager(new Auth());
                Mockito.when(env.getAccessManager()).thenReturn(access);
                CatalogAccessController oldController = Mockito.mock(CatalogAccessController.class);
                CatalogAccessController replacementController = Mockito.mock(CatalogAccessController.class);
                AccessControllerFactory factory = Mockito.mock(AccessControllerFactory.class);
                ConcurrentHashMap<String, AccessControllerFactory> factories =
                        Deencapsulation.getField(access, "accessControllerFactoriesCache");
                factories.put("removal-test", factory);
                Mockito.when(factory.createAccessController(Collections.emptyMap()))
                        .thenReturn(oldController, replacementController);
                Mockito.doAnswer(invocation -> {
                    oldCloseStarted.countDown();
                    await(allowOldClose);
                    oldClosed.countDown();
                    return null;
                }).when(oldController).close();
                AuthorizationPlugin defaultController = access.getAccessControllerOrDefault("missing");
                access.createAccessController(catalog, "removal-test", Collections.emptyMap(), false);
                Future<?> mutation = executor.submit(() -> inEnv(env, () -> {
                    remove(mgr, catalog, rename, replay);
                    return null;
                }));
                Assertions.assertTrue(retirementStarted.await(10L, TimeUnit.SECONDS));
                // This lookup completes while onClose() is paused after detaching the old controller.
                Assertions.assertSame(defaultController, access.getAccessControllerOrDefault("retiring"));
                Assertions.assertFalse(access.checkIfAccessControllerExist("retiring"));
                Mockito.verify(factory, Mockito.times(1)).createAccessController(Collections.emptyMap());
                allowRetirement.countDown();
                mutation.get(5L, TimeUnit.SECONDS);
                Assertions.assertTrue(oldCloseStarted.await(10L, TimeUnit.SECONDS));
                Assertions.assertFalse(access.checkIfAccessControllerExist("retiring"));

                String replacementName = rename ? "renamed" : "retiring";
                if (!rename) {
                    register(mgr, new LatchingCatalog(73002L, replacementName));
                }
                AuthorizationPlugin replacement = access.getAccessControllerOrDefault(replacementName);
                Assertions.assertNotSame(defaultController, replacement);
                Assertions.assertTrue(access.checkIfAccessControllerExist(replacementName));
                allowOldClose.countDown();
                Assertions.assertTrue(oldClosed.await(10L, TimeUnit.SECONDS));
                Assertions.assertSame(replacement, access.getAccessControllerOrDefault(replacementName));
                Mockito.verify(replacementController, Mockito.never()).close();
                access.removeAccessController(replacementName, mgr.getCatalog(replacementName).getId());
                return null;
            });
        } finally {
            allowRetirement.countDown();
            allowOldClose.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10L, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest(name = "replay={0}, fallback={1}")
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void lateControllerPublicationDuringRenameUsesOriginalName(boolean replay, boolean fallback) throws Exception {
        CatalogMgr mgr = new CatalogMgr();
        Env env = mockEnv(mgr);
        LatchingCatalog catalog = new LatchingCatalog(73006L, "retiring");
        if (fallback) {
            catalog.catalogProperty = new CatalogProperty(null, new HashMap<>());
        }
        register(mgr, catalog);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch allowPublication = new CountDownLatch(1);
        CountDownLatch controllerClosed = new CountDownLatch(1);
        try {
            inEnv(env, () -> {
                AccessControllerManager access = new AccessControllerManager(new Auth());
                Mockito.when(env.getAccessManager()).thenReturn(access);
                CatalogAccessController controller = Mockito.mock(CatalogAccessController.class);
                Mockito.doAnswer(invocation -> {
                    controllerClosed.countDown();
                    return null;
                }).when(controller).close();
                AccessControllerFactory factory = Mockito.mock(AccessControllerFactory.class);
                ConcurrentHashMap<String, AccessControllerFactory> factories =
                        Deencapsulation.getField(access, "accessControllerFactoriesCache");
                factories.put("removal-test", factory);
                Mockito.when(factory.createAccessController(Collections.emptyMap())).thenReturn(controller);
                Map<String, Object> entries = Deencapsulation.getField(access, "ctlToCtlAccessController");
                Map<String, Object> watchedEntries = Mockito.spy(entries);
                Mockito.doAnswer(invocation -> {
                    publishing.countDown();
                    await(allowPublication);
                    return invocation.callRealMethod();
                }).when(watchedEntries).put(Mockito.eq("retiring"), Mockito.any());
                Deencapsulation.setField(access, "ctlToCtlAccessController", watchedEntries);
                Future<?> publication = executor.submit(() -> inEnv(env, () -> {
                    if (fallback) {
                        access.getAccessControllerOrDefault("retiring");
                    } else {
                        access.createAccessController(catalog, "removal-test", Collections.emptyMap(), false);
                    }
                    return null;
                }));
                Assertions.assertTrue(publishing.await(10L, TimeUnit.SECONDS));
                // The original-name identity check has passed, but the old-name put has not happened.
                remove(mgr, catalog, true, replay);
                Assertions.assertSame(catalog, mgr.getCatalog("renamed"));
                allowPublication.countDown();
                publication.get(5L, TimeUnit.SECONDS);
                Assertions.assertFalse(access.checkIfAccessControllerExist("retiring"));
                if (fallback) {
                    Mockito.verify(controller, Mockito.never()).close();
                } else {
                    Assertions.assertTrue(controllerClosed.await(10L, TimeUnit.SECONDS));
                    Mockito.verify(controller, Mockito.times(1)).close();
                }
                return null;
            });
        } finally {
            allowPublication.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10L, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest(name = "rename={0}, replay={1}")
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void slowInitializationDoesNotHoldManagerLockDuringRemoval(boolean rename, boolean replay) throws Exception {
        CatalogMgr mgr = new CatalogMgr();
        Env env = mockEnv(mgr);
        LatchingCatalog catalog = new LatchingCatalog(73003L, "initializing");
        CountDownLatch initializationStarted = new CountDownLatch(1);
        CountDownLatch allowInitialization = new CountDownLatch(1);
        catalog.initialize = () -> {
            initializationStarted.countDown();
            await(allowInitialization);
            throw new IllegalStateException("stop after the simulated remote initialization");
        };
        register(mgr, catalog);
        LatchingCatalog unrelated = new LatchingCatalog(73004L, "unrelated");
        register(mgr, unrelated);
        ExecutorService executor = Executors.newFixedThreadPool(3);
        AtomicReference<Thread> removalThread = new AtomicReference<>();
        try {
            Future<?> initialization = executor.submit(() -> inEnv(env, () -> {
                Assertions.assertThrows(RuntimeException.class, catalog::makeSureInitialized);
                return null;
            }));
            Assertions.assertTrue(initializationStarted.await(10L, TimeUnit.SECONDS));
            Future<?> mutation = executor.submit(() -> inEnv(env, () -> {
                removalThread.set(Thread.currentThread());
                remove(mgr, catalog, rename, replay);
                return null;
            }));
            // Observe the actual monitor wait, rather than assuming the mutation has been scheduled.
            Awaitility.await().atMost(10L, TimeUnit.SECONDS).until(() -> {
                Thread thread = removalThread.get();
                if (thread == null) {
                    return false;
                }
                ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(thread.getId());
                return info != null && info.getThreadState() == Thread.State.BLOCKED
                        && info.getLockInfo().getIdentityHashCode() == System.identityHashCode(catalog);
            });
            Future<?> otherMutation = executor.submit(() -> inEnv(env, () -> {
                CatalogLog comment = new CatalogLog();
                comment.setCatalogId(unrelated.getId());
                comment.setComment("applied during initialization");
                mgr.replayAlterCatalogComment(comment);
                return null;
            }));
            otherMutation.get(3L, TimeUnit.SECONDS);
            Assertions.assertEquals("applied during initialization", unrelated.getComment());
            Assertions.assertFalse(mutation.isDone());
            allowInitialization.countDown();
            initialization.get(5L, TimeUnit.SECONDS);
            mutation.get(5L, TimeUnit.SECONDS);
            Assertions.assertNull(mgr.getCatalog("initializing"));
            if (rename) {
                Assertions.assertSame(catalog, mgr.getCatalog("renamed"));
            } else {
                Assertions.assertNull(mgr.getCatalog(catalog.getId()));
            }
        } finally {
            allowInitialization.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10L, TimeUnit.SECONDS));
        }
    }

    @Test
    void managerContentionReleasesCatalogMonitorBeforeWaiting() throws Exception {
        CatalogMgr mgr = new CatalogMgr();
        Env env = mockEnv(mgr);
        LatchingCatalog catalog = new LatchingCatalog(73005L, "contended");
        register(mgr, catalog);
        MonitoredReentrantReadWriteLock lock = Deencapsulation.getField(mgr, "lock");
        AtomicReference<Thread> removalThread = new AtomicReference<>();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<?> mutation;
        lock.writeLock().lock();
        try {
            mutation = executor.submit(() -> inEnv(env, () -> {
                removalThread.set(Thread.currentThread());
                remove(mgr, catalog, false, true);
                return null;
            }));
            Awaitility.await().atMost(10L, TimeUnit.SECONDS).until(() ->
                    removalThread.get() != null && lock.hasQueuedThread(removalThread.get()));
            Future<?> monitorProbe = executor.submit(() -> {
                synchronized (catalog) {
                    return null;
                }
            });
            monitorProbe.get(3L, TimeUnit.SECONDS);
        } finally {
            lock.writeLock().unlock();
            executor.shutdown();
            Assertions.assertTrue(executor.awaitTermination(10L, TimeUnit.SECONDS));
        }
        mutation.get(5L, TimeUnit.SECONDS);
        Assertions.assertNull(mgr.getCatalog(catalog.getId()));
    }

    private static Env mockEnv(CatalogMgr mgr) {
        Env env = Mockito.mock(Env.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(env.getCatalogMgr()).thenReturn(mgr);
        return env;
    }

    private static <T> T inEnv(Env env, Callable<T> action) throws Exception {
        // Static mocks are thread-local; each simulated query/mutation must use the same serving Env.
        try (MockedStatic<Env> mocked = Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            return action.call();
        }
    }

    private static void remove(CatalogMgr mgr, ExternalCatalog catalog, boolean rename, boolean replay)
            throws Exception {
        CatalogLog log = new CatalogLog();
        log.setCatalogId(catalog.getId());
        log.setNewCatalogName("renamed");
        if (replay) {
            if (rename) {
                mgr.replayAlterCatalogName(log);
            } else {
                mgr.replayDropCatalog(log);
            }
        } else if (rename) {
            mgr.alterCatalogName(catalog.getName(), "renamed");
        } else {
            mgr.dropCatalog(catalog.getName(), false);
        }
    }

    private static void register(CatalogMgr mgr, ExternalCatalog catalog) {
        mgr.getIdToCatalog().put(catalog.getId(), catalog);
        Map<String, CatalogIf> names = Deencapsulation.getField(mgr, "nameToCatalog");
        names.put(catalog.getName(), catalog);
    }

    private static void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(20L, TimeUnit.SECONDS), "test latch was not released");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static final class LatchingCatalog extends ExternalCatalog {
        private Runnable initialize = () -> { };
        private Runnable retireCaches = () -> { };

        private LatchingCatalog(long id, String name) {
            super(id, name, InitCatalogLog.Type.TEST, "");
            catalogProperty = new CatalogProperty(null,
                    new HashMap<>(Collections.singletonMap(CatalogMgr.ACCESS_CONTROLLER_CLASS_PROP, "removal-test")));
        }

        @Override
        protected void initLocalObjectsImpl() {
            initialize.run();
        }

        @Override
        protected Runnable captureConnectorCacheRetirement() {
            return retireCaches;
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext context, String dbName) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExist(SessionContext context, String dbName, String tableName) {
            return false;
        }
    }
}
