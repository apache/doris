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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ReauthenticatingRestSessionCatalogTest {

    private static final Namespace NS = Namespace.empty();

    /**
     * A stand-in for a live REST catalog: throws the configured failure on every namespace listing until the
     * failure is cleared, and records whether it was closed. All overridden methods avoid the real
     * RESTSessionCatalog internals, so no initialization or network is involved.
     */
    private static final class FakeRestSessionCatalog extends RESTSessionCatalog {
        private final String label;
        private final RuntimeException failure;
        private final AtomicInteger listCalls = new AtomicInteger();
        private volatile Table loadedTable;
        private volatile CountDownLatch loadStarted;
        private volatile CountDownLatch finishLoad;
        private volatile boolean closed;

        FakeRestSessionCatalog(String label, RuntimeException failure) {
            this.label = label;
            this.failure = failure;
        }

        @Override
        public String name() {
            return label;
        }

        @Override
        public List<Namespace> listNamespaces(SessionContext context, Namespace ns) {
            listCalls.incrementAndGet();
            if (failure != null) {
                throw failure;
            }
            return Collections.singletonList(Namespace.of(label));
        }

        void blockLoad(Table table, CountDownLatch started, CountDownLatch finish) {
            loadedTable = table;
            loadStarted = started;
            finishLoad = finish;
        }

        @Override
        public Table loadTable(SessionContext context, TableIdentifier ident) {
            if (loadStarted != null) {
                loadStarted.countDown();
            }
            if (finishLoad != null) {
                try {
                    if (!finishLoad.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting to finish fake table load");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
            return loadedTable;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static NotAuthorizedException notAuthorized() {
        return new NotAuthorizedException("Not authorized: %s", "the token expired");
    }

    private static SessionContext delegatedUserContext() {
        return new SessionContext(UUID.randomUUID().toString(), "alice",
                Collections.singletonMap("token", "user-token"), Collections.emptyMap());
    }

    @Test
    public void testNotAuthorizedRebuildsClientAndRetriesOnce() {
        FakeRestSessionCatalog wedged = new FakeRestSessionCatalog("wedged", notAuthorized());
        FakeRestSessionCatalog fresh = new FakeRestSessionCatalog("fresh", null);
        AtomicInteger rebuilds = new AtomicInteger();
        ReauthenticatingRestSessionCatalog catalog = new ReauthenticatingRestSessionCatalog(wedged, () -> {
            rebuilds.incrementAndGet();
            return fresh;
        });

        List<Namespace> namespaces = catalog.listNamespaces(SessionContext.createEmpty(), NS);

        Assertions.assertEquals(Collections.singletonList(Namespace.of("fresh")), namespaces);
        Assertions.assertEquals(1, rebuilds.get());
        Assertions.assertEquals(1, wedged.listCalls.get());
        Assertions.assertEquals(1, fresh.listCalls.get());
        Assertions.assertTrue(wedged.closed, "the wedged client must be closed after replacement");
        Assertions.assertSame(fresh, catalog.currentDelegate());
    }

    @Test
    public void testNotAuthorizedWrappedInAnotherExceptionIsDetected() {
        FakeRestSessionCatalog wedged = new FakeRestSessionCatalog("wedged",
                new RuntimeException("Failed to list database names", notAuthorized()));
        FakeRestSessionCatalog fresh = new FakeRestSessionCatalog("fresh", null);
        ReauthenticatingRestSessionCatalog catalog =
                new ReauthenticatingRestSessionCatalog(wedged, () -> fresh);

        List<Namespace> namespaces = catalog.listNamespaces(SessionContext.createEmpty(), NS);

        Assertions.assertEquals(Collections.singletonList(Namespace.of("fresh")), namespaces);
    }

    @Test
    public void testStillNotAuthorizedAfterRebuildPropagatesWithoutLooping() {
        FakeRestSessionCatalog wedged = new FakeRestSessionCatalog("wedged", notAuthorized());
        FakeRestSessionCatalog stillWedged = new FakeRestSessionCatalog("still-wedged", notAuthorized());
        AtomicInteger rebuilds = new AtomicInteger();
        ReauthenticatingRestSessionCatalog catalog = new ReauthenticatingRestSessionCatalog(wedged, () -> {
            rebuilds.incrementAndGet();
            return stillWedged;
        });

        Assertions.assertThrows(NotAuthorizedException.class,
                () -> catalog.listNamespaces(SessionContext.createEmpty(), NS));
        Assertions.assertEquals(1, rebuilds.get(), "exactly one rebuild, no retry loop");
        Assertions.assertEquals(1, wedged.listCalls.get());
        Assertions.assertEquals(1, stillWedged.listCalls.get());
    }

    @Test
    public void testNonAuthFailuresAreNotRetried() {
        FakeRestSessionCatalog failing = new FakeRestSessionCatalog("failing",
                new RuntimeException("connection reset"));
        AtomicInteger rebuilds = new AtomicInteger();
        ReauthenticatingRestSessionCatalog catalog = new ReauthenticatingRestSessionCatalog(failing, () -> {
            rebuilds.incrementAndGet();
            return new FakeRestSessionCatalog("fresh", null);
        });

        Assertions.assertThrows(RuntimeException.class,
                () -> catalog.listNamespaces(SessionContext.createEmpty(), NS));
        Assertions.assertEquals(0, rebuilds.get());
        Assertions.assertEquals(1, failing.listCalls.get());
        Assertions.assertFalse(failing.closed);
    }

    @Test
    public void testDelegatedUserSessionIsNotRecovered() {
        // A 401 for a request carrying a per-user delegated credential means that user's token is invalid.
        // Rebuilding the shared client cannot fix it and must not be triggered by it.
        FakeRestSessionCatalog wedged = new FakeRestSessionCatalog("wedged", notAuthorized());
        AtomicInteger rebuilds = new AtomicInteger();
        ReauthenticatingRestSessionCatalog catalog = new ReauthenticatingRestSessionCatalog(wedged, () -> {
            rebuilds.incrementAndGet();
            return new FakeRestSessionCatalog("fresh", null);
        });

        Assertions.assertThrows(NotAuthorizedException.class,
                () -> catalog.listNamespaces(delegatedUserContext(), NS));
        Assertions.assertEquals(0, rebuilds.get());
        Assertions.assertFalse(wedged.closed);
    }

    @Test
    public void testAsCatalogViewRoutesThroughRecovery() {
        // The default Catalog handed to the connector's catalog ops is asCatalog(empty); it must inherit the
        // same recovery because it calls back into this session catalog.
        FakeRestSessionCatalog wedged = new FakeRestSessionCatalog("wedged", notAuthorized());
        FakeRestSessionCatalog fresh = new FakeRestSessionCatalog("fresh", null);
        ReauthenticatingRestSessionCatalog catalog =
                new ReauthenticatingRestSessionCatalog(wedged, () -> fresh);

        List<Namespace> namespaces = ((org.apache.iceberg.catalog.SupportsNamespaces)
                catalog.asCatalog(SessionContext.createEmpty())).listNamespaces(NS);

        Assertions.assertEquals(Collections.singletonList(Namespace.of("fresh")), namespaces);
        Assertions.assertTrue(wedged.closed);
    }

    @Test
    public void testCloseClosesCurrentDelegate() throws Exception {
        FakeRestSessionCatalog delegate = new FakeRestSessionCatalog("delegate", null);
        ReauthenticatingRestSessionCatalog catalog =
                new ReauthenticatingRestSessionCatalog(delegate, () -> delegate);

        catalog.close();

        Assertions.assertTrue(delegate.closed);
    }

    @Test
    public void testCloseWinningReplacementClosesUnpublishedDelegateAndPreserves401() {
        FakeRestSessionCatalog wedged = new FakeRestSessionCatalog("wedged", notAuthorized());
        FakeRestSessionCatalog unpublished = new FakeRestSessionCatalog("unpublished", null);
        IcebergCatalogResourceTracker tracker = new IcebergCatalogResourceTracker();
        ReauthenticatingRestSessionCatalog catalog = new ReauthenticatingRestSessionCatalog(
                wedged,
                () -> {
                    tracker.close(() -> { });
                    return unpublished;
                },
                tracker,
                () -> { });

        NotAuthorizedException failure = Assertions.assertThrows(NotAuthorizedException.class,
                () -> catalog.listNamespaces(SessionContext.createEmpty(), NS));

        Assertions.assertTrue(unpublished.closed, "the rejected replacement must be closed");
        Assertions.assertSame(wedged, catalog.currentDelegate());
        Assertions.assertEquals(1, failure.getSuppressed().length);
        Assertions.assertInstanceOf(IllegalStateException.class, failure.getSuppressed()[0]);
    }

    @Test
    public void testReplacementIsPublishedBeforeTableLoadsAreInvalidated() {
        FakeRestSessionCatalog wedged = new FakeRestSessionCatalog("wedged", notAuthorized());
        FakeRestSessionCatalog fresh = new FakeRestSessionCatalog("fresh", null);
        IcebergCatalogResourceTracker tracker = new IcebergCatalogResourceTracker();
        AtomicReference<ReauthenticatingRestSessionCatalog> catalogRef = new AtomicReference<>();
        AtomicInteger invalidations = new AtomicInteger();
        ReauthenticatingRestSessionCatalog catalog = new ReauthenticatingRestSessionCatalog(
                wedged,
                () -> fresh,
                tracker,
                () -> {
                    Assertions.assertSame(fresh, catalogRef.get().currentDelegate(),
                            "cache invalidation must fence misses only after the replacement generation is live");
                    invalidations.incrementAndGet();
                });
        catalogRef.set(catalog);

        List<Namespace> namespaces = catalog.listNamespaces(SessionContext.createEmpty(), NS);

        Assertions.assertEquals(Collections.singletonList(Namespace.of("fresh")), namespaces);
        Assertions.assertEquals(1, invalidations.get());
    }

    @Test
    public void testTableCleanupUsesDelegateThatProducedTableAcrossRotation() throws Exception {
        FakeRestSessionCatalog oldDelegate = new FakeRestSessionCatalog("old", notAuthorized());
        FakeRestSessionCatalog freshDelegate = new FakeRestSessionCatalog("fresh", null);
        Table table = interfaceProxy(Table.class);
        FileIO oldFileIo = interfaceProxy(FileIO.class);
        FileIO freshFileIo = interfaceProxy(FileIO.class);
        CountDownLatch classificationStarted = new CountDownLatch(1);
        CountDownLatch finishClassification = new CountDownLatch(1);
        oldDelegate.blockLoad(table, null, null);
        ReauthenticatingRestSessionCatalog catalog = new ReauthenticatingRestSessionCatalog(
                oldDelegate, () -> freshDelegate) {
            @Override
            FileIO catalogFileIo(RESTSessionCatalog delegate) {
                if (delegate != oldDelegate) {
                    return freshFileIo;
                }
                classificationStarted.countDown();
                try {
                    if (!finishClassification.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("timed out waiting to classify table FileIO");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                return oldFileIo;
            }
        };
        AtomicReference<Table> loaded = new AtomicReference<>();
        Thread loader = new Thread(() -> loaded.set(catalog.loadTable(
                SessionContext.createEmpty(), TableIdentifier.of("db", "tbl"))));

        loader.start();
        Assertions.assertTrue(classificationStarted.await(10, TimeUnit.SECONDS));
        catalog.listNamespaces(SessionContext.createEmpty(), NS);
        finishClassification.countDown();
        loader.join(TimeUnit.SECONDS.toMillis(10));

        Assertions.assertFalse(loader.isAlive());
        Assertions.assertSame(table, loaded.get());
        Assertions.assertSame(oldFileIo, catalog.takeCatalogFileIo(table));
    }

    private static <T> T interfaceProxy(Class<T> type) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        if ("hashCode".equals(method.getName())) {
                            return System.identityHashCode(proxy);
                        }
                        if ("equals".equals(method.getName())) {
                            return proxy == args[0];
                        }
                        return type.getSimpleName() + "Proxy";
                    }
                    Class<?> returnType = method.getReturnType();
                    if (!returnType.isPrimitive()) {
                        return null;
                    }
                    if (returnType == boolean.class) {
                        return false;
                    }
                    if (returnType == char.class) {
                        return '\0';
                    }
                    return 0;
                }));
    }
}
