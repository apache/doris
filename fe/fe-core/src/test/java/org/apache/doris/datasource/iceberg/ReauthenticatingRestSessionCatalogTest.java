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

package org.apache.doris.datasource.iceberg;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

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
        // The default Catalog handed to IcebergMetadataOps is asCatalog(empty); it must inherit the same
        // recovery because it calls back into this session catalog.
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
}
