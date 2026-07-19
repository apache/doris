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

package org.apache.doris.datasource.plugin;

import org.apache.doris.connector.ConnectorStatementScopeImpl;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorStatementScope;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pins the engine-side metadata funnel {@link PluginDrivenMetadata}: within one statement it hands every caller
 * the SAME memoized {@link ConnectorMetadata} per catalog (so read / scan / DDL / MVCC share one), isolates
 * distinct catalogs, runs the factory on every call under {@link ConnectorStatementScope#NONE} (offline /
 * byte-identical to today), and lets the scope close the memoized instance exactly once at statement end.
 */
public class PluginDrivenMetadataTest {

    private static ConnectorSession session(long catalogId, ConnectorStatementScope scope) {
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Mockito.when(session.getCatalogId()).thenReturn(catalogId);
        Mockito.when(session.getStatementScope()).thenReturn(scope);
        return session;
    }

    private static ConnectorSession session(long catalogId, ConnectorStatementScope scope, String user) {
        ConnectorSession session = session(catalogId, scope);
        Mockito.when(session.getUser()).thenReturn(user);
        return session;
    }

    private static Connector countingConnector(AtomicInteger builds) {
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenAnswer(inv -> {
            builds.incrementAndGet();
            return Mockito.mock(ConnectorMetadata.class);
        });
        return connector;
    }

    @Test
    public void memoizesOneMetadataPerStatement() {
        // The read/scan/DDL/MVCC resolvers of one statement all route through the funnel and must collapse onto a
        // single getMetadata build and share the one instance. MUTATION: not memoizing -> two builds / two
        // instances -> red.
        ConnectorStatementScope scope = new ConnectorStatementScopeImpl();
        ConnectorSession session = session(7L, scope);
        AtomicInteger builds = new AtomicInteger();
        Connector connector = countingConnector(builds);

        ConnectorMetadata first = PluginDrivenMetadata.get(session, connector);
        ConnectorMetadata second = PluginDrivenMetadata.get(session, connector);

        Assertions.assertSame(first, second, "one statement + one catalog -> one shared metadata instance");
        Assertions.assertEquals(1, builds.get(), "connector.getMetadata is built once per statement");
    }

    @Test
    public void noneBuildsMetadataEachCall() {
        // No live statement scope (offline / tests / no ConnectContext): the funnel must degrade to today's
        // behavior -> a fresh getMetadata every call, byte-identical to pre-funnel. MUTATION: NONE memoizing ->
        // one build -> red.
        ConnectorSession session = session(7L, ConnectorStatementScope.NONE);
        AtomicInteger builds = new AtomicInteger();
        Connector connector = countingConnector(builds);

        ConnectorMetadata a = PluginDrivenMetadata.get(session, connector);
        ConnectorMetadata b = PluginDrivenMetadata.get(session, connector);

        Assertions.assertNotSame(a, b, "NONE memoizes nothing -> a fresh metadata each call");
        Assertions.assertEquals(2, builds.get(), "NONE runs the factory (getMetadata) every call");
    }

    @Test
    public void differentCatalogIdIsolates() {
        // A cross-catalog statement (e.g. a MERGE reading two catalogs) resolves each catalog's metadata
        // independently, because the memo key carries the catalog id. MUTATION: dropping the catalog id from the
        // key -> the two catalogs collide onto one instance -> red.
        ConnectorStatementScope scope = new ConnectorStatementScopeImpl();
        AtomicInteger builds = new AtomicInteger();
        Connector connector = countingConnector(builds);

        ConnectorMetadata c1 = PluginDrivenMetadata.get(session(1L, scope), connector);
        ConnectorMetadata c2 = PluginDrivenMetadata.get(session(2L, scope), connector);

        Assertions.assertNotSame(c1, c2, "distinct catalogs are isolated");
        Assertions.assertEquals(2, builds.get(), "one build per distinct catalog");
    }

    @Test
    public void sameIdentityReusesTheMemoizedInstance() {
        // The write arm now reuses the read arm's memoized instance. Within one statement read and write are the
        // same user, so the identity guard is satisfied and the instance is shared. MUTATION: guard throwing on a
        // matching identity -> red.
        ConnectorStatementScope scope = new ConnectorStatementScopeImpl();
        AtomicInteger builds = new AtomicInteger();
        Connector connector = countingConnector(builds);

        ConnectorMetadata first = PluginDrivenMetadata.get(session(7L, scope, "alice"), connector);
        ConnectorMetadata second = PluginDrivenMetadata.get(session(7L, scope, "alice"), connector);

        Assertions.assertSame(first, second, "same user in one statement -> one shared instance");
        Assertions.assertEquals(1, builds.get(), "built once");
    }

    @Test
    public void differentIdentityReusingTheInstanceFailsLoud() {
        // A session=user connector bakes the querying user's delegated credential into the instance at build time.
        // Reusing that instance under a second identity would execute one user's operation with another's
        // credentials, so the funnel fails loud rather than serving it. MUTATION: dropping the identity guard ->
        // the mismatched reuse silently returns the first user's instance -> red.
        ConnectorStatementScope scope = new ConnectorStatementScopeImpl();
        AtomicInteger builds = new AtomicInteger();
        Connector connector = countingConnector(builds);

        PluginDrivenMetadata.get(session(7L, scope, "alice"), connector);

        IllegalStateException ex = Assertions.assertThrows(IllegalStateException.class,
                () -> PluginDrivenMetadata.get(session(7L, scope, "bob"), connector));
        Assertions.assertTrue(ex.getMessage().contains("identity mismatch"),
                "message names the identity mismatch: " + ex.getMessage());
    }

    @Test
    public void closeAllClosesTheMemoizedMetadataOnce() throws Exception {
        // The statement's one memoized ConnectorMetadata (which is Closeable) is closed deterministically at
        // statement end, exactly once even though the engine fires closeAll from more than one locus (query-finish
        // callback + prepared-statement reset). MUTATION: closeAll not idempotent -> close() called twice -> red.
        ConnectorStatementScopeImpl scope = new ConnectorStatementScopeImpl();
        ConnectorSession session = session(7L, scope);
        ConnectorMetadata md = Mockito.mock(ConnectorMetadata.class);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(md);

        PluginDrivenMetadata.get(session, connector);
        scope.closeAll();
        scope.closeAll();

        Mockito.verify(md, Mockito.times(1)).close();
    }
}
