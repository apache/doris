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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * The lifecycle of the per-configuration {@code UserGroupInformation} that keeps two catalogs from
 * sharing one cached {@code FileSystem}.
 *
 * <p>This is a destructive mechanism - the last release closes every filesystem Hadoop cached under
 * the UGI - so what it must never do is act on somebody else's entry. The cases below pin the two
 * halves of that: a connector acquires exactly one hold and releases exactly the hold it took, and a
 * configuration shared by two catalogs survives until BOTH of them are closed.
 *
 * <p>Each case uses a marker property of its own, so the keys are disjoint and the shared static map
 * cannot carry state from one case into the next.
 */
public class HudiConnectorFileSystemScopeTest {

    @Test
    public void twoCatalogsOnOneConfigurationShareOneScopeAndTheLastCloseReleasesIt() throws Exception {
        Map<String, String> props = propertiesFor("shared");
        String key = HudiConnector.fileSystemScopeKey(props);
        Assertions.assertEquals(0, HudiConnector.scopeOwners(key), "nothing holds this key yet");

        HudiConnector first = connector(props, 1L);
        HudiConnector second = connector(props, 2L);

        UserGroupInformation firstScope = first.fileSystemScope();
        UserGroupInformation secondScope = second.fileSystemScope();
        Assertions.assertNotNull(firstScope, "a non-Kerberos catalog must get a scope of its own");
        Assertions.assertSame(firstScope, secondScope,
                "two catalogs defined on byte-identical properties may share a filesystem, so they share the "
                        + "UGI that keys it - one per configuration, not one per catalog");
        Assertions.assertEquals(2, HudiConnector.scopeOwners(key), "both connectors hold it");

        first.close();
        Assertions.assertEquals(1, HudiConnector.scopeOwners(key),
                "the entry outlives the first close: the other catalog is still reading through it");
        Assertions.assertSame(secondScope, second.fileSystemScope(),
                "closing a sibling must not take the surviving catalog's scope away");

        second.close();
        Assertions.assertEquals(0, HudiConnector.scopeOwners(key),
                "the last holder releases the entry, which is what closes the filesystems cached under it");
    }

    @Test
    public void connectorAcquiresOneHoldNoMatterHowOftenItIsAsked() throws Exception {
        Map<String, String> props = propertiesFor("memoized");
        String key = HudiConnector.fileSystemScopeKey(props);
        HudiConnector connector = connector(props, 3L);

        UserGroupInformation scope = connector.fileSystemScope();
        for (int i = 0; i < 5; i++) {
            Assertions.assertSame(scope, connector.fileSystemScope(), "the scope is memoized per connector");
        }
        Assertions.assertEquals(1, HudiConnector.scopeOwners(key),
                "the hold is taken when the scope is built, not on every read - otherwise the count could "
                        + "never come back down");

        connector.close();
        Assertions.assertEquals(0, HudiConnector.scopeOwners(key), "one hold, one release");
    }

    @Test
    public void closingTwiceReleasesOnce() throws Exception {
        Map<String, String> props = propertiesFor("double-close");
        String key = HudiConnector.fileSystemScopeKey(props);

        HudiConnector holder = connector(props, 4L);
        HudiConnector closedTwice = connector(props, 5L);
        holder.fileSystemScope();
        closedTwice.fileSystemScope();
        Assertions.assertEquals(2, HudiConnector.scopeOwners(key));

        closedTwice.close();
        closedTwice.close();
        Assertions.assertEquals(1, HudiConnector.scopeOwners(key),
                "a second close must not decrement again - it would close the filesystems the other catalog "
                        + "is still reading through");

        holder.close();
        Assertions.assertEquals(0, HudiConnector.scopeOwners(key));
    }

    @Test
    public void connectorThatNeverBuiltAScopeReleasesNothing() throws Exception {
        Map<String, String> props = propertiesFor("never-used");
        String key = HudiConnector.fileSystemScopeKey(props);

        HudiConnector user = connector(props, 6L);
        user.fileSystemScope();
        Assertions.assertEquals(1, HudiConnector.scopeOwners(key));

        // The throwaway connector CatalogFactory builds during checkWhenCreating is exactly this: created,
        // never queried, closed. It must not touch an entry it never took.
        connector(props, 7L).close();
        Assertions.assertEquals(1, HudiConnector.scopeOwners(key),
                "closing a connector that never computed a scope must leave the live entry alone");

        user.close();
        Assertions.assertEquals(0, HudiConnector.scopeOwners(key));
    }

    @Test
    public void closedConnectorDoesNotAcquireAgain() throws Exception {
        Map<String, String> props = propertiesFor("closed-then-asked");
        String key = HudiConnector.fileSystemScopeKey(props);

        HudiConnector connector = connector(props, 8L);
        connector.close();

        // A statement still holding a connector the FE has replaced can reach this. Acquiring here would
        // take a hold nothing is ever going to release, since close() has already happened.
        Assertions.assertNull(connector.fileSystemScope(),
                "after close the connector falls back to the FE-injected authenticator");
        Assertions.assertEquals(0, HudiConnector.scopeOwners(key), "and it takes no new hold");
    }

    @Test
    public void differentConfigurationsGetDifferentScopes() throws Exception {
        Map<String, String> left = propertiesFor("distinct-left");
        Map<String, String> right = propertiesFor("distinct-right");
        Assertions.assertNotEquals(HudiConnector.fileSystemScopeKey(left),
                HudiConnector.fileSystemScopeKey(right),
                "the key is the configuration digest, so different properties are different keys");

        HudiConnector one = connector(left, 9L);
        HudiConnector other = connector(right, 10L);
        Assertions.assertNotSame(one.fileSystemScope(), other.fileSystemScope(),
                "two catalogs that may NOT share a filesystem must not share the UGI that keys it");

        one.close();
        Assertions.assertEquals(1, HudiConnector.scopeOwners(HudiConnector.fileSystemScopeKey(right)),
                "releasing one configuration says nothing about another");
        other.close();
    }

    @Test
    public void theKeyIsTheConfigurationAndNotTheCatalogItBelongsTo() throws Exception {
        Map<String, String> props = propertiesFor("id-independent");
        // Two catalog ids, one configuration. Prefixing the key with the id would double the UGIs, the
        // filesystems cached under them and the SDK client threads this whole mechanism exists to bound.
        HudiConnector low = connector(props, 11L);
        HudiConnector high = connector(props, 4242L);
        Assertions.assertSame(low.fileSystemScope(), high.fileSystemScope());
        low.close();
        high.close();
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────────────────────

    /** The minimal catalog properties plus a marker, so each case owns a disjoint key. */
    private static Map<String, String> propertiesFor(String marker) {
        Map<String, String> props = HudiTestProperties.minimalMap();
        props.put("hudi.fs.scope.test.marker", marker);
        return props;
    }

    private static HudiConnector connector(Map<String, String> props, long catalogId) {
        return new HudiConnector(props, new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "catalog_" + catalogId;
            }

            @Override
            public long getCatalogId() {
                return catalogId;
            }
        });
    }
}
