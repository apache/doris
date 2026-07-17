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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.datasource.DelegatedCredential;
import org.apache.doris.datasource.SessionContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Pins the per-user shared-cache-bypass DECISION for a {@code iceberg.rest.session=user} catalog (#63068, the
 * highest-severity concern — cross-user cache leakage). Under a {@link ConnectorCapability#SUPPORTS_USER_SESSION}
 * connector carrying a per-request delegated credential the remote source returns PER-USER metadata, so the shared
 * (catalog+name-keyed, NOT user-keyed) db/table-name caches must be bypassed; without a credential — or for a
 * connector that never opts in — the shared cache is kept (the fail-closed rejection then happens connector-side
 * on the actual read, never by serving/poisoning a shared entry). The data-flow consequence (a bypassing read
 * never mutates the shared lookup) is covered by the connector-suite + docker e2e; this pins the gate itself.
 */
public class PluginDrivenExternalCatalogSessionBypassTest {

    private static PluginDrivenExternalCatalog catalogWith(Set<ConnectorCapability> capabilities) {
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getCapabilities()).thenReturn(capabilities);
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        return new PluginDrivenExternalCatalog(1L, "test_ctl", null, props, "", connector);
    }

    private static SessionContext credentialed() {
        return SessionContext.of("sess-1",
                new DelegatedCredential(DelegatedCredential.Type.ACCESS_TOKEN, "user-token"));
    }

    @Test
    public void bypassesSharedCachesForCapableConnectorCarryingCredential() {
        PluginDrivenExternalCatalog userSession =
                catalogWith(EnumSet.of(ConnectorCapability.SUPPORTS_USER_SESSION));

        Assertions.assertTrue(userSession.shouldBypassTableNameCache(credentialed()),
                "session=user + credential must bypass the shared table-name cache (per-user metadata)");
        Assertions.assertTrue(userSession.shouldBypassDbNameCache(credentialed()),
                "session=user + credential must bypass the shared db-name cache (per-user metadata)");
    }

    @Test
    public void keepsSharedCachesWhenNoCredentialPresent() {
        PluginDrivenExternalCatalog userSession =
                catalogWith(EnumSet.of(ConnectorCapability.SUPPORTS_USER_SESSION));

        // No credential (background/internal work, or a password login to a user-session catalog): keep the shared
        // cache here. The fail-closed rejection is enforced connector-side on the actual metadata read, not by
        // bypassing into a per-user read that has no identity to authorize with.
        Assertions.assertFalse(userSession.shouldBypassTableNameCache(SessionContext.empty()));
        Assertions.assertFalse(userSession.shouldBypassDbNameCache(SessionContext.empty()));
        Assertions.assertFalse(userSession.shouldBypassTableNameCache(null),
                "a null session context must not bypass (no credential to key a per-user read)");
        Assertions.assertFalse(userSession.shouldBypassDbNameCache(null));
    }

    @Test
    public void neverBypassesForNonUserSessionConnectorEvenWithCredential() {
        // A connector that does not declare SUPPORTS_USER_SESSION never bypasses — ordinary catalogs keep their
        // shared caches unconditionally (zero-regression gate; least-privilege).
        PluginDrivenExternalCatalog plain = catalogWith(EnumSet.noneOf(ConnectorCapability.class));

        Assertions.assertFalse(plain.shouldBypassTableNameCache(credentialed()));
        Assertions.assertFalse(plain.shouldBypassDbNameCache(credentialed()));
    }
}
