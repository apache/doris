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

import org.apache.doris.catalog.Env;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.datasource.ExternalMetaCacheMgr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

/**
 * Pins {@link PluginDrivenExternalCatalog#onRefreshCache} (H-5): {@code REFRESH CATALOG} with cache
 * invalidation must also drop the connector's OWN caches (e.g. the iceberg latest-snapshot cache, default TTL
 * 24h), which the base engine route resolver never reaches for a plugin catalog. {@code REFRESH CATALOG} does
 * not rebuild the connector (only {@code ADD}/{@code MODIFY CATALOG} does), so this override is the only thing
 * that drops the connector caches on refresh.
 */
public class PluginDrivenExternalCatalogCacheTest {

    private static PluginDrivenExternalCatalog catalogWith(Connector connector) {
        Map<String, String> props = new HashMap<>();
        props.put("type", "iceberg");
        return new PluginDrivenExternalCatalog(1L, "test_ctl", null, props, "", connector);
    }

    @Test
    public void refreshCatalogWithInvalidateDropsConnectorCaches() {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = catalogWith(connector);
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr cacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(cacheMgr);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            catalog.onRefreshCache(true);
        }
        // H-5 has TWO halves; pin both. (a) the base engine invalidation must STILL run: super.onRefreshCache(true)
        // -> Env...getExtMetaCacheMgr().invalidateCatalog(id) flushes the engine route-resolver/schema cache for
        // this plugin catalog. MUTATION: dropping the super.onRefreshCache(...) delegation -> verify fails.
        Mockito.verify(cacheMgr).invalidateCatalog(1L);
        // (b) the connector's OWN caches must be dropped too (the part the base path never reaches). MUTATION:
        // removing connector.invalidateAll() -> the connector keeps serving stale snapshots up to 24h -> fails.
        Mockito.verify(connector, Mockito.times(1)).invalidateAll();
    }

    @Test
    public void refreshCatalogWithoutInvalidateDoesNotTouchConnector() {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = catalogWith(connector);
        Env env = Mockito.mock(Env.class);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            // invalidCache=false (the plain "reload metadata, keep caches" refresh) must NOT drop connector
            // caches. MUTATION: dropping the invalidCache guard -> connector cleared unconditionally -> fails.
            catalog.onRefreshCache(false);
        }
        Mockito.verify(connector, Mockito.never()).invalidateAll();
    }

    @Test
    public void refreshCatalogWithNullConnectorIsSafe() {
        // resetToUninitialized() nulls the connector (onClose) BEFORE calling onRefreshCache, and an
        // uninitialized catalog has no connector yet. The override must be a safe no-op, not an NPE.
        PluginDrivenExternalCatalog catalog = catalogWith(null);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(Mockito.mock(ExternalMetaCacheMgr.class));
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertDoesNotThrow(() -> catalog.onRefreshCache(true));
        }
    }
}
