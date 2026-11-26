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
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.datasource.ExternalMetaCacheMgr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

/** Verifies connector-owned caches follow catalog refresh semantics. */
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
        // Connector state must be cleared before the engine invalidates row counts derived from it.
        InOrder order = Mockito.inOrder(connector, cacheMgr);
        order.verify(connector).invalidateAll();
        order.verify(cacheMgr).invalidateCatalog(1L);
    }

    @Test
    public void refreshCatalogWithoutInvalidateDoesNotTouchConnector() {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = catalogWith(connector);
        Env env = Mockito.mock(Env.class);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            catalog.onRefreshCache(false);
        }
        Mockito.verify(connector, Mockito.never()).invalidateAll();
    }

    @Test
    public void refreshCatalogWithNullConnectorIsSafe() {
        // A reset catalog has no connector to invalidate.
        PluginDrivenExternalCatalog catalog = catalogWith(null);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(Mockito.mock(ExternalMetaCacheMgr.class));
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertDoesNotThrow(() -> catalog.onRefreshCache(true));
        }
    }
}
