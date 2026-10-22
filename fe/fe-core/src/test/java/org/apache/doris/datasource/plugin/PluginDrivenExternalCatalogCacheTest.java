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
import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.ScopePath;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalMetaCacheMgr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

/** Catalog refresh reaches connector domains through the root even before schema caches are initialized. */
public class PluginDrivenExternalCatalogCacheTest {
    @Test
    public void refreshCatalogWithInvalidateDropsConnectorCaches() {
        refresh(true);
    }

    @Test
    public void refreshCatalogWithoutInvalidatePreservesConnectorCaches() {
        refresh(false);
    }

    private void refresh(boolean invalidate) {
        Connector connector = Mockito.mock(Connector.class);
        PluginDrivenExternalCatalog catalog = new PluginDrivenExternalCatalog(1L, "c", null,
                Collections.singletonMap("type", "iceberg"), "", connector);
        try (CatalogMetaCache root = catalog.getFeMetaCacheOwner()) {
            MetaCache<String, String> remote = root.openDomain("iceberg", ScopePath.Coordinates.REMOTE,
                    getClass().getClassLoader()).create(MetaCacheDefinition.<String, String>builder("table",
                            CacheSpec.of(true, -1, 10), key -> ScopePath.table("DB", key)).build());
            remote.put("T", "old");
            Env env = Mockito.mock(Env.class);
            CatalogMgr catalogs = Mockito.mock(CatalogMgr.class);
            Mockito.doReturn(catalog).when(catalogs).getCatalog(1L);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogs);
            Mockito.when(env.getExtMetaCacheMgr()).thenReturn(new ExternalMetaCacheMgr(true));
            try (MockedStatic<Env> current = Mockito.mockStatic(Env.class)) {
                current.when(Env::getCurrentEnv).thenReturn(env);
                catalog.onRefreshCache(invalidate);
            }
            Assertions.assertEquals(invalidate ? null : "old", remote.getIfPresent("T"));
            Mockito.verify(connector, Mockito.never()).invalidateAll();
        }
    }

    @Test
    public void refreshCatalogWithNullConnectorIsSafe() {
        PluginDrivenExternalCatalog catalog = new PluginDrivenExternalCatalog(1L, "c", null,
                Collections.singletonMap("type", "iceberg"), "", null);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(Mockito.mock(ExternalMetaCacheMgr.class));
        try (MockedStatic<Env> current = Mockito.mockStatic(Env.class)) {
            current.when(Env::getCurrentEnv).thenReturn(env);
            Assertions.assertDoesNotThrow(() -> catalog.onRefreshCache(true));
        }
    }
}
