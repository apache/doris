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

package org.apache.doris.datasource.metacache;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.Util;
import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.ScopePath;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.datasource.log.ExternalObjectLog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.persist.CreateDbInfo;
import org.apache.doris.persist.CreateTableInfo;
import org.apache.doris.persist.DropDbInfo;
import org.apache.doris.persist.DropInfo;
import org.apache.doris.persist.TruncateTableInfo;
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

class CatalogMetadataChangesTest {
    private final CatalogMetaCache root = CatalogMetaCache.unmanaged();
    private PluginDrivenExternalCatalog catalog;
    private MockedStatic<Env> env;

    @BeforeEach
    void setup() {
        env = Mockito.mockStatic(Env.class);
        env.when(Env::getCurrentEnv).thenReturn(Mockito.mock(Env.class));
        catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("c");
        Mockito.when(catalog.getMetaCacheOwnerIfPresent()).thenReturn(root);
        Mockito.when(catalog.getDbForReplay("db")).thenReturn(Optional.empty());
        Mockito.when(catalog.resolveDatabaseNameForInvalidation("db")).thenReturn("db");
    }

    @AfterEach
    void cleanup() {
        root.close();
        env.close();
    }

    private MetaCache<String, String> cache(String name, ScopePath scope) {
        MetaCache<String, String> cache = root.create(MetaCacheDefinition.<String, String>builder(name,
                CacheSpec.of(true, -1, 100), key -> scope).build());
        cache.put("key", "value");
        return cache;
    }

    private ScopePath local(ScopePath scope) {
        return scope.in(ScopePath.Coordinates.LOCAL);
    }

    private ScopePath remote(ScopePath scope) {
        return scope.in(ScopePath.Coordinates.REMOTE);
    }

    @Test
    void contentChangePreservesNamesAndLookupButInvalidatesEveryContentDependency() {
        MetaCache<String, String> names = cache("names", local(ScopePath.tableCollection("db")));
        MetaCache<String, String> object = cache("object", local(ScopePath.table("db", "t")));
        MetaCache<String, String> schema = cache("schema", local(ScopePath.tableMetadata("db", "t")));
        MetaCache<String, String> hive = cache("hive", remote(ScopePath.table("DB", "T")));
        MetaCache<String, String> iceberg = cache("iceberg", remote(ScopePath.table("DB", "T")));
        MetaCache<String, String> other = cache("other", remote(ScopePath.table("DB", "OTHER")));
        MetaCache<String, String> rows = cache("rows", ScopePath.table(
                Long.toString(Util.genIdByName("c", "db")), Long.toString(Util.genIdByName("c", "db", "t")))
                .in(ScopePath.Coordinates.ID));
        CatalogMetadataChanges.apply(catalog, ExternalObjectLog.createForRefreshTable(1, "db", "t", 0)
                .withRemoteNames("DB", "T"));
        Assertions.assertEquals("value", names.getIfPresent("key"));
        Assertions.assertEquals("value", object.getIfPresent("key"));
        Assertions.assertNull(schema.getIfPresent("key"));
        Assertions.assertNull(hive.getIfPresent("key"));
        Assertions.assertNull(iceberg.getIfPresent("key"));
        Assertions.assertNull(rows.getIfPresent("key"));
        Assertions.assertEquals("value", other.getIfPresent("key"));
        Mockito.verify(catalog, Mockito.never()).getConnector();
        Mockito.verify(catalog, Mockito.never()).getFeMetaCacheOwner();
    }

    @Test
    void coldRenameUsesPersistedRemoteNamesAndFencesOldAndNewResourcesOnly() {
        MetaCache<String, String> old = cache("old", local(ScopePath.table("db", "old")));
        MetaCache<String, String> replacement = cache("new", local(ScopePath.table("db", "new")));
        MetaCache<String, String> remoteOld = cache("remoteOld", remote(ScopePath.table("DB", "OLD")));
        MetaCache<String, String> remoteNew = cache("remoteNew", remote(ScopePath.table("DB", "NEW")));
        MetaCache<String, String> sibling = cache("sibling", remote(ScopePath.table("DB", "OTHER")));
        ExternalObjectLog change = ExternalObjectLog.createForRenameTable(1, "db", "old", "new")
                .withRemoteNames("DB", "OLD");
        change.setRemoteNewTableName("NEW");
        change = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(change), ExternalObjectLog.class);
        CatalogMetadataChanges.apply(catalog, change);
        Assertions.assertNull(old.getIfPresent("key"));
        Assertions.assertNull(replacement.getIfPresent("key"));
        Assertions.assertNull(remoteOld.getIfPresent("key"));
        Assertions.assertNull(remoteNew.getIfPresent("key"));
        Assertions.assertEquals("value", sibling.getIfPresent("key"));
    }

    @Test
    void oldColdLogWidensOnlyRemoteCoordinates() {
        MetaCache<String, String> remote = cache("remote", remote(ScopePath.table("ANY", "T")));
        MetaCache<String, String> names = cache("names", local(ScopePath.tableCollection("db")));
        MetaCache<String, String> sibling = cache("sibling", local(ScopePath.table("db", "other")));
        CatalogMetadataChanges.apply(catalog, ExternalObjectLog.createForRefreshTable(1, "db", "t", 0));
        Assertions.assertNull(remote.getIfPresent("key"));
        Assertions.assertEquals("value", names.getIfPresent("key"));
        Assertions.assertEquals("value", sibling.getIfPresent("key"));
    }

    @Test
    void gatewayDependencyProjectionParticipatesInTheSameChange() {
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(catalog.getConnectorIfPresent()).thenReturn(connector);
        Mockito.when(connector.cacheDependencies(ScopePath.table("DB", "T")))
                .thenReturn(Collections.singletonList(ScopePath.table("lake", "physical")));
        MetaCache<String, String> source = cache("source", remote(ScopePath.table("DB", "T")));
        MetaCache<String, String> lake = cache("lake", remote(ScopePath.table("lake", "physical")));
        CatalogMetadataChanges.apply(catalog, ExternalObjectLog.createForRefreshTable(1, "db", "t", 0)
                .withRemoteNames("DB", "T"));
        Assertions.assertNull(source.getIfPresent("key"));
        Assertions.assertNull(lake.getIfPresent("key"));
        Mockito.verify(connector, Mockito.never()).invalidateTable(Mockito.anyString(), Mockito.anyString());
    }

    @Test
    void ddlJournalRecordsPreserveTheSameChangeForReplay() {
        ExternalObjectLog change = ExternalObjectLog.createForExplicitRefreshTable(1, "db", "t", 0)
                .withRemoteNames("DB", "T");
        Object[] records = {
                new CreateDbInfo("c", "db", null).withExternalCacheChange(change),
                new CreateTableInfo("c", "db", "t").withExternalCacheChange(change),
                new DropDbInfo("c", "db").withExternalCacheChange(change),
                new DropInfo("c", "db", "t").withExternalCacheChange(change),
                new TruncateTableInfo("c", "db", "t", null, 0).withExternalCacheChange(change)
        };
        for (Object record : records) {
            String json = GsonUtils.GSON.toJson(record);
            Object restored = GsonUtils.GSON.fromJson(json, record.getClass());
            Assertions.assertEquals(GsonUtils.GSON.toJsonTree(record).getAsJsonObject().get("externalCacheChange"),
                    GsonUtils.GSON.toJsonTree(restored).getAsJsonObject().get("externalCacheChange"));
            Assertions.assertTrue(json.contains("remoteTableName"));
        }
    }
}
