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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalDatabase;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

/**
 * Guards loading the Lance catalogs/databases/tables that 4.1.4+ FEs persisted (4.1.x and 4.2 shipped Lance as
 * fe-core classes; this branch has no such classes). Their GSON "clazz" labels must be redirected by the
 * {@code registerCompatibleSubtype} mappings in {@link GsonUtils} to the generic PluginDriven classes —
 * otherwise an image from a cluster with a Lance catalog fails with a {@code JsonParseException} (label not
 * registered) and the FE cannot start.
 *
 * <p>The table label lands on the MVCC variant: {@code LanceExternalTable} implemented {@code MvccTable}
 * (dataset versions back snapshots and time travel), so replaying it as the base
 * {@code PluginDrivenExternalTable} would silently downgrade it.</p>
 *
 * <p>Each case round-trips a valid PluginDriven object through GSON and rewrites only the "clazz" discriminator
 * to the legacy label, like {@link PaimonGsonCompatReplayTest}. The real 4.1.5 bytes of a Lance catalog are
 * covered by {@link Legacy415LanceUpgradeTest}; a database or table is never persisted by 4.1.5.</p>
 */
public class LanceGsonCompatReplayTest {

    private static String swapClazz(String json, String currentTag, String legacyTag) {
        String needle = "\"clazz\":\"" + currentTag + "\"";
        // Sanity: the polymorphic serialization must emit the discriminator we are about to rewrite.
        Assertions.assertTrue(json.contains(needle),
                "expected discriminator " + needle + " in serialized json: " + json);
        return json.replace(needle, "\"clazz\":\"" + legacyTag + "\"");
    }

    @Test
    public void testLegacyLanceCatalogTagReplaysAsPluginDriven() {
        Map<String, String> props = Maps.newHashMap();
        props.put("type", "lance");
        PluginDrivenExternalCatalog catalog =
                new PluginDrivenExternalCatalog(1L, "lance_ctl", "", props, "c", null);
        String baseJson = GsonUtils.GSON.toJson(catalog, CatalogIf.class);

        String json = swapClazz(baseJson, "PluginDrivenExternalCatalog", "LanceExternalCatalog");
        // MUTATION: removing the registerCompatibleSubtype throws "cannot deserialize ... subtype named
        // LanceExternalCatalog" here; a wrong target class fails the assertSame.
        CatalogIf<?> restored = GsonUtils.GSON.fromJson(json, CatalogIf.class);
        Assertions.assertSame(PluginDrivenExternalCatalog.class, restored.getClass(),
                "legacy 'LanceExternalCatalog' tag must replay as PluginDrivenExternalCatalog");
    }

    @Test
    public void testLegacyLanceDatabaseTagReplaysAsPluginDriven() {
        PluginDrivenExternalDatabase db = new PluginDrivenExternalDatabase();
        db.id = 2L;
        db.name = "lance_db";
        String baseJson = GsonUtils.GSON.toJson(db, DatabaseIf.class);

        String json = swapClazz(baseJson, "PluginDrivenExternalDatabase", "LanceExternalDatabase");
        // MUTATION: dropping the db registerCompatibleSubtype makes this throw on deserialize.
        DatabaseIf<?> restored = GsonUtils.GSON.fromJson(json, DatabaseIf.class);
        Assertions.assertSame(PluginDrivenExternalDatabase.class, restored.getClass(),
                "legacy 'LanceExternalDatabase' tag must replay as PluginDrivenExternalDatabase");
    }

    @Test
    public void testLegacyLanceTableTagReplaysAsMvccPluginDriven() {
        PluginDrivenMvccExternalTable table = new PluginDrivenMvccExternalTable();
        table.id = 3L;
        table.name = "lance_tbl";
        table.dbName = "lance_db";
        String baseJson = GsonUtils.GSON.toJson(table, TableIf.class);

        String json = swapClazz(baseJson, "PluginDrivenMvccExternalTable", "LanceExternalTable");
        TableIf restored = GsonUtils.GSON.fromJson(json, TableIf.class);
        // assertSame, not instanceof: a mapping to the base PluginDrivenExternalTable would also pass instanceof.
        Assertions.assertSame(PluginDrivenMvccExternalTable.class, restored.getClass(),
                "legacy 'LanceExternalTable' tag must replay as PluginDrivenMvccExternalTable,"
                        + " not the base PluginDrivenExternalTable");
    }

    @Test
    public void lanceLabelsAreTheOnlyOnesAReleaseAddedAfter413() throws Exception {
        // The three cases above are complete only if Lance is all that 4.1.5's registries added on top of
        // 4.1.3's (which Legacy413LabelRegistryTest covers label by label). Both inventories are dumped
        // reflectively by the fixture generators, so a label added in a later release cannot hide here.
        Assertions.assertEquals(ImmutableSet.of("LanceExternalCatalog"), addedSince413("ds"));
        Assertions.assertEquals(ImmutableSet.of("LanceExternalDatabase"), addedSince413("db"));
        Assertions.assertEquals(ImmutableSet.of("LanceExternalTable"), addedSince413("tbl"));
    }

    private static Set<String> addedSince413(String which) throws Exception {
        return Sets.difference(Legacy415Fixtures.legacyLabels(which).keySet(),
                Legacy413Fixtures.legacyLabels(which).keySet());
    }
}
