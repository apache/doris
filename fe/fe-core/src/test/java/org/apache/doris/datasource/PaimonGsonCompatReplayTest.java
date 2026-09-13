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

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Guards the P5 paimon SPI cutover edit-log compatibility: an FE image / edit log written by a
 * pre-cutover version persisted paimon catalogs/databases/tables under their legacy class simple
 * names (the GSON "clazz" discriminator). After the cutover those legacy classes are no longer
 * {@code registerSubtype}'d, so on replay the {@code registerCompatibleSubtype} mappings in
 * {@link GsonUtils} MUST redirect every legacy tag to the generic PluginDriven class — otherwise
 * the FE crashes on startup with a {@code JsonParseException} (tag not registered) or a downstream
 * {@code ClassCastException}.
 *
 * <p><b>Why this matters / what would break it:</b> the three GSON registries (catalog, db, table)
 * must migrate atomically. If any one of the 7 legacy tags is left unmapped, replaying an image
 * from a cluster that had a paimon catalog would fail. The table tag is special (D-042): paimon
 * supports MVCC/MTMV/time-travel, so {@code PaimonExternalTable} must replay as the MVCC variant,
 * not the base {@code PluginDrivenExternalTable} — replaying as the base would silently downgrade a
 * persisted paimon table and lose the MVCC behavior on every FE restart.</p>
 *
 * <p>Each case round-trips a valid PluginDriven object through GSON, rewrites only the "clazz"
 * discriminator to the legacy tag (faithfully reproducing old-image bytes without depending on the
 * soon-to-be-deleted Paimon* classes), then deserializes and asserts the resolved runtime class.</p>
 */
public class PaimonGsonCompatReplayTest {

    private static String swapClazz(String json, String currentTag, String legacyTag) {
        String needle = "\"clazz\":\"" + currentTag + "\"";
        // Sanity: the polymorphic serialization must emit the discriminator we are about to rewrite.
        Assertions.assertTrue(json.contains(needle),
                "expected discriminator " + needle + " in serialized json: " + json);
        return json.replace(needle, "\"clazz\":\"" + legacyTag + "\"");
    }

    @Test
    public void testLegacyPaimonCatalogTagsReplayAsPluginDriven() {
        Map<String, String> props = Maps.newHashMap();
        props.put("type", "paimon");
        // 6-arg ctor sets logType=PLUGIN and a non-null catalogProperty, so gsonPostProcess replays
        // cleanly (the legacy-logType backfill branch is skipped and setDefaultPropsIfMissing has a
        // catalogProperty to write into).
        PluginDrivenExternalCatalog catalog =
                new PluginDrivenExternalCatalog(1L, "pmn_ctl", "", props, "c", null);
        String baseJson = GsonUtils.GSON.toJson(catalog, CatalogIf.class);

        // All 5 paimon catalog flavors persisted by a pre-cutover FE.
        String[] legacyTags = {
                "PaimonExternalCatalog",
                "PaimonHMSExternalCatalog",
                "PaimonFileExternalCatalog",
                "PaimonRestExternalCatalog",
                "PaimonDLFExternalCatalog",
        };
        for (String tag : legacyTags) {
            String json = swapClazz(baseJson, "PluginDrivenExternalCatalog", tag);
            // MUTATION: removing the registerCompatibleSubtype for this flavor throws
            // "cannot deserialize ... subtype named <tag>" here; a wrong target class fails instanceof.
            CatalogIf<?> restored = GsonUtils.GSON.fromJson(json, CatalogIf.class);
            Assertions.assertTrue(restored instanceof PluginDrivenExternalCatalog,
                    "legacy edit-log tag '" + tag
                            + "' must replay as PluginDrivenExternalCatalog (no crash/ClassCastException)");
        }
    }

    @Test
    public void testLegacyPaimonDatabaseTagReplaysAsPluginDriven() {
        PluginDrivenExternalDatabase db = new PluginDrivenExternalDatabase();
        db.id = 2L;
        db.name = "pmn_db";
        String baseJson = GsonUtils.GSON.toJson(db, DatabaseIf.class);

        String json = swapClazz(baseJson, "PluginDrivenExternalDatabase", "PaimonExternalDatabase");
        // MUTATION: dropping the db registerCompatibleSubtype makes this throw on deserialize.
        DatabaseIf<?> restored = GsonUtils.GSON.fromJson(json, DatabaseIf.class);
        Assertions.assertTrue(restored instanceof PluginDrivenExternalDatabase,
                "legacy 'PaimonExternalDatabase' tag must replay as PluginDrivenExternalDatabase");
    }

    @Test
    public void testLegacyPaimonTableTagReplaysAsMvccPluginDriven() {
        PluginDrivenMvccExternalTable table = new PluginDrivenMvccExternalTable();
        table.id = 3L;
        table.name = "pmn_tbl";
        table.dbName = "pmn_db";
        String baseJson = GsonUtils.GSON.toJson(table, TableIf.class);

        String json = swapClazz(baseJson, "PluginDrivenMvccExternalTable", "PaimonExternalTable");
        TableIf restored = GsonUtils.GSON.fromJson(json, TableIf.class);
        // D-042: paimon tables must replay as the MVCC variant. instanceof would also pass for a
        // subclass, so assert the EXACT class to catch a mistaken mapping to the base table.
        Assertions.assertSame(PluginDrivenMvccExternalTable.class, restored.getClass(),
                "legacy 'PaimonExternalTable' tag must replay as PluginDrivenMvccExternalTable (D-042),"
                        + " not the base PluginDrivenExternalTable");
    }
}
