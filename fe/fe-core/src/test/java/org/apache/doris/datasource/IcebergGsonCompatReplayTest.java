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
 * Guards the iceberg SPI cutover edit-log compatibility (mirrors {@link PaimonGsonCompatReplayTest}): an
 * FE image / edit log written by a pre-cutover version persisted iceberg catalogs/databases/tables under
 * their legacy class simple names (the GSON {@code "clazz"} discriminator). After the cutover those legacy
 * classes are no longer {@code registerSubtype}'d, so on replay the {@code registerCompatibleSubtype}
 * mappings in {@link GsonUtils} MUST redirect every legacy tag to the generic PluginDriven class — otherwise
 * the FE crashes on startup with a {@code JsonParseException} (tag not registered) or a downstream
 * {@code ClassCastException}.
 *
 * <p><b>Why this matters / what would break it:</b> the three GSON registries (catalog, db, table) must
 * migrate atomically. Iceberg has EIGHT catalog flavors persisted under distinct legacy tags; leaving any
 * one unmapped fails replay of an image from a cluster that had that flavor. The table tag is special:
 * iceberg exposes snapshots/time-travel (declares {@code SUPPORTS_MVCC_SNAPSHOT}), so a flipped iceberg
 * table is a {@code PluginDrivenMvccExternalTable}; therefore {@code IcebergExternalTable} MUST replay as
 * the MVCC variant, not the base {@code PluginDrivenExternalTable} — replaying as the base would silently
 * downgrade a persisted iceberg table and lose the MVCC behavior on every FE restart.</p>
 *
 * <p>Each case round-trips a valid PluginDriven object through GSON, rewrites only the {@code "clazz"}
 * discriminator to the legacy tag (faithfully reproducing old-image bytes without depending on the legacy
 * Iceberg* classes), then deserializes and asserts the resolved runtime class.</p>
 */
public class IcebergGsonCompatReplayTest {

    private static String swapClazz(String json, String currentTag, String legacyTag) {
        String needle = "\"clazz\":\"" + currentTag + "\"";
        // Sanity: the polymorphic serialization must emit the discriminator we are about to rewrite.
        Assertions.assertTrue(json.contains(needle),
                "expected discriminator " + needle + " in serialized json: " + json);
        return json.replace(needle, "\"clazz\":\"" + legacyTag + "\"");
    }

    @Test
    public void testLegacyIcebergCatalogTagsReplayAsPluginDriven() {
        Map<String, String> props = Maps.newHashMap();
        props.put("type", "iceberg");
        // 6-arg ctor sets logType=PLUGIN and a non-null catalogProperty, so gsonPostProcess replays cleanly.
        PluginDrivenExternalCatalog catalog =
                new PluginDrivenExternalCatalog(1L, "ice_ctl", "", props, "c", null);
        String baseJson = GsonUtils.GSON.toJson(catalog, CatalogIf.class);

        // All 8 iceberg catalog flavors persisted by a pre-cutover FE.
        String[] legacyTags = {
                "IcebergExternalCatalog",
                "IcebergHMSExternalCatalog",
                "IcebergGlueExternalCatalog",
                "IcebergRestExternalCatalog",
                "IcebergDLFExternalCatalog",
                "IcebergHadoopExternalCatalog",
                "IcebergJdbcExternalCatalog",
                "IcebergS3TablesExternalCatalog",
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
    public void testLegacyIcebergDatabaseTagReplaysAsPluginDriven() {
        PluginDrivenExternalDatabase db = new PluginDrivenExternalDatabase();
        db.id = 2L;
        db.name = "ice_db";
        String baseJson = GsonUtils.GSON.toJson(db, DatabaseIf.class);

        String json = swapClazz(baseJson, "PluginDrivenExternalDatabase", "IcebergExternalDatabase");
        // MUTATION: dropping the db registerCompatibleSubtype makes this throw on deserialize.
        DatabaseIf<?> restored = GsonUtils.GSON.fromJson(json, DatabaseIf.class);
        Assertions.assertTrue(restored instanceof PluginDrivenExternalDatabase,
                "legacy 'IcebergExternalDatabase' tag must replay as PluginDrivenExternalDatabase");
    }

    @Test
    public void testLegacyIcebergTableTagReplaysAsMvccPluginDriven() {
        PluginDrivenMvccExternalTable table = new PluginDrivenMvccExternalTable();
        table.id = 3L;
        table.name = "ice_tbl";
        table.dbName = "ice_db";
        String baseJson = GsonUtils.GSON.toJson(table, TableIf.class);

        String json = swapClazz(baseJson, "PluginDrivenMvccExternalTable", "IcebergExternalTable");
        TableIf restored = GsonUtils.GSON.fromJson(json, TableIf.class);
        // iceberg tables must replay as the MVCC variant. instanceof would also pass for a subclass, so
        // assert the EXACT class to catch a mistaken mapping to the base PluginDrivenExternalTable.
        Assertions.assertSame(PluginDrivenMvccExternalTable.class, restored.getClass(),
                "legacy 'IcebergExternalTable' tag must replay as PluginDrivenMvccExternalTable (iceberg is"
                        + " MVCC-capable), not the base PluginDrivenExternalTable");
    }
}
