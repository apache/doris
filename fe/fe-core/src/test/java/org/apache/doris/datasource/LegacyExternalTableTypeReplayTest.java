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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Guards the OTHER half of edit-log compatibility for the SPI cutover. The {@code *GsonCompatReplayTest}
 * family covers the persisted {@code "clazz"} discriminator; this covers the persisted {@code "type"} value,
 * which is a {@link TableType} enum name serialized as a bare string.
 *
 * <p><b>Why this test exists:</b> the per-data-source {@code TableType} constants (HMS_EXTERNAL_TABLE,
 * ICEBERG_EXTERNAL_TABLE, ...) were deleted so that fe-core holds no data source names, but an image written
 * by a pre-cutover FE still contains those strings. Deleting a persisted enum constant is only safe because
 * of a two-step property that nothing in the compiler enforces: GSON returns {@code null} (rather than
 * throwing) for an enum name it does not know, and {@link PluginDrivenExternalTable#gsonPostProcess()} then
 * normalizes whatever it got to {@code PLUGIN_EXTERNAL_TABLE}. If either step regresses, the failure is not
 * a compile error but an FE that replays a persisted external table with a {@code null} type and NPEs (or
 * silently misroutes it) on the next query — so it must be pinned by a test.
 *
 * <p>MUTATION: dropping the type normalization in {@code gsonPostProcess} leaves the restored type
 * {@code null} -&gt; red for every legacy name below.</p>
 */
public class LegacyExternalTableTypeReplayTest {

    /**
     * Every per-source table type a pre-cutover FE could have written into an external table's {@code type}
     * field, and which this branch no longer declares. Deliberately spelled as string literals: these names
     * must NOT come back as constants just to keep a test compiling.
     */
    private static final String[] DELETED_LEGACY_TYPES = {
            "HMS_EXTERNAL_TABLE",
            "ES_EXTERNAL_TABLE",
            "JDBC_EXTERNAL_TABLE",
            "ICEBERG_EXTERNAL_TABLE",
            "PAIMON_EXTERNAL_TABLE",
            "MAX_COMPUTE_EXTERNAL_TABLE",
            "HUDI_EXTERNAL_TABLE",
            "TRINO_CONNECTOR_EXTERNAL_TABLE",
            "LAKESOUl_EXTERNAL_TABLE",
    };

    @Test
    public void deletedLegacyTypeNamesReplayAsPluginExternalTable() {
        for (String legacyType : DELETED_LEGACY_TYPES) {
            // Both targets of the compatible-subtype registry: the base class (es/jdbc/maxcompute/trino/
            // lakesoul migrate here) and the MVCC variant (hms/iceberg/paimon migrate here).
            assertReplaysAsPluginType(new PluginDrivenExternalTable(), legacyType);
            assertReplaysAsPluginType(new PluginDrivenMvccExternalTable(), legacyType);
        }
    }

    /**
     * Round-trips a valid plugin-driven table through GSON, rewrites only the persisted {@code "type"} value
     * to a legacy name (faithfully reproducing old-image bytes without depending on the deleted constants),
     * then asserts the replay both survives and lands on {@code PLUGIN_EXTERNAL_TABLE}.
     */
    private void assertReplaysAsPluginType(PluginDrivenExternalTable table, String legacyType) {
        table.id = 7L;
        table.name = "legacy_tbl";
        table.dbName = "legacy_db";
        table.type = TableType.PLUGIN_EXTERNAL_TABLE;

        String needle = "\"type\":\"PLUGIN_EXTERNAL_TABLE\"";
        String json = GsonUtils.GSON.toJson(table, TableIf.class);
        // Sanity: the type must actually be persisted, or rewriting it below would prove nothing.
        Assertions.assertTrue(json.contains(needle),
                "expected " + needle + " in serialized json: " + json);
        String legacyJson = json.replace(needle, "\"type\":\"" + legacyType + "\"");

        TableIf restored = GsonUtils.GSON.fromJson(legacyJson, TableIf.class);
        Assertions.assertSame(table.getClass(), restored.getClass(),
                "a legacy '" + legacyType + "' type value must not change which class replays");
        Assertions.assertEquals(TableType.PLUGIN_EXTERNAL_TABLE, restored.getType(),
                "a table persisted as '" + legacyType + "' must replay as PLUGIN_EXTERNAL_TABLE, not null");
    }
}
