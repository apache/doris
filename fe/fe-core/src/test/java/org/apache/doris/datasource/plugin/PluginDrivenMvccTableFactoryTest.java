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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

/**
 * Tests for the capability-selected table factory in {@link PluginDrivenExternalDatabase} and the
 * GSON registration of {@link PluginDrivenMvccExternalTable}.
 *
 * <p><b>Why these matter:</b> the factory is the ONLY place a connector's MVCC capability is turned
 * into the right table class. If it always built the base class, an MTMV over a Paimon table would
 * never see the MvccTable/MTMV hooks (no snapshot pinning, broken incremental refresh); if it always
 * built the subclass, plain jdbc/es/max_compute tables would acquire MTMV behavior they do not
 * support. The GSON test guards edit-log durability: a persisted MVCC table must deserialize back as
 * the SAME subclass, otherwise an FE restart would silently downgrade it to the base table and lose
 * the MVCC behavior on replay.</p>
 */
public class PluginDrivenMvccTableFactoryTest {

    // ==================== factory: capability selects the subclass ====================

    @Test
    public void testBuildsMvccTableWhenConnectorDeclaresMvccCapability() {
        PluginDrivenExternalDatabase db = new PluginDrivenExternalDatabase();
        PluginDrivenExternalCatalog catalog = catalogWithCapabilities(
                ConnectorCapability.SUPPORTS_MVCC_SNAPSHOT);

        ExternalTable table = db.buildTableInternal("rt", "lt", 1L, catalog, db);

        // MUTATION: always returning the base class (dropping the capability branch) makes this red.
        Assertions.assertTrue(table instanceof PluginDrivenMvccExternalTable,
                "an MVCC-capable connector must yield the MVCC/MTMV subclass");
    }

    @Test
    public void testBuildsBaseTableWhenConnectorLacksMvccCapability() {
        PluginDrivenExternalDatabase db = new PluginDrivenExternalDatabase();
        // jdbc/es/max_compute/trino-connector advertise no MVCC capability.
        PluginDrivenExternalCatalog catalog = catalogWithCapabilities(
                ConnectorCapability.SUPPORTS_VIEW);

        ExternalTable table = db.buildTableInternal("rt", "lt", 1L, catalog, db);

        // MUTATION: always returning the subclass makes this red — a non-MVCC connector must NOT get
        // MTMV behavior. instanceof would still pass on a subclass, so assert the EXACT class.
        Assertions.assertSame(PluginDrivenExternalTable.class, table.getClass(),
                "a connector without SUPPORTS_MVCC_SNAPSHOT must keep the base PluginDrivenExternalTable");
    }

    @Test
    public void testBuildsBaseTableWhenConnectorIsNull() {
        PluginDrivenExternalDatabase db = new PluginDrivenExternalDatabase();
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(null);

        ExternalTable table = db.buildTableInternal("rt", "lt", 1L, catalog, db);

        // MUTATION: a missing null-guard (NPE on getCapabilities) makes this red. Lazy-init catalogs
        // whose connector is not yet built must fall back to the base class, not crash.
        Assertions.assertSame(PluginDrivenExternalTable.class, table.getClass(),
                "a not-yet-built connector must degrade to the base table, never NPE");
    }

    // ==================== GSON: MVCC subclass survives a round-trip ====================

    @Test
    public void testMvccTableGsonRoundTripPreservesSubclass() {
        PluginDrivenMvccExternalTable table = new PluginDrivenMvccExternalTable();
        // Set only the GSON-serialized fields; catalog/db are not @SerializedName so they are not
        // persisted (and need not be set for a pure serialization round-trip). These fields are
        // declared protected in ExternalTable (a different package after the datasource split), so
        // they are injected reflectively rather than assigned directly.
        setExternalTableField(table, "id", 7L);
        setExternalTableField(table, "name", "mvcc_tbl");
        setExternalTableField(table, "remoteName", "REMOTE_MVCC_TBL");
        setExternalTableField(table, "dbName", "mvcc_db");

        // Round-trip through the TableIf hierarchy so the polymorphic "clazz" discriminator is used.
        String json = GsonUtils.GSON.toJson(table, TableIf.class);
        TableIf restored = GsonUtils.GSON.fromJson(json, TableIf.class);

        // MUTATION: omitting the registerSubtype(PluginDrivenMvccExternalTable) makes serialization
        // throw "subtype not registered", failing this test. A wrong registration (e.g. tagging it as
        // the base class) would deserialize to PluginDrivenExternalTable and fail the instanceof.
        Assertions.assertTrue(restored instanceof PluginDrivenMvccExternalTable,
                "a persisted MVCC table must deserialize back as PluginDrivenMvccExternalTable");
        Assertions.assertEquals(7L, restored.getId());
        Assertions.assertEquals("mvcc_tbl", restored.getName());
    }

    // ==================== helpers ====================

    private static void setExternalTableField(ExternalTable table, String field, Object value) {
        try {
            java.lang.reflect.Field f = ExternalTable.class.getDeclaredField(field);
            f.setAccessible(true);
            f.set(table, value);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("failed to set ExternalTable." + field, e);
        }
    }

    private static PluginDrivenExternalCatalog catalogWithCapabilities(ConnectorCapability... caps) {
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getCapabilities()).thenReturn(
                caps.length == 0 ? Collections.emptySet() : Sets.newHashSet(caps));
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        return catalog;
    }
}
