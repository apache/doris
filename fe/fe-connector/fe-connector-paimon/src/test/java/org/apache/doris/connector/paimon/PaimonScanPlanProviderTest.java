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

package org.apache.doris.connector.paimon;

import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Tests for {@link PaimonScanPlanProvider#resolveTable}, pinning the transient-Table reload
 * fallback on the scan path (P5-T06). The scan path reads the handle's transient Paimon
 * {@link Table}, which becomes null after any Java serialization round-trip (cross-node /
 * plan-reuse); the reload mirrors the proven fallback in
 * {@link PaimonConnectorMetadata#getColumnHandles}.
 *
 * <p>Driven directly against {@code resolveTable} (package-private) rather than {@code planScan}
 * end-to-end: {@link FakePaimonTable#newReadBuilder()} throws, so the full scan cannot be driven
 * offline. The seam fully covers the remote {@code getTable} call, so each test uses a
 * {@link RecordingPaimonCatalogOps} fake and a {@code null} real catalog — entirely offline.
 */
public class PaimonScanPlanProviderTest {

    private static RowType rowType(String... columnNames) {
        RowType.Builder builder = RowType.builder();
        for (String name : columnNames) {
            builder.field(name, DataTypes.INT());
        }
        return builder.build();
    }

    @Test
    public void resolveTableReloadsWhenTransientTableNull() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        Table reloaded = new FakePaimonTable(
                "t1",
                rowType("id", "name"),
                Collections.emptyList(),
                Collections.emptyList());
        ops.table = reloaded;
        // A handle whose transient Table is null (e.g. after serialization across the FE/BE
        // boundary or plan reuse) — the scan path must reload via the seam rather than NPE.
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        Assertions.assertNull(handle.getPaimonTable(), "precondition: transient table is null");

        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), ops);
        Table table = provider.resolveTable(handle);

        // WHY: this is the serde-survival safety net. With a null transient Table, the scan path's
        // only way to read rowType()/serialize the table for BE is to re-fetch it from the catalog
        // seam. MUTATION: removing the `if (table == null) { table = catalogOps.getTable(id); }`
        // block -> returns null -> downstream NPE on table.rowType() -> red. The recorded getTable
        // call proves the reload happened.
        Assertions.assertSame(reloaded, table,
                "scan path must return the table reloaded from the seam when the transient ref is null");
        Assertions.assertTrue(ops.log.contains("getTable:db1.t1"),
                "reload-fallback must re-fetch the table from the seam when the transient ref is null");
    }

    @Test
    public void resolveTableUsesTransientWithoutReload() {
        RecordingPaimonCatalogOps ops = new RecordingPaimonCatalogOps();
        FakePaimonTable table = new FakePaimonTable(
                "t1",
                rowType("id", "name"),
                Collections.emptyList(),
                Collections.emptyList());
        PaimonTableHandle handle = new PaimonTableHandle(
                "db1", "t1", Collections.emptyList(), Collections.emptyList());
        handle.setPaimonTable(table);

        PaimonScanPlanProvider provider = new PaimonScanPlanProvider(Collections.emptyMap(), ops);
        Table resolved = provider.resolveTable(handle);

        // WHY: the fast path — when the transient Table is already present, resolveTable must use it
        // and NOT make a redundant remote getTable call. MUTATION: always reloading would record a
        // getTable entry -> red. This pins the reload as a fallback, not the default.
        Assertions.assertSame(table, resolved);
        Assertions.assertTrue(ops.log.isEmpty(),
                "with a present transient table, no remote getTable reload must happen");
    }
}
