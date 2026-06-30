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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.EnumSet;
import java.util.Set;

/**
 * Pins the system-table opt-out from Top-N lazy materialization on {@link PluginDrivenSysExternalTable}.
 *
 * <p>WHY this matters: a system/metadata table (e.g. {@code tbl$snapshots}) is served by the connector's JNI
 * serialized-split metadata reader, which synthesizes rows and produces no file+position row-id. Top-N lazy
 * materialization injects the engine-wide row-id slot ({@code __DORIS_GLOBAL_ROWID_COL__}) and expects the
 * scan to re-fetch survivors by row-id, so admitting a sys table makes BE abort with
 * {@code __DORIS_GLOBAL_ROWID_COL__... return column size 0 not equal to expected size 1}. Legacy never lazy-
 * materialized sys tables ({@code IcebergSysExternalTable} is absent from
 * {@code MaterializeProbeVisitor.SUPPORT_RELATION_TYPES}); the base {@link PluginDrivenExternalTable} keys the
 * capability off the connector alone, so the sys table must opt out itself.
 *
 * <p>Mockito {@code CALLS_REAL_METHODS} runs the real capability methods over a stubbed connector chain,
 * mirroring {@code PluginDrivenExternalTableTest}.
 */
public class PluginDrivenSysExternalTableTest {

    /**
     * A CALLS_REAL_METHODS {@link PluginDrivenSysExternalTable} whose connector declares exactly
     * {@code capabilities}, to exercise the capability-helper methods over the real connector chain. Only the
     * {@code catalog} field is set — the methods under test never touch the sys-table's source/name fields.
     */
    private static PluginDrivenSysExternalTable sysTableWithCapabilities(Set<ConnectorCapability> capabilities) {
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getCapabilities()).thenReturn(capabilities);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        PluginDrivenSysExternalTable table =
                Mockito.mock(PluginDrivenSysExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(table, "catalog", catalog);
        return table;
    }

    @Test
    public void systemTableNeverSupportsTopNLazyMaterializeEvenWhenConnectorDeclaresIt() {
        // The BE JNI metadata reader cannot produce the lazy-mat row-id for a synthesized sys-table row, so the
        // sys table must opt out of Top-N lazy materialization even though its connector declares the
        // capability. MUTATION: deleting the override re-inherits the connector-capability answer -> true -> red.
        Assertions.assertFalse(sysTableWithCapabilities(
                        EnumSet.of(ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE)).supportsTopNLazyMaterialize(),
                "a system/metadata table must never lazy-materialize, even when the connector supports it");
    }

    @Test
    public void systemTableStillSupportsNestedColumnPruneFromConnectorCapability() {
        // Parity guard: legacy DOES prune nested columns on sys tables (LogicalFileScan.supportPruneNestedColumn
        // lists IcebergSysExternalTable), so the sys-table lazy-mat opt-out must NOT bleed into nested prune.
        // MUTATION: an over-broad opt-out that also disables nested prune turns this red.
        Assertions.assertTrue(sysTableWithCapabilities(
                        EnumSet.of(ConnectorCapability.SUPPORTS_NESTED_COLUMN_PRUNE)).supportsNestedColumnPrune(),
                "the lazy-mat opt-out must not disable nested-column prune, which legacy keeps on for sys tables");
    }
}
