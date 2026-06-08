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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

/**
 * Tests for {@link InsertOverwriteTableCommand}'s {@code allowInsertOverwrite} type gate
 * (FIX-OVERWRITE-GATE).
 *
 * <p><b>Why this matters:</b> after the MaxCompute SPI cutover, a MaxCompute table is a
 * {@link PluginDrivenExternalTable} (TableType.PLUGIN_EXTERNAL_TABLE), no longer a
 * {@code MaxComputeExternalTable}. The pre-fix gate only allow-listed
 * OlapTable/RemoteDoris/HMS/Iceberg/MaxCompute, so {@code run()} rejected the whole command before the
 * (already-wired) lower OVERWRITE machinery could run. The fix adds a {@code PluginDrivenExternalTable}
 * arm, but <b>gated on the connector's {@code supportsInsertOverwrite()} capability</b>: all SPI
 * connectors (jdbc/es/trino/max_compute) are {@code PluginDrivenExternalTable}, but only some honor
 * overwrite. A bare {@code instanceof} would admit jdbc (which silently degrades OVERWRITE to a plain
 * INSERT) — so the capability gate is the regression guard. These tests lock all three behaviors:
 * overwrite-capable plugin table allowed, non-overwrite-capable plugin table rejected, and unsupported
 * table types still rejected.</p>
 */
public class InsertOverwriteTableCommandTest {

    private static InsertOverwriteTableCommand newCommand() {
        // allowInsertOverwrite is field-independent; a minimal command (mock query plan) suffices.
        return new InsertOverwriteTableCommand(
                Mockito.mock(LogicalPlan.class), Optional.empty(), Optional.empty(), Optional.empty());
    }

    /**
     * A PluginDrivenExternalTable whose connector reports {@code supportsInsertOverwrite()==supported},
     * stubbing the exact catalog -> connector -> metadata chain the production gate walks.
     */
    private static PluginDrivenExternalTable pluginTable(boolean supported) {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Connector connector = Mockito.mock(Connector.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(connector.getMetadata(session)).thenReturn(metadata);
        Mockito.when(metadata.supportsInsertOverwrite()).thenReturn(supported);
        return table;
    }

    @Test
    public void testAllowInsertOverwriteForOverwriteCapablePluginDrivenTable() {
        // An overwrite-capable connector (e.g. MaxCompute) MUST pass the gate, otherwise INSERT
        // OVERWRITE throws before reaching the connector sink machinery.
        // Mutation guard: removing the production PluginDrivenExternalTable arm makes this fall
        // through to false -> assertion red.
        boolean allowed = Deencapsulation.invoke(newCommand(), "allowInsertOverwrite", pluginTable(true));
        Assertions.assertTrue(allowed,
                "an overwrite-capable plugin-driven table (e.g. MaxCompute) must be allowed for INSERT OVERWRITE");
    }

    @Test
    public void testDisallowInsertOverwriteForNonOverwriteCapablePluginDrivenTable() {
        // A plugin-driven table whose connector does NOT support overwrite (e.g. jdbc) MUST be
        // rejected at the gate (fail loud), NOT admitted to silently degrade OVERWRITE to a plain
        // INSERT. This is the regression guard.
        // Mutation guard: dropping the `&& supportsInsertOverwrite(...)` from the production gate
        // makes this return true -> assertion red.
        boolean allowed = Deencapsulation.invoke(newCommand(), "allowInsertOverwrite", pluginTable(false));
        Assertions.assertFalse(allowed,
                "a plugin-driven table whose connector does not support overwrite must be rejected, not silently degraded");
    }

    @Test
    public void testDisallowInsertOverwriteForUnsupportedTableType() {
        // A table type in none of the allow-listed arms must still be rejected, proving the fix
        // added a specific arm rather than loosening the gate to admit everything.
        boolean allowed = Deencapsulation.invoke(newCommand(), "allowInsertOverwrite",
                Mockito.mock(TableIf.class));
        Assertions.assertFalse(allowed,
                "an unsupported table type must NOT be allowed for INSERT OVERWRITE");
    }
}
