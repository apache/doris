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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundConnectorTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand.ConnectorSourceSnapshot;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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

    @Test
    void copyOnWriteDeleteRequiresOneConsistentSnapshot() {
        ConnectorTableHandle target = Mockito.mock(ConnectorTableHandle.class);
        ConnectorTableHandle changed = Mockito.mock(ConnectorTableHandle.class);

        Assertions.assertSame(target,
                InsertOverwriteTableCommand.requireConsistentConnectorOverwriteSnapshot(
                        target, List.of(target, target)));
        Assertions.assertThrows(AnalysisException.class,
                () -> InsertOverwriteTableCommand.requireConsistentConnectorOverwriteSnapshot(
                        target, List.of(changed)));
    }

    @Test
    void connectorSourceSnapshotIncludesSchemaAndPartitionVersions() {
        OlapTable table = Mockito.mock(OlapTable.class);
        Partition partition = Mockito.mock(Partition.class);
        Column column = new Column("id", Type.BIGINT);
        Mockito.when(table.getId()).thenReturn(7L);
        Mockito.when(table.getFullSchema()).thenReturn(List.of(column));
        Mockito.when(table.getPartitions()).thenReturn(List.of(partition));
        Mockito.when(partition.getId()).thenReturn(11L);
        Mockito.when(partition.getVisibleVersion()).thenReturn(3L, 3L, 4L, 4L);

        ConnectorSourceSnapshot first = InsertOverwriteTableCommand.snapshotConnectorSource(table);
        ConnectorSourceSnapshot unchanged = InsertOverwriteTableCommand.snapshotConnectorSource(table);
        ConnectorSourceSnapshot advanced = InsertOverwriteTableCommand.snapshotConnectorSource(table);
        column.setName("renamed_id");
        ConnectorSourceSnapshot schemaChanged = InsertOverwriteTableCommand.snapshotConnectorSource(table);

        Assertions.assertEquals(first, unchanged);
        Assertions.assertNotEquals(first, advanced);
        Assertions.assertNotEquals(advanced, schemaChanged);
        Mockito.verify(table, Mockito.times(4)).readLock();
        Mockito.verify(table, Mockito.times(4)).readUnlock();
    }

    @Test
    void copyOnWriteHandleResolutionRestoresPluginClassLoaderOnSuccessAndFailure() {
        UnboundConnectorTableSink<?> sink = Mockito.mock(UnboundConnectorTableSink.class);
        Mockito.when(sink.getDMLCommandType()).thenReturn(DMLCommandType.DELETE);
        InsertOverwriteTableCommand command = new InsertOverwriteTableCommand(
                sink, Optional.empty(), Optional.empty(), Optional.empty());
        NereidsPlanner planner = Mockito.mock(NereidsPlanner.class);
        Mockito.when(planner.getScanNodes()).thenReturn(List.of());
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Connector connector = Mockito.mock(Connector.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(table.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        Mockito.when(session.getStatementScope()).thenReturn(ConnectorStatementScope.NONE);
        Mockito.when(session.getUser()).thenReturn("test");
        Mockito.when(connector.getMetadata(session)).thenAnswer(invocation -> {
            Assertions.assertSame(connector.getClass().getClassLoader(),
                    Thread.currentThread().getContextClassLoader());
            return metadata;
        });
        Mockito.when(table.resolveConnectorTableHandle(session, metadata)).thenAnswer(invocation -> {
            Assertions.assertSame(connector.getClass().getClassLoader(),
                    Thread.currentThread().getContextClassLoader());
            return Optional.of(handle);
        });

        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        ClassLoader callerClassLoader = new ClassLoader(previous) { };
        try {
            Thread.currentThread().setContextClassLoader(callerClassLoader);
            Optional<ConnectorTableHandle> resolved = Deencapsulation.invoke(command,
                    "resolveConnectorOverwriteBaseHandle", planner, table);
            Assertions.assertSame(handle, resolved.orElseThrow());
            Assertions.assertSame(callerClassLoader, Thread.currentThread().getContextClassLoader());

            Mockito.doAnswer(invocation -> {
                Assertions.assertSame(connector.getClass().getClassLoader(),
                        Thread.currentThread().getContextClassLoader());
                throw new IllegalStateException("table resolution failed");
            }).when(table).resolveConnectorTableHandle(session, metadata);
            Assertions.assertThrows(IllegalStateException.class, () -> Deencapsulation.invoke(command,
                    "resolveConnectorOverwriteBaseHandle", planner, table));
            Assertions.assertSame(callerClassLoader, Thread.currentThread().getContextClassLoader());
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    private static InsertOverwriteTableCommand newCommand() {
        // allowInsertOverwrite is field-independent; a minimal command (mock query plan) suffices.
        return new InsertOverwriteTableCommand(
                Mockito.mock(LogicalPlan.class), Optional.empty(), Optional.empty(), Optional.empty());
    }

    /**
     * A PluginDrivenExternalTable whose connector reports {@code supportedWriteOperations()} containing
     * (or omitting) {@code OVERWRITE}, stubbing the exact catalog -> connector chain the production gate walks.
     */
    private static PluginDrivenExternalTable pluginTable(boolean supported) {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Set<WriteOperation> ops = supported ? EnumSet.of(WriteOperation.OVERWRITE) : EnumSet.noneOf(WriteOperation.class);
        // The OVERWRITE gate now probes the per-handle write ops via the table helper; stub it directly.
        Mockito.when(table.connectorSupportedWriteOperations()).thenReturn(ops);
        return table;
    }

    /**
     * A PluginDrivenExternalTable whose connector reports {@code supportsWriteBranch()==supported},
     * stubbing the exact catalog -> connector chain the @branch gate walks.
     */
    private static PluginDrivenExternalTable pluginTableForWriteBranch(boolean supported) {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        // The @branch gate now probes the per-handle capability via the table helper; stub it directly.
        Mockito.when(table.connectorSupportsWriteBranch()).thenReturn(supported);
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

    @Test
    public void testWriteBranchAllowedForBranchCapablePluginDrivenTable() {
        // INSERT OVERWRITE t@branch: post-cutover an iceberg table is plugin-driven (generic sink, not
        // PhysicalIcebergTableSink), so the @branch guard admits it via the connector capability. Without
        // this, a branch overwrite is rejected post-flip even though the connector threads the branch.
        // Mutation guard: dropping the production `&& !pluginConnectorSupportsWriteBranch(...)` arm makes
        // this probe irrelevant; flipping it to false here would (in production) wrongly reject -> red.
        boolean supported = Deencapsulation.invoke(newCommand(),
                "pluginConnectorSupportsWriteBranch", pluginTableForWriteBranch(true));
        Assertions.assertTrue(supported,
                "a branch-capable plugin-driven table (iceberg) must be admitted for INSERT OVERWRITE @branch");
    }

    @Test
    public void testWriteBranchRejectedForNonBranchCapablePluginDrivenTable() {
        // A plugin-driven table whose connector does NOT support branch writes (jdbc/maxcompute) MUST be
        // rejected (fail loud), NOT admitted to silently drop the branch and overwrite the default ref.
        // Mutation guard: dropping the `&& supportsWriteBranch()` chain -> returns true -> red.
        boolean supported = Deencapsulation.invoke(newCommand(),
                "pluginConnectorSupportsWriteBranch", pluginTableForWriteBranch(false));
        Assertions.assertFalse(supported,
                "a plugin-driven table whose connector lacks write-branch support must be rejected");
    }

    @Test
    public void testWriteBranchRejectedForNonPluginTableType() {
        // A non-plugin table type must short-circuit to false (the helper's instanceof guard), proving the
        // probe targets a specific arm rather than admitting every table.
        boolean supported = Deencapsulation.invoke(newCommand(),
                "pluginConnectorSupportsWriteBranch", Mockito.mock(TableIf.class));
        Assertions.assertFalse(supported,
                "a non-plugin table type must NOT be treated as write-branch capable");
    }
}
