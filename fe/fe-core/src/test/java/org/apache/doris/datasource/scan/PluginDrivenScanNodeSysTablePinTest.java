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

package org.apache.doris.datasource.scan;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenSysExternalTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

/**
 * Guards {@link PluginDrivenScanNode#resolveSysTableSnapshotPin()}, the P6.6-C1 (WS-PIN) sys-table
 * time-travel pin-feed.
 *
 * <p>WHY this matters: a {@code FOR TIME AS OF} / {@code @branch} / {@code @tag} query against a
 * plugin <b>system</b> table ({@link PluginDrivenSysExternalTable}) never materializes its query
 * snapshot into the {@code StatementContext} MVCC map — the sys table is NOT an {@code MvccTable}
 * and {@code BindRelation} short-circuits {@code loadSnapshots} for {@code $}-suffixed relations, so
 * the pin is keyed/looked-up under mismatched names and is starved. Without this fallback the sys
 * handle stays unpinned and {@code t$snapshots FOR TIME AS OF X} silently reads the LATEST metadata
 * (a correctness regression vs legacy {@code IcebergScanNode.createTableScan}, which honors the pin).
 * The fix resolves the pin directly off the SOURCE table's {@code MvccTable.loadSnapshot} so the
 * generic scan node's {@code applyMvccSnapshotPin} threads it onto the sys handle.</p>
 *
 * <p>Driven on a Mockito {@code CALLS_REAL_METHODS} mock (no constructor — building a full
 * {@link FileQueryScanNode} needs a harness this module lacks) with the three accessors
 * ({@code getTargetTable}, {@code getQueryTableSnapshot}, {@code getScanParams}) stubbed, so the real
 * resolution runs against controlled state. {@code resolveSysTableSnapshotPin} is package-private
 * exactly to enable this (mirrors the sibling guard/pin tests).
 *
 * <p>The fallback only ever runs for a pin-capable connector because {@code resolveSysTableSnapshotPin}
 * checks {@code sysTableSupportsTimeTravel()} ITSELF. It must not rely on
 * {@code checkSysTableScanConstraints} for that: despite what this javadoc used to claim, that guard runs
 * at split generation, long AFTER this resolution runs at init, so a non-capable connector (paimon) got
 * the source pin anyway and blew up before ever reaching it (CI 996541, paimon_system_table).</p>
 */
public class PluginDrivenScanNodeSysTablePinTest {

    private static PluginDrivenScanNode sysPinNode() {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        // Default: no time travel (plain scan). Time-travel cases override these.
        Mockito.doReturn(null).when(node).getQueryTableSnapshot();
        Mockito.doReturn(null).when(node).getScanParams();
        // Default: a pin-capable connector (iceberg). The real method would need a live scan provider;
        // sysTableSupportsTimeTravelFalseDoesNotPin overrides this to exercise the non-capable side.
        Mockito.doReturn(true).when(node).sysTableSupportsTimeTravel();
        return node;
    }

    @Test
    public void sysTableForTimeAsOfDelegatesToSourceLoadSnapshot() throws Exception {
        PluginDrivenScanNode node = sysPinNode();
        PluginDrivenSysExternalTable sysTable = Mockito.mock(PluginDrivenSysExternalTable.class);
        // PluginDrivenMvccExternalTable IS-A PluginDrivenExternalTable AND implements MvccTable.
        PluginDrivenMvccExternalTable source = Mockito.mock(PluginDrivenMvccExternalTable.class);
        MvccSnapshot resolved = Mockito.mock(MvccSnapshot.class);
        TableSnapshot ts = Mockito.mock(TableSnapshot.class);
        Mockito.doReturn(sysTable).when(node).getTargetTable();
        Mockito.doReturn(ts).when(node).getQueryTableSnapshot();
        Mockito.doReturn(source).when(sysTable).getSourceTable();
        Mockito.when(source.loadSnapshot(Optional.of(ts), Optional.empty())).thenReturn(resolved);

        // WHY: FOR TIME AS OF on a sys table resolves the pin off the source table. MUTATION: returning
        // Optional.empty() instead of the source fallback -> pin lost -> sys reads latest -> red.
        Optional<MvccSnapshot> result = node.resolveSysTableSnapshotPin();
        Assertions.assertTrue(result.isPresent(), "sys FOR TIME AS OF must resolve a pin off the source");
        Assertions.assertSame(resolved, result.get());
        Mockito.verify(source).loadSnapshot(Optional.of(ts), Optional.empty());
    }

    @Test
    public void sysTableBranchTagDelegatesScanParams() throws Exception {
        PluginDrivenScanNode node = sysPinNode();
        PluginDrivenSysExternalTable sysTable = Mockito.mock(PluginDrivenSysExternalTable.class);
        PluginDrivenMvccExternalTable source = Mockito.mock(PluginDrivenMvccExternalTable.class);
        MvccSnapshot resolved = Mockito.mock(MvccSnapshot.class);
        TableScanParams sp = Mockito.mock(TableScanParams.class);
        Mockito.doReturn(sysTable).when(node).getTargetTable();
        Mockito.doReturn(sp).when(node).getScanParams();
        Mockito.doReturn(source).when(sysTable).getSourceTable();
        Mockito.when(source.loadSnapshot(Optional.empty(), Optional.of(sp))).thenReturn(resolved);

        // WHY: t$files@branch('b') / @tag is a snapshot selector legacy iceberg honors for sys tables;
        // it arrives as scan-params, not a TableSnapshot. MUTATION: dropping the getScanParams()
        // threading (passing Optional.empty()) -> branch/tag pin lost -> red.
        Optional<MvccSnapshot> result = node.resolveSysTableSnapshotPin();
        Assertions.assertTrue(result.isPresent(), "sys @branch/@tag must resolve a pin off the source");
        Assertions.assertSame(resolved, result.get());
        Mockito.verify(source).loadSnapshot(Optional.empty(), Optional.of(sp));
    }

    @Test
    public void sysTablePlainScanDoesNotPin() throws Exception {
        PluginDrivenScanNode node = sysPinNode();
        PluginDrivenSysExternalTable sysTable = Mockito.mock(PluginDrivenSysExternalTable.class);
        PluginDrivenMvccExternalTable source = Mockito.mock(PluginDrivenMvccExternalTable.class);
        Mockito.doReturn(sysTable).when(node).getTargetTable();
        Mockito.doReturn(source).when(sysTable).getSourceTable();
        // No snapshot, no scan-params (sysPinNode default).

        // WHY: a plain sys scan (no time travel) must NOT resolve a pin — doing so would force an
        // unnecessary remote round-trip and pin nothing meaningful. MUTATION: removing the
        // "both null -> empty" short-circuit -> loadSnapshot is invoked -> red.
        Optional<MvccSnapshot> result = node.resolveSysTableSnapshotPin();
        Assertions.assertFalse(result.isPresent(), "plain sys scan must not pin");
        Mockito.verifyNoInteractions(source);
    }

    @Test
    public void normalTableNeverUsesSysFallback() throws Exception {
        PluginDrivenScanNode node = sysPinNode();
        // A NON-sys plugin table, even with a snapshot set: its pin comes from StatementContext, never
        // from this fallback.
        Mockito.doReturn(Mockito.mock(TableIf.class)).when(node).getTargetTable();
        Mockito.doReturn(Mockito.mock(TableSnapshot.class)).when(node).getQueryTableSnapshot();

        // WHY: the sys fallback is SYS-table only. MUTATION: widening the
        // instanceof PluginDrivenSysExternalTable check -> non-empty here -> red (pins the scope limit).
        Optional<MvccSnapshot> result = node.resolveSysTableSnapshotPin();
        Assertions.assertFalse(result.isPresent(), "normal-table pin must not flow through the sys fallback");
    }

    @Test
    public void sysTableWithNonMvccSourceDoesNotPin() throws Exception {
        PluginDrivenScanNode node = sysPinNode();
        PluginDrivenSysExternalTable sysTable = Mockito.mock(PluginDrivenSysExternalTable.class);
        // A source that is NOT an MvccTable (no time-travel capability).
        PluginDrivenExternalTable nonMvccSource = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.doReturn(sysTable).when(node).getTargetTable();
        Mockito.doReturn(Mockito.mock(TableSnapshot.class)).when(node).getQueryTableSnapshot();
        Mockito.doReturn(nonMvccSource).when(sysTable).getSourceTable();

        // WHY: defensive — a connector whose sys tables are not MVCC-capable is already rejected by the
        // guard, but if it ever reaches here we fall back to no-pin rather than ClassCastException.
        // MUTATION: dropping the instanceof MvccTable guard -> CCE on the cast -> red.
        Optional<MvccSnapshot> result = node.resolveSysTableSnapshotPin();
        Assertions.assertFalse(result.isPresent(), "non-MVCC source must not pin (no CCE)");
    }

    @Test
    public void sysTableSupportsTimeTravelFalseDoesNotPin() throws Exception {
        PluginDrivenScanNode node = sysPinNode();
        PluginDrivenSysExternalTable sysTable = Mockito.mock(PluginDrivenSysExternalTable.class);
        PluginDrivenMvccExternalTable source = Mockito.mock(PluginDrivenMvccExternalTable.class);
        Mockito.doReturn(sysTable).when(node).getTargetTable();
        Mockito.doReturn(Mockito.mock(TableSnapshot.class)).when(node).getQueryTableSnapshot();
        Mockito.doReturn(source).when(sysTable).getSourceTable();
        // A connector whose sys tables do NOT honor a pin (paimon, the default).
        Mockito.doReturn(false).when(node).sysTableSupportsTimeTravel();

        // WHY: the capability must be checked HERE, at init. checkSysTableScanConstraints owns the
        // user-facing "Plugin system tables do not support time travel." message but only runs at split
        // generation; pinning first hands the SOURCE's snapshot (and its non-null pinned schema) to a tuple
        // carrying the SYS table's columns, and the L17 guard / loadSnapshot's own RuntimeException fires
        // first, masking that message (CI 996541, paimon_system_table).
        // MUTATION: dropping the sysTableSupportsTimeTravel() gate -> a pin is resolved -> red.
        Optional<MvccSnapshot> result = node.resolveSysTableSnapshotPin();
        Assertions.assertFalse(result.isPresent(),
                "a connector that rejects sys-table time travel must not resolve a pin");
        Mockito.verify(source, Mockito.never()).loadSnapshot(Mockito.any(), Mockito.any());
    }
}
