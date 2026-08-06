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

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccSnapshot;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Guards {@link PluginDrivenSysExternalTable#resolveScanPin}, the system table's own pin resolution.
 *
 * <p>A system table is not an {@code MvccTable}, and {@code BindRelation} returns from
 * {@code handleMetaTable} BEFORE {@code StatementContext.loadSnapshots}, so the statement's MVCC map never
 * holds an entry for it. Everything that needs this reference's version — the bound output schema
 * ({@code LogicalFileScan.computePluginDrivenOutput}), the scan's snapshot
 * ({@code PluginDrivenScanNode.resolveSysTableSnapshotPin}) and its column projection
 * ({@code PluginDrivenScanNode.buildColumnHandles}) — comes through here.
 *
 * <p>The memoization is the load-bearing part, not an optimization: a MUTABLE selector
 * ({@code scan.mode=latest}, a wall-clock {@code scan.timestamp-millis}) is re-evaluated against the LIVE
 * table on every {@code loadSnapshot}, so resolving once per consumer would let a commit landing between
 * binding and scanning give them different versions — the exact bind-vs-scan schema skew this whole path
 * exists to close.
 */
public class PluginDrivenSysExternalTableScanPinTest {

    private static PluginDrivenSysExternalTable sysTableOver(PluginDrivenExternalTable source) {
        Mockito.when(source.getName()).thenReturn("t");
        Mockito.when(source.getRemoteName()).thenReturn("t");
        // ExternalTable's ctor dereferences both (db.getFullName(), catalog.getId()) to build the
        // NameMapping, so they must be present even though this test never exercises either.
        ExternalCatalog catalog = Mockito.mock(ExternalCatalog.class);
        ExternalDatabase<?> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getFullName()).thenReturn("db");
        Mockito.when(db.getRemoteName()).thenReturn("db");
        Mockito.when(source.getCatalog()).thenReturn(catalog);
        Mockito.doReturn(db).when(source).getDb();
        PluginDrivenSysExternalTable sysTable =
                Mockito.spy(new PluginDrivenSysExternalTable(source, "audit_log"));
        // The real method needs a live connector; the capability itself is covered by the scan-node guard
        // suite. Default to "connector honors it" and let the declining case override.
        Mockito.doReturn(true).when(sysTable).selectorSupported(Mockito.any());
        return sysTable;
    }

    private static TableScanParams options(String key, String value) {
        return new TableScanParams(TableScanParams.OPTIONS,
                ImmutableMap.of(key, value), Collections.emptyList());
    }

    @Test
    public void resolvesThePinOffTheSourceTable() {
        PluginDrivenMvccExternalTable source = Mockito.mock(PluginDrivenMvccExternalTable.class);
        PluginDrivenSysExternalTable sysTable = sysTableOver(source);
        MvccSnapshot resolved = Mockito.mock(MvccSnapshot.class);
        TableScanParams sp = options("scan.tag-name", "top_cp0");
        Mockito.when(source.loadSnapshot(Optional.empty(), Optional.of(sp))).thenReturn(resolved);

        // WHY: the pin lives on the SOURCE table, and the selector must reach it verbatim.
        // MUTATION: passing Optional.empty() for scanParams -> the stub never matches -> red.
        Optional<MvccSnapshot> pin = sysTable.resolveScanPin(Optional.empty(), Optional.of(sp));

        Assertions.assertTrue(pin.isPresent(), "an @options selector must resolve a pin off the source");
        Assertions.assertSame(resolved, pin.get());
    }

    @Test
    public void resolvesOncePerSelectorSoBindAndScanCannotDisagree() {
        PluginDrivenMvccExternalTable source = Mockito.mock(PluginDrivenMvccExternalTable.class);
        PluginDrivenSysExternalTable sysTable = sysTableOver(source);
        TableScanParams sp = options("scan.mode", "latest");
        Mockito.when(source.loadSnapshot(Mockito.any(), Mockito.any()))
                .thenReturn(Mockito.mock(MvccSnapshot.class));

        // Two DISTINCT but equal-valued selector objects, because binding and scanning each hand in their
        // own instance. TableScanParams defines no value equality and no toString(), so a memo keyed on
        // the objects (or on their toString) would miss and resolve twice.
        Optional<MvccSnapshot> first = sysTable.resolveScanPin(
                Optional.empty(), Optional.of(sp));
        Optional<MvccSnapshot> second = sysTable.resolveScanPin(
                Optional.empty(), Optional.of(options("scan.mode", "latest")));

        // WHY: a mutable selector resolved twice can straddle a commit, handing the bound schema and the
        // scanned data different versions. MUTATION: keying the memo on the selector object identity (or
        // dropping the memo) -> loadSnapshot runs twice -> red.
        Assertions.assertSame(first.get(), second.get(), "both consumers must see ONE resolution");
        Mockito.verify(source, Mockito.times(1)).loadSnapshot(Mockito.any(), Mockito.any());
    }

    @Test
    public void statementScopedLoaderSeedsTheSystemTableMemo() {
        PluginDrivenMvccExternalTable source = Mockito.mock(PluginDrivenMvccExternalTable.class);
        PluginDrivenSysExternalTable sysTable = sysTableOver(source);
        TableScanParams sp = options("scan.mode", "latest");
        MvccSnapshot statementPin = Mockito.mock(MvccSnapshot.class);
        AtomicInteger statementLoads = new AtomicInteger();

        Optional<MvccSnapshot> first = sysTable.resolveScanPin(
                Optional.empty(), Optional.of(sp), () -> {
                    statementLoads.incrementAndGet();
                    return Optional.of(statementPin);
                });
        Optional<MvccSnapshot> second = sysTable.resolveScanPin(
                Optional.empty(), Optional.of(options("scan.mode", "latest")));

        Assertions.assertSame(statementPin, first.orElse(null));
        Assertions.assertSame(statementPin, second.orElse(null));
        Assertions.assertEquals(1, statementLoads.get());
        Mockito.verify(source, Mockito.never()).loadSnapshot(Mockito.any(), Mockito.any());
    }

    @Test
    public void rowCountUsesTheMemoizedSystemTablePin() {
        long[] snapshotIds = {7L, -1L};
        long[] expectedRows = {19L, 0L};
        for (int i = 0; i < snapshotIds.length; i++) {
            PluginDrivenMvccExternalTable source = Mockito.mock(PluginDrivenMvccExternalTable.class);
            PluginDrivenSysExternalTable sysTable = sysTableOver(source);
            PluginDrivenMvccSnapshot pin = Mockito.mock(PluginDrivenMvccSnapshot.class);
            ConnectorMvccSnapshot connectorSnapshot = ConnectorMvccSnapshot.builder()
                    .snapshotId(snapshotIds[i]).build();
            Mockito.when(pin.getConnectorSnapshot()).thenReturn(connectorSnapshot);
            Mockito.doReturn(expectedRows[i]).when(sysTable)
                    .fetchRowCountAtSnapshot(connectorSnapshot);
            sysTable.resolveScanPin(Optional.empty(),
                    Optional.of(options("scan.snapshot-id", String.valueOf(snapshotIds[i]))),
                    () -> Optional.of(pin));

            Assertions.assertEquals(expectedRows[i], sysTable.getRowCount(),
                    "positive and empty system pins must both use snapshot-aware statistics");
            Mockito.verify(sysTable).fetchRowCountAtSnapshot(connectorSnapshot);
        }
    }

    @Test
    public void differentSelectorsResolveIndependently() {
        PluginDrivenMvccExternalTable source = Mockito.mock(PluginDrivenMvccExternalTable.class);
        PluginDrivenSysExternalTable sysTable = sysTableOver(source);
        Mockito.when(source.loadSnapshot(Mockito.any(), Mockito.any()))
                .thenReturn(Mockito.mock(MvccSnapshot.class));

        // WHY: the memo separates pins the way StatementContext's version key does, so one statement can
        // read the same view at two versions. MUTATION: keying on the table alone -> the second selector
        // reuses the first's pin -> only one loadSnapshot -> red.
        sysTable.resolveScanPin(Optional.empty(), Optional.of(options("scan.tag-name", "a")));
        sysTable.resolveScanPin(Optional.empty(), Optional.of(options("scan.tag-name", "b")));

        Mockito.verify(source, Mockito.times(2)).loadSnapshot(Mockito.any(), Mockito.any());
    }

    @Test
    public void noSelectorDoesNotPin() {
        PluginDrivenMvccExternalTable source = Mockito.mock(PluginDrivenMvccExternalTable.class);
        PluginDrivenSysExternalTable sysTable = sysTableOver(source);

        // WHY: a plain sys scan must not pay a remote round-trip to pin nothing. MUTATION: removing the
        // both-absent short-circuit -> loadSnapshot invoked -> red.
        Assertions.assertFalse(
                sysTable.resolveScanPin(Optional.empty(), Optional.empty()).isPresent(),
                "a plain sys scan must not pin");
        Mockito.verify(source, Mockito.never()).loadSnapshot(Mockito.any(), Mockito.any());
    }

    @Test
    public void connectorDecliningTheSelectorDoesNotPin() {
        PluginDrivenMvccExternalTable source = Mockito.mock(PluginDrivenMvccExternalTable.class);
        PluginDrivenSysExternalTable sysTable = sysTableOver(source);
        Mockito.doReturn(false).when(sysTable).selectorSupported(Mockito.any());

        // WHY: when the connector rejects this selector on this view, resolving anyway would surface a
        // worse error (or the source's own not-found RuntimeException) BEFORE
        // PluginDrivenScanNode.checkSysTableScanConstraints can produce the intended message. Fall back to
        // latest and let that guard speak. MUTATION: dropping the capability gate -> a pin is resolved -> red.
        Assertions.assertFalse(
                sysTable.resolveScanPin(Optional.empty(),
                        Optional.of(options("scan.tag-name", "x"))).isPresent(),
                "a declined selector must not pin");
        Mockito.verify(source, Mockito.never()).loadSnapshot(Mockito.any(), Mockito.any());
    }

    @Test
    public void nonMvccSourceDoesNotPin() {
        // A source with no time-travel capability at all: fall back rather than ClassCastException.
        PluginDrivenExternalTable source = Mockito.mock(PluginDrivenExternalTable.class);
        PluginDrivenSysExternalTable sysTable = sysTableOver(source);

        // MUTATION: dropping the instanceof MvccTable guard -> CCE on the cast -> red.
        Assertions.assertFalse(
                sysTable.resolveScanPin(Optional.of(Mockito.mock(TableSnapshot.class)),
                        Optional.empty()).isPresent(),
                "a non-MVCC source must not pin");
    }
}
