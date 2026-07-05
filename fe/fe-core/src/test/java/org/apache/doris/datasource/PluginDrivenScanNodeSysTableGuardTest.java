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

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Guards the fail-loud sys-table scan-constraint check in
 * {@link PluginDrivenScanNode#checkSysTableScanConstraints()} (P5-T19 Part C).
 *
 * <p>WHY this matters: for a connector whose sys tables have no point-in-time semantics (paimon —
 * the default capability), a {@code FOR TIME AS OF} (snapshot) or {@code @incr}/scan-params query
 * against a plugin <b>system</b> table ({@link PluginDrivenSysExternalTable}) is undefined; legacy
 * {@code PaimonScanNode.getProcessedTable} throws for exactly this case. Without the guard the
 * scan-params / snapshot would be silently dropped and the query would return the plain sys-table
 * contents, masking a user error. These tests pin that the guard fails loud (Rule 12).</p>
 *
 * <p>P6.5-T07: the guard is now CONNECTOR-CAPABILITY-AWARE. A connector reporting
 * {@code supportsSystemTableTimeTravel()==true} (iceberg — whose metadata tables legally time-travel)
 * lets a pinned read ({@code FOR TIME AS OF}, {@code @branch}/{@code @tag}) through to the connector,
 * which retains+honors the pin; {@code @incr} is rejected for EVERY connector (undefined on a synthetic
 * metadata table). Paimon keeps the default {@code false} and the original blanket rejection.</p>
 *
 * <p>Driven on a Mockito mock with {@code CALLS_REAL_METHODS} (no constructor — building a full
 * {@link FileQueryScanNode} needs a harness this module lacks) and the four accessors
 * ({@code getTargetTable}, {@code getScanParams}, {@code getQueryTableSnapshot},
 * {@code sysTableSupportsTimeTravel}) stubbed, so the real guard runs against controlled state. The
 * guard and the capability hook are package-private exactly to enable this.</p>
 */
public class PluginDrivenScanNodeSysTableGuardTest {

    private static PluginDrivenScanNode guardOnlyNode() throws Exception {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        // Default: no scan-params, no snapshot, and a connector whose sys tables do NOT time-travel
        // (paimon-like — the default capability). Time-travel-capable cases (iceberg) override the flag.
        Mockito.doReturn(null).when(node).getScanParams();
        Mockito.doReturn(null).when(node).getQueryTableSnapshot();
        Mockito.doReturn(false).when(node).sysTableSupportsTimeTravel();
        return node;
    }

    @Test
    public void sysTableRejectsScanParams() throws Exception {
        PluginDrivenScanNode node = guardOnlyNode();
        Mockito.doReturn(Mockito.mock(PluginDrivenSysExternalTable.class)).when(node).getTargetTable();
        Mockito.doReturn(Mockito.mock(TableScanParams.class)).when(node).getScanParams();

        // WHY: an @incr / scan-params query on a sys table must fail loud, not silently ignore the
        // params. MUTATION: removing the getScanParams() throw in the guard -> no exception -> red.
        UserException ex = Assertions.assertThrows(UserException.class,
                node::checkSysTableScanConstraints);
        Assertions.assertTrue(ex.getMessage().contains("scan params"),
                "scan-params rejection must carry the expected message, got: " + ex.getMessage());
    }

    @Test
    public void sysTableRejectsTimeTravel() throws Exception {
        PluginDrivenScanNode node = guardOnlyNode();
        Mockito.doReturn(Mockito.mock(PluginDrivenSysExternalTable.class)).when(node).getTargetTable();
        Mockito.doReturn(Mockito.mock(TableSnapshot.class)).when(node).getQueryTableSnapshot();

        // WHY: a FOR TIME AS OF query on a sys table must fail loud. MUTATION: removing the
        // getQueryTableSnapshot() throw in the guard -> no exception -> red.
        UserException ex = Assertions.assertThrows(UserException.class,
                node::checkSysTableScanConstraints);
        Assertions.assertTrue(ex.getMessage().contains("time travel"),
                "time-travel rejection must carry the expected message, got: " + ex.getMessage());
    }

    @Test
    public void sysTableWithoutScanParamsOrSnapshotDoesNotThrow() throws Exception {
        PluginDrivenScanNode node = guardOnlyNode();
        Mockito.doReturn(Mockito.mock(PluginDrivenSysExternalTable.class)).when(node).getTargetTable();

        // WHY: a plain sys-table scan (no params, no snapshot) is valid and must pass the guard.
        // This pins that the guard only rejects the two unsupported features, not all sys scans.
        Assertions.assertDoesNotThrow(node::checkSysTableScanConstraints);
    }

    @Test
    public void normalTableWithScanParamsDoesNotThrowFromGuard() throws Exception {
        PluginDrivenScanNode node = guardOnlyNode();
        // A NON-sys plugin table: even with scan-params/snapshot set, this guard is a no-op
        // (normal-table time-travel is B5/MVCC, out of scope here).
        Mockito.doReturn(Mockito.mock(TableIf.class)).when(node).getTargetTable();
        Mockito.doReturn(Mockito.mock(TableScanParams.class)).when(node).getScanParams();
        Mockito.doReturn(Mockito.mock(TableSnapshot.class)).when(node).getQueryTableSnapshot();

        // WHY: the guard is SYS-table only. MUTATION: widening the instanceof check to all tables
        // would throw here -> red. Pins the scope limit.
        Assertions.assertDoesNotThrow(node::checkSysTableScanConstraints);
    }

    // ---------------------------------------------------------------------
    // P6.5-T07: connector-capability-aware gating (iceberg sys tables time-travel)
    // ---------------------------------------------------------------------

    @Test
    public void timeTravelCapableSysTableAllowsTimeTravel() throws Exception {
        PluginDrivenScanNode node = guardOnlyNode();
        Mockito.doReturn(true).when(node).sysTableSupportsTimeTravel();
        Mockito.doReturn(Mockito.mock(PluginDrivenSysExternalTable.class)).when(node).getTargetTable();
        Mockito.doReturn(Mockito.mock(TableSnapshot.class)).when(node).getQueryTableSnapshot();

        // WHY: iceberg metadata tables legally time-travel (t$snapshots FOR TIME AS OF ...); legacy
        // IcebergScanNode.createTableScan honors the pin for sys tables. A connector reporting
        // supportsSystemTableTimeTravel()==true must let the pinned read through (the connector retains +
        // honors the pin), NOT reject it as paimon does. MUTATION: dropping the `&& !timeTravelSupported`
        // gate on the snapshot throw -> rejects even capable connectors -> red.
        Assertions.assertDoesNotThrow(node::checkSysTableScanConstraints);
    }

    @Test
    public void timeTravelCapableSysTableAllowsBranchTagScanParams() throws Exception {
        PluginDrivenScanNode node = guardOnlyNode();
        Mockito.doReturn(true).when(node).sysTableSupportsTimeTravel();
        Mockito.doReturn(Mockito.mock(PluginDrivenSysExternalTable.class)).when(node).getTargetTable();
        // A @branch/@tag scan-param: incrementalRead()==false (the generic mock default).
        Mockito.doReturn(Mockito.mock(TableScanParams.class)).when(node).getScanParams();

        // WHY: t$files@branch('b') / @tag is a valid snapshot selector legacy iceberg honors for sys
        // tables. A time-travel-capable connector must allow it through. MUTATION: removing the
        // timeTravelSupported gate on the scan-params throw -> rejects branch/tag too -> red.
        Assertions.assertDoesNotThrow(node::checkSysTableScanConstraints);
    }

    @Test
    public void timeTravelCapableSysTableStillRejectsIncrementalRead() throws Exception {
        PluginDrivenScanNode node = guardOnlyNode();
        Mockito.doReturn(true).when(node).sysTableSupportsTimeTravel();
        Mockito.doReturn(Mockito.mock(PluginDrivenSysExternalTable.class)).when(node).getTargetTable();
        TableScanParams incr = Mockito.mock(TableScanParams.class);
        Mockito.doReturn(true).when(incr).incrementalRead();
        Mockito.doReturn(incr).when(node).getScanParams();

        // WHY: @incr (incremental read) is undefined on a synthetic metadata table even for iceberg —
        // legacy silently ignored it; the guard fails loud instead (strictly safer). MUTATION: dropping
        // the `|| getScanParams().incrementalRead()` clause -> @incr slips through on a capable connector
        // -> red.
        UserException ex = Assertions.assertThrows(UserException.class,
                node::checkSysTableScanConstraints);
        Assertions.assertTrue(ex.getMessage().contains("scan params"),
                "incremental-read rejection must carry the scan-params message, got: " + ex.getMessage());
    }

    @Test
    public void scanParamsRejectionTakesPrecedenceOverTimeTravel() throws Exception {
        PluginDrivenScanNode node = guardOnlyNode();
        // Paimon-like (NOT time-travel-capable, the guardOnlyNode default), a sys table, with BOTH a
        // non-incremental scan-param AND a snapshot pin set at once.
        Mockito.doReturn(Mockito.mock(PluginDrivenSysExternalTable.class)).when(node).getTargetTable();
        Mockito.doReturn(Mockito.mock(TableScanParams.class)).when(node).getScanParams();
        Mockito.doReturn(Mockito.mock(TableSnapshot.class)).when(node).getQueryTableSnapshot();

        // WHY: when both unsupported features are present the guard checks scan-params FIRST (its block
        // precedes the snapshot block), so the user sees the scan-params diagnostic, not time travel.
        // Pinning the order keeps the error message stable. MUTATION: swapping the two blocks (snapshot
        // check first) -> the message becomes "time travel" -> red.
        UserException ex = Assertions.assertThrows(UserException.class,
                node::checkSysTableScanConstraints);
        Assertions.assertTrue(ex.getMessage().contains("scan params"),
                "scan-params rejection must take precedence over time travel, got: " + ex.getMessage());
    }
}
