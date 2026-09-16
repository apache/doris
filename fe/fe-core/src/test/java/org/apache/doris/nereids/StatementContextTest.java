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

package org.apache.doris.nereids;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.rules.analysis.PreloadExternalMetadata;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class StatementContextTest {

    @Test
    public void testSkipPreloadWhenSessionVariableDisabled() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenExternalTable hmsExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();

        // Keep the preload switch disabled so no external access should happen.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(hmsExternalTable.getId()).thenReturn(11L);
        Mockito.when(hmsExternalTable.supportsExternalMetadataPreload()).thenReturn(true);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(hmsExternalTable, Optional.empty(), Optional.empty());

            ExternalMetadataPreloadResult result = executePreload(statementContext);

            org.junit.jupiter.api.Assertions.assertFalse(result.isExecuted());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getCandidateTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(0, result.getPreloadedTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(
                    "session variable enable_preload_external_metadata is disabled", result.getSkipReason());
            Mockito.verify(hmsExternalTable, Mockito.never()).getBaseSchema();
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testPreloadJdbcExternalTablesBeforeLock() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenExternalTable jdbcExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        // Route preload through the JDBC plugin catalog and keep it schema-only.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(connectContext.getQueryIdentifier()).thenReturn("query-3");
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(jdbcExternalTable.getId()).thenReturn(13L);
        Mockito.when(jdbcExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(jdbcExternalTable.getBaseSchema()).thenReturn(Collections.emptyList());
        Mockito.when(jdbcExternalTable.supportInternalPartitionPruned()).thenReturn(false);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(jdbcExternalTable, Optional.empty(), Optional.empty());

            ExternalMetadataPreloadResult result = executePreload(statementContext);

            org.junit.jupiter.api.Assertions.assertTrue(result.isExecuted());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getCandidateTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getPreloadedTableCount());
            Mockito.verify(jdbcExternalTable, Mockito.times(1)).getBaseSchema();
            Mockito.verify(jdbcExternalTable, Mockito.never()).initSelectedPartitions(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testSkipPreloadForNonJdbcPluginExternalTable() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenExternalTable pluginExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        // Keep non-JDBC plugin catalogs outside the preload whitelist.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(pluginExternalTable.getId()).thenReturn(14L);
        Mockito.when(pluginExternalTable.supportsExternalMetadataPreload()).thenReturn(false);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(pluginExternalTable, Optional.empty(), Optional.empty());

            ExternalMetadataPreloadResult result = executePreload(statementContext);

            org.junit.jupiter.api.Assertions.assertFalse(result.isExecuted());
            org.junit.jupiter.api.Assertions.assertEquals(0, result.getCandidateTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(0, result.getPreloadedTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(
                    "no external preload candidates were collected", result.getSkipReason());
            Mockito.verify(pluginExternalTable, Mockito.never()).getBaseSchema();
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testSkipPreloadWhenNoInternalTableNeedsPlanReadLock() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenExternalTable hmsExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        // Skip preload when the statement does not require any internal plan-time read lock.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(false);
        Mockito.when(hmsExternalTable.getId()).thenReturn(15L);
        Mockito.when(hmsExternalTable.supportsExternalMetadataPreload()).thenReturn(true);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(hmsExternalTable, Optional.empty(), Optional.empty());

            ExternalMetadataPreloadResult result = executePreload(statementContext);

            org.junit.jupiter.api.Assertions.assertFalse(result.isExecuted());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getCandidateTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(0, result.getPreloadedTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(
                    "no internal tables require plan-time read lock", result.getSkipReason());
            Mockito.verify(hmsExternalTable, Mockito.never()).getBaseSchema();
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testPreloadIcebergLatestSnapshotBeforeLock() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenMvccExternalTable icebergExternalTable = Mockito.mock(PluginDrivenMvccExternalTable.class);
        DatabaseIf<TableIf> database = mockDatabase();
        CatalogIf<?> catalog = mockCatalog();
        MvccSnapshot mvccSnapshot = Mockito.mock(MvccSnapshot.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        // Cover the dedicated Iceberg latest-snapshot preload branch before the lock phase.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(icebergExternalTable.getId()).thenReturn(16L);
        Mockito.when(icebergExternalTable.getName()).thenReturn("iceberg_tbl");
        Mockito.when(icebergExternalTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        Mockito.when(icebergExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(icebergExternalTable.supportsLatestSnapshotPreload()).thenReturn(true);
        Mockito.when(icebergExternalTable.loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any()))
                .thenReturn(mvccSnapshot);
        Mockito.when(icebergExternalTable.getBaseSchema()).thenReturn(Collections.emptyList());
        Mockito.when(icebergExternalTable.supportInternalPartitionPruned()).thenReturn(false);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(icebergExternalTable, Optional.empty(), Optional.empty());

            ExternalMetadataPreloadResult result = executePreload(statementContext);

            org.junit.jupiter.api.Assertions.assertTrue(result.isExecuted());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getCandidateTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getPreloadedTableCount());
            Mockito.verify(icebergExternalTable, Mockito.times(1))
                    .loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any());
            Mockito.verify(icebergExternalTable, Mockito.times(1)).getBaseSchema();
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testSkipIcebergPreloadWhenOnlyNonLatestRelationExists() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenMvccExternalTable icebergExternalTable = Mockito.mock(PluginDrivenMvccExternalTable.class);
        DatabaseIf<TableIf> database = mockDatabase();
        CatalogIf<?> catalog = mockCatalog();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        // Skip schema and partition warmup when Iceberg is referenced only by non-latest relations.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(icebergExternalTable.getId()).thenReturn(18L);
        Mockito.when(icebergExternalTable.getName()).thenReturn("iceberg_tbl");
        Mockito.when(icebergExternalTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        Mockito.when(icebergExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(icebergExternalTable.supportsLatestSnapshotPreload()).thenReturn(true);
        Mockito.when(icebergExternalTable.supportInternalPartitionPruned()).thenReturn(true);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(icebergExternalTable,
                    Optional.of(new TableSnapshot("2024-01-01 00:00:00", TableSnapshot.VersionType.TIME)),
                    Optional.empty());

            ExternalMetadataPreloadResult result = executePreload(statementContext);

            org.junit.jupiter.api.Assertions.assertTrue(result.isExecuted());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getCandidateTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(0, result.getPreloadedTableCount());
            Mockito.verify(icebergExternalTable, Mockito.never())
                    .loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any());
            Mockito.verify(icebergExternalTable, Mockito.never()).getBaseSchema();
            Mockito.verify(icebergExternalTable, Mockito.never()).initSelectedPartitions(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testPreloadPaimonLatestSnapshotBeforeLock() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenMvccExternalTable paimonExternalTable = Mockito.mock(PluginDrivenMvccExternalTable.class);
        DatabaseIf<TableIf> database = mockDatabase();
        CatalogIf<?> catalog = mockCatalog();
        MvccSnapshot mvccSnapshot = Mockito.mock(MvccSnapshot.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        // Cover the dedicated Paimon latest-snapshot preload branch before the lock phase.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(paimonExternalTable.getId()).thenReturn(17L);
        Mockito.when(paimonExternalTable.getName()).thenReturn("paimon_tbl");
        Mockito.when(paimonExternalTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        Mockito.when(paimonExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(paimonExternalTable.supportsLatestSnapshotPreload()).thenReturn(true);
        Mockito.when(paimonExternalTable.loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any()))
                .thenReturn(mvccSnapshot);
        Mockito.when(paimonExternalTable.getBaseSchema()).thenReturn(Collections.emptyList());
        Mockito.when(paimonExternalTable.supportInternalPartitionPruned()).thenReturn(true);
        Mockito.when(paimonExternalTable.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(paimonExternalTable, Optional.empty(), Optional.empty());

            ExternalMetadataPreloadResult result = executePreload(statementContext);

            org.junit.jupiter.api.Assertions.assertTrue(result.isExecuted());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getCandidateTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getPreloadedTableCount());
            // Verify the latest snapshot is loaded before partition metadata warmup consumes it.
            InOrder inOrder = Mockito.inOrder(paimonExternalTable);
            inOrder.verify(paimonExternalTable, Mockito.times(1))
                    .loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any());
            inOrder.verify(paimonExternalTable, Mockito.times(1)).getBaseSchema();
            inOrder.verify(paimonExternalTable, Mockito.times(1)).initSelectedPartitions(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testScanParamOptionsRelationIsTreatedAsNonLatest() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenMvccExternalTable table = Mockito.mock(PluginDrivenMvccExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        // WHY: an @options relation carries a relation-scoped selector, so it must NOT be counted as a
        // latest-only relation -- the same rule @branch/@tag/@incr follow. Upstream #65984 kept the latest
        // warmup for an @options map that happens to select no version, but deciding that needs the
        // connector's option vocabulary and this runs BEFORE binding resolves any pin. Skipping the warmup
        // costs only latency (the metadata is then loaded lazily under the lock), never correctness.
        // MUTATION: restoring a selector-free exemption here -> loadSnapshot/getBaseSchema get called.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(table.getId()).thenReturn(18L);
        Mockito.when(table.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(table.supportsLatestSnapshotPreload()).thenReturn(true);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(
                    table,
                    Optional.empty(),
                    Optional.of(new TableScanParams(
                            TableScanParams.OPTIONS,
                            ImmutableMap.of("scan.plan-sort-partition", "true"),
                            Collections.emptyList())));

            ExternalMetadataPreloadResult result = executePreload(statementContext);

            org.junit.jupiter.api.Assertions.assertTrue(result.isExecuted());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getCandidateTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(0, result.getPreloadedTableCount());
            Mockito.verify(table, Mockito.never())
                    .loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any());
            Mockito.verify(table, Mockito.never()).getBaseSchema();
        } finally {
            statementContext.close();
        }
    }

    @SuppressWarnings("unchecked")
    private DatabaseIf<TableIf> mockDatabase() {
        return Mockito.mock(DatabaseIf.class);
    }

    @Test
    public void testResetMvccSnapshotsClearsPreloadCompletionButKeepsCandidates() {
        StatementContext statementContext = new StatementContext();
        // Keep this test on the connector-neutral table seam available across FE branches.
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getId()).thenReturn(42L);
        Mockito.when(table.supportsExternalMetadataPreload()).thenReturn(true);
        statementContext.registerExternalTableForPreload(table, Optional.empty(), Optional.empty());
        statementContext.setExternalMetadataPreloadResult(
                ExternalMetadataPreloadResult.executed(1, 1, 1L));

        statementContext.resetMvccSnapshots();

        org.junit.jupiter.api.Assertions.assertFalse(
                statementContext.getExternalMetadataPreloadResult().isPresent());
        org.junit.jupiter.api.Assertions.assertEquals(1,
                statementContext.getExternalTablePreloadCandidateCount());

        statementContext.setExternalMetadataPreloadResult(
                ExternalMetadataPreloadResult.executed(1, 1, 1L));
        statementContext.resetMvccSnapshots();

        org.junit.jupiter.api.Assertions.assertFalse(
                statementContext.getExternalMetadataPreloadResult().isPresent());
        org.junit.jupiter.api.Assertions.assertEquals(1,
                statementContext.getExternalTablePreloadCandidateCount());
    }

    private CatalogIf<?> mockCatalog() {
        return Mockito.mock(CatalogIf.class);
    }

    @Test
    public void testPreloadDeferredScanPartitionViewBeforeLock() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenExternalTable hiveExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        // WHY: a connector-pruning table materializes NO partition view in initSelectedPartitions (it returns
        // DEFERRED), yet the MV partition collector needs the full view after the locks are taken. The preload
        // pass must therefore materialize it HERE and hand it to that collector.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(connectContext.getQueryIdentifier()).thenReturn("query-deferred");
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hiveExternalTable.getId()).thenReturn(19L);
        Mockito.when(hiveExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hiveExternalTable.supportInternalPartitionPruned()).thenReturn(true);
        Mockito.when(hiveExternalTable.getBaseSchema()).thenReturn(Collections.emptyList());
        Mockito.when(hiveExternalTable.initSelectedPartitions(Mockito.any()))
                .thenReturn(SelectedPartitions.DEFERRED_PARTITION_PRUNING);
        Mockito.when(hiveExternalTable.supportsConnectorPartitionPruning()).thenReturn(true);
        Optional<Map<String, PartitionItem>> scanView =
                Optional.of(ImmutableMap.of("p1", Mockito.mock(PartitionItem.class)));
        Mockito.when(hiveExternalTable.getNameToPartitionItemsForScan(Mockito.any())).thenReturn(scanView);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(hiveExternalTable, Optional.empty(), Optional.empty());

            ExternalMetadataPreloadResult result = executePreload(statementContext);

            org.junit.jupiter.api.Assertions.assertTrue(result.isExecuted());
            ExternalTablePreloadInfo preloadInfo = statementContext.getExternalTablePreloadInfo(19L).get();
            org.junit.jupiter.api.Assertions.assertTrue(preloadInfo.hasScanPartitionView());
            org.junit.jupiter.api.Assertions.assertEquals(scanView, preloadInfo.getScanPartitionView());
            Mockito.verify(hiveExternalTable, Mockito.times(1)).initSelectedPartitions(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testPreloadSkipsScanPartitionViewForTableWithoutConnectorPruning() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenExternalTable plainTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(connectContext.getQueryIdentifier()).thenReturn("query-plain");
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(plainTable.getId()).thenReturn(20L);
        Mockito.when(plainTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(plainTable.supportInternalPartitionPruned()).thenReturn(true);
        Mockito.when(plainTable.getBaseSchema()).thenReturn(Collections.emptyList());
        Mockito.when(plainTable.initSelectedPartitions(Mockito.any()))
                .thenReturn(SelectedPartitions.NOT_PRUNED);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(plainTable, Optional.empty(), Optional.empty());

            executePreload(statementContext);

            // A connector that cannot prune from a predicate already enumerated its view eagerly, so there is
            // nothing to warm and no connector call to add.
            org.junit.jupiter.api.Assertions.assertFalse(
                    statementContext.getExternalTablePreloadInfo(20L).get().hasScanPartitionView());
            Mockito.verify(plainTable, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testDeferredScanPartitionViewIsMaterializedOnTheDefaultConfiguration() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenExternalTable hiveExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        // Deliberately the DEFAULT switch state: the lock-scope step must not depend on the opt-in preload var.
        SessionVariable sessionVariable = new SessionVariable();
        org.junit.jupiter.api.Assertions.assertFalse(sessionVariable.isEnablePreloadExternalMetadata());
        org.junit.jupiter.api.Assertions.assertTrue(sessionVariable.isEnableMaterializedViewRewrite());

        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hiveExternalTable.getId()).thenReturn(21L);
        Mockito.when(hiveExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hiveExternalTable.supportsConnectorPartitionPruning()).thenReturn(true);
        Optional<Map<String, PartitionItem>> scanView =
                Optional.of(ImmutableMap.of("p1", Mockito.mock(PartitionItem.class)));
        Mockito.when(hiveExternalTable.getNameToPartitionItemsForScan(Mockito.any())).thenReturn(scanView);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(hiveExternalTable, Optional.empty(), Optional.empty());

            statementContext.preloadDeferredScanPartitionViewsBeforeLock();

            // MUTATION: dropping this pre-lock step moves the enumeration back inside the MV partition
            // collector, i.e. under the statement's internal table read locks.
            ExternalTablePreloadInfo preloadInfo = statementContext.getExternalTablePreloadInfo(21L).get();
            org.junit.jupiter.api.Assertions.assertTrue(preloadInfo.hasScanPartitionView());
            org.junit.jupiter.api.Assertions.assertEquals(scanView, preloadInfo.getScanPartitionView());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testDeferredScanPartitionViewIsSkippedWithoutInternalReadLock() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenExternalTable hiveExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();

        // No internal table of this statement is locked during planning, so enumerating the external view
        // blocks nothing and must not be paid for.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(false);
        Mockito.when(hiveExternalTable.getId()).thenReturn(22L);
        Mockito.when(hiveExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hiveExternalTable.supportsConnectorPartitionPruning()).thenReturn(true);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(hiveExternalTable, Optional.empty(), Optional.empty());

            statementContext.preloadDeferredScanPartitionViewsBeforeLock();

            org.junit.jupiter.api.Assertions.assertFalse(
                    statementContext.getExternalTablePreloadInfo(22L).get().hasScanPartitionView());
            Mockito.verify(hiveExternalTable, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testDeferredScanPartitionViewIsSkippedWhenMvRewriteIsDisabled() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenExternalTable hiveExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableMaterializedViewRewrite(false);

        // The materialized view has no consumer once MV rewrite is off, so this warmup is pure cost there.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hiveExternalTable.getId()).thenReturn(23L);
        Mockito.when(hiveExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hiveExternalTable.supportsConnectorPartitionPruning()).thenReturn(true);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(hiveExternalTable, Optional.empty(), Optional.empty());

            statementContext.preloadDeferredScanPartitionViewsBeforeLock();

            org.junit.jupiter.api.Assertions.assertFalse(
                    statementContext.getExternalTablePreloadInfo(23L).get().hasScanPartitionView());
            Mockito.verify(hiveExternalTable, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    private ExternalMetadataPreloadResult executePreload(StatementContext statementContext) {
        ExternalMetadataPreloadResult result = new PreloadExternalMetadata().executePreload(statementContext);
        statementContext.setExternalMetadataPreloadResult(result);
        return result;
    }
}
