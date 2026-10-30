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

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.View;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.FilterApplicationResult;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable.ConnectorFilteredPartitionView;
import org.apache.doris.mtmv.BaseColInfo;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.CollectRelation;
import org.apache.doris.nereids.rules.analysis.PreloadExternalMetadata;
import org.apache.doris.nereids.rules.rewrite.PruneFileScanPartition;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class StatementContextTest {

    @Test
    public void testChangelogReferenceTsoIsStatementScoped() {
        StatementContext statementContext = new StatementContext();
        AtomicInteger calls = new AtomicInteger();

        statementContext.requireChangelogReferenceTso();
        org.junit.jupiter.api.Assertions.assertTrue(statementContext.isChangelogReferenceTsoRequired());
        org.junit.jupiter.api.Assertions.assertEquals(101L,
                statementContext.getOrRegisterChangelogReferenceTso(() -> 100L + calls.incrementAndGet()));
        org.junit.jupiter.api.Assertions.assertEquals(101L,
                statementContext.getOrRegisterChangelogReferenceTso(() -> 100L + calls.incrementAndGet()));
        org.junit.jupiter.api.Assertions.assertEquals(1, calls.get());

        statementContext.resetConnectorStatementScope();
        org.junit.jupiter.api.Assertions.assertFalse(statementContext.isChangelogReferenceTsoRequired());
        org.junit.jupiter.api.Assertions.assertThrows(AnalysisException.class,
                statementContext::getChangelogReferenceTso);
    }

    @Test
    public void testExecutionVersionIsFreshForEachPreparedExecution() {
        StatementContext firstExecution = new StatementContext();
        firstExecution.initializeBeExecVersion(13);

        StatementContext secondExecution = firstExecution.createNextExecuteContext();
        org.junit.jupiter.api.Assertions.assertFalse(secondExecution.hasBeExecVersion());
        secondExecution.initializeBeExecVersion(16);

        org.junit.jupiter.api.Assertions.assertEquals(13, firstExecution.getBeExecVersion());
        org.junit.jupiter.api.Assertions.assertEquals(16, secondExecution.getBeExecVersion());
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> secondExecution.initializeBeExecVersion(15));
    }

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
        ExternalTablePreloadInfo preloadInfo = statementContext.getExternalTablePreloadInfo(42L).get();
        preloadInfo.setScanPartitionView(Optional.of(Collections.emptyMap()));
        preloadInfo.setMtmvPartitionView(Optional.of(Collections.emptyMap()));
        statementContext.setExternalMetadataPreloadResult(
                ExternalMetadataPreloadResult.executed(1, 1, 1L));

        statementContext.resetMvccSnapshots();

        org.junit.jupiter.api.Assertions.assertFalse(
                statementContext.getExternalMetadataPreloadResult().isPresent());
        org.junit.jupiter.api.Assertions.assertEquals(1,
                statementContext.getExternalTablePreloadCandidateCount());
        org.junit.jupiter.api.Assertions.assertFalse(preloadInfo.hasScanPartitionView());
        org.junit.jupiter.api.Assertions.assertFalse(preloadInfo.hasMtmvPartitionView());

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
    public void testDeferredScanPartitionViewIsMaterializedWithOnlyDmlMvRewrite() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenExternalTable hiveExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();
        // The DML switch alone: AddInitMaterializationHook registers InitConsistentMaterializationContextHook
        // (a subclass of the hook whose afterRewrite collects partitions) from this variable, so the collector
        // still runs - and a gate that only looked at the query-level switch would enumerate under the lock.
        sessionVariable.setEnableMaterializedViewRewrite(false);
        sessionVariable.setEnableDmlMaterializedViewRewrite(true);

        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hiveExternalTable.getId()).thenReturn(24L);
        Mockito.when(hiveExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hiveExternalTable.supportsConnectorPartitionPruning()).thenReturn(true);
        Optional<Map<String, PartitionItem>> scanView =
                Optional.of(ImmutableMap.of("p1", Mockito.mock(PartitionItem.class)));
        Mockito.when(hiveExternalTable.getNameToPartitionItemsForScan(Mockito.any())).thenReturn(scanView);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(hiveExternalTable, Optional.empty(), Optional.empty());

            statementContext.preloadDeferredScanPartitionViewsBeforeLock(false);

            org.junit.jupiter.api.Assertions.assertEquals(scanView,
                    statementContext.getExternalTablePreloadInfo(24L).get().getScanPartitionView());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testResolveScanPartitionViewMaterializesOncePerTableReference() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        PluginDrivenExternalTable hiveExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(hiveExternalTable.getId()).thenReturn(25L);
        Mockito.when(hiveExternalTable.supportsExternalMetadataPreload()).thenReturn(true);

        Optional<Map<String, PartitionItem>> scanView =
                Optional.of(ImmutableMap.of("p1", Mockito.mock(PartitionItem.class)));
        AtomicInteger enumerations = new AtomicInteger();

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.registerExternalTableForPreload(hiveExternalTable, Optional.empty(), Optional.empty());

            // The MV partition collector reads first and records; the physical scan must then see the SAME
            // generation instead of enumerating its own (a second read can observe a partition the compensation
            // union never covers, which silently drops that partition's rows from a rewritten query).
            Optional<Map<String, PartitionItem>> first = statementContext.resolveScanPartitionView(hiveExternalTable,
                    Optional.empty(), Optional.empty(), () -> {
                        enumerations.incrementAndGet();
                        return scanView;
                    });
            Optional<Map<String, PartitionItem>> second = statementContext.resolveScanPartitionView(hiveExternalTable,
                    Optional.empty(), Optional.empty(), () -> {
                        enumerations.incrementAndGet();
                        return Optional.of(ImmutableMap.of("later", Mockito.mock(PartitionItem.class)));
                    });

            org.junit.jupiter.api.Assertions.assertEquals(scanView, first);
            org.junit.jupiter.api.Assertions.assertEquals(scanView, second,
                    "the recorded generation must be reused, not re-enumerated");
            org.junit.jupiter.api.Assertions.assertEquals(1, enumerations.get());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testResolveScanPartitionViewRemembersTheUnavailableView() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        PluginDrivenExternalTable hiveExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(hiveExternalTable.getId()).thenReturn(26L);
        Mockito.when(hiveExternalTable.supportsExternalMetadataPreload()).thenReturn(true);

        AtomicInteger enumerations = new AtomicInteger();
        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.registerExternalTableForPreload(hiveExternalTable, Optional.empty(), Optional.empty());

            Optional<Map<String, PartitionItem>> view = statementContext.resolveScanPartitionView(hiveExternalTable,
                    Optional.empty(), Optional.empty(), () -> {
                        enumerations.incrementAndGet();
                        return Optional.empty();
                    });
            Optional<Map<String, PartitionItem>> again = statementContext.resolveScanPartitionView(hiveExternalTable,
                    Optional.empty(), Optional.empty(), () -> {
                        enumerations.incrementAndGet();
                        return Optional.of(ImmutableMap.of("p1", Mockito.mock(PartitionItem.class)));
                    });

            org.junit.jupiter.api.Assertions.assertFalse(view.isPresent());
            org.junit.jupiter.api.Assertions.assertFalse(again.isPresent(),
                    "an unavailable view must stay unavailable for the whole statement");
            org.junit.jupiter.api.Assertions.assertEquals(1, enumerations.get());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testResolveScanPartitionViewLeavesVersionedReferencesAlone() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        PluginDrivenExternalTable hiveExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(hiveExternalTable.getId()).thenReturn(27L);
        Mockito.when(hiveExternalTable.supportsExternalMetadataPreload()).thenReturn(true);

        AtomicInteger enumerations = new AtomicInteger();
        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.registerExternalTableForPreload(hiveExternalTable,
                    Optional.of(new TableSnapshot("2024-01-01 00:00:00", TableSnapshot.VersionType.TIME)),
                    Optional.empty());

            for (int i = 0; i < 2; i++) {
                statementContext.resolveScanPartitionView(hiveExternalTable,
                        Optional.of(new TableSnapshot("2024-01-01 00:00:00", TableSnapshot.VersionType.TIME)),
                        Optional.empty(), () -> {
                            enumerations.incrementAndGet();
                            return Optional.of(ImmutableMap.of("asOf", Mockito.mock(PartitionItem.class)));
                        });
            }

            // The record is per table and holds the LATEST generation, so a FOR-TIME reference must enumerate its
            // own generation every time rather than be served a partition set it never reads.
            org.junit.jupiter.api.Assertions.assertEquals(2, enumerations.get());
            org.junit.jupiter.api.Assertions.assertFalse(
                    statementContext.getExternalTablePreloadInfo(27L).get().hasScanPartitionView());
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

            statementContext.preloadDeferredScanPartitionViewsBeforeLock(false);

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
    public void testFilteredScanDoesNotWarmUnfilteredPartitionViewBeforeLock() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenExternalTable hiveExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();

        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hiveExternalTable.getId()).thenReturn(28L);
        Mockito.when(hiveExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hiveExternalTable.supportsConnectorPartitionPruning()).thenReturn(true);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            LogicalPlan plan = new NereidsParser().parseSingle(
                    "select * from hive_catalog.db.hive_table where hive_table.p = 1");
            UnboundRelation relation = findUnboundRelation(plan);
            CollectRelation collectRelation = new CollectRelation(false);
            CascadesContext cascadesContext = CascadesContext.initContext(
                    statementContext, plan, PhysicalProperties.ANY);
            boolean hasInitialFilter = Deencapsulation.invoke(
                    collectRelation, "isUnderInitialFilter", cascadesContext, plan, relation);
            org.junit.jupiter.api.Assertions.assertTrue(hasInitialFilter,
                    "the parsed mixed-plan Hive scan must be recognized as filtered");

            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(
                    hiveExternalTable, Optional.empty(), Optional.empty(), hasInitialFilter);

            statementContext.preloadDeferredScanPartitionViewsBeforeLock(false);

            org.junit.jupiter.api.Assertions.assertFalse(
                    statementContext.getExternalTablePreloadInfo(28L).get().hasScanPartitionView());
            Mockito.verify(hiveExternalTable, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testActualParsedMixedJoinRecognizesHiveEqualityFilter() {
        LogicalPlan plan = new NereidsParser().parseSingle(
                "select h.p from internal_catalog.db.internal_table i join hive_catalog.db.hive_table h "
                        + "on i.p = h.p where h.p = 1");
        UnboundRelation hiveRelation = plan.collect(node -> node instanceof UnboundRelation).stream()
                .map(node -> (UnboundRelation) node)
                .filter(relation -> relation.getNameParts().contains("hive_table"))
                .findFirst().get();
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        CascadesContext cascadesContext = CascadesContext.initContext(
                statementContext, plan, PhysicalProperties.ANY);
        boolean filtered = Deencapsulation.invoke(new CollectRelation(false),
                "isUnderInitialFilter", cascadesContext, plan, hiveRelation);
        org.junit.jupiter.api.Assertions.assertTrue(filtered,
                "a real Filter -> Join -> aliased Hive relation must avoid unfiltered enumeration");
    }

    @Test
    public void testParsedMixedJoinPreloadsFilteredViewBeforeInternalLock() throws Exception {
        assertParsedMixedJoinPreloadsFilteredViewBeforeInternalLock(true);
    }

    @Test
    public void testExplainMixedJoinPreloadsFilteredViewWithMvRewriteDisabled() throws Exception {
        assertParsedMixedJoinPreloadsFilteredViewBeforeInternalLock(false);
    }

    @Test
    public void testReparsedInternalViewUsesPrelockedFullPartitionView() throws Exception {
        assertReparsedInternalViewUsesPrelockedFullPartitionView(
                "select h.p from hive_catalog.db.hive_table h where h.p = 1");
    }

    @Test
    public void testReparsedCteViewUsesPrelockedFullPartitionView() throws Exception {
        assertReparsedInternalViewUsesPrelockedFullPartitionView(
                "with c as (select h.p from hive_catalog.db.hive_table h where h.p = 1) select p from c");
    }

    @Test
    public void testReparsedInSubqueryViewUsesPrelockedFullPartitionView() throws Exception {
        assertReparsedInternalViewUsesPrelockedFullPartitionView(
                "select 1 as p where 1 in (select h.p from hive_catalog.db.hive_table h where h.p = 1)");
    }

    @Test
    public void testReparsedExistsSubqueryViewUsesPrelockedFullPartitionView() throws Exception {
        assertReparsedInternalViewUsesPrelockedFullPartitionView(
                "select 1 as p where exists (select h.p from hive_catalog.db.hive_table h where h.p = 1)");
    }

    @Test
    public void testReparsedScalarSubqueryViewUsesPrelockedFullPartitionView() throws Exception {
        assertReparsedInternalViewUsesPrelockedFullPartitionView(
                "select (select h.p from hive_catalog.db.hive_table h where h.p = 1) as p");
    }

    private void assertReparsedInternalViewUsesPrelockedFullPartitionView(String viewSql) throws Exception {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(context.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        SessionVariable variables = new SessionVariable();
        variables.setEnableMaterializedViewRewrite(false);
        variables.setEnableDmlMaterializedViewRewrite(false);
        variables.setEnableSqlCache(false);
        variables.enableNereidsTimeout = false;
        variables.enableBinarySearchFilteringPartitions = false;
        Mockito.when(context.getSessionVariable()).thenReturn(variables);
        Mockito.doCallRealMethod().when(context).setThreadLocalInfo();
        StatementContext statement = Mockito.spy(new StatementContext(context, new OriginStatement("select 1", 0)));
        Mockito.when(context.getStatementContext()).thenReturn(statement);
        context.setThreadLocalInfo();
        try {
            List<String> viewQualifier = List.of("internal_catalog", "db", "filtered_view");
            List<String> hiveQualifier = List.of("hive_catalog", "db", "hive_table");
            View view = Mockito.mock(View.class);
            Mockito.when(view.getId()).thenReturn(42L);
            // Only the cached view definition is mocked: parsing and the nested table collector are real.
            Mockito.doReturn(Pair.of(viewSql, Collections.<String, String>emptyMap()))
                    .when(statement).getAndCacheViewInfo(viewQualifier, view);
            PluginDrivenExternalTable hive = Mockito.mock(PluginDrivenExternalTable.class);
            Mockito.when(hive.getId()).thenReturn(41L);
            Mockito.when(hive.supportsExternalMetadataPreload()).thenReturn(true);
            Mockito.when(hive.supportsConnectorPartitionPruning()).thenReturn(true);
            Mockito.when(hive.supportInternalPartitionPruned(Mockito.any())).thenReturn(true);
            Column partitionColumn = new Column("p", Type.INT);
            Mockito.when(hive.getPartitionColumns(Mockito.any())).thenReturn(List.of(partitionColumn));
            Mockito.when(hive.initSelectedPartitions(Mockito.any()))
                    .thenReturn(SelectedPartitions.DEFERRED_PARTITION_PRUNING);
            Map<String, PartitionItem> fullView = ImmutableMap.of(
                    "p=1", new ListPartitionItem(List.of(PartitionKey.createPartitionKey(
                            List.of(new PartitionValue("1")), List.of(partitionColumn)))),
                    "p=2", new ListPartitionItem(List.of(PartitionKey.createPartitionKey(
                            List.of(new PartitionValue("2")), List.of(partitionColumn)))));
            AtomicBoolean locked = new AtomicBoolean();
            Mockito.when(hive.getNameToPartitionItemsForScan(Mockito.any())).thenAnswer(invocation -> {
                org.junit.jupiter.api.Assertions.assertFalse(locked.get(), "full enumeration must precede lock");
                return Optional.of(fullView);
            });
            ConnectorFilteredPartitionView selectiveView = Mockito.mock(ConnectorFilteredPartitionView.class);
            Mockito.when(selectiveView.getItems()).thenReturn(ImmutableMap.of("p=1", fullView.get("p=1")));
            Mockito.when(hive.applyPartitionFilterForScan(Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
                org.junit.jupiter.api.Assertions.assertFalse(locked.get(), "selective request must precede lock");
                return Optional.of(selectiveView);
            });
            TableIf internal = Mockito.mock(TableIf.class);
            Mockito.when(internal.getId()).thenReturn(1L);
            Mockito.when(internal.needReadLockWhenPlan()).thenReturn(true);
            Mockito.when(internal.tryReadLock(1, TimeUnit.MINUTES)).thenAnswer(invocation -> {
                locked.set(true);
                return true;
            });
            Mockito.doAnswer(invocation -> {
                locked.set(false);
                return null;
            }).when(internal).readUnlock();
            // Prepopulate the real statement cache so table collection never falls through to Env.
            statement.getTables().put(viewQualifier, view);
            statement.getTables().put(hiveQualifier, hive);
            statement.getTables().put(List.of("internal_catalog", "db", "internal_table"), internal);
            AtomicReference<RelationId> collectedRelation = new AtomicReference<>();
            Mockito.doAnswer(invocation -> {
                Set<RelationId> ids = invocation.getArgument(0);
                ExternalTablePreloadInfo info = statement.getExternalTablePreloadInfo(hive.getId()).get();
                org.junit.jupiter.api.Assertions.assertEquals(1, info.getInitialScanPredicates().size(),
                        "the real nested collector must first register the filtered Hive reference");
                RelationId relation = info.getInitialScanPredicates().keySet().iterator().next();
                org.junit.jupiter.api.Assertions.assertTrue(ids.contains(relation));
                collectedRelation.set(relation);
                return invocation.callRealMethod();
            }).when(statement).markReparsedViewRelationsForPreload(Mockito.anySet());
            LogicalPlan outerPlan = new NereidsParser().parseSingle(
                    "select v.p from internal_catalog.db.internal_table i "
                            + "join internal_catalog.db.filtered_view v on i.p = v.p");
            CascadesContext parent = CascadesContext.initContext(statement, outerPlan, PhysicalProperties.ANY);
            parent.newTableCollector(true).collect();
            Mockito.verify(statement).getAndCacheViewInfo(viewQualifier, view);
            Mockito.verify(statement).registerExternalTableForPreload(hive, Optional.empty(), Optional.empty(), true);
            org.junit.jupiter.api.Assertions.assertNotNull(collectedRelation.get());
            ExternalTablePreloadInfo info = statement.getExternalTablePreloadInfo(hive.getId()).get();
            org.junit.jupiter.api.Assertions.assertTrue(info.getInitialScanPredicates().isEmpty());
            org.junit.jupiter.api.Assertions.assertTrue(info.shouldPreloadUnfilteredScanPartitionView());
            statement.preloadDeferredScanPartitionViewsBeforeLock(true);
            org.junit.jupiter.api.Assertions.assertSame(fullView, info.getScanPartitionView().get());
            Mockito.verify(hive, Mockito.never()).applyPartitionFilterForScan(Mockito.any(), Mockito.any());
            statement.lock();
            org.junit.jupiter.api.Assertions.assertTrue(locked.get());
            Mockito.verify(internal).tryReadLock(1, TimeUnit.MINUTES);

            // Binding reparses the view SQL. Reproduce the Hive subplan's identity change with a
            // second real parse, then supply typed slots instead of invoking Env-backed analysis.
            LogicalPlan reparsedPlan = new NereidsParser().parseSingle(
                    "select h.p from hive_catalog.db.hive_table h where h.p = 1");
            UnboundRelation reparsedRelation = findUnboundRelationByName(reparsedPlan, "hive_table");
            org.junit.jupiter.api.Assertions.assertNotEquals(collectedRelation.get(), reparsedRelation.getRelationId());
            org.junit.jupiter.api.Assertions.assertFalse(info.hasFilteredScanView(reparsedRelation.getRelationId()));
            SlotReference partitionSlot = new SlotReference("p", IntegerType.INSTANCE, false);
            LogicalFilter<?> parsedFilter = reparsedPlan.<LogicalFilter<?>>collect(
                    node -> node instanceof LogicalFilter).iterator().next();
            Expression boundPredicate = TypeCoercionUtils.processComparisonPredicate(
                    (ComparisonPredicate) parsedFilter.getPredicate().rewriteUp(expression ->
                            expression instanceof UnboundSlot ? partitionSlot : expression));
            LogicalFileScan scan = new LogicalFileScan(reparsedRelation.getRelationId(), hive,
                    List.of("hive_catalog", "db"), List.of(), Optional.empty(), Optional.empty(),
                    Optional.empty(), Optional.of(List.of(partitionSlot)));
            LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(Set.of(boundPredicate), scan);
            CascadesContext pruneContext = CascadesContext.initContext(statement, filter, PhysicalProperties.ANY);
            SelectedPartitions selected = Deencapsulation.invoke(new PruneFileScanPartition(),
                    "pruneExternalPartitions", hive, filter, scan, pruneContext);
            org.junit.jupiter.api.Assertions.assertTrue(locked.get(),
                    "exercise the real pruner inside the lock window");
            org.junit.jupiter.api.Assertions.assertEquals(Set.of("p=1"), selected.selectedPartitions.keySet());
            org.junit.jupiter.api.Assertions.assertSame(fullView.get("p=1"), selected.selectedPartitions.get("p=1"));
            org.junit.jupiter.api.Assertions.assertEquals(2, selected.totalPartitionNum);
            org.junit.jupiter.api.Assertions.assertTrue(selected.isPruned);
            org.junit.jupiter.api.Assertions.assertTrue(selected.getConnectorFilterResult().isEmpty());
            org.junit.jupiter.api.Assertions.assertTrue(selected.getCoveredConnectorFilter().isEmpty());
            org.junit.jupiter.api.Assertions.assertEquals(boundPredicate, filter.getPredicate());
            Mockito.verify(hive, Mockito.never()).applyPartitionFilterForScan(Mockito.any(), Mockito.any());
            Mockito.verify(hive, Mockito.times(1)).getNameToPartitionItemsForScan(Mockito.any());
            Mockito.verify(hive, Mockito.never()).getSortedPartitionRanges(Mockito.any());
            Mockito.verify(context, Mockito.never()).getEnv();
        } finally {
            try {
                statement.close();
            } finally {
                ConnectContext.remove();
                if (previousContext != null) {
                    previousContext.setThreadLocalInfo();
                }
            }
        }
    }

    private void assertParsedMixedJoinPreloadsFilteredViewBeforeInternalLock(boolean willFinalizePhysicalPlan)
            throws Exception {
        LogicalPlan plan = new NereidsParser().parseSingle(
                "select h.p from internal_catalog.db.internal_table i join hive_catalog.db.hive_table h "
                        + "on i.p = h.p where h.p = 1");
        UnboundRelation relation = plan.collect(node -> node instanceof UnboundRelation).stream()
                .map(node -> (UnboundRelation) node)
                .filter(node -> node.getNameParts().contains("hive_table")).findFirst().get();
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(context.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        SessionVariable variables = new SessionVariable();
        variables.setEnableMaterializedViewRewrite(false);
        variables.setEnableDmlMaterializedViewRewrite(false);
        Mockito.when(context.getSessionVariable()).thenReturn(variables);
        TableIf internal = Mockito.mock(TableIf.class);
        Mockito.when(internal.getId()).thenReturn(1L);
        Mockito.when(internal.needReadLockWhenPlan()).thenReturn(true);
        AtomicBoolean locked = new AtomicBoolean();
        Mockito.when(internal.tryReadLock(1, TimeUnit.MINUTES)).thenAnswer(invocation -> {
            locked.set(true);
            return true;
        });
        PluginDrivenExternalTable hive = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(hive.getId()).thenReturn(41L);
        Mockito.when(hive.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hive.supportsConnectorPartitionPruning()).thenReturn(true);
        Mockito.when(hive.getPartitionColumns(Mockito.any())).thenReturn(List.of(new Column("p", Type.INT)));
        Map<String, PartitionItem> items = ImmutableMap.of("p=1", new ListPartitionItem(List.of(
                PartitionKey.createPartitionKey(List.of(new PartitionValue("1")), List.of(new Column("p", Type.INT))))));
        ConnectorFilteredPartitionView view = Mockito.mock(ConnectorFilteredPartitionView.class);
        Mockito.when(view.getItems()).thenReturn(items);
        Mockito.when(view.getFilterResult()).thenReturn(new FilterApplicationResult<>(
                Mockito.mock(ConnectorTableHandle.class), Mockito.mock(ConnectorExpression.class), false));
        Mockito.when(hive.applyPartitionFilterForScan(Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
            org.junit.jupiter.api.Assertions.assertFalse(locked.get(), "HMS selection must precede internal lock");
            return Optional.of(view);
        });
        StatementContext statement = new StatementContext(context, new OriginStatement("select 1", 0));
        try {
            statement.getTables().put(List.of("internal_catalog", "db", "internal_table"), internal);
            statement.registerExternalTableForPreload(hive, Optional.empty(), Optional.empty(), true);
            CascadesContext cascades = CascadesContext.initContext(statement, plan, PhysicalProperties.ANY);
            org.apache.doris.nereids.trees.expressions.Expression predicate =
                    ((Optional<org.apache.doris.nereids.trees.expressions.Expression>) Deencapsulation.invoke(
                            new CollectRelation(false), "initialScanPredicate", cascades, plan, relation)).get();
            ExternalTablePreloadInfo info = statement.getExternalTablePreloadInfo(41L).get();
            info.registerScanPredicate(relation.getRelationId(), predicate);
            statement.preloadDeferredScanPartitionViewsBeforeLock(willFinalizePhysicalPlan);
            statement.lock();
            org.junit.jupiter.api.Assertions.assertTrue(locked.get());
            org.junit.jupiter.api.Assertions.assertSame(view, info.getFilteredScanView(relation.getRelationId()).get());
            org.junit.jupiter.api.Assertions.assertFalse(info.hasScanPartitionView());
            Mockito.verify(hive, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
            Mockito.verify(hive, Mockito.times(1)).applyPartitionFilterForScan(Mockito.any(), Mockito.any());
            statement.resetMvccSnapshots();
            org.junit.jupiter.api.Assertions.assertFalse(info.hasFilteredScanView(relation.getRelationId()));
        } finally {
            statement.close();
        }
    }

    @Test
    public void testFilteredPctTableWarmsOnlyMtmvPartitionViewBeforeLock() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenMvccExternalTable hiveExternalTable = Mockito.mock(PluginDrivenMvccExternalTable.class);
        DatabaseIf<TableIf> database = mockDatabase();
        CatalogIf<?> catalog = mockCatalog();
        SessionVariable sessionVariable = new SessionVariable();

        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hiveExternalTable.getId()).thenReturn(32L);
        Mockito.when(hiveExternalTable.getName()).thenReturn("hive_table");
        Mockito.when(hiveExternalTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getId()).thenReturn(2L);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.when(catalog.getName()).thenReturn("hive_catalog");
        Mockito.when(hiveExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hiveExternalTable.supportsConnectorPartitionPruning()).thenReturn(true);
        Mockito.when(hiveExternalTable.loadSnapshot(Mockito.any(), Mockito.any()))
                .thenReturn(Mockito.mock(MvccSnapshot.class));
        Optional<Map<String, PartitionItem>> fullView =
                Optional.of(ImmutableMap.of("p=1", Mockito.mock(PartitionItem.class)));
        Mockito.when(hiveExternalTable.getNameToPartitionItemsForScan(Mockito.any())).thenReturn(fullView);

        MTMVPartitionInfo partitionInfo = new MTMVPartitionInfo(MTMVPartitionType.FOLLOW_BASE_TABLE);
        partitionInfo.setPctInfos(ImmutableList.of(
                new BaseColInfo("p", new BaseTableInfo(hiveExternalTable))));
        MTMV candidateMtmv = Mockito.mock(MTMV.class);
        Mockito.when(candidateMtmv.getMvPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(candidateMtmv.isUseForRewrite()).thenReturn(true);
        // An all-internal MV stays eligible under the default external-MV switch.
        MTMVRelation mtmvRelation = Mockito.mock(MTMVRelation.class);
        Mockito.when(mtmvRelation.getBaseTablesOneLevelAndFromView()).thenReturn(Collections.emptySet());
        Mockito.when(candidateMtmv.getRelation()).thenReturn(mtmvRelation);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.getCandidateMTMVs().add(candidateMtmv);
            statementContext.registerExternalTableForPreload(
                    hiveExternalTable, Optional.empty(), Optional.empty(), true);

            statementContext.preloadDeferredScanPartitionViewsBeforeLock(false);

            ExternalTablePreloadInfo preloadInfo = statementContext.getExternalTablePreloadInfo(32L).get();
            org.junit.jupiter.api.Assertions.assertFalse(preloadInfo.hasScanPartitionView(),
                    "a filtered scan must keep connector predicate pruning");
            org.junit.jupiter.api.Assertions.assertTrue(preloadInfo.hasMtmvPartitionView());
            org.junit.jupiter.api.Assertions.assertEquals(fullView, preloadInfo.getMtmvPartitionView());
            // The warmed view must come from the statement's pinned latest snapshot, not from an unrecorded
            // materialization that binding would later re-pin to a different generation.
            org.junit.jupiter.api.Assertions.assertTrue(statementContext.getSnapshot(hiveExternalTable).isPresent(),
                    "the warmup must pin the snapshot it materialized from");
            InOrder inOrder = Mockito.inOrder(candidateMtmv, hiveExternalTable);
            inOrder.verify(candidateMtmv).readMvLock();
            inOrder.verify(candidateMtmv).getMvPartitionInfo();
            inOrder.verify(candidateMtmv).readMvUnlock();
            inOrder.verify(hiveExternalTable).getNameToPartitionItemsForScan(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testIneligibleExternalMtmvDoesNotWarmPartitionViewBeforeLock() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenMvccExternalTable hiveExternalTable = Mockito.mock(PluginDrivenMvccExternalTable.class);
        DatabaseIf<TableIf> database = mockDatabase();
        CatalogIf<?> catalog = mockCatalog();
        SessionVariable sessionVariable = new SessionVariable();

        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hiveExternalTable.getId()).thenReturn(32L);
        Mockito.when(hiveExternalTable.getName()).thenReturn("hive_table");
        Mockito.when(hiveExternalTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getId()).thenReturn(2L);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getId()).thenReturn(1L);
        Mockito.when(catalog.getName()).thenReturn("hive_catalog");
        Mockito.when(hiveExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hiveExternalTable.supportsConnectorPartitionPruning()).thenReturn(true);

        MTMVPartitionInfo partitionInfo = new MTMVPartitionInfo(MTMVPartitionType.FOLLOW_BASE_TABLE);
        partitionInfo.setPctInfos(ImmutableList.of(
                new BaseColInfo("p", new BaseTableInfo(hiveExternalTable))));
        MTMV candidateMtmv = Mockito.mock(MTMV.class);
        Mockito.when(candidateMtmv.getMvPartitionInfo()).thenReturn(partitionInfo);
        Mockito.when(candidateMtmv.isUseForRewrite()).thenReturn(true);
        MTMVRelation mtmvRelation = Mockito.mock(MTMVRelation.class);
        BaseTableInfo externalBase = Mockito.mock(BaseTableInfo.class);
        Mockito.when(externalBase.isInternalTable()).thenReturn(false);
        Mockito.when(mtmvRelation.getBaseTablesOneLevelAndFromView()).thenReturn(Collections.singleton(externalBase));
        Mockito.when(candidateMtmv.getRelation()).thenReturn(mtmvRelation);
        org.junit.jupiter.api.Assertions.assertFalse(
                sessionVariable.isEnableMaterializedViewRewriteWhenBaseTableUnawareness(),
                "the default must discard an external MV before MTMV partition validation");

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.getCandidateMTMVs().add(candidateMtmv);
            statementContext.registerExternalTableForPreload(
                    hiveExternalTable, Optional.empty(), Optional.empty(), true);

            statementContext.preloadDeferredScanPartitionViewsBeforeLock(false);

            ExternalTablePreloadInfo preloadInfo = statementContext.getExternalTablePreloadInfo(32L).get();
            org.junit.jupiter.api.Assertions.assertFalse(preloadInfo.hasMtmvPartitionView(),
                    "an ineligible MV never reaches partition mapping, so its full view must not be warmed");
            Mockito.verify(hiveExternalTable, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testEliminatedFilterWarmsUnfilteredPartitionViewBeforeLock() {
        LogicalPlan plan = new NereidsParser().parseSingle(
                "select * from hive_catalog.db.hive_table where true = true or hive_table.p = 1");
        UnboundRelation relation = findUnboundRelation(plan);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenExternalTable hiveExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();

        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hiveExternalTable.getId()).thenReturn(31L);
        Mockito.when(hiveExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hiveExternalTable.supportsConnectorPartitionPruning()).thenReturn(true);
        Optional<Map<String, PartitionItem>> scanView =
                Optional.of(ImmutableMap.of("p1", Mockito.mock(PartitionItem.class)));
        Mockito.when(hiveExternalTable.getNameToPartitionItemsForScan(Mockito.any())).thenReturn(scanView);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            CollectRelation collectRelation = new CollectRelation(false);
            CascadesContext cascadesContext = CascadesContext.initContext(
                    statementContext, plan, PhysicalProperties.ANY);
            boolean hasInitialFilter = Deencapsulation.invoke(
                    collectRelation, "isUnderInitialFilter", cascadesContext, plan, relation);
            org.junit.jupiter.api.Assertions.assertFalse(hasInitialFilter,
                    "a foldable tautology is removed before partition pruning and must warm as unfiltered");

            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(
                    hiveExternalTable, Optional.empty(), Optional.empty(), hasInitialFilter);
            statementContext.preloadDeferredScanPartitionViewsBeforeLock(false);

            org.junit.jupiter.api.Assertions.assertEquals(scanView,
                    statementContext.getExternalTablePreloadInfo(31L).get().getScanPartitionView());
            Mockito.verify(hiveExternalTable, Mockito.times(1)).getNameToPartitionItemsForScan(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void collectRelationRegistersFilteredScanWithInitialFilter() {
        LogicalPlan plan = new NereidsParser().parseSingle(
                "select * from hive_catalog.db.hive_table where hive_table.p = 1");
        UnboundRelation relation = findUnboundRelation(plan);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        StatementContext statementContext = Mockito.mock(StatementContext.class);
        CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        PluginDrivenExternalTable hiveExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        List<String> qualifier = ImmutableList.of("hive_catalog", "db", "hive_table");

        Mockito.when(cascadesContext.getConnectContext()).thenReturn(connectContext);
        Mockito.when(cascadesContext.getStatementContext()).thenReturn(statementContext);
        Mockito.when(cascadesContext.getRewritePlan()).thenReturn(plan);
        Mockito.when(cascadesContext.getRecursiveCteContext()).thenReturn(Optional.empty());
        Mockito.when(cascadesContext.getCteContext()).thenReturn(new CTEContext());
        Mockito.when(connectContext.getStatementContext()).thenReturn(statementContext);
        Mockito.when(statementContext.getAndCacheTable(Mockito.eq(qualifier),
                Mockito.eq(StatementContext.TableFrom.QUERY), Mockito.eq(Optional.of(relation))))
                .thenReturn(hiveExternalTable);
        Mockito.when(statementContext.getPlannerHooks()).thenReturn(Collections.emptySet());

        CollectRelation collectRelation = new CollectRelation(false);
        Deencapsulation.invoke(collectRelation, "collectFromUnboundRelation", cascadesContext,
                relation.getNameParts(), StatementContext.TableFrom.QUERY, Optional.of(relation));

        Mockito.verify(statementContext).registerExternalTableForPreload(hiveExternalTable,
                Optional.empty(), Optional.empty(), true);
    }

    @Test
    public void testUnrelatedJoinFilterKeepsHiveWarmupBeforeLock() {
        LogicalPlan plan = new NereidsParser().parseSingle(
                "select * from internal_catalog.db.internal_table i "
                        + "join hive_catalog.db.hive_table h on i.id = h.id where i.k = 1");
        UnboundRelation relation = findUnboundRelationByName(plan, "hive_table");
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenExternalTable hiveExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();

        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hiveExternalTable.getId()).thenReturn(30L);
        Mockito.when(hiveExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hiveExternalTable.supportsConnectorPartitionPruning()).thenReturn(true);
        Optional<Map<String, PartitionItem>> scanView =
                Optional.of(ImmutableMap.of("p1", Mockito.mock(PartitionItem.class)));
        Mockito.when(hiveExternalTable.getNameToPartitionItemsForScan(Mockito.any())).thenReturn(scanView);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            CollectRelation collectRelation = new CollectRelation(false);
            CascadesContext cascadesContext = CascadesContext.initContext(
                    statementContext, plan, PhysicalProperties.ANY);
            boolean hasInitialFilter = Deencapsulation.invoke(
                    collectRelation, "isUnderInitialFilter", cascadesContext, plan, relation);
            org.junit.jupiter.api.Assertions.assertFalse(hasInitialFilter,
                    "a predicate on the internal join branch must not mark the Hive scan as filtered");

            statementContext.getTables().put(ImmutableList.of("internal_catalog", "db", "internal_table"),
                    internalTable);
            statementContext.registerExternalTableForPreload(hiveExternalTable, Optional.empty(),
                    Optional.empty(), hasInitialFilter);
            statementContext.preloadDeferredScanPartitionViewsBeforeLock(false);

            org.junit.jupiter.api.Assertions.assertEquals(scanView,
                    statementContext.getExternalTablePreloadInfo(30L).get().getScanPartitionView());
            Mockito.verify(hiveExternalTable, Mockito.times(1)).getNameToPartitionItemsForScan(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    private static UnboundRelation findUnboundRelation(Plan plan) {
        if (plan instanceof UnboundRelation) {
            return (UnboundRelation) plan;
        }
        for (Plan child : plan.children()) {
            UnboundRelation relation = findUnboundRelation(child);
            if (relation != null) {
                return relation;
            }
        }
        return null;
    }

    private static UnboundRelation findUnboundRelationByName(Plan plan, String tableName) {
        if (plan instanceof UnboundRelation
                && ((UnboundRelation) plan).getNameParts().contains(tableName)) {
            return (UnboundRelation) plan;
        }
        for (Plan child : plan.children()) {
            UnboundRelation relation = findUnboundRelationByName(child, tableName);
            if (relation != null) {
                return relation;
            }
        }
        return null;
    }

    @Test
    public void testBoundOnlyTautologyWarmsUnfilteredPartitionViewBeforeLock() {
        // `abs(1) = 1` is a tautology, but this classification runs on the PARSED plan, i.e. before function
        // binding, and the folding rule has no implementation for an unbound function. The conjunct nevertheless
        // references no column of the relation, so it cannot select a partition: it is either removed before
        // pruning or it discards every row. Warming as filtered would leave the scan DEFERRED for the collector
        // that runs while the internal read locks are held.
        LogicalPlan plan = new NereidsParser().parseSingle(
                "select * from hive_catalog.db.hive_table where abs(1) = 1");
        UnboundRelation relation = findUnboundRelation(plan);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenExternalTable hiveExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();

        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hiveExternalTable.getId()).thenReturn(33L);
        Mockito.when(hiveExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hiveExternalTable.supportsConnectorPartitionPruning()).thenReturn(true);
        Optional<Map<String, PartitionItem>> scanView =
                Optional.of(ImmutableMap.of("p1", Mockito.mock(PartitionItem.class)));
        Mockito.when(hiveExternalTable.getNameToPartitionItemsForScan(Mockito.any())).thenReturn(scanView);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            CollectRelation collectRelation = new CollectRelation(false);
            CascadesContext cascadesContext = CascadesContext.initContext(
                    statementContext, plan, PhysicalProperties.ANY);
            boolean hasInitialFilter = Deencapsulation.invoke(
                    collectRelation, "isUnderInitialFilter", cascadesContext, plan, relation);
            org.junit.jupiter.api.Assertions.assertFalse(hasInitialFilter,
                    "a conjunct that names no column of the relation cannot select its partitions");

            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(
                    hiveExternalTable, Optional.empty(), Optional.empty(), hasInitialFilter);
            statementContext.preloadDeferredScanPartitionViewsBeforeLock(false);

            org.junit.jupiter.api.Assertions.assertEquals(scanView,
                    statementContext.getExternalTablePreloadInfo(33L).get().getScanPartitionView());
            Mockito.verify(hiveExternalTable, Mockito.times(1)).getNameToPartitionItemsForScan(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testJoinedBoundOnlyTautologyKeepsHiveWarmupBeforeLock() {
        // The same conjunct below a JOIN: the logical selected view of the joined Hive reference must not be
        // narrowed by a predicate that names none of its columns.
        LogicalPlan plan = new NereidsParser().parseSingle(
                "select * from internal_catalog.db.internal_table i join "
                        + "(select * from hive_catalog.db.hive_table where abs(1) = 1) h on i.id = h.id");
        UnboundRelation relation = findUnboundRelationByName(plan, "hive_table");
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        Mockito.when(connectContext.getCurrentUserIdentity()).thenReturn(UserIdentity.ROOT);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenExternalTable hiveExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();

        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hiveExternalTable.getId()).thenReturn(34L);
        Mockito.when(hiveExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hiveExternalTable.supportsConnectorPartitionPruning()).thenReturn(true);
        Optional<Map<String, PartitionItem>> scanView =
                Optional.of(ImmutableMap.of("p1", Mockito.mock(PartitionItem.class)));
        Mockito.when(hiveExternalTable.getNameToPartitionItemsForScan(Mockito.any())).thenReturn(scanView);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            CollectRelation collectRelation = new CollectRelation(false);
            CascadesContext cascadesContext = CascadesContext.initContext(
                    statementContext, plan, PhysicalProperties.ANY);
            boolean hasInitialFilter = Deencapsulation.invoke(
                    collectRelation, "isUnderInitialFilter", cascadesContext, plan, relation);
            org.junit.jupiter.api.Assertions.assertFalse(hasInitialFilter,
                    "a bound-only tautology below a join must not mark the Hive scan as filtered");

            statementContext.getTables().put(ImmutableList.of("internal_catalog", "db", "internal_table"),
                    internalTable);
            statementContext.registerExternalTableForPreload(hiveExternalTable, Optional.empty(),
                    Optional.empty(), hasInitialFilter);
            statementContext.preloadDeferredScanPartitionViewsBeforeLock(false);

            org.junit.jupiter.api.Assertions.assertEquals(scanView,
                    statementContext.getExternalTablePreloadInfo(34L).get().getScanPartitionView());
            Mockito.verify(hiveExternalTable, Mockito.times(1)).getNameToPartitionItemsForScan(Mockito.any());
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

            statementContext.preloadDeferredScanPartitionViewsBeforeLock(false);

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
        sessionVariable.setEnableDmlMaterializedViewRewrite(false);

        // The materialized view has no consumer once BOTH MV rewrite switches are off, so this warmup is pure
        // cost there.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hiveExternalTable.getId()).thenReturn(23L);
        Mockito.when(hiveExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hiveExternalTable.supportsConnectorPartitionPruning()).thenReturn(true);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(hiveExternalTable, Optional.empty(), Optional.empty());

            statementContext.preloadDeferredScanPartitionViewsBeforeLock(false);

            org.junit.jupiter.api.Assertions.assertFalse(
                    statementContext.getExternalTablePreloadInfo(23L).get().hasScanPartitionView());
            Mockito.verify(hiveExternalTable, Mockito.never()).getNameToPartitionItemsForScan(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testPhysicalFinalizationKeepsPrelockWarmupWhenMvRewriteIsDisabled() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenExternalTable hiveExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableMaterializedViewRewrite(false);
        sessionVariable.setEnableDmlMaterializedViewRewrite(false);

        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hiveExternalTable.getId()).thenReturn(29L);
        Mockito.when(hiveExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hiveExternalTable.supportsConnectorPartitionPruning()).thenReturn(true);
        Optional<Map<String, PartitionItem>> scanView =
                Optional.of(ImmutableMap.of("p1", Mockito.mock(PartitionItem.class)));
        Mockito.when(hiveExternalTable.getNameToPartitionItemsForScan(Mockito.any())).thenReturn(scanView);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(hiveExternalTable, Optional.empty(), Optional.empty());

            statementContext.preloadDeferredScanPartitionViewsBeforeLock(true);

            org.junit.jupiter.api.Assertions.assertEquals(scanView,
                    statementContext.getExternalTablePreloadInfo(29L).get().getScanPartitionView());
            Mockito.verify(hiveExternalTable, Mockito.times(1)).getNameToPartitionItemsForScan(Mockito.any());
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
