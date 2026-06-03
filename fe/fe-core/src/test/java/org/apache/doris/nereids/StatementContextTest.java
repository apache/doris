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

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.PluginDrivenExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

public class StatementContextTest {

    @Test
    public void testPreloadExternalTablesBeforeLock() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        HMSExternalTable hmsExternalTable = Mockito.mock(HMSExternalTable.class);
        DatabaseIf<TableIf> database = mockDatabase();
        CatalogIf<?> catalog = mockCatalog();
        MvccSnapshot mvccSnapshot = Mockito.mock(MvccSnapshot.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        // Mock the latest Hudi preload path and a lock-requiring internal table.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(connectContext.getQueryIdentifier()).thenReturn("query-1");
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hmsExternalTable.getId()).thenReturn(10L);
        Mockito.when(hmsExternalTable.getName()).thenReturn("hudi_tbl");
        Mockito.when(hmsExternalTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        Mockito.when(hmsExternalTable.getDlaType()).thenReturn(DLAType.HUDI);
        Mockito.when(hmsExternalTable.loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any()))
                .thenReturn(mvccSnapshot);
        Mockito.when(hmsExternalTable.getBaseSchema()).thenReturn(Collections.emptyList());
        Mockito.when(hmsExternalTable.supportInternalPartitionPruned()).thenReturn(true);
        Mockito.when(hmsExternalTable.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(hmsExternalTable, Optional.empty(), Optional.empty());

            StatementContext.ExternalMetadataPreloadResult result =
                    statementContext.preloadExternalTablesBeforeLock();

            org.junit.jupiter.api.Assertions.assertTrue(result.isExecuted());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getCandidateTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getPreloadedTableCount());
            Mockito.verify(hmsExternalTable, Mockito.times(1))
                    .loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any());
            Mockito.verify(hmsExternalTable, Mockito.times(1)).getBaseSchema();
            Mockito.verify(hmsExternalTable, Mockito.times(1)).initSelectedPartitions(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testSkipPreloadWhenSessionVariableDisabled() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        HMSExternalTable hmsExternalTable = Mockito.mock(HMSExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();

        // Keep the preload switch disabled so no external access should happen.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(hmsExternalTable.getId()).thenReturn(11L);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(hmsExternalTable, Optional.empty(), Optional.empty());

            StatementContext.ExternalMetadataPreloadResult result =
                    statementContext.preloadExternalTablesBeforeLock();

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
    public void testSkipLatestPreloadWhenExplicitSnapshotExists() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        HMSExternalTable hmsExternalTable = Mockito.mock(HMSExternalTable.class);
        DatabaseIf<TableIf> database = mockDatabase();
        CatalogIf<?> catalog = mockCatalog();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        // Mark one relation as latest and another relation as explicit snapshot, then skip latest snapshot preload.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(connectContext.getQueryIdentifier()).thenReturn("query-2");
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hmsExternalTable.getId()).thenReturn(12L);
        Mockito.when(hmsExternalTable.getName()).thenReturn("hudi_tbl");
        Mockito.when(hmsExternalTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        Mockito.when(hmsExternalTable.getDlaType()).thenReturn(DLAType.HUDI);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(hmsExternalTable, Optional.empty(), Optional.empty());
            statementContext.registerExternalTableForPreload(hmsExternalTable,
                    Optional.of(new TableSnapshot("2024-01-01 00:00:00", TableSnapshot.VersionType.TIME)),
                    Optional.empty());

            StatementContext.ExternalMetadataPreloadResult result =
                    statementContext.preloadExternalTablesBeforeLock();

            org.junit.jupiter.api.Assertions.assertTrue(result.isExecuted());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getCandidateTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(0, result.getPreloadedTableCount());
            Mockito.verify(hmsExternalTable, Mockito.never())
                    .loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any());
            Mockito.verify(hmsExternalTable, Mockito.never()).getBaseSchema();
            Mockito.verify(hmsExternalTable, Mockito.never()).initSelectedPartitions(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testPreloadJdbcExternalTablesBeforeLock() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PluginDrivenExternalCatalog jdbcCatalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        PluginDrivenExternalTable jdbcExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        // Route preload through the JDBC plugin catalog and keep it schema-only.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(connectContext.getQueryIdentifier()).thenReturn("query-3");
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(jdbcExternalTable.getId()).thenReturn(13L);
        Mockito.when(jdbcExternalTable.getCatalog()).thenReturn(jdbcCatalog);
        Mockito.when(jdbcCatalog.getType()).thenReturn("jdbc");
        Mockito.when(jdbcExternalTable.getBaseSchema()).thenReturn(Collections.emptyList());
        Mockito.when(jdbcExternalTable.supportInternalPartitionPruned()).thenReturn(false);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(jdbcExternalTable, Optional.empty(), Optional.empty());

            StatementContext.ExternalMetadataPreloadResult result =
                    statementContext.preloadExternalTablesBeforeLock();

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
        PluginDrivenExternalCatalog esCatalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        PluginDrivenExternalTable pluginExternalTable = Mockito.mock(PluginDrivenExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        // Keep non-JDBC plugin catalogs outside the preload whitelist.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(pluginExternalTable.getId()).thenReturn(14L);
        Mockito.when(pluginExternalTable.getCatalog()).thenReturn(esCatalog);
        Mockito.when(esCatalog.getType()).thenReturn("es");

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(pluginExternalTable, Optional.empty(), Optional.empty());

            StatementContext.ExternalMetadataPreloadResult result =
                    statementContext.preloadExternalTablesBeforeLock();

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
        HMSExternalTable hmsExternalTable = Mockito.mock(HMSExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        // Skip preload when the statement does not require any internal plan-time read lock.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(false);
        Mockito.when(hmsExternalTable.getId()).thenReturn(15L);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(hmsExternalTable, Optional.empty(), Optional.empty());

            StatementContext.ExternalMetadataPreloadResult result =
                    statementContext.preloadExternalTablesBeforeLock();

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
        IcebergExternalTable icebergExternalTable = Mockito.mock(IcebergExternalTable.class);
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
        Mockito.when(icebergExternalTable.loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any()))
                .thenReturn(mvccSnapshot);
        Mockito.when(icebergExternalTable.getBaseSchema()).thenReturn(Collections.emptyList());
        Mockito.when(icebergExternalTable.supportInternalPartitionPruned()).thenReturn(false);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(icebergExternalTable, Optional.empty(), Optional.empty());

            StatementContext.ExternalMetadataPreloadResult result =
                    statementContext.preloadExternalTablesBeforeLock();

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
        IcebergExternalTable icebergExternalTable = Mockito.mock(IcebergExternalTable.class);
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
        Mockito.when(icebergExternalTable.supportInternalPartitionPruned()).thenReturn(true);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(icebergExternalTable,
                    Optional.of(new TableSnapshot("2024-01-01 00:00:00", TableSnapshot.VersionType.TIME)),
                    Optional.empty());

            StatementContext.ExternalMetadataPreloadResult result =
                    statementContext.preloadExternalTablesBeforeLock();

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
        PaimonExternalTable paimonExternalTable = Mockito.mock(PaimonExternalTable.class);
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
        Mockito.when(paimonExternalTable.loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any()))
                .thenReturn(mvccSnapshot);
        Mockito.when(paimonExternalTable.getBaseSchema()).thenReturn(Collections.emptyList());
        Mockito.when(paimonExternalTable.supportInternalPartitionPruned()).thenReturn(false);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(paimonExternalTable, Optional.empty(), Optional.empty());

            StatementContext.ExternalMetadataPreloadResult result =
                    statementContext.preloadExternalTablesBeforeLock();

            org.junit.jupiter.api.Assertions.assertTrue(result.isExecuted());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getCandidateTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getPreloadedTableCount());
            Mockito.verify(paimonExternalTable, Mockito.times(1))
                    .loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any());
            Mockito.verify(paimonExternalTable, Mockito.times(1)).getBaseSchema();
        } finally {
            statementContext.close();
        }
    }

    @SuppressWarnings("unchecked")
    private DatabaseIf<TableIf> mockDatabase() {
        return Mockito.mock(DatabaseIf.class);
    }

    private CatalogIf<?> mockCatalog() {
        return Mockito.mock(CatalogIf.class);
    }
}
