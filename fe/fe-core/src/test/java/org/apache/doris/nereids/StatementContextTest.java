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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalScanTaskCacheKey;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.jdbc.JdbcExternalTable;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

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

        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(connectContext.getQueryIdentifier()).thenReturn("query-1");
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hmsExternalTable.getId()).thenReturn(10L);
        Mockito.when(hmsExternalTable.getName()).thenReturn("hudi_tbl");
        Mockito.when(hmsExternalTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        Mockito.when(hmsExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hmsExternalTable.supportsLatestSnapshotPreload()).thenReturn(true);
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

            ExternalMetadataPreloadResult result = executePreload(statementContext);

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
    public void testPreloadHiveSchemaAndPartitionsBeforeLock() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        HMSExternalTable hmsExternalTable = Mockito.mock(HMSExternalTable.class);
        DatabaseIf<TableIf> database = mockDatabase();
        CatalogIf<?> catalog = mockCatalog();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(connectContext.getQueryIdentifier()).thenReturn("query-hive");
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hmsExternalTable.getId()).thenReturn(19L);
        Mockito.when(hmsExternalTable.getName()).thenReturn("hive_tbl");
        Mockito.when(hmsExternalTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        Mockito.when(hmsExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hmsExternalTable.supportsLatestSnapshotPreload()).thenReturn(false);
        Mockito.when(hmsExternalTable.getDlaType()).thenReturn(DLAType.HIVE);
        Mockito.when(hmsExternalTable.getBaseSchema()).thenReturn(Collections.emptyList());
        Mockito.when(hmsExternalTable.supportInternalPartitionPruned()).thenReturn(true);
        Mockito.when(hmsExternalTable.initSelectedPartitions(Mockito.any())).thenReturn(SelectedPartitions.NOT_PRUNED);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(hmsExternalTable, Optional.empty(), Optional.empty());

            ExternalMetadataPreloadResult result = executePreload(statementContext);

            org.junit.jupiter.api.Assertions.assertTrue(result.isExecuted());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getCandidateTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getPreloadedTableCount());
            Mockito.verify(hmsExternalTable, Mockito.never())
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
    public void testPreloadLatestRelationWhenExplicitSnapshotAliasExists() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        HMSExternalTable hmsExternalTable = Mockito.mock(HMSExternalTable.class);
        DatabaseIf<TableIf> database = mockDatabase();
        CatalogIf<?> catalog = mockCatalog();
        MvccSnapshot mvccSnapshot = Mockito.mock(MvccSnapshot.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        // A historical alias must not cancel the metadata warmup required by a latest alias.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(connectContext.getQueryIdentifier()).thenReturn("query-2");
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hmsExternalTable.getId()).thenReturn(12L);
        Mockito.when(hmsExternalTable.getName()).thenReturn("hudi_tbl");
        Mockito.when(hmsExternalTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        Mockito.when(hmsExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.when(hmsExternalTable.supportsLatestSnapshotPreload()).thenReturn(true);
        Mockito.when(hmsExternalTable.getDlaType()).thenReturn(DLAType.HUDI);
        // StatementContext stores the returned snapshot, so null is not a valid preload result.
        Mockito.when(hmsExternalTable.loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any()))
                .thenReturn(mvccSnapshot);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(hmsExternalTable, Optional.empty(), Optional.empty());
            statementContext.registerExternalTableForPreload(hmsExternalTable,
                    Optional.of(new TableSnapshot("2024-01-01 00:00:00", TableSnapshot.VersionType.TIME)),
                    Optional.empty());

            ExternalMetadataPreloadResult result = executePreload(statementContext);

            org.junit.jupiter.api.Assertions.assertTrue(result.isExecuted());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getCandidateTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getPreloadedTableCount());
            Mockito.verify(hmsExternalTable, Mockito.times(1))
                    .loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any());
            Mockito.verify(hmsExternalTable, Mockito.times(1)).getBaseSchema();
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testPreloadHmsIcebergLatestSnapshotBeforeLock() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        HMSExternalTable hmsExternalTable = Mockito.mock(HMSExternalTable.class);
        DatabaseIf<TableIf> database = mockDatabase();
        CatalogIf<?> catalog = mockCatalog();
        MvccSnapshot mvccSnapshot = Mockito.mock(MvccSnapshot.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hmsExternalTable.getId()).thenReturn(14L);
        Mockito.when(hmsExternalTable.getName()).thenReturn("hms_iceberg_tbl");
        Mockito.when(hmsExternalTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        Mockito.when(hmsExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.doCallRealMethod().when(hmsExternalTable).supportsLatestSnapshotPreload();
        Mockito.when(hmsExternalTable.getDlaType()).thenReturn(DLAType.ICEBERG);
        Mockito.when(hmsExternalTable.loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any()))
                .thenReturn(mvccSnapshot);
        Mockito.when(hmsExternalTable.getBaseSchema()).thenReturn(Collections.emptyList());
        Mockito.when(hmsExternalTable.supportInternalPartitionPruned()).thenReturn(false);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(hmsExternalTable, Optional.empty(), Optional.empty());

            ExternalMetadataPreloadResult result = executePreload(statementContext);

            org.junit.jupiter.api.Assertions.assertTrue(result.isExecuted());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getCandidateTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(1, result.getPreloadedTableCount());
            Mockito.verify(hmsExternalTable, Mockito.times(1))
                    .loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any());
            Mockito.verify(hmsExternalTable, Mockito.times(1)).getBaseSchema();
            Mockito.verify(hmsExternalTable, Mockito.never()).initSelectedPartitions(Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testSkipHmsIcebergPreloadWhenOnlyNonLatestRelationExists() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        HMSExternalTable hmsExternalTable = Mockito.mock(HMSExternalTable.class);
        DatabaseIf<TableIf> database = mockDatabase();
        CatalogIf<?> catalog = mockCatalog();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hmsExternalTable.getId()).thenReturn(15L);
        Mockito.when(hmsExternalTable.getName()).thenReturn("hms_iceberg_tbl");
        Mockito.when(hmsExternalTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        Mockito.when(hmsExternalTable.supportsExternalMetadataPreload()).thenReturn(true);
        Mockito.doCallRealMethod().when(hmsExternalTable).supportsLatestSnapshotPreload();
        Mockito.when(hmsExternalTable.getDlaType()).thenReturn(DLAType.ICEBERG);
        Mockito.when(hmsExternalTable.supportInternalPartitionPruned()).thenReturn(false);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(hmsExternalTable,
                    Optional.of(new TableSnapshot("2024-01-01 00:00:00", TableSnapshot.VersionType.TIME)),
                    Optional.empty());

            ExternalMetadataPreloadResult result = executePreload(statementContext);

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
        JdbcExternalTable jdbcExternalTable = Mockito.mock(JdbcExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

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
    public void testSkipPreloadForUnsupportedExternalTable() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        ExternalTable unsupportedExternalTable = Mockito.mock(ExternalTable.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(unsupportedExternalTable.supportsExternalMetadataPreload()).thenReturn(false);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(unsupportedExternalTable, Optional.empty(), Optional.empty());

            ExternalMetadataPreloadResult result = executePreload(statementContext);

            org.junit.jupiter.api.Assertions.assertFalse(result.isExecuted());
            org.junit.jupiter.api.Assertions.assertEquals(0, result.getCandidateTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(0, result.getPreloadedTableCount());
            org.junit.jupiter.api.Assertions.assertEquals(
                    "no external preload candidates were collected", result.getSkipReason());
            Mockito.verify(unsupportedExternalTable, Mockito.never()).getBaseSchema();
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
        IcebergExternalTable icebergExternalTable = Mockito.mock(IcebergExternalTable.class);
        DatabaseIf<TableIf> database = mockDatabase();
        CatalogIf<?> catalog = mockCatalog();
        MvccSnapshot mvccSnapshot = Mockito.mock(MvccSnapshot.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

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
        IcebergExternalTable icebergExternalTable = Mockito.mock(IcebergExternalTable.class);
        DatabaseIf<TableIf> database = mockDatabase();
        CatalogIf<?> catalog = mockCatalog();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

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
        PaimonExternalTable paimonExternalTable = Mockito.mock(PaimonExternalTable.class);
        DatabaseIf<TableIf> database = mockDatabase();
        CatalogIf<?> catalog = mockCatalog();
        MvccSnapshot mvccSnapshot = Mockito.mock(MvccSnapshot.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

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
    public void testPaimonOptionsRelationSkipsLatestSnapshotPreloadBeforeLock() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        TableIf internalTable = Mockito.mock(TableIf.class);
        PaimonExternalTable paimonExternalTable = Mockito.mock(PaimonExternalTable.class);
        DatabaseIf<TableIf> database = mockDatabase();
        CatalogIf<?> catalog = mockCatalog();
        MvccSnapshot mvccSnapshot = Mockito.mock(MvccSnapshot.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(paimonExternalTable.getId()).thenReturn(18L);
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
        Mockito.when(paimonExternalTable.initSelectedPartitions(Mockito.any()))
                .thenReturn(SelectedPartitions.NOT_PRUNED);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(
                    paimonExternalTable,
                    Optional.empty(),
                    Optional.of(new TableScanParams(
                            TableScanParams.OPTIONS,
                            ImmutableMap.of("scan.plan-sort-partition", "true"),
                            Collections.emptyList())));

            ExternalMetadataPreloadResult result = executePreload(statementContext);

            org.junit.jupiter.api.Assertions.assertEquals(0, result.getPreloadedTableCount());
            Mockito.verify(paimonExternalTable, Mockito.never())
                    .loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any());
            Mockito.verify(paimonExternalTable, Mockito.never()).getBaseSchema();
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testLoadSnapshotsKeepsEachRelationSnapshotCurrent() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        DatabaseIf<TableIf> database = mockDatabase();
        CatalogIf<?> catalog = mockCatalog();
        MvccSnapshot firstSnapshot = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot secondSnapshot = Mockito.mock(MvccSnapshot.class);

        Mockito.when(table.getName()).thenReturn("historical_table");
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        Mockito.when(table.loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any()))
                .thenReturn(firstSnapshot, secondSnapshot);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.loadSnapshots(table,
                    Optional.of(new TableSnapshot("1", TableSnapshot.VersionType.VERSION)), Optional.empty());
            org.junit.jupiter.api.Assertions.assertSame(firstSnapshot,
                    statementContext.getSnapshot(table).orElseThrow(AssertionError::new));

            statementContext.loadSnapshots(table,
                    Optional.of(new TableSnapshot("2", TableSnapshot.VersionType.VERSION)), Optional.empty());

            org.junit.jupiter.api.Assertions.assertSame(secondSnapshot,
                    statementContext.getSnapshot(table).orElseThrow(AssertionError::new));
            Mockito.verify(table, Mockito.times(2))
                    .loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testLatestSnapshotIsIndependentOfHistoricalRelationOrder() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        DatabaseIf<TableIf> database = mockDatabase();
        CatalogIf<?> catalog = mockCatalog();
        MvccSnapshot latestSnapshot = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot historicalSnapshot = Mockito.mock(MvccSnapshot.class);

        Mockito.when(table.getName()).thenReturn("mixed_version_table");
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        Mockito.when(table.loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any()))
                .thenAnswer(invocation -> ((Optional<?>) invocation.getArgument(0)).isPresent()
                        ? historicalSnapshot : latestSnapshot);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.loadSnapshots(table, Optional.empty(), Optional.empty());
            statementContext.loadSnapshots(table,
                    Optional.of(new TableSnapshot("1", TableSnapshot.VersionType.VERSION)), Optional.empty());
            org.junit.jupiter.api.Assertions.assertSame(historicalSnapshot,
                    statementContext.getSnapshot(table).orElseThrow(AssertionError::new));

            statementContext.loadSnapshots(table, Optional.empty(), Optional.empty());

            org.junit.jupiter.api.Assertions.assertSame(latestSnapshot,
                    statementContext.getSnapshot(table).orElseThrow(AssertionError::new));
            Mockito.verify(table, Mockito.times(2))
                    .loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any());
        } finally {
            statementContext.close();
        }

        StatementContext reverseStatementContext = new StatementContext(
                connectContext, new OriginStatement("select 1", 0));
        try {
            reverseStatementContext.loadSnapshots(table,
                    Optional.of(new TableSnapshot("1", TableSnapshot.VersionType.VERSION)), Optional.empty());
            org.junit.jupiter.api.Assertions.assertSame(historicalSnapshot,
                    reverseStatementContext.getSnapshot(table).orElseThrow(AssertionError::new));

            reverseStatementContext.loadSnapshots(table, Optional.empty(), Optional.empty());

            org.junit.jupiter.api.Assertions.assertSame(latestSnapshot,
                    reverseStatementContext.getSnapshot(table).orElseThrow(AssertionError::new));
            Mockito.verify(table, Mockito.times(4))
                    .loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any());
        } finally {
            reverseStatementContext.close();
        }
    }

    @Test
    public void testInjectedSnapshotRemainsAuthoritativeForLatestRelation() {
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        DatabaseIf<TableIf> database = mockDatabase();
        CatalogIf<?> catalog = mockCatalog();
        MvccSnapshot injectedSnapshot = Mockito.mock(MvccSnapshot.class);
        MvccSnapshot reloadedSnapshot = Mockito.mock(MvccSnapshot.class);

        Mockito.when(table.getName()).thenReturn("mtmv_base_table");
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");
        Mockito.when(table.loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any()))
                .thenReturn(reloadedSnapshot);

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.setSnapshot(new org.apache.doris.datasource.mvcc.MvccTableInfo(table),
                    injectedSnapshot);

            Optional<MvccSnapshot> loaded = statementContext.loadSnapshots(
                    table, Optional.empty(), Optional.empty());

            org.junit.jupiter.api.Assertions.assertSame(injectedSnapshot,
                    loaded.orElseThrow(AssertionError::new));
            Mockito.verify(table, Mockito.never()).loadSnapshot(Mockito.any(), Mockito.any());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testExternalScanTasksUseSingleFlight() throws Exception {
        StatementContext statementContext = new StatementContext();
        StatementContext.ExternalScanTaskCache cache = statementContext.getExternalScanTaskCache();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch waiterLookedUpKey = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        AtomicInteger loadCount = new AtomicInteger();
        AtomicInteger hashCalls = new AtomicInteger();
        ExternalScanTaskCacheKey<String> observedKey =
                new ObservableScanTaskCacheKey("same-scan", hashCalls, waiterLookedUpKey);
        try {
            Future<List<String>> first = executor.submit(
                    () -> cache.getOrLoad(observedKey, () -> {
                        loadCount.incrementAndGet();
                        loaderStarted.countDown();
                        releaseLoader.await();
                        return Collections.singletonList("task");
                    }));
            loaderStarted.await();
            Future<List<String>> second = executor.submit(
                    () -> cache.getOrLoad(observedKey, () -> {
                        loadCount.incrementAndGet();
                        return Collections.singletonList("duplicate");
                    }));

            waiterLookedUpKey.await();
            releaseLoader.countDown();

            org.junit.jupiter.api.Assertions.assertEquals(
                    Collections.singletonList("task"), first.get());
            org.junit.jupiter.api.Assertions.assertEquals(
                    Collections.singletonList("task"), second.get());
            org.junit.jupiter.api.Assertions.assertEquals(1, loadCount.get());
        } finally {
            executor.shutdownNow();
            statementContext.close();
        }
    }

    @Test
    public void testExternalScanTaskFailureCanRetry() throws Exception {
        StatementContext statementContext = new StatementContext();
        StatementContext.ExternalScanTaskCache cache = statementContext.getExternalScanTaskCache();
        ExternalScanTaskCacheKey<String> key = new TestScanTaskCacheKey("retry");
        AtomicInteger loadCount = new AtomicInteger();
        try {
            IllegalStateException failure = org.junit.jupiter.api.Assertions.assertThrows(
                    IllegalStateException.class,
                    () -> cache.getOrLoad(key, () -> {
                        loadCount.incrementAndGet();
                        throw new IllegalStateException("load failed");
                    }));
            org.junit.jupiter.api.Assertions.assertEquals("load failed", failure.getMessage());
            List<String> tasks = cache.getOrLoad(key, () -> {
                loadCount.incrementAndGet();
                return Collections.singletonList("retry-task");
            });

            org.junit.jupiter.api.Assertions.assertEquals(
                    Collections.singletonList("retry-task"), tasks);
            org.junit.jupiter.api.Assertions.assertEquals(2, loadCount.get());
        } finally {
            statementContext.close();
        }
    }

    @Test
    public void testExternalScanTaskGenerationIsIsolatedByResetAndExecutionEnd() throws Exception {
        StatementContext statementContext = new StatementContext();
        CountDownLatch loaderStarted = new CountDownLatch(1);
        CountDownLatch waiterLookedUpKey = new CountDownLatch(1);
        CountDownLatch releaseLoader = new CountDownLatch(1);
        AtomicInteger hashCalls = new AtomicInteger();
        ExternalScanTaskCacheKey<String> key =
                new ObservableScanTaskCacheKey("lifecycle", hashCalls, waiterLookedUpKey);
        AtomicInteger loadCount = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        StatementContext.ExternalScanTaskCache oldGeneration =
                statementContext.getExternalScanTaskCache();
        try {
            Future<List<String>> oldOwner = executor.submit(
                    () -> oldGeneration.getOrLoad(key, () -> {
                        int loadNumber = loadCount.incrementAndGet();
                        loaderStarted.countDown();
                        releaseLoader.await();
                        return Collections.singletonList("old-" + loadNumber);
                    }));
            loaderStarted.await();
            Future<List<String>> oldWaiter = executor.submit(
                    () -> oldGeneration.getOrLoad(key,
                            () -> Collections.singletonList("duplicate")));
            waiterLookedUpKey.await();

            statementContext.resetMvccSnapshots();
            StatementContext.ExternalScanTaskCache newGeneration =
                    statementContext.getExternalScanTaskCache();
            org.junit.jupiter.api.Assertions.assertEquals(
                    Collections.singletonList("new-2"),
                    newGeneration.getOrLoad(key,
                        () -> Collections.singletonList("new-" + loadCount.incrementAndGet())));
            releaseLoader.countDown();
            org.junit.jupiter.api.Assertions.assertEquals(
                    Collections.singletonList("old-1"), oldOwner.get());
            org.junit.jupiter.api.Assertions.assertEquals(
                    Collections.singletonList("old-1"), oldWaiter.get());
            org.junit.jupiter.api.Assertions.assertEquals(
                    Collections.singletonList("new-2"),
                    newGeneration.getOrLoad(key,
                            () -> Collections.singletonList("duplicate")));
            org.junit.jupiter.api.Assertions.assertEquals(
                    Collections.singletonList("old-3"),
                    oldGeneration.getOrLoad(key,
                        () -> Collections.singletonList("old-" + loadCount.incrementAndGet())));

            statementContext.clearExternalScanTasks();
            org.junit.jupiter.api.Assertions.assertEquals(
                    Collections.singletonList("ended-4"),
                    newGeneration.getOrLoad(key,
                        () -> Collections.singletonList("ended-" + loadCount.incrementAndGet())));
            org.junit.jupiter.api.Assertions.assertEquals(
                    Collections.singletonList("ended-5"),
                    newGeneration.getOrLoad(key,
                        () -> Collections.singletonList("ended-" + loadCount.incrementAndGet())));

            org.junit.jupiter.api.Assertions.assertEquals(5, loadCount.get());
        } finally {
            executor.shutdownNow();
            statementContext.close();
        }
    }

    private static void assertFutureFailedWith(
            Future<?> future, Class<? extends Throwable> causeType, String message) {
        ExecutionException exception = org.junit.jupiter.api.Assertions.assertThrows(
                ExecutionException.class, future::get);
        org.junit.jupiter.api.Assertions.assertInstanceOf(causeType, exception.getCause());
        org.junit.jupiter.api.Assertions.assertEquals(message, exception.getCause().getMessage());
    }

    private static final class TestScanTaskCacheKey implements ExternalScanTaskCacheKey<String> {
        private final String value;

        private TestScanTaskCacheKey(String value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object object) {
            return object instanceof TestScanTaskCacheKey
                    && value.equals(((TestScanTaskCacheKey) object).value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

    private static final class ObservableScanTaskCacheKey implements ExternalScanTaskCacheKey<String> {
        private final String value;
        private final AtomicInteger hashCalls;
        private final CountDownLatch secondLookup;

        private ObservableScanTaskCacheKey(
                String value, AtomicInteger hashCalls, CountDownLatch secondLookup) {
            this.value = value;
            this.hashCalls = hashCalls;
            this.secondLookup = secondLookup;
        }

        @Override
        public boolean equals(Object object) {
            return object instanceof ObservableScanTaskCacheKey
                    && value.equals(((ObservableScanTaskCacheKey) object).value);
        }

        @Override
        public int hashCode() {
            if (hashCalls.incrementAndGet() == 2) {
                secondLookup.countDown();
            }
            return value.hashCode();
        }
    }

    @SuppressWarnings("unchecked")
    private DatabaseIf<TableIf> mockDatabase() {
        return Mockito.mock(DatabaseIf.class);
    }

    @Test
    public void testResetMvccSnapshotsClearsPreloadCompletionButKeepsCandidates() {
        StatementContext statementContext = new StatementContext();
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
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

    private ExternalMetadataPreloadResult executePreload(StatementContext statementContext) {
        ExternalMetadataPreloadResult result = new PreloadExternalMetadata().executePreload(statementContext);
        statementContext.setExternalMetadataPreloadResult(result);
        return result;
    }
}
