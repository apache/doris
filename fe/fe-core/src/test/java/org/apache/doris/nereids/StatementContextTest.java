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
import org.apache.doris.datasource.mvcc.MvccSnapshot;
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
        DatabaseIf<TableIf> database = Mockito.mock(DatabaseIf.class);
        CatalogIf<DatabaseIf> catalog = Mockito.mock(CatalogIf.class);
        MvccSnapshot mvccSnapshot = Mockito.mock(MvccSnapshot.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        // Mock the latest Hudi preload path and a lock-requiring internal table.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
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

            statementContext.preloadExternalTablesBeforeLock();

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

            statementContext.preloadExternalTablesBeforeLock();

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
        DatabaseIf<TableIf> database = Mockito.mock(DatabaseIf.class);
        CatalogIf<DatabaseIf> catalog = Mockito.mock(CatalogIf.class);
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnablePreloadExternalMetadata(true);

        // Mark one relation as latest and another relation as explicit snapshot, then skip latest snapshot preload.
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(internalTable.needReadLockWhenPlan()).thenReturn(true);
        Mockito.when(hmsExternalTable.getId()).thenReturn(12L);
        Mockito.when(hmsExternalTable.getName()).thenReturn("hudi_tbl");
        Mockito.when(hmsExternalTable.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("ctl");

        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement("select 1", 0));
        try {
            statementContext.getTables().put(ImmutableList.of("ctl", "db", "internal"), internalTable);
            statementContext.registerExternalTableForPreload(hmsExternalTable, Optional.empty(), Optional.empty());
            statementContext.registerExternalTableForPreload(hmsExternalTable,
                    Optional.of(new TableSnapshot("2024-01-01 00:00:00", TableSnapshot.VersionType.TIME)),
                    Optional.empty());

            statementContext.preloadExternalTablesBeforeLock();

            Mockito.verify(hmsExternalTable, Mockito.never())
                    .loadSnapshot(Mockito.<Optional<TableSnapshot>>any(), Mockito.any());
            Mockito.verify(hmsExternalTable, Mockito.times(1)).getBaseSchema();
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

            statementContext.preloadExternalTablesBeforeLock();

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

            statementContext.preloadExternalTablesBeforeLock();

            Mockito.verify(pluginExternalTable, Mockito.never()).getBaseSchema();
        } finally {
            statementContext.close();
        }
    }
}
