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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ShowTableStatsCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static final String tableNotExist = "table_not_exist";
    private static final String partitionNotExist = "partition_not_exist";

    private Env env = Mockito.mock(Env.class);
    private InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
    private AccessControllerManager accessControllerManager = Mockito.mock(AccessControllerManager.class);
    private ConnectContext connectContext = Mockito.mock(ConnectContext.class);
    private CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
    private Database db;

    private MockedStatic<Env> mockedEnv;
    private MockedStatic<ConnectContext> mockedConnectContext;

    @BeforeEach
    void setUp() throws Exception {
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedConnectContext = Mockito.mockStatic(ConnectContext.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnv.when(Env::getCurrentInvertedIndex).thenReturn(Mockito.mock(TabletInvertedIndex.class));
        mockedConnectContext.when(ConnectContext::get).thenReturn(connectContext);
        Mockito.when(connectContext.getState()).thenReturn(Mockito.mock(QueryState.class));
        db = CatalogMocker.mockDb();
    }

    @AfterEach
    void tearDown() {
        if (mockedConnectContext != null) {
            mockedConnectContext.close();
        }
        if (mockedEnv != null) {
            mockedEnv.close();
        }
    }

    private void runBefore() throws Exception {
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(ArgumentMatchers.anyString());
        Mockito.doReturn(Optional.of(db)).when(catalog).getDb(ArgumentMatchers.anyString());

        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        Mockito.when(connectContext.isSkipAuth()).thenReturn(true);
        Mockito.when(accessControllerManager.checkGlobalPriv(ArgumentMatchers.nullable(ConnectContext.class),
                ArgumentMatchers.any(PrivPredicate.class))).thenReturn(true);
        Mockito.when(accessControllerManager.checkTblPriv(ArgumentMatchers.nullable(ConnectContext.class), ArgumentMatchers.eq(internalCtl),
                ArgumentMatchers.eq(CatalogMocker.TEST_DB_NAME), ArgumentMatchers.eq(CatalogMocker.TEST_TBL_NAME),
                ArgumentMatchers.any(PrivPredicate.class))).thenReturn(true);
    }

    private Set<String> getColumns() {
        Set<String> columnNames = new HashSet<>();
        List<Column> columns = CatalogMocker.TEST_TBL_BASE_SCHEMA;
        for (Column column : columns) {
            columnNames.add(column.getName());
        }
        return columnNames;
    }

    @Test
    public void testValidateNormal() throws Exception {
        runBefore();
        //test useTableId is true
        ShowTableStatsCommand command = new ShowTableStatsCommand(CatalogMocker.TEST_TBL_ID);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //test useTableId is false
        TableNameInfo tableNameInfo =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false,
                ImmutableList.of(CatalogMocker.TEST_SINGLE_PARTITION_NAME));
        List<String> columns = new ArrayList<>(getColumns());
        ShowTableStatsCommand command2 = new ShowTableStatsCommand(tableNameInfo, columns, partitionNamesInfo);
        Assertions.assertDoesNotThrow(() -> command2.validate(connectContext));

        //test tableName is not exist
        TableNameInfo tableNameInfo2 =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, tableNotExist);
        ShowTableStatsCommand command3 = new ShowTableStatsCommand(tableNameInfo2, columns, partitionNamesInfo);
        Assertions.assertThrows(AnalysisException.class, () -> command3.validate(connectContext),
                "Table: " + tableNotExist + " not exists");

        //test partitionName is not exist
        PartitionNamesInfo partitionNamesInfo2 = new PartitionNamesInfo(false,
                ImmutableList.of(partitionNotExist));
        ShowTableStatsCommand command4 = new ShowTableStatsCommand(tableNameInfo, columns, partitionNamesInfo2);
        Assertions.assertThrows(AnalysisException.class, () -> command4.validate(connectContext),
                "Partition: " + partitionNotExist + " not exists");
    }

    @Test
    void testValidateNoPrivilege() {
        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        Mockito.when(accessControllerManager.checkGlobalPriv(ArgumentMatchers.nullable(ConnectContext.class),
                ArgumentMatchers.any(PrivPredicate.class))).thenReturn(false);
        Mockito.when(accessControllerManager.checkTblPriv(ArgumentMatchers.nullable(ConnectContext.class), ArgumentMatchers.eq(internalCtl),
                ArgumentMatchers.eq(CatalogMocker.TEST_DB_NAME), ArgumentMatchers.eq(CatalogMocker.TEST_TBL2_NAME),
                ArgumentMatchers.any(PrivPredicate.class))).thenReturn(false);

        // set up catalog chain for the non-useTableId case
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.doReturn(catalog).when(catalogMgr).getCatalog(ArgumentMatchers.anyString());
        Mockito.doReturn(Optional.of(db)).when(catalog).getDb(ArgumentMatchers.anyString());

        //test useTableId is true
        ShowTableStatsCommand command = new ShowTableStatsCommand(CatalogMocker.TEST_TBL_ID);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "Permission denied command denied to user 'null'@'null' for table '30000'");

        //test useTableId is false
        TableNameInfo tableNameInfo2 =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME);
        PartitionNamesInfo partitionNamesInfo2 = new PartitionNamesInfo(false,
                ImmutableList.of(CatalogMocker.TEST_PARTITION1_NAME));
        List<String> columns = new ArrayList<>(getColumns());
        ShowTableStatsCommand command2 = new ShowTableStatsCommand(tableNameInfo2, columns, partitionNamesInfo2);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext),
                "Permission denied command denied to user 'null'@'null' for table 'test_db: test_tbl2'");
    }
}
