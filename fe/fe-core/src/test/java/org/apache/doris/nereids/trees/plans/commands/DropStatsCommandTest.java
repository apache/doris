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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class DropStatsCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static final String catalogNotExist = "catalog_not_exist";
    private static final String dbNotExist = "db_not_exist";
    private static final String tblNotExist = "tbl_not_exist";

    private Env env;
    private AccessControllerManager accessControllerManager;
    private InternalCatalog catalog;
    private ConnectContext connectContext;
    private CatalogMgr catalogMgr;
    private Database db;
    private SessionVariable sessionVariable;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> ctxMockedStatic;

    @BeforeEach
    public void setUp() throws Exception {
        env = Mockito.mock(Env.class);
        accessControllerManager = Mockito.mock(AccessControllerManager.class);
        catalog = Mockito.mock(InternalCatalog.class);
        connectContext = Mockito.mock(ConnectContext.class);
        catalogMgr = Mockito.mock(CatalogMgr.class);
        sessionVariable = new SessionVariable();

        envMockedStatic = Mockito.mockStatic(Env.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(connectContext);

        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        Mockito.doReturn(catalog).when(catalogMgr)
                .getCatalogOrAnalysisException(Mockito.anyString());
        Mockito.when(connectContext.getSessionVariable()).thenReturn(sessionVariable);
        Mockito.when(connectContext.getState()).thenReturn(new QueryState());
        Mockito.when(connectContext.getEnv()).thenReturn(env);

        TabletInvertedIndex tabletInvertedIndex = Mockito.mock(TabletInvertedIndex.class);
        envMockedStatic.when(Env::getCurrentInvertedIndex).thenReturn(tabletInvertedIndex);
    }

    @AfterEach
    public void tearDown() {
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
        if (ctxMockedStatic != null) {
            ctxMockedStatic.close();
        }
    }

    @Test
    public void testValidateNormal() throws Exception {
        db = CatalogMocker.mockDb();
        Mockito.when(catalog.getDb(Mockito.anyString())).thenReturn(Optional.of(db));
        Mockito.doReturn(db).when(catalog).getDbOrAnalysisException(Mockito.anyString());
        Mockito.when(connectContext.isSkipAuth()).thenReturn(true);
        Mockito.when(accessControllerManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.eq(internalCtl),
                Mockito.eq(CatalogMocker.TEST_DB_NAME), Mockito.eq(CatalogMocker.TEST_TBL_NAME),
                Mockito.eq(PrivPredicate.DROP))).thenReturn(true);
        Mockito.when(accessControllerManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.eq(internalCtl),
                Mockito.eq(CatalogMocker.TEST_DB_NAME), Mockito.eq(CatalogMocker.TEST_TBL2_NAME),
                Mockito.eq(PrivPredicate.DROP))).thenReturn(true);

        //test normal
        sessionVariable.enableStats = true;
        TableNameInfo tableNameInfo =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME);
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false,
                ImmutableList.of(CatalogMocker.TEST_PARTITION1_NAME));
        Set<String> columns = new HashSet<>();
        for (Column column : CatalogMocker.TEST_TBL_BASE_SCHEMA) {
            columns.add(column.getName());
        }
        DropStatsCommand command = new DropStatsCommand(tableNameInfo, columns, partitionNamesInfo);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //test table is empty
        TableNameInfo tableNameInfo2 =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, "");
        DropStatsCommand command2 = new DropStatsCommand(tableNameInfo2, columns, partitionNamesInfo);
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class, () -> command2.validate(connectContext),
                "Table name is null");

        //test unkown catalog
        List<String> parts = new ArrayList<>();
        parts.add(catalogNotExist);
        parts.add(dbNotExist);
        parts.add(tblNotExist);
        TableNameInfo tableNameInfo3 = new TableNameInfo(parts);
        DropStatsCommand command3 = new DropStatsCommand(tableNameInfo3, columns, partitionNamesInfo);
        Assertions.assertThrows(AnalysisException.class, () -> command3.validate(connectContext),
                "Unknown catalog 'catalog_not_exist'");

        //test unkown db
        TableNameInfo tableNameInfo4 = new TableNameInfo(dbNotExist, CatalogMocker.TEST_TBL_NAME);
        DropStatsCommand command4 = new DropStatsCommand(tableNameInfo4, columns, partitionNamesInfo);
        Assertions.assertThrows(AnalysisException.class, () -> command4.validate(connectContext),
                "Unknown database 'db_not_exist'");

        //test unkown tbl
        TableNameInfo tableNameInfo5 = new TableNameInfo(CatalogMocker.TEST_DB_NAME, tblNotExist);
        DropStatsCommand command5 = new DropStatsCommand(tableNameInfo5, columns, partitionNamesInfo);
        Assertions.assertThrows(AnalysisException.class, () -> command5.validate(connectContext),
                "Unknown table 'tbl_not_exist'");

        //test columns' size > 100
        Set<String> columns2 = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            columns2.add(String.valueOf(i));
        }
        DropStatsCommand command6 = new DropStatsCommand(tableNameInfo, columns2, partitionNamesInfo);
        Assertions.assertThrows(UserException.class, () -> command6.validate(connectContext),
                "Can't delete more that 100 columns at one time.");

        //test partitions' size > 100
        List<String> partitionNames = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            partitionNames.add(String.valueOf(i));
        }
        PartitionNamesInfo partitionNamesInfo2 = new PartitionNamesInfo(false, partitionNames);
        DropStatsCommand command7 = new DropStatsCommand(tableNameInfo, columns, partitionNamesInfo2);
        Assertions.assertThrows(UserException.class, () -> command7.validate(connectContext),
                "Can't delete more that 100 partitions at one time");

        //test enable stats is false
        sessionVariable.enableStats = false;
        Assertions.assertThrows(UserException.class, () -> command.validate(connectContext),
                "Analyze function is forbidden, you should add `enable_stats=true` in your FE conf file");
    }

    @Test
    void testValidateNoPrivilege() throws Exception {
        db = CatalogMocker.mockDb();
        Mockito.doReturn(db).when(catalog).getDbOrAnalysisException(Mockito.anyString());
        Mockito.when(connectContext.isSkipAuth()).thenReturn(false);
        Mockito.when(accessControllerManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.eq(internalCtl),
                Mockito.eq(CatalogMocker.TEST_DB_NAME), Mockito.eq(CatalogMocker.TEST_TBL2_NAME),
                Mockito.eq(PrivPredicate.DROP))).thenReturn(false);

        sessionVariable.enableStats = true;
        TableNameInfo tableNameInfo =
                    new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME);
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false,
                ImmutableList.of(CatalogMocker.TEST_PARTITION1_NAME));
        Set<String> columns = new HashSet<>();
        for (Column column : CatalogMocker.TEST_TBL_BASE_SCHEMA) {
            columns.add(column.getName());
        }
        DropStatsCommand command = new DropStatsCommand(tableNameInfo, columns, partitionNamesInfo);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "DROP command denied to user 'null'@'null' for table 'test_db.test_tbl2'");
    }
}
