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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.NameSpaceContext;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Optional;

public class ShowIndexStatsCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static final String tableNotExist = "table_not_exist";

    private Env env = Mockito.mock(Env.class);
    private InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
    private AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
    private ConnectContext ctx = Mockito.mock(ConnectContext.class);
    private CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
    private NameSpaceContext nameSpaceContext = Mockito.mock(NameSpaceContext.class);
    private Database db;

    private MockedStatic<Env> mockedEnv;
    private MockedStatic<ConnectContext> mockedConnectContext;

    @BeforeEach
    public void setUp() throws Exception {
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedConnectContext = Mockito.mockStatic(ConnectContext.class);

        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedConnectContext.when(ConnectContext::get).thenReturn(ctx);

        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(ctx.getNameSpaceContext()).thenReturn(nameSpaceContext);
        Mockito.when(nameSpaceContext.getDefaultCatalog()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(ctx.getState()).thenReturn(new QueryState());

        TabletInvertedIndex tabletInvertedIndex = Mockito.mock(TabletInvertedIndex.class);
        mockedEnv.when(Env::getCurrentInvertedIndex).thenReturn(tabletInvertedIndex);

        db = CatalogMocker.mockDb();
        Mockito.doReturn(Optional.of(db)).when(catalog).getDb(Mockito.anyString());

        Mockito.when(accessManager.checkTblPriv(
                Mockito.nullable(ConnectContext.class),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.eq(CatalogMocker.TEST_TBL_NAME),
                Mockito.any(PrivPredicate.class))).thenReturn(true);
    }

    @AfterEach
    public void tearDown() {
        mockedConnectContext.close();
        mockedEnv.close();
    }

    @Test
    public void testValidateNormal() throws Exception {
        TableNameInfo tableNameInfo =
                new TableNameInfo(internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        ShowIndexStatsCommand command = new ShowIndexStatsCommand(tableNameInfo, CatalogMocker.TEST_TBL_NAME);
        Assertions.assertDoesNotThrow(() -> command.validate(ctx));
    }

    @Test
    public void testValidateFail() throws Exception {
        TableNameInfo tableNameInfo =
                new TableNameInfo(internalCtl, CatalogMocker.TEST_DB_NAME, tableNotExist);
        ShowIndexStatsCommand command = new ShowIndexStatsCommand(tableNameInfo, tableNotExist);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(ctx),
                "Table: " + tableNotExist + " not exists");

        TableNameInfo tableNameInfo2 =
                new TableNameInfo(internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME);
        ShowIndexStatsCommand command2 = new ShowIndexStatsCommand(tableNameInfo2, CatalogMocker.TEST_TBL2_NAME);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(ctx),
                "Permission denied command denied to user 'null'@'null' for table 'test_db: test_tbl2'");
    }
}
