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

import org.apache.doris.analysis.AnalyzeProperties;
import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
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
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class AnalyzeTableCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;

    private Env env;
    private AccessControllerManager accessManager;
    private ConnectContext ctx;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> ctxMockedStatic;

    @BeforeEach
    public void setUp() {
        env = Mockito.mock(Env.class);
        accessManager = Mockito.mock(AccessControllerManager.class);
        ctx = Mockito.mock(ConnectContext.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(ctx);

        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(ctx.getState()).thenReturn(new QueryState());
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
    void testCheckAnalyzePrivilege() {
        Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.eq(internalCtl),
                Mockito.eq(CatalogMocker.TEST_DB_NAME), Mockito.eq(CatalogMocker.TEST_TBL_NAME),
                Mockito.eq(PrivPredicate.SELECT))).thenReturn(true);

        Mockito.when(accessManager.checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.eq(internalCtl),
                Mockito.eq(CatalogMocker.TEST_DB_NAME), Mockito.eq(CatalogMocker.TEST_TBL2_NAME),
                Mockito.eq(PrivPredicate.SELECT))).thenReturn(false);

        TableNameInfo tableNameInfo = new TableNameInfo(internalCtl,
                CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false,
                ImmutableList.of(CatalogMocker.TEST_PARTITION1_NAME));
        AnalyzeTableCommand analyzeTableCommand = new AnalyzeTableCommand(tableNameInfo,
                partitionNamesInfo, ImmutableList.of("k1"), AnalyzeProperties.DEFAULT_PROP);
        // normal
        Assertions.assertDoesNotThrow(() -> analyzeTableCommand.checkAnalyzePrivilege(tableNameInfo));

        // no privilege
        TableNameInfo tableNameInfo2 = new TableNameInfo(internalCtl,
                CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME);
        Assertions.assertThrows(AnalysisException.class,
                () -> analyzeTableCommand.checkAnalyzePrivilege(tableNameInfo2),
                "ANALYZE command denied to user 'null'@'null' for table 'test_db: test_tbl2'");
    }
}

