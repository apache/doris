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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
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

public class ShowTabletIdCommandTest {
    private Env env;
    private AccessControllerManager manager;
    private ConnectContext ctx;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> ctxMockedStatic;

    @BeforeEach
    public void setUp() {
        env = Mockito.mock(Env.class);
        manager = Mockito.mock(AccessControllerManager.class);
        ctx = Mockito.mock(ConnectContext.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(ctx);

        Mockito.when(env.getAccessManager()).thenReturn(manager);
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

    void runBefore(String dbName, boolean hasGlobalPriv) {
        Mockito.when(ctx.getDatabase()).thenReturn(dbName);
        Mockito.when(manager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN))).thenReturn(hasGlobalPriv);
    }

    @Test
    public void testValidate() throws Exception {
        runBefore(CatalogMocker.TEST_DB_NAME, true);
        ShowTabletIdCommand command = new ShowTabletIdCommand(CatalogMocker.TEST_TBL_ID);
        Assertions.assertDoesNotThrow(() -> command.validate(ctx));
    }

    @Test
    void noGlobalPriv() {
        runBefore(CatalogMocker.TEST_DB_NAME, false);
        ShowTabletIdCommand command = new ShowTabletIdCommand(CatalogMocker.TEST_TBL_ID);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(ctx));
    }

    @Test
    void dbIsEmpty() {
        runBefore("", true);
        ShowTabletIdCommand command = new ShowTabletIdCommand(CatalogMocker.TEST_TBL_ID);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(ctx));
    }
}
