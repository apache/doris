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
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class ShowTableStatusCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;

    private Env env;
    private AccessControllerManager accessManager;
    private ConnectContext ctx;
    private InternalCatalog catalog;
    private CatalogMgr catalogMgr;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> ctxMockedStatic;

    @BeforeEach
    public void setUp() {
        env = Mockito.mock(Env.class);
        accessManager = Mockito.mock(AccessControllerManager.class);
        ctx = Mockito.mock(ConnectContext.class);
        catalog = Mockito.mock(InternalCatalog.class);
        catalogMgr = Mockito.mock(CatalogMgr.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(ctx);

        Mockito.when(ctx.isSkipAuth()).thenReturn(true);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
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
    void testValidate() {
        Mockito.when(accessManager.checkDbPriv(Mockito.nullable(ConnectContext.class),
                Mockito.eq(InternalCatalog.INTERNAL_CATALOG_NAME),
                Mockito.eq(CatalogMocker.TEST_DB_NAME),
                Mockito.eq(PrivPredicate.SHOW))).thenReturn(true);

        EqualTo equalTo = new EqualTo(new UnboundSlot("name"),
                new StringLiteral(CatalogMocker.TEST_DB_NAME));

        ShowTableStatusCommand command = new ShowTableStatusCommand(CatalogMocker.TEST_DB_NAME,
                InternalCatalog.INTERNAL_CATALOG_NAME);
        Assertions.assertDoesNotThrow(() -> command.validate(ctx));

        ShowTableStatusCommand command2 = new ShowTableStatusCommand(CatalogMocker.TEST_DB_NAME,
                InternalCatalog.INTERNAL_CATALOG_NAME, "%example%", equalTo);
        Assertions.assertDoesNotThrow(() -> command2.validate(ctx));
    }

    @Test
    void testInvalidate() {
        Mockito.when(accessManager.checkDbPriv(Mockito.nullable(ConnectContext.class),
                Mockito.eq(InternalCatalog.INTERNAL_CATALOG_NAME),
                Mockito.eq(CatalogMocker.TEST_DB_NAME),
                Mockito.eq(PrivPredicate.SHOW))).thenReturn(false);

        EqualTo equalTo = new EqualTo(new UnboundSlot("name"),
                new StringLiteral(CatalogMocker.TEST_DB_NAME));

        ShowTableStatusCommand command = new ShowTableStatusCommand("", InternalCatalog.INTERNAL_CATALOG_NAME);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(ctx));

        ShowTableStatusCommand command1 = new ShowTableStatusCommand(CatalogMocker.TEST_DB_NAME, "");
        Assertions.assertThrows(AnalysisException.class, () -> command1.validate(ctx));

        ShowTableStatusCommand command2 = new ShowTableStatusCommand(CatalogMocker.TEST_DB_NAME,
                InternalCatalog.INTERNAL_CATALOG_NAME, "%example%", equalTo);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(ctx));
    }
}

