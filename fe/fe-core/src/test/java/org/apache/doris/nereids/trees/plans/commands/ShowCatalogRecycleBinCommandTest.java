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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class ShowCatalogRecycleBinCommandTest {
    private Env env;
    private ConnectContext connectContext;
    private AccessControllerManager accessControllerManager;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> ctxMockedStatic;

    @BeforeEach
    public void setUp() {
        env = Mockito.mock(Env.class);
        connectContext = Mockito.mock(ConnectContext.class);
        accessControllerManager = Mockito.mock(AccessControllerManager.class);
        envMockedStatic = Mockito.mockStatic(Env.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(connectContext);
        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        Mockito.when(connectContext.getState()).thenReturn(new QueryState());
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
    void testValidateNormal() {
        Mockito.when(accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN))
                .thenReturn(true);

        // Name = "value"
        EqualTo whereClause = new EqualTo(new UnboundSlot("name"),
                new StringLiteral("p1000"));
        ShowCatalogRecycleBinCommand command = new ShowCatalogRecycleBinCommand(whereClause);
        Assertions.assertDoesNotThrow(() -> command.validate());

        // Type = "Table"
        EqualTo typeWhere = new EqualTo(new UnboundSlot("type"),
                new StringLiteral("Table"));
        ShowCatalogRecycleBinCommand typeCommand = new ShowCatalogRecycleBinCommand(typeWhere);
        Assertions.assertDoesNotThrow(() -> typeCommand.validate());

        // DbId = "12345"
        EqualTo dbIdWhere = new EqualTo(new UnboundSlot("dbid"),
                new StringLiteral("12345"));
        ShowCatalogRecycleBinCommand dbIdCommand = new ShowCatalogRecycleBinCommand(dbIdWhere);
        Assertions.assertDoesNotThrow(() -> dbIdCommand.validate());

        // TableId = "67890"
        EqualTo tableIdWhere = new EqualTo(new UnboundSlot("tableid"),
                new StringLiteral("67890"));
        ShowCatalogRecycleBinCommand tableIdCommand = new ShowCatalogRecycleBinCommand(tableIdWhere);
        Assertions.assertDoesNotThrow(() -> tableIdCommand.validate());

        // Name = "value" AND Type = "Table"
        And andWhere = new And(whereClause, typeWhere);
        ShowCatalogRecycleBinCommand andCommand = new ShowCatalogRecycleBinCommand(andWhere);
        Assertions.assertDoesNotThrow(() -> andCommand.validate());

        // Name = "value" AND Type = "Table" AND DbId = "12345"
        And tripleAnd = new And(andWhere, dbIdWhere);
        ShowCatalogRecycleBinCommand tripleCommand = new ShowCatalogRecycleBinCommand(tripleAnd);
        Assertions.assertDoesNotThrow(() -> tripleCommand.validate());

        // Name = "value" AND Type = "Table" AND DbId = "12345" AND TableId = "67890"
        And quadAnd = new And(tripleAnd, tableIdWhere);
        ShowCatalogRecycleBinCommand quadCommand = new ShowCatalogRecycleBinCommand(quadAnd);
        Assertions.assertDoesNotThrow(() -> quadCommand.validate());

        // wrong where: GreaterThan not supported
        GreaterThan whereClause2 = new GreaterThan(new UnboundSlot("name"),
                new StringLiteral("p1000"));
        ShowCatalogRecycleBinCommand command2 = new ShowCatalogRecycleBinCommand(whereClause2);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate());

        // wrong where: unsupported column
        EqualTo unsupportedCol = new EqualTo(new UnboundSlot("datasize"),
                new StringLiteral("100"));
        ShowCatalogRecycleBinCommand command3 = new ShowCatalogRecycleBinCommand(unsupportedCol);
        Assertions.assertThrows(AnalysisException.class, () -> command3.validate());
    }

    @Test
    void testValidateNoPrivilege() {
        Mockito.when(accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN))
                .thenReturn(false);

        EqualTo whereClause = new EqualTo(new UnboundSlot("name"),
                new StringLiteral("p1000"));
        ShowCatalogRecycleBinCommand command = new ShowCatalogRecycleBinCommand(whereClause);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate());
    }
}
