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
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableRefInfo;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

public class ShowReplicaStatusCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private Env env;
    private AccessControllerManager accessControllerManager;
    private ConnectContext connectContext;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> ctxMockedStatic;

    @BeforeEach
    public void setUp() {
        env = Mockito.mock(Env.class);
        accessControllerManager = Mockito.mock(AccessControllerManager.class);
        connectContext = Mockito.mock(ConnectContext.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(connectContext);

        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
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

    private void runBefore() {
        Mockito.when(connectContext.isSkipAuth()).thenReturn(true);
        Mockito.when(accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN)).thenReturn(true);
    }

    @Test
    public void testValidate() throws AnalysisException {
        runBefore();
        TableNameInfo tableNameInfo = new TableNameInfo(internalCtl, "test_db", "test_tbl");
        List<String> partitionNames = new ArrayList<>();
        partitionNames.add("p1");
        partitionNames.add("p2");
        PartitionNamesInfo partitionNamesInfo = new PartitionNamesInfo(false, partitionNames);
        TableRefInfo tableRefInfo = new TableRefInfo(tableNameInfo, null, null, partitionNamesInfo, null, null, null, null);

        //test where is null
        ShowReplicaStatusCommand command = new ShowReplicaStatusCommand(tableRefInfo, null);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //test where is Equalto
        Expression where = new EqualTo(new UnboundSlot(Lists.newArrayList("status")),
                new StringLiteral("VERSION_ERROR"));
        ShowReplicaStatusCommand command2 = new ShowReplicaStatusCommand(tableRefInfo, where);
        Assertions.assertDoesNotThrow(() -> command2.validate(connectContext));

        //test where is Not, child(0) is EqualTo
        Expression equalTo = new EqualTo(new UnboundSlot(Lists.newArrayList("status")),
                new StringLiteral("OK"));
        Expression where2 = new Not(equalTo);
        ShowReplicaStatusCommand command3 = new ShowReplicaStatusCommand(tableRefInfo, where2);
        Assertions.assertDoesNotThrow(() -> command3.validate(connectContext));

        //test status is wrong
        Expression where3 = new EqualTo(new UnboundSlot(Lists.newArrayList("status")),
                new StringLiteral("test_status"));
        ShowReplicaStatusCommand command4 = new ShowReplicaStatusCommand(tableRefInfo, where3);
        Assertions.assertThrows(AnalysisException.class, () -> command4.validate(connectContext));

        //test catalog is hive
        TableNameInfo tableNameInfo2 = new TableNameInfo("hive", "test_db", "test_tbl");
        TableRefInfo tableRefInfo2 = new TableRefInfo(tableNameInfo2, null, null, partitionNamesInfo, null, null, null, null);
        ShowReplicaStatusCommand command5 = new ShowReplicaStatusCommand(tableRefInfo2, null);
        Assertions.assertThrows(AnalysisException.class, () -> command5.validate(connectContext));
    }
}
