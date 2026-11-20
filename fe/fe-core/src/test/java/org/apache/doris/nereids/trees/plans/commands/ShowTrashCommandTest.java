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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

public class ShowTrashCommandTest {

    private Env env;
    private AccessControllerManager accessControllerManager;
    private ConnectContext ctx;
    private MockedStatic<Env> envMockedStatic;
    private MockedStatic<ConnectContext> ctxMockedStatic;

    @BeforeEach
    public void setUp() {
        env = Mockito.mock(Env.class);
        accessControllerManager = Mockito.mock(AccessControllerManager.class);
        ctx = Mockito.mock(ConnectContext.class);

        envMockedStatic = Mockito.mockStatic(Env.class);
        ctxMockedStatic = Mockito.mockStatic(ConnectContext.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(env);
        ctxMockedStatic.when(ConnectContext::get).thenReturn(ctx);

        Mockito.when(ctx.getState()).thenReturn(new QueryState());
        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);

        SystemInfoService systemInfoService = Mockito.mock(SystemInfoService.class);
        try {
            Mockito.when(systemInfoService.getAllBackendsByAllCluster())
                    .thenReturn(ImmutableMap.of());
        } catch (org.apache.doris.common.AnalysisException e) {
            throw new RuntimeException(e);
        }
        envMockedStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfoService);
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
    public void testMetaDataColumnNamesAndTypes() {
        ShowTrashCommand command = new ShowTrashCommand();
        List<Column> columns = command.getMetaData().getColumns();

        List<String> expectedNames = Arrays.asList(
                "BackendId", "Backend", "RootPath", "State",
                "TrashUsedCapacity", "TrashFileNum", "DiskCapacity", "AvailableCapacity");
        Assertions.assertEquals(expectedNames.size(), columns.size(),
                "Column count should be " + expectedNames.size());

        for (int i = 0; i < expectedNames.size(); i++) {
            Assertions.assertEquals(expectedNames.get(i), columns.get(i).getName(),
                    "Column at index " + i + " should be " + expectedNames.get(i));
        }

        Assertions.assertEquals(PrimitiveType.BIGINT,
                columns.get(0).getType().getPrimitiveType(), "BackendId should be BIGINT");
        Assertions.assertEquals(PrimitiveType.STRING,
                columns.get(1).getType().getPrimitiveType(), "Backend should be STRING");
        Assertions.assertEquals(PrimitiveType.STRING,
                columns.get(2).getType().getPrimitiveType(), "RootPath should be STRING");
        Assertions.assertEquals(PrimitiveType.STRING,
                columns.get(3).getType().getPrimitiveType(), "State should be STRING");
        Assertions.assertEquals(PrimitiveType.STRING,
                columns.get(4).getType().getPrimitiveType(), "TrashUsedCapacity should be STRING");
        Assertions.assertEquals(PrimitiveType.STRING,
                columns.get(5).getType().getPrimitiveType(), "TrashFileNum should be STRING");
        Assertions.assertEquals(PrimitiveType.STRING,
                columns.get(6).getType().getPrimitiveType(), "DiskCapacity should be STRING");
        Assertions.assertEquals(PrimitiveType.STRING,
                columns.get(7).getType().getPrimitiveType(), "AvailableCapacity should be STRING");
    }

    @Test
    public void testMetaDataHasEightColumns() {
        ShowTrashCommand command = new ShowTrashCommand();
        List<Column> columns = command.getMetaData().getColumns();
        Assertions.assertEquals(8, columns.size(),
                "SHOW TRASH should have 8 columns: BackendId, Backend, RootPath, State, "
                        + "TrashUsedCapacity, TrashFileNum, DiskCapacity, AvailableCapacity");
    }

    @Test
    public void testNoPrivilege() {
        Mockito.when(accessControllerManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN)))
                .thenReturn(false);
        Mockito.when(accessControllerManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.OPERATOR)))
                .thenReturn(false);

        ShowTrashCommand command = new ShowTrashCommand();
        Assertions.assertThrows(AnalysisException.class, () -> command.doRun(ctx, null));
    }

    @Test
    public void testWithAdminPrivilege() throws Exception {
        Mockito.when(accessControllerManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN)))
                .thenReturn(true);

        ShowTrashCommand command = new ShowTrashCommand();
        ShowResultSet result = command.doRun(ctx, null);
        Assertions.assertNotNull(result);
    }

    @Test
    public void testWithOperatorPrivilege() throws Exception {
        Mockito.when(accessControllerManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN)))
                .thenReturn(false);
        Mockito.when(accessControllerManager.checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.OPERATOR)))
                .thenReturn(true);

        ShowTrashCommand command = new ShowTrashCommand();
        ShowResultSet result = command.doRun(ctx, null);
        Assertions.assertNotNull(result);
    }
}
