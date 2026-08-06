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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService.HostInfo;
import org.apache.doris.thrift.TMetaScanRange;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;

public class FrontendsTableValuedFunctionTest {
    private static final String INTERNAL_CTL = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static final String INFO_DB = InfoSchemaDb.DATABASE_NAME;

    private Env env = Mockito.mock(Env.class);
    private AccessControllerManager accessControllerManager = Mockito.mock(AccessControllerManager.class);
    private ConnectContext ctx = Mockito.mock(ConnectContext.class);

    private MockedStatic<Env> mockedEnvStatic;
    private MockedStatic<ConnectContext> mockedCtxStatic;

    @After
    public void tearDown() {
        if (mockedCtxStatic != null) {
            mockedCtxStatic.close();
            mockedCtxStatic = null;
        }
        if (mockedEnvStatic != null) {
            mockedEnvStatic.close();
            mockedEnvStatic = null;
        }
    }

    private void mockContext(String selfHost, String currentConnectedFe) {
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);

        mockedCtxStatic = Mockito.mockStatic(ConnectContext.class);
        mockedCtxStatic.when(ConnectContext::get).thenReturn(ctx);

        Mockito.when(env.getAccessManager()).thenReturn(accessControllerManager);
        Mockito.when(accessControllerManager.checkDbPriv(ctx, InternalCatalog.INTERNAL_CATALOG_NAME,
                InfoSchemaDb.DATABASE_NAME, PrivPredicate.SELECT)).thenReturn(true);
        Mockito.when(env.getSelfNode()).thenReturn(new HostInfo(selfHost, 9010));
        Mockito.when(ctx.getCurrentConnectedFEIp()).thenReturn(currentConnectedFe);
    }

    @Test
    public void testGetMetaScanRangeUseCurrentConnectedFe() throws Exception {
        mockContext("self-fe-host", "connected-fe-host");
        FrontendsTableValuedFunction tvf = new FrontendsTableValuedFunction(new HashMap<>());
        TMetaScanRange range = tvf.getMetaScanRange(Collections.emptyList());
        Assert.assertEquals("connected-fe-host", range.getFrontendsParams().getCurrentConnectedFeHost());
    }

    @Test
    public void testGetMetaScanRangeFallbackToSelfNode() throws Exception {
        mockContext("self-fe-host", "");
        FrontendsTableValuedFunction tvf = new FrontendsTableValuedFunction(new HashMap<>());
        TMetaScanRange range = tvf.getMetaScanRange(Collections.emptyList());
        Assert.assertEquals("self-fe-host", range.getFrontendsParams().getCurrentConnectedFeHost());
    }
}
