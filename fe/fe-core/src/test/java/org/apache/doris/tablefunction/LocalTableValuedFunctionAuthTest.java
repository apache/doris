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
import org.apache.doris.common.Config;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.proto.InternalService.PGlobResponse;
import org.apache.doris.proto.Types.PStatus;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class LocalTableValuedFunctionAuthTest {
    private static final Map<String, String> PROPERTIES = ImmutableMap.of(
            "file_path", "local_privilege_test.csv", "backend_id", "1", "format", "csv");

    private final Env env = Mockito.mock(Env.class);
    private final ConnectContext context = Mockito.mock(ConnectContext.class);
    private final AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
    private final SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
    private final BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
    private boolean originalEnableLocalTvf;
    private MockedStatic<Env> mockedEnv;
    private MockedStatic<ConnectContext> mockedContext;
    private MockedStatic<BackendServiceProxy> mockedProxy;

    @BeforeEach
    public void setUp() throws Exception {
        originalEnableLocalTvf = Config.enable_local_tvf;
        Config.enable_local_tvf = true;
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
        mockedContext = Mockito.mockStatic(ConnectContext.class);
        mockedContext.when(ConnectContext::get).thenReturn(context);
        mockedProxy = Mockito.mockStatic(BackendServiceProxy.class);
        mockedProxy.when(BackendServiceProxy::getInstance).thenReturn(proxy);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        Mockito.when(context.getSessionVariable()).thenReturn(new SessionVariable());
        Mockito.when(context.getQueryTimeoutS()).thenReturn(60);

        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(systemInfo.getBackend(1L)).thenReturn(backend);
        Mockito.when(backend.getBrpcAddress()).thenReturn(new TNetworkAddress("127.0.0.1", 8060));
        Mockito.when(proxy.glob(Mockito.any(), Mockito.any())).thenReturn(CompletableFuture.completedFuture(
                PGlobResponse.newBuilder().setStatus(PStatus.newBuilder().setStatusCode(0)).build()));
    }

    @AfterEach
    public void tearDown() {
        Config.enable_local_tvf = originalEnableLocalTvf;
        mockedProxy.close();
        mockedContext.close();
        mockedEnv.close();
    }

    @Test
    public void testLocalRejectsUnprivilegedUserBeforeFileAccess() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> new LocalTableValuedFunction(PROPERTIES));
        Assertions.assertTrue(exception.getMessage().contains("ADMIN/OPERATOR"));
        Mockito.verifyNoInteractions(systemInfo, proxy);
    }

    @Test
    public void testFileRejectsUnprivilegedLocalAccess() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> new FileTableValuedFunction(PROPERTIES));
        Assertions.assertTrue(exception.getMessage().contains("ADMIN/OPERATOR"));
        Mockito.verifyNoInteractions(systemInfo, proxy);
    }

    @Test
    public void testAdminCanAccessLocalFiles() throws Exception {
        assertPrivilegedAccess(PrivPredicate.ADMIN);
    }

    @Test
    public void testOperatorCanAccessLocalFiles() throws Exception {
        assertPrivilegedAccess(PrivPredicate.OPERATOR);
    }

    @Test
    public void testCheckAuthRejectsRevokedPrivileges() throws Exception {
        Mockito.when(accessManager.checkGlobalPriv(context, PrivPredicate.ADMIN)).thenReturn(true);
        LocalTableValuedFunction local = new LocalTableValuedFunction(PROPERTIES);
        FileTableValuedFunction file = new FileTableValuedFunction(PROPERTIES);
        Mockito.when(accessManager.checkGlobalPriv(context, PrivPredicate.ADMIN)).thenReturn(false);

        Assertions.assertThrows(AnalysisException.class, () -> local.checkAuth(context));
        Assertions.assertThrows(AnalysisException.class, () -> file.checkAuth(context));
    }

    private void assertPrivilegedAccess(PrivPredicate privilege) throws Exception {
        Mockito.when(accessManager.checkGlobalPriv(context, privilege)).thenReturn(true);
        LocalTableValuedFunction local = new LocalTableValuedFunction(PROPERTIES);
        FileTableValuedFunction file = new FileTableValuedFunction(PROPERTIES);
        Assertions.assertDoesNotThrow(() -> local.checkAuth(context));
        Assertions.assertDoesNotThrow(() -> file.checkAuth(context));
        Mockito.verify(proxy, Mockito.times(2)).glob(Mockito.any(), Mockito.any());
    }
}
