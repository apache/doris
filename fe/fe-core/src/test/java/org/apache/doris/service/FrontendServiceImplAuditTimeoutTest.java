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

package org.apache.doris.service;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AuthenticationException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.StreamLoadHandler;
import org.apache.doris.nereids.load.NereidsStreamLoadTask;
import org.apache.doris.plugin.audit.AuditLoader;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TLoadTxnBeginResult;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TStreamLoadPutResult;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

public class FrontendServiceImplAuditTimeoutTest {
    private boolean alwaysOn;
    private int minimum;
    private int maximum;

    @BeforeEach
    public void save() {
        alwaysOn = Config.query_audit_always_on;
        minimum = Config.min_load_timeout_second;
        maximum = Config.max_stream_load_timeout_second;
        Config.query_audit_always_on = true;
    }

    @AfterEach
    public void restore() {
        Config.query_audit_always_on = alwaysOn;
        Config.min_load_timeout_second = minimum;
        Config.max_stream_load_timeout_second = maximum;
    }

    @Test
    public void testAuditTimeoutFitsMasterTransactionLimits() {
        GlobalTransactionMgrIface transactions = Mockito.mock(GlobalTransactionMgrIface.class,
                Mockito.CALLS_REAL_METHODS);
        Config.min_load_timeout_second = 700;
        Config.max_stream_load_timeout_second = 900;
        TLoadTxnBeginRequest request = auditRequest();
        for (int timeout : new int[] {1, 600, 700, 900, 901, Integer.MAX_VALUE}) {
            request.setTimeout(timeout);
            long effective = FrontendServiceImpl.getLoadTxnTimeout(request);
            Assertions.assertDoesNotThrow(() -> transactions.checkValidTimeoutSecond(effective,
                    Config.max_stream_load_timeout_second, Config.min_load_timeout_second,
                    LoadJobSourceType.BACKEND_STREAMING));
        }
        Assertions.assertEquals(900, FrontendServiceImpl.getLoadTxnTimeout(request));
        Config.min_load_timeout_second = 1;
        Config.max_stream_load_timeout_second = 300;
        request.setTimeout(600);
        Assertions.assertEquals(300, FrontendServiceImpl.getLoadTxnTimeout(request));
        request.unsetTimeout();
        Assertions.assertEquals(300, FrontendServiceImpl.getLoadTxnTimeout(request));
        Config.max_stream_load_timeout_second = Integer.MAX_VALUE;
        request.setTimeout(Integer.MAX_VALUE);
        Assertions.assertEquals(Integer.MAX_VALUE / 1000, FrontendServiceImpl.getLoadTxnTimeout(request));
    }

    @Test
    public void testOtherLoadsAndDisabledPolicyKeepTheirTimeouts() {
        Config.min_load_timeout_second = 1;
        Config.max_stream_load_timeout_second = 300;
        TLoadTxnBeginRequest request = auditRequest();
        for (TLoadTxnBeginRequest other : List.of(
                request.deepCopy().setDb("other_database"),
                request.deepCopy().setTbl("other_table"),
                request.deepCopy().setToken(""),
                request.deepCopy().setAuthCode(1))) {
            Assertions.assertEquals(Integer.MAX_VALUE, FrontendServiceImpl.getLoadTxnTimeout(other));
        }
        Config.query_audit_always_on = false;
        Assertions.assertEquals(Integer.MAX_VALUE, FrontendServiceImpl.getLoadTxnTimeout(request));
    }

    @Test
    public void testAuditPutUsesMasterTimeoutBeforePlanning() throws Exception {
        Config.min_load_timeout_second = 1;
        Config.max_stream_load_timeout_second = 300;
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.isMaster()).thenReturn(true);
        TStreamLoadPutRequest request = new TStreamLoadPutRequest().setDb(FeConstants.INTERNAL_DB_NAME)
                .setTbl(AuditLoader.AUDIT_LOG_TABLE).setUser("").setToken("audit-token")
                .setTimeout(Integer.MAX_VALUE).setLoadId(new TUniqueId(1, 1)).setTxnId(123)
                .setFileType(TFileType.FILE_STREAM).setFormatType(TFileFormatType.FORMAT_CSV_PLAIN);
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
                MockedConstruction<StreamLoadHandler> handlers = Mockito.mockConstruction(StreamLoadHandler.class,
                        (handler, construction) -> {
                            TStreamLoadPutRequest planned = (TStreamLoadPutRequest) construction.arguments().get(0);
                            Mockito.doAnswer(invocation -> {
                                int timeout = NereidsStreamLoadTask.fromTStreamLoadPutRequest(planned).getTimeout();
                                Assertions.assertEquals(300, timeout);
                                Assertions.assertDoesNotThrow(() -> Math.multiplyExact(timeout, 1000));
                                return null;
                            }).when(handler).generatePlan();
                            Mockito.when(handler.getFragmentParams()).thenReturn(List.of(new TPipelineFragmentParams()));
                        })) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            FrontendServiceImpl service = Mockito.spy(new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class)));
            Mockito.doReturn(true).when(service).checkToken("audit-token");
            TStreamLoadPutResult result = service.streamLoadPut(request);
            Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatusCode());
            Mockito.verify(handlers.constructed().get(0)).generatePlan();
            Assertions.assertEquals(300, request.getTimeout());

            // A forwarded plan keeps the master's timeout despite different follower-local limits.
            Mockito.when(env.isMaster()).thenReturn(false);
            Config.min_load_timeout_second = 700;
            Config.max_stream_load_timeout_second = 900;
            Assertions.assertEquals(TStatusCode.OK, service.streamLoadPut(request).getStatus().getStatusCode());
            Mockito.verify(handlers.constructed().get(1)).generatePlan();
            Assertions.assertEquals(300, request.getTimeout());

            Mockito.when(env.isMaster()).thenReturn(true);
            Mockito.doReturn(false).when(service).checkToken("audit-token");
            Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR,
                    service.streamLoadPut(request).getStatus().getStatusCode());
            Mockito.verify(handlers.constructed().get(2), Mockito.never()).generatePlan();
        }
    }

    @Test
    public void testMasterPassesClampedTimeoutAfterTokenAuthentication() throws Exception {
        Config.min_load_timeout_second = 1;
        Config.max_stream_load_timeout_second = 300;
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        GlobalTransactionMgrIface transactions = Mockito.mock(GlobalTransactionMgrIface.class,
                Mockito.CALLS_REAL_METHODS);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getDbNullable(FeConstants.INTERNAL_DB_NAME)).thenReturn(db);
        Mockito.when(db.getTableOrMetaException(AuditLoader.AUDIT_LOG_TABLE, TableType.OLAP)).thenReturn(table);
        Mockito.when(db.getId()).thenReturn(1L);
        Mockito.when(table.getId()).thenReturn(2L);
        Mockito.when(transactions.beginTransaction(Mockito.eq(1L), Mockito.eq(List.of(2L)), Mockito.anyString(),
                Mockito.any(), Mockito.any(), Mockito.eq(LoadJobSourceType.BACKEND_STREAMING),
                Mockito.eq(-1L), Mockito.anyLong())).thenAnswer(invocation -> {
                    long timeout = invocation.getArgument(7);
                    transactions.checkValidTimeoutSecond(timeout, Config.max_stream_load_timeout_second,
                            Config.min_load_timeout_second, LoadJobSourceType.BACKEND_STREAMING);
                    Assertions.assertEquals(300, timeout);
                    return 123L;
                });
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            mockedEnv.when(Env::getCurrentGlobalTransactionMgr).thenReturn(transactions);
            mockedEnv.when(Env::getCurrentSystemInfo).thenReturn(Mockito.mock(SystemInfoService.class));
            FrontendServiceImpl service = Mockito.spy(new FrontendServiceImpl(Mockito.mock(ExecuteEnv.class)));
            Mockito.doReturn(true).when(service).checkToken("audit-token");
            Method begin = FrontendServiceImpl.class.getDeclaredMethod("loadTxnBeginImpl",
                    TLoadTxnBeginRequest.class, String.class);
            begin.setAccessible(true);
            TLoadTxnBeginResult result = (TLoadTxnBeginResult) begin.invoke(service, auditRequest(), "127.0.0.1");
            Assertions.assertEquals(123L, result.getTxnId());
            Mockito.verify(service).checkToken("audit-token");

            Mockito.doReturn(false).when(service).checkToken("audit-token");
            InvocationTargetException failure = Assertions.assertThrows(InvocationTargetException.class,
                    () -> begin.invoke(service, auditRequest(), "127.0.0.1"));
            Assertions.assertInstanceOf(AuthenticationException.class, failure.getCause());
            Mockito.verify(transactions, Mockito.times(1)).beginTransaction(Mockito.anyLong(), Mockito.anyList(),
                    Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyLong(),
                    Mockito.anyLong());
        }
    }

    private static TLoadTxnBeginRequest auditRequest() {
        return new TLoadTxnBeginRequest().setDb(FeConstants.INTERNAL_DB_NAME).setTbl(AuditLoader.AUDIT_LOG_TABLE)
                .setToken("audit-token").setLabel("audit-timeout").setTimeout(Integer.MAX_VALUE);
    }
}
