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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.master.MasterImpl;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MysqlConnectProcessor;
import org.apache.doris.system.Frontend;
import org.apache.doris.thrift.TFetchLoadJobRequest;
import org.apache.doris.thrift.TMasterOpRequest;
import org.apache.doris.thrift.TMasterOpResult;
import org.apache.doris.thrift.TUniqueId;

import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

class FrontendServiceContextLifecycleTest {
    private boolean savedRunningUnitTest;
    private Env env;
    private InternalCatalog catalog;
    private MockedStatic<Env> envMock;
    private MockedConstruction<MasterImpl> masterMock;
    private FrontendServiceImpl service;

    @BeforeEach
    void setUp() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        env = Mockito.mock(Env.class);
        catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        envMock = Mockito.mockStatic(Env.class);
        envMock.when(Env::getCurrentEnv).thenReturn(env);
        envMock.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        masterMock = Mockito.mockConstruction(MasterImpl.class);
        service = new FrontendServiceImpl(null);
        ConnectContext.remove();
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
        masterMock.close();
        envMock.close();
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void fetchLoadJobsClosesContextAndRestoresCaller(boolean fail) {
        Mockito.when(env.isReady()).thenReturn(true);
        StatementContext statement = Mockito.mock(StatementContext.class);
        AtomicReference<ConnectContext> requestContext = new AtomicReference<>();
        IllegalStateException failure = new IllegalStateException("catalog unavailable");
        Mockito.when(catalog.getDbNames()).thenAnswer(call -> {
            ConnectContext context = ConnectContext.get();
            requestContext.set(context);
            context.setStatementContext(statement);
            if (fail) {
                throw failure;
            }
            return Collections.emptyList();
        });
        ConnectContext caller = new ConnectContext();
        caller.setThreadLocalInfo();
        try {
            if (fail) {
                Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                        () -> service.fetchLoadJob(new TFetchLoadJobRequest())));
            } else {
                Assertions.assertTrue(service.fetchLoadJob(new TFetchLoadJobRequest()).getLoadJobs().isEmpty());
            }
            Assertions.assertTrue(requestContext.get().isClosingOrClosed());
            Mockito.verify(statement).close();
            Assertions.assertSame(caller, ConnectContext.get());
            Assertions.assertFalse(caller.isClosingOrClosed());
        } finally {
            caller.cleanup();
        }
    }

    @Test
    void forwardInitializationFailureClosesContextBeforeQueryRegistration() {
        TMasterOpRequest request = forwardRequest();
        Frontend requester = Mockito.mock(Frontend.class);
        Mockito.when(env.checkFeExist("follower", 9020)).thenReturn(requester);
        AtomicReference<ConnectContext> requestContext = new AtomicReference<>();
        StatementContext statement = Mockito.mock(StatementContext.class);
        IllegalStateException failure = new IllegalStateException("resource group unavailable");
        Mockito.when(requester.getLocalResourceGroup()).thenAnswer(call -> {
            ConnectContext context = ConnectContext.get();
            requestContext.set(context);
            context.setStatementContext(statement);
            throw failure;
        });
        Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                () -> service.forward(request)));
        Assertions.assertTrue(requestContext.get().isClosingOrClosed());
        Mockito.verify(statement).close();
        Assertions.assertNull(ConnectContext.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void forwardClosesRequestAndUnregistersQueryAfterResponseOrFailure(boolean fail) throws Exception {
        TMasterOpRequest request = forwardRequest().setQueryId(new TUniqueId(1, 2));
        Mockito.when(env.checkFeExist("follower", 9020)).thenReturn(Mockito.mock(Frontend.class));
        AtomicReference<ConnectContext> requestContext = new AtomicReference<>();
        StatementContext statement = Mockito.mock(StatementContext.class);
        TException failure = new TException("execution failed");
        byte[] packet = new byte[] {1, 2, 3};
        ConnectContext caller = new ConnectContext();
        caller.setThreadLocalInfo();
        try (MockedConstruction<MysqlConnectProcessor> ignored = Mockito.mockConstruction(
                MysqlConnectProcessor.class, (processor, construction) -> {
                    ConnectContext context = (ConnectContext) construction.arguments().get(0);
                    requestContext.set(context);
                    context.setStatementContext(statement);
                    Mockito.when(processor.proxyExecute(request)).thenAnswer(call -> {
                        if (fail) {
                            throw failure;
                        }
                        return new TMasterOpResult().setStatus("OK").setPacket(packet).setMaxJournalId(1);
                    });
                })) {
            if (fail) {
                Assertions.assertSame(failure, Assertions.assertThrows(TException.class,
                        () -> service.forward(request)));
            } else {
                Assertions.assertArrayEquals(packet, service.forward(request).getPacket());
            }
            Assertions.assertTrue(requestContext.get().isClosingOrClosed());
            Mockito.verify(statement).close();
            Assertions.assertSame(caller, ConnectContext.get());
            Assertions.assertFalse(caller.isClosingOrClosed());
            Field queries = FrontendServiceImpl.class.getDeclaredField("proxyQueryIdToConnCtx");
            queries.setAccessible(true);
            Assertions.assertTrue(((Map<?, ?>) queries.get(service)).isEmpty());
        } finally {
            caller.cleanup();
        }
    }

    private static TMasterOpRequest forwardRequest() {
        return new TMasterOpRequest().setClientNodeHost("follower").setClientNodePort(9020)
                .setSessionId("origin-session");
    }
}
