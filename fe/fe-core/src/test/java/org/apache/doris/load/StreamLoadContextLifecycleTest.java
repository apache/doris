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

package org.apache.doris.load;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TStreamLoadPutResult;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicReference;

public class StreamLoadContextLifecycleTest {
    private boolean savedUnitTest;
    private MockedStatic<Env> environment;
    private MockedStatic<Config> config;

    @BeforeEach
    public void setUp() {
        savedUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        environment = Mockito.mockStatic(Env.class);
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        environment.when(Env::getCurrentEnv).thenReturn(env);
        config = Mockito.mockStatic(Config.class);
        ConnectContext.remove();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        config.close();
        environment.close();
        FeConstants.runningUnitTest = savedUnitTest;
    }

    private TStreamLoadPutRequest request() {
        return new TStreamLoadPutRequest().setUser("loader").setLoadId(new TUniqueId(1, 2));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void planningScopeReleasesContextOnSuccessAndFailure(boolean fail) throws Exception {
        AtomicReference<ConnectContext> planned = new AtomicReference<>();
        StatementContext statement = Mockito.mock(StatementContext.class);
        RuntimeException failure = new RuntimeException("planning failed");
        try {
            try (StreamLoadHandler handler = new StreamLoadHandler(request(), null,
                    new TStreamLoadPutResult(), "127.0.0.1")) {
                handler.setCloudCluster();
                planned.set(ConnectContext.get());
                planned.get().setStatementContext(statement);
                if (fail) {
                    throw failure;
                }
            }
        } catch (RuntimeException caught) {
            Assertions.assertSame(failure, caught);
        }
        Assertions.assertNull(ConnectContext.get());
        Assertions.assertTrue(planned.get().isClosingOrClosed());
        Mockito.verify(statement).close();
    }

    @Test
    public void borrowedMysqlLoadContextRemainsOpen() throws Exception {
        ConnectContext parent = new ConnectContext();
        parent.setThreadLocalInfo();
        try (StreamLoadHandler handler = new StreamLoadHandler(request(), null,
                new TStreamLoadPutResult(), "127.0.0.1")) {
            handler.setCloudCluster();
            Assertions.assertSame(parent, ConnectContext.get());
        }
        Assertions.assertSame(parent, ConnectContext.get());
        Assertions.assertFalse(parent.isClosingOrClosed());
        parent.cleanup();
    }

    @Test
    public void cloudInitializationFailureReleasesItsContext() {
        config.when(Config::isCloudMode).thenReturn(true);
        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        environment.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
        AtomicReference<ConnectContext> created = new AtomicReference<>();
        Mockito.when(systemInfo.getBackend(99L)).thenAnswer(call -> {
            created.set(ConnectContext.get());
            return null;
        });
        Assertions.assertThrows(UserException.class, () -> {
            try (StreamLoadHandler handler = new StreamLoadHandler(request().setAuthCode(1L).setBackendId(99L),
                    null, new TStreamLoadPutResult(), "127.0.0.1")) {
                handler.setCloudCluster();
            }
        });
        Assertions.assertNull(ConnectContext.get());
        Assertions.assertNotNull(created.get());
        Assertions.assertTrue(created.get().isClosingOrClosed());
    }
}
