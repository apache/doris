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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

class MTMVContextLifecycleTest {
    private ConnectContext previous;
    private ConnectContext parent;
    private boolean savedUnitTest;
    private MockedStatic<Env> environment;

    @BeforeEach
    void setUp() {
        previous = ConnectContext.get();
        savedUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        environment = Mockito.mockStatic(Env.class);
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        environment.when(Env::getCurrentEnv).thenReturn(env);
        parent = new ConnectContext();
        parent.setThreadLocalInfo();
    }

    @AfterEach
    void tearDown() {
        parent.requestClose(ConnectContext.CloseMode.AFTER_RESPONSE);
        environment.close();
        if (previous == null) {
            ConnectContext.remove();
        } else {
            previous.setThreadLocalInfo();
        }
        FeConstants.runningUnitTest = savedUnitTest;
    }

    @Test
    void factoryTransfersUnboundContextToCallerScope() {
        ConnectContext child = MTMVPlanUtil.createBasicMvContext(null, Collections.emptyList(), Collections.emptyMap());
        Assertions.assertSame(parent, ConnectContext.get());
        Assertions.assertFalse(child.isClosingOrClosed());
        StatementContext statement = Mockito.mock(StatementContext.class);
        child.setStatementContext(statement);
        try (AutoCloseConnectContext scope = new AutoCloseConnectContext(child)) {
            Assertions.assertSame(scope.connectContext, ConnectContext.get());
        }
        Assertions.assertTrue(child.isClosingOrClosed());
        Assertions.assertFalse(parent.isClosingOrClosed());
        Assertions.assertSame(parent, ConnectContext.get());
        Mockito.verify(statement).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void failedBasicInitializationClosesChildAndRestoresCaller() {
        List<RuleType> rules = Mockito.mock(List.class);
        AtomicReference<ConnectContext> child = new AtomicReference<>();
        StatementContext statement = Mockito.mock(StatementContext.class);
        RuntimeException failure = new IllegalStateException("invalid rules");
        Mockito.when(rules.stream()).thenAnswer(call -> {
            child.set(ConnectContext.get());
            child.get().setStatementContext(statement);
            throw failure;
        });
        Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class,
                () -> MTMVPlanUtil.createBasicMvContext(null, rules, Collections.emptyMap())));
        Assertions.assertTrue(child.get().isClosingOrClosed());
        Assertions.assertFalse(parent.isClosingOrClosed());
        Assertions.assertSame(parent, ConnectContext.get());
        Mockito.verify(statement).close();
    }

    @Test
    void failedMvConfigurationClosesReturnedBasicContext() {
        ConnectContext child = new ConnectContext();
        StatementContext statement = Mockito.mock(StatementContext.class);
        child.setStatementContext(statement);
        MTMV mtmv = Mockito.mock(MTMV.class);
        RuntimeException failure = new IllegalStateException("invalid workload group");
        Mockito.when(mtmv.getSessionVariables()).thenReturn(Collections.emptyMap());
        Mockito.when(mtmv.getWorkloadGroup()).thenThrow(failure);
        try (MockedStatic<MTMVPlanUtil> utility = Mockito.mockStatic(MTMVPlanUtil.class)) {
            utility.when(() -> MTMVPlanUtil.createBasicMvContext(Mockito.isNull(), Mockito.anyList(), Mockito.anyMap()))
                    .thenReturn(child);
            utility.when(() -> MTMVPlanUtil.createMTMVContext(mtmv, Collections.emptyList())).thenCallRealMethod();
            Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class,
                    () -> MTMVPlanUtil.createMTMVContext(mtmv, Collections.emptyList())));
        }
        Assertions.assertTrue(child.isClosingOrClosed());
        Assertions.assertFalse(parent.isClosingOrClosed());
        Assertions.assertSame(parent, ConnectContext.get());
        Mockito.verify(statement).close();
    }
}
