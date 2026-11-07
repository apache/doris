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

package org.apache.doris.nereids;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.connector.ConnectorStatementScopeImpl;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

class CascadesContextLifecycleTest {
    @ParameterizedTest
    @CsvSource({"false, false, false", "false, false, true", "true, false, false", "true, false, true",
            "true, true, false", "true, true, true"})
    void temporaryResourcesCloseButBorrowedResourcesRemainOpen(
            boolean hasParent, boolean hasStatement, boolean fails) {
        ConnectContext previous = ConnectContext.get();
        boolean savedUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        AtomicReference<ConnectContext> usedContext = new AtomicReference<>();
        AtomicReference<ConnectorStatementScopeImpl> connectorScope = new AtomicReference<>();
        ConnectContext parent = hasParent ? new ConnectContext() : null;
        RuntimeException failure = new IllegalStateException("rewrite failed");
        try (MockedStatic<Env> environment = Mockito.mockStatic(Env.class)) {
            environment.when(Env::getCurrentEnv).thenReturn(Mockito.mock(Env.class));
            if (parent == null) {
                ConnectContext.remove();
            } else {
                parent.setThreadLocalInfo();
                if (hasStatement) {
                    parent.setStatementContext(new StatementContext(parent, new OriginStatement("", 0)));
                }
            }
            Function<CascadesContext, String> action = context -> {
                usedContext.set(context.getConnectContext());
                connectorScope.set((ConnectorStatementScopeImpl)
                        context.getStatementContext().getOrCreateConnectorStatementScope());
                Assertions.assertFalse(context.getConnectContext().isClosingOrClosed());
                if (fails) {
                    throw failure;
                }
                return "rewritten";
            };
            if (fails) {
                Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class,
                        () -> CascadesContext.withTempContext(action)));
            } else {
                Assertions.assertEquals("rewritten", CascadesContext.withTempContext(action));
            }
            Assertions.assertSame(parent, ConnectContext.get());
            Assertions.assertEquals(!hasParent, usedContext.get().isClosingOrClosed());
            Assertions.assertEquals(!hasStatement, connectorScope.get().isClosed());
            if (hasParent) {
                Assertions.assertSame(parent, usedContext.get());
            }
        } finally {
            if (parent != null) {
                parent.requestClose(ConnectContext.CloseMode.AFTER_RESPONSE);
            }
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
            FeConstants.runningUnitTest = savedUnitTest;
        }
    }
}
