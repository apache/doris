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

package org.apache.doris.qe;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Status;
import org.apache.doris.nereids.StatementContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;

public class AutoCloseConnectContextTest {
    private boolean savedUnitTest;
    private MockedStatic<Env> env;

    @BeforeEach
    public void setUp() {
        savedUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        env = Mockito.mockStatic(Env.class);
        env.when(Env::getCurrentEnv).thenReturn(Mockito.mock(Env.class));
        ConnectContext.remove();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        env.close();
        FeConstants.runningUnitTest = savedUnitTest;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void scopeReleasesStatementAndRestoresCaller(boolean hasParent) {
        ConnectContext parent = hasParent ? new ConnectContext() : null;
        if (parent != null) {
            parent.setThreadLocalInfo();
        }
        ConnectContext child = new ConnectContext();
        StatementContext statement = Mockito.mock(StatementContext.class);
        child.setStatementContext(statement);
        StmtExecutor completed = Mockito.mock(StmtExecutor.class);
        child.setExecutor(completed);
        try (AutoCloseConnectContext scope = new AutoCloseConnectContext(child)) {
            Assertions.assertSame(child, ConnectContext.get());
        }
        Assertions.assertSame(parent, ConnectContext.get());
        Assertions.assertTrue(child.isClosingOrClosed());
        Assertions.assertNull(child.getStatementContext());
        Mockito.verify(statement).close();
        Mockito.verify(completed, Mockito.never()).cancel(Mockito.any(Status.class), Mockito.anyBoolean());
        if (parent != null) {
            Assertions.assertFalse(parent.isClosingOrClosed());
            parent.cleanup();
        }
    }

    @Test
    public void bodyAndStatementFailuresStillReleaseScope() {
        ConnectContext parent = new ConnectContext();
        parent.setThreadLocalInfo();
        ConnectContext child = new ConnectContext();
        StatementContext statement = Mockito.mock(StatementContext.class);
        Mockito.doThrow(new IllegalStateException("statement close failed")).when(statement).close();
        child.setStatementContext(statement);
        RuntimeException failure = new RuntimeException("body failed");
        Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class, () -> {
            try (AutoCloseConnectContext scope = new AutoCloseConnectContext(child)) {
                throw failure;
            }
        }));
        Assertions.assertSame(parent, ConnectContext.get());
        Assertions.assertTrue(child.isClosingOrClosed());
        Assertions.assertNull(child.getStatementContext());
        Mockito.verify(statement).close();
        child.cleanup();
        Mockito.verify(statement).close();
        parent.cleanup();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void sessionVariableScopeClosesOnlyCreatedContext(boolean hasParent) {
        ConnectContext parent = hasParent ? new ConnectContext() : null;
        SessionVariable original = parent == null ? null : parent.getSessionVariable();
        if (parent != null) {
            parent.setThreadLocalInfo();
        }
        StatementContext statement = Mockito.mock(StatementContext.class);
        ConnectContext scoped;
        try (AutoCloseSessionVariable scope = new AutoCloseSessionVariable(parent,
                Collections.singletonMap(SessionVariable.SQL_MODE, "0"))) {
            scoped = ConnectContext.get();
            scoped.setStatementContext(statement);
        }
        Assertions.assertSame(parent, ConnectContext.get());
        if (parent == null) {
            Assertions.assertTrue(scoped.isClosingOrClosed());
            Mockito.verify(statement).close();
        } else {
            Assertions.assertSame(original, parent.getSessionVariable());
            Assertions.assertFalse(parent.isClosingOrClosed());
            Mockito.verify(statement, Mockito.never()).close();
            parent.cleanup();
        }
    }

}
