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
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterOpExecutor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicReference;

public class GroupCommitContextLifecycleTest {
    private boolean savedUnitTest;
    private MockedStatic<Env> environment;

    @BeforeEach
    public void setUp() {
        savedUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        Env env = Mockito.mock(Env.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        environment = Mockito.mockStatic(Env.class);
        environment.when(Env::getCurrentEnv).thenReturn(env);
        ConnectContext.remove();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
        environment.close();
        FeConstants.runningUnitTest = savedUnitTest;
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    public void forwardingReleasesPrivateContextAndRestoresCaller(boolean hasParent, boolean fail) throws Exception {
        ConnectContext parent = hasParent ? new ConnectContext() : null;
        if (parent != null) {
            parent.setThreadLocalInfo();
        }
        AtomicReference<ConnectContext> forwarded = new AtomicReference<>();
        StatementContext statement = Mockito.mock(StatementContext.class);
        Exception failure = new Exception("forward failed");
        try (MockedConstruction<MasterOpExecutor> construction = Mockito.mockConstruction(MasterOpExecutor.class,
                (executor, context) -> {
                    ConnectContext child = (ConnectContext) context.arguments().get(0);
                    Assertions.assertSame(child, ConnectContext.get());
                    child.setStatementContext(statement);
                    forwarded.set(child);
                    if (fail) {
                        Mockito.doThrow(failure).when(executor).updateLoadData(7L, 11L);
                    }
                })) {
            GroupCommitManager manager = new GroupCommitManager();
            if (fail) {
                RuntimeException caught = Assertions.assertThrows(RuntimeException.class,
                        () -> manager.updateLoadData(7L, 11L));
                Assertions.assertSame(failure, caught.getCause());
            } else {
                manager.updateLoadData(7L, 11L);
            }
            Assertions.assertEquals(1, construction.constructed().size());
            Mockito.verify(construction.constructed().get(0)).updateLoadData(7L, 11L);
        }
        Assertions.assertSame(parent, ConnectContext.get());
        Assertions.assertNotSame(parent, forwarded.get());
        Assertions.assertTrue(forwarded.get().isClosingOrClosed());
        Assertions.assertNull(forwarded.get().getStatementContext());
        Mockito.verify(statement).close();
        if (parent != null) {
            Assertions.assertFalse(parent.isClosingOrClosed());
            parent.cleanup();
        }
    }
}
