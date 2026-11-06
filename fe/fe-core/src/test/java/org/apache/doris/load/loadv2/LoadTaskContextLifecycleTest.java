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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.EnvFactory;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.load.NereidsLoadingTaskPlanner;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

public class LoadTaskContextLifecycleTest {
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
    @ValueSource(booleans = {false, true})
    public void coordinatorInitializationFailureClosesOnlyOwnedContext(boolean hasParent) throws Exception {
        ConnectContext parent = hasParent ? new ConnectContext() : null;
        if (parent != null) {
            parent.setThreadLocalInfo();
        }
        LoadTaskCallback callback = Mockito.mock(LoadTaskCallback.class);
        Mockito.when(callback.getCallbackId()).thenReturn(17L);
        LoadLoadingTask task = new LoadLoadingTask(UserIdentity.ROOT, null, null, null, Collections.emptyList(),
                Long.MAX_VALUE, 0, false, false, null, 1, callback, "UTC", 60, 1, 1,
                false, null, false, LoadTask.Priority.NORMAL, false, 1024);
        NereidsLoadingTaskPlanner planner = Mockito.mock(NereidsLoadingTaskPlanner.class);
        Mockito.when(planner.getTimezone()).thenReturn("UTC");
        Field plannerField = LoadLoadingTask.class.getDeclaredField("planner");
        plannerField.setAccessible(true);
        plannerField.set(task, planner);

        AtomicReference<ConnectContext> execution = new AtomicReference<>();
        StatementContext statement = Mockito.mock(StatementContext.class);
        RuntimeException failure = new IllegalStateException("coordinator initialization failed");
        EnvFactory factory = Mockito.mock(EnvFactory.class);
        Mockito.when(factory.createCoordinator(Mockito.eq(17L), Mockito.isNull(), Mockito.isNull(),
                Mockito.anyList(), Mockito.anyList(), Mockito.eq("UTC"), Mockito.eq(false), Mockito.eq(false)))
                .thenAnswer(invocation -> {
                    execution.set(ConnectContext.get());
                    execution.get().setStatementContext(statement);
                    throw failure;
                });
        try (MockedStatic<EnvFactory> factoryClass = Mockito.mockStatic(EnvFactory.class)) {
            factoryClass.when(EnvFactory::getInstance).thenReturn(factory);
            Assertions.assertSame(failure, Assertions.assertThrows(RuntimeException.class, task::executeOnce));
        }
        Assertions.assertSame(parent, ConnectContext.get());
        if (hasParent) {
            Assertions.assertSame(parent, execution.get());
            Assertions.assertFalse(parent.isClosingOrClosed());
            Assertions.assertSame(statement, parent.getStatementContext());
            Mockito.verify(statement, Mockito.never()).close();
            parent.cleanup();
        } else {
            Assertions.assertNotNull(execution.get());
            Assertions.assertTrue(execution.get().isClosingOrClosed());
            Assertions.assertNull(execution.get().getStatementContext());
            Mockito.verify(statement).close();
        }
    }
}
