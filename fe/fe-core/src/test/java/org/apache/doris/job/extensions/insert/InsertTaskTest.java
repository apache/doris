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

package org.apache.doris.job.extensions.insert;

import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

public class InsertTaskTest {
    @Test
    public void testMakeConnection() {
        ConnectContext previousContext = new ConnectContext();
        try (AutoCloseConnectContext parent = new AutoCloseConnectContext(previousContext)) {
            ConnectContext ctx = InsertTask.makeConnectContext(null, null);
            Assertions.assertSame(previousContext, ConnectContext.get());
            try (AutoCloseConnectContext scope = new AutoCloseConnectContext(ctx)) {
                Assertions.assertTrue(ctx.getState().isNereids());
                Assertions.assertSame(ctx, ConnectContext.get());
            }
            Assertions.assertTrue(ctx.isClosingOrClosed());
            Assertions.assertSame(previousContext, ConnectContext.get());
            Assertions.assertFalse(previousContext.isClosingOrClosed());
        }
    }

    @Test
    public void testFailedDatabaseSetupClosesOnlyNewContext() {
        ConnectContext parent = new ConnectContext();
        try (AutoCloseConnectContext scope = new AutoCloseConnectContext(parent);
                MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class,
                        (context, construction) -> {
                            Mockito.when(context.getState()).thenReturn(new QueryState());
                            Mockito.doThrow(new IllegalStateException("database setup failed"))
                                    .when(context).setDatabase("db");
                        })) {
            Assertions.assertThrows(IllegalStateException.class,
                    () -> InsertTask.makeConnectContext(null, "db"));
            Assertions.assertEquals(1, contexts.constructed().size());
            Mockito.verify(contexts.constructed().get(0)).cleanup();
            Assertions.assertSame(parent, ConnectContext.get());
            Assertions.assertFalse(parent.isClosingOrClosed());
        }
    }

}
