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

import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.nereids.StatementContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

class AutoCloseConnectContextTest {

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    @Test
    void closeReleasesConnectorScopeAndRestoresPreviousContext() {
        ConnectContext previous = new ConnectContext();
        previous.setThreadLocalInfo();
        ConnectContext current = new ConnectContext();
        StatementContext statementContext = new StatementContext(current, null);
        current.setStatementContext(statementContext);
        ConnectorStatementScope scope = statementContext.getOrCreateConnectorStatementScope();
        AtomicBoolean closed = new AtomicBoolean();
        scope.computeIfAbsent("resource", () -> (AutoCloseable) () -> closed.set(true));

        try (AutoCloseConnectContext ignored = new AutoCloseConnectContext(current)) {
            Assertions.assertSame(current, ConnectContext.get());
        }

        Assertions.assertTrue(closed.get());
        Assertions.assertSame(previous, ConnectContext.get());
    }
}
