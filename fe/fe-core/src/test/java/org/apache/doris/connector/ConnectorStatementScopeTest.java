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

package org.apache.doris.connector;

import org.apache.doris.connector.api.ConnectorStatementScope;
import org.apache.doris.nereids.StatementContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for the per-statement {@link ConnectorStatementScope}: the {@link ConnectorStatementScope#NONE} no-op,
 * the memoizing {@link ConnectorStatementScopeImpl}, and the {@link StatementContext} hosting + per-execution
 * reset a reused prepared statement relies on.
 */
public class ConnectorStatementScopeTest {

    @Test
    public void noneNeverMemoizes() {
        // NONE = the off-context default (offline / no live statement): the loader runs every call, so a
        // connector under NONE behaves byte-identically to loading every time. MUTATION: NONE memoizing -> the
        // second call reuses the first value -> red.
        AtomicInteger loads = new AtomicInteger();
        ConnectorStatementScope none = ConnectorStatementScope.NONE;
        Object first = none.computeIfAbsent("k", () -> {
            loads.incrementAndGet();
            return new Object();
        });
        Object second = none.computeIfAbsent("k", () -> {
            loads.incrementAndGet();
            return new Object();
        });
        Assertions.assertEquals(2, loads.get(), "NONE must run the loader every time (no memo)");
        Assertions.assertNotSame(first, second, "NONE returns a fresh value each call");
    }

    @Test
    public void implMemoizesPerKeyAndIsolatesKeys() {
        // The real scope memoizes per key: the same key returns the same instance to every caller (this is what
        // makes a statement's read/scan/write resolvers share ONE loaded table), and the loader runs once.
        // MUTATION: not memoizing -> two instances / two loads -> red.
        ConnectorStatementScope scope = new ConnectorStatementScopeImpl();
        AtomicInteger loads = new AtomicInteger();
        Object firstA = scope.computeIfAbsent("A", () -> {
            loads.incrementAndGet();
            return new Object();
        });
        Object secondA = scope.computeIfAbsent("A", () -> {
            loads.incrementAndGet();
            return new Object();
        });
        Assertions.assertSame(firstA, secondA, "the same key returns the same instance (read/write share one load)");
        Assertions.assertEquals(1, loads.get(), "the loader runs once per key");

        Object b = scope.computeIfAbsent("B", Object::new);
        Assertions.assertNotSame(firstA, b, "different keys are isolated (cross-catalog / cross-table)");
    }

    @Test
    public void statementContextScopeIsStableThenResetsForReusedPreparedContext() {
        // StatementContext lazily builds one scope and reuses it across the statement (getOrCreate...). A prepared
        // EXECUTE reuses one StatementContext across executions; resetConnectorStatementScope() (called by
        // ExecuteCommand each execution) drops it so a prior execution's memoized state never leaks into the next.
        // MUTATION: reset not clearing the field -> s2 == s1 and the stale value leaks -> red.
        StatementContext ctx = new StatementContext();
        ConnectorStatementScope s1 = ctx.getOrCreateConnectorStatementScope();
        Assertions.assertSame(s1, ctx.getOrCreateConnectorStatementScope(),
                "the scope is lazily created once and reused across a statement");

        Object memoized = s1.computeIfAbsent("k", Object::new);
        ctx.resetConnectorStatementScope();

        ConnectorStatementScope s2 = ctx.getOrCreateConnectorStatementScope();
        Assertions.assertNotSame(s1, s2, "reset drops the scope so a reused prepared context starts fresh");
        Assertions.assertNotSame(memoized, s2.computeIfAbsent("k", Object::new),
                "a prior execution's memoized value must not leak into the next execution");
    }
}
