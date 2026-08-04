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

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.write.ConnectorSinkPlan;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.EnumSet;
import java.util.Set;

/**
 * Pins the ONE forwarding the entry interface still performs on the write side: a connector that does not
 * override the per-table {@link Connector#getWritePlanProvider(ConnectorTableHandle)} must fall back to its
 * connector-level {@link Connector#getWritePlanProvider()}.
 *
 * <p><b>Why this matters:</b> the engine always asks for the per-table provider, because a heterogeneous
 * gateway needs the handle to pick the right one. Every single-format connector overrides only the no-arg
 * getter, so if that default stopped forwarding, each of them would silently answer "no write support" and
 * every INSERT into a jdbc / maxcompute / iceberg catalog would be rejected at planning. The gateway side of
 * the same seam is pinned by {@code HiveConnectorWriteProviderDivertTest}.</p>
 *
 * <p>The write traits themselves are NOT reachable from {@link Connector} and deliberately have no test here:
 * they are declared on {@link ConnectorWritePlanProvider} and read from there, so there is no second answer
 * that could drift from the first.</p>
 */
public class ConnectorWriteDelegationTest {

    @Test
    void perTableProviderFallsBackToTheConnectorLevelOne() {
        ConnectorWritePlanProvider prov = new ConnectorWritePlanProvider() {
            @Override
            public ConnectorSinkPlan planWrite(ConnectorSession s, ConnectorWriteHandle h) {
                return null;
            }

            @Override
            public Set<WriteOperation> supportedOperations() {
                return EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE);
            }
        };
        // CALLS_REAL_METHODS so the interface default runs; only the no-arg getter is overridden, exactly as
        // every single-format connector does.
        Connector single = Mockito.mock(Connector.class, Mockito.CALLS_REAL_METHODS);
        Mockito.when(single.getWritePlanProvider()).thenReturn(prov);

        // MUTATION: making the per-table default return null instead of forwarding -> red, and every
        // single-format connector loses writes.
        Assertions.assertSame(prov, single.getWritePlanProvider(Mockito.mock(ConnectorTableHandle.class)),
                "a connector that does not select a provider per table must reuse its connector-level one");
        Assertions.assertEquals(EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE),
                single.getWritePlanProvider(Mockito.mock(ConnectorTableHandle.class)).supportedOperations());
    }

    @Test
    void connectorWithoutWriteSupportAnswersNullOnBothGetters() {
        // The null provider IS the "no write support" declaration -- every engine-side write gate is written
        // as a null check against it, so both getters must agree.
        Connector noWrite = Mockito.mock(Connector.class, Mockito.CALLS_REAL_METHODS);

        Assertions.assertNull(noWrite.getWritePlanProvider());
        Assertions.assertNull(noWrite.getWritePlanProvider(Mockito.mock(ConnectorTableHandle.class)));
    }
}
