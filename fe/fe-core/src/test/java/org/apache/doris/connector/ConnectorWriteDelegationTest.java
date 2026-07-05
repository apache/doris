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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorWriteHandle;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.api.write.ConnectorSinkPlan;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.EnumSet;
import java.util.Set;

/**
 * Pins {@link Connector}'s null-safe write-capability delegators
 * ({@code supportedWriteOperations} + the 5 {@code requiresXxx}/{@code supportsWriteBranch} views): a
 * connector with no write provider must report no write capability (empty operation set, every trait
 * {@code false}) rather than NPE, and a connector whose provider overrides
 * {@link ConnectorWritePlanProvider#supportedOperations()} / {@link ConnectorWritePlanProvider#requiresParallelWrite()}
 * must have that reflected unchanged through {@link Connector}.
 */
public class ConnectorWriteDelegationTest {

    @Test
    void delegatorsReflectProviderAndAreNullSafe() {
        Connector noWrite = Mockito.mock(Connector.class, Mockito.CALLS_REAL_METHODS);
        Mockito.when(noWrite.getWritePlanProvider()).thenReturn(null);
        Assertions.assertTrue(noWrite.supportedWriteOperations().isEmpty());
        Assertions.assertFalse(noWrite.supportsWriteBranch());
        Assertions.assertFalse(noWrite.requiresParallelWrite());
        Assertions.assertFalse(noWrite.requiresFullSchemaWriteOrder());
        Assertions.assertFalse(noWrite.requiresPartitionLocalSort());
        Assertions.assertFalse(noWrite.requiresMaterializeStaticPartitionValues());

        ConnectorWritePlanProvider prov = new ConnectorWritePlanProvider() {
            @Override
            public ConnectorSinkPlan planWrite(ConnectorSession s, ConnectorWriteHandle h) {
                return null;
            }

            @Override
            public Set<WriteOperation> supportedOperations() {
                return EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE);
            }

            @Override
            public boolean requiresParallelWrite() {
                return true;
            }
        };
        Connector w = Mockito.mock(Connector.class, Mockito.CALLS_REAL_METHODS);
        Mockito.when(w.getWritePlanProvider()).thenReturn(prov);
        Assertions.assertEquals(EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE),
                w.supportedWriteOperations());
        Assertions.assertTrue(w.requiresParallelWrite());
        Assertions.assertFalse(w.requiresPartitionLocalSort());
    }
}
