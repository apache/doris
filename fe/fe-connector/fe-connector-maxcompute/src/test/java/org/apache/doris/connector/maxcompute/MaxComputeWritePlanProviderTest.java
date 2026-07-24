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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.handle.WriteOperation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.HashMap;

/**
 * Pins {@link MaxComputeWritePlanProvider}'s write capability declarations (the connector module has
 * no fe-core / Mockito, so the provider is constructed directly with no network or live ODPS —
 * {@code planWrite} is the only method that touches the connector's initialized state).
 *
 * <p><b>WHY this matters:</b> the write plan provider is now the single source of truth for a
 * connector's write capabilities (supportedOperations + the sink-trait defaults from
 * {@code ConnectorWritePlanProvider}). MaxCompute supports INSERT/OVERWRITE only (no DELETE/MERGE),
 * requires parallel write, full-schema write order, and partition-local sort — but does NOT support a
 * write-targeted branch and does NOT require materializing static partition values, so those two
 * traits stay at their interface default (false).</p>
 */
public class MaxComputeWritePlanProviderTest {

    private static MaxComputeWritePlanProvider provider() {
        return new MaxComputeWritePlanProvider(new MaxComputeDorisConnector(new HashMap<>(), null));
    }

    @Test
    public void declaresInsertOverwriteAndSinkTraits() {
        MaxComputeWritePlanProvider p = provider();

        Assertions.assertEquals(EnumSet.of(WriteOperation.INSERT, WriteOperation.OVERWRITE), p.supportedOperations());
        Assertions.assertTrue(p.requiresParallelWrite());
        Assertions.assertTrue(p.requiresFullSchemaWriteOrder());
        Assertions.assertTrue(p.requiresPartitionLocalSort());
        Assertions.assertFalse(p.supportsWriteBranch(), "MaxCompute does not support a write-targeted branch");
        Assertions.assertFalse(p.requiresMaterializeStaticPartitionValues(),
                "MaxCompute does not require materializing static partition values");
    }
}
