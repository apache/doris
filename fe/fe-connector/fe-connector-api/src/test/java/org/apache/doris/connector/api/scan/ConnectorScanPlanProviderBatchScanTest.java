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

package org.apache.doris.connector.api.scan;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * FIX-BATCH-MODE-SPLIT (P4-T06e / NG-7) — guards the two additive SPI defaults on
 * {@link ConnectorScanPlanProvider}: {@code supportsBatchScan} and {@code planScanForPartitionBatch}.
 *
 * <p><b>Why this matters:</b> these defaults are what keep the change zero-break for the other
 * connectors (es/jdbc/hive/paimon/hudi/trino). {@code supportsBatchScan} MUST default to false so no
 * connector silently enters batch mode without opting in; {@code planScanForPartitionBatch} MUST
 * delegate to the 6-arg {@code planScan} with the batch as the required partitions (and forward the
 * limit), so a connector whose {@code planScan} is partition-set-scoped — like MaxCompute — gets
 * correct per-batch behaviour without overriding it.</p>
 */
public class ConnectorScanPlanProviderBatchScanTest {

    /** Records the partition list / limit the default planScanForPartitionBatch forwards. */
    private static final class RecordingProvider implements ConnectorScanPlanProvider {
        static final List<ConnectorScanRange> MARKER = Collections.emptyList();
        List<String> recordedRequiredPartitions;
        long recordedLimit = Long.MIN_VALUE;
        boolean fourArgCalled;

        @Override
        public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorTableHandle handle,
                List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter) {
            fourArgCalled = true;
            return MARKER;
        }

        @Override
        public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorTableHandle handle,
                List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter,
                long limit, List<String> requiredPartitions) {
            this.recordedLimit = limit;
            this.recordedRequiredPartitions = requiredPartitions;
            return MARKER;
        }
    }

    @Test
    public void testSupportsBatchScanDefaultsFalse() {
        // Default MUST be false: any connector that does not opt in stays on the synchronous path.
        ConnectorScanPlanProvider provider = new RecordingProvider();
        Assertions.assertFalse(provider.supportsBatchScan(null, null));
    }

    @Test
    public void testPlanScanForPartitionBatchDelegatesToSixArgPlanScan() {
        // Default MUST forward the batch as requiredPartitions and pass the limit through to the
        // 6-arg planScan, returning its result. A connector with partition-set-scoped planScan
        // (MaxCompute) relies on this to avoid overriding the method.
        RecordingProvider provider = new RecordingProvider();
        List<String> batch = Arrays.asList("pt=1", "pt=2");

        List<ConnectorScanRange> result =
                provider.planScanForPartitionBatch(null, null, Collections.emptyList(),
                        Optional.empty(), -1L, batch);

        Assertions.assertSame(RecordingProvider.MARKER, result);
        Assertions.assertSame(batch, provider.recordedRequiredPartitions);
        Assertions.assertEquals(-1L, provider.recordedLimit);
        // It must route through the 6-arg overload, not collapse to the 4-arg one.
        Assertions.assertFalse(provider.fourArgCalled);
    }
}
