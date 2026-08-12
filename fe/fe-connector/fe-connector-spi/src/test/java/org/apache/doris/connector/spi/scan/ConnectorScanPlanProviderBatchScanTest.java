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

package org.apache.doris.connector.spi.scan;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;

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
 * connector silently enters batch mode without opting in; {@code planScanForPartitionBatch} MUST call
 * {@code planScan} with the batch as the required partitions AND everything else about the request
 * unchanged, so a connector whose {@code planScan} is partition-set-scoped — like MaxCompute — gets
 * correct per-batch behaviour without overriding it.</p>
 */
public class ConnectorScanPlanProviderBatchScanTest {

    /** Records the request the default planScanForPartitionBatch forwards. */
    private static final class RecordingProvider implements ConnectorScanPlanProvider {
        static final List<ConnectorScanRange> MARKER = Collections.emptyList();
        ConnectorScanRequest recordedRequest;
        boolean eagerProfilesCollected;

        @Override
        public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
            this.recordedRequest = request;
            return MARKER;
        }

        @Override
        public List<ConnectorScanProfile> collectScanProfiles(ConnectorSession session) {
            eagerProfilesCollected = true;
            return Collections.singletonList(new ConnectorScanProfile("group", "scan", Collections.emptyMap()));
        }
    }

    private static final ConnectorTableHandle HANDLE = new ConnectorTableHandle() {
    };

    @Test
    public void testSupportsBatchScanDefaultsFalse() {
        // Default MUST be false: any connector that does not opt in stays on the synchronous path.
        ConnectorScanPlanProvider provider = new RecordingProvider();
        Assertions.assertFalse(provider.supportsBatchScan(null, null));
    }

    @Test
    public void testStreamingSplitEstimateDefaultsNegative() {
        // FIX-M3: default MUST be < 0 so no connector silently enters file-count streaming without opting in
        // (the engine treats < 0 as "stay on the synchronous planScan path").
        ConnectorScanPlanProvider provider = new RecordingProvider();
        Assertions.assertTrue(provider.streamingSplitEstimate(null, null, Optional.empty(), false) < 0);
    }

    @Test
    public void testStreamSplitsDefaultThrows() {
        // FIX-M3: the default producer MUST fail loud — it is only reachable if a connector returns a
        // non-negative streamingSplitEstimate without implementing streamSplits, which is a connector bug.
        ConnectorScanPlanProvider provider = new RecordingProvider();
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> provider.streamSplits(null, null, Collections.emptyList(), Optional.empty(), -1L));
    }

    @Test
    public void testStreamingProfilesRequireExplicitOptIn() {
        RecordingProvider provider = new RecordingProvider();

        Assertions.assertTrue(provider.collectStreamingScanProfiles(null).isEmpty());
        Assertions.assertFalse(provider.eagerProfilesCollected,
                "the streaming hook must not call an eager collector with different lifecycle assumptions");
    }

    @Test
    public void testPlanScanForPartitionBatchRescopesTheRequestToTheBatch() {
        // Default MUST call planScan with the batch as the required partitions and EVERY other field of
        // the request carried over untouched. A connector with partition-set-scoped planScan (MaxCompute)
        // relies on this to avoid overriding the method. MUTATION: a withRequiredPartitions that dropped
        // any other field would leave the batched scan planning without the filter or the limit, which is
        // exactly the silent capability loss the request object replaced -> red here.
        RecordingProvider provider = new RecordingProvider();
        List<String> batch = Arrays.asList("pt=1", "pt=2");
        List<ConnectorColumnHandle> columns = Collections.emptyList();
        ConnectorExpression filter = new ConnectorColumnRef("c1", ConnectorType.of("INT"));
        ConnectorScanRequest request = ConnectorScanRequest.builder(HANDLE, columns)
                .filter(Optional.of(filter))
                .limit(7L)
                .countPushdown(true)
                .explainOnly(true)
                .build();

        List<ConnectorScanRange> result = provider.planScanForPartitionBatch(null, request, batch);

        Assertions.assertSame(RecordingProvider.MARKER, result);
        ConnectorScanRequest forwarded = provider.recordedRequest;
        Assertions.assertEquals(batch, forwarded.getRequiredPartitions());
        Assertions.assertSame(HANDLE, forwarded.getTableHandle());
        Assertions.assertSame(columns, forwarded.getColumns());
        Assertions.assertSame(filter, forwarded.getFilter().orElse(null));
        Assertions.assertEquals(7L, forwarded.getLimit());
        Assertions.assertTrue(forwarded.isCountPushdown());
        // Dropping this one would silently make a batched EXPLAIN plan the way a real scan does --
        // which for a connector whose planning has a side effect on the source means EXPLAIN runs the
        // query. Losing it is invisible in the plan output.
        Assertions.assertTrue(forwarded.isExplainOnly());
    }

    @Test
    public void testRequestDefaultsAskForNothingSpecial() {
        // The fields a connector may ignore must default to "the engine is not asking for anything":
        // no filter, no limit, every partition, no COUNT(*) pushdown. A different default would make a
        // connector that reads them behave differently depending on which builder setters the caller used.
        ConnectorScanRequest request =
                ConnectorScanRequest.builder(HANDLE, Collections.emptyList()).build();

        Assertions.assertFalse(request.getFilter().isPresent());
        Assertions.assertEquals(-1L, request.getLimit());
        Assertions.assertTrue(request.getRequiredPartitions().isEmpty());
        Assertions.assertFalse(request.isCountPushdown());
        // Default false = "this plan will be run": a connector that reads it takes its normal path
        // unless the engine says otherwise.
        Assertions.assertFalse(request.isExplainOnly());
        // null is accepted for the partition set and means the same as empty: scan everything.
        Assertions.assertTrue(ConnectorScanRequest.builder(HANDLE, Collections.emptyList())
                .requiredPartitions(null).build().getRequiredPartitions().isEmpty());
    }
}
