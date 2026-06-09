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

package org.apache.doris.datasource;

import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * FIX-BATCH-MODE-SPLIT (P4-T06e / NG-7) — guards {@link PluginDrivenScanNode#shouldUseBatchMode},
 * the pure four-input gate deciding whether a plugin-driven partitioned scan uses batched/streaming
 * split generation instead of synchronous enumeration.
 *
 * <p><b>Why this matters:</b> batch mode mirrors legacy {@code MaxComputeScanNode.isBatchMode()}.
 * Getting any gate wrong has real consequences: enabling batch when it should not (e.g. dropping the
 * "must be pruned" or "must have files" guard) spins up async read sessions for the wrong tables;
 * disabling it when it should fire (e.g. an off-by-one on the partition-count threshold) silently
 * regresses large-partition scans back to slow synchronous planning + large single sessions (the
 * exact OOM/latency risk this fix removes). The connector {@code fileNum > 0} check is folded into
 * the {@code supportsBatchScan} input.</p>
 *
 * <p><b>Coverage scope:</b> these tests pin the PURE static gate only. The wiring method
 * {@code computeBatchMode} — including its {@code scanProvider != null} null-guard (SF-1), which
 * maps a provider-less connector to {@code supportsBatchScan=false} — is NOT exercised here
 * (constructing a {@code PluginDrivenScanNode} needs a harness this module lacks). That null-guard
 * and the async {@code startSplit} path are live-only / DV-019 gaps.</p>
 */
public class PluginDrivenScanNodeBatchModeTest {

    private static final int THRESHOLD = 1024; // num_partitions_in_batch_mode default; pinned (it is fuzzy at runtime)

    private static SelectedPartitions pruned(int count) {
        Map<String, PartitionItem> items = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            items.put("pt=" + i, Mockito.mock(PartitionItem.class));
        }
        return new SelectedPartitions(count, items, true);
    }

    @Test
    public void testNotPrunedNeverBatches() {
        // NOT_PRUNED = non-partitioned / pruning not applied -> never batch. NOTE: NOT_PRUNED carries
        // an EMPTY map, so this case is non-discriminating for the !isPruned guard alone (0 >= THRESHOLD
        // is false regardless); the guard mutant is killed by testUnprocessedPruningNeverBatches
        // (populated map). This test documents the legacy NOT_PRUNED singleton path.
        Assertions.assertFalse(
                PluginDrivenScanNode.shouldUseBatchMode(SelectedPartitions.NOT_PRUNED, true, true, THRESHOLD));
    }

    @Test
    public void testNullSelectionNeverBatches() {
        Assertions.assertFalse(PluginDrivenScanNode.shouldUseBatchMode(null, true, true, THRESHOLD));
    }

    @Test
    public void testUnprocessedPruningNeverBatches() {
        // isPruned=false with a populated map is "pruning not processed" -> not batch. Pins the
        // !isPruned guard: dropping it would batch on an unpruned (effectively full) selection.
        Map<String, PartitionItem> items = new LinkedHashMap<>();
        for (int i = 0; i < THRESHOLD; i++) {
            items.put("pt=" + i, Mockito.mock(PartitionItem.class));
        }
        SelectedPartitions notProcessed = new SelectedPartitions(THRESHOLD, items, false);
        Assertions.assertFalse(PluginDrivenScanNode.shouldUseBatchMode(notProcessed, true, true, THRESHOLD));
    }

    @Test
    public void testNoSlotsNeverBatches() {
        // No required slots (e.g. count-only) -> not batch. Pins the hasSlots guard.
        Assertions.assertFalse(PluginDrivenScanNode.shouldUseBatchMode(pruned(THRESHOLD), false, true, THRESHOLD));
    }

    @Test
    public void testConnectorWithoutBatchSupportNeverBatches() {
        // supportsBatchScan=false -> not batch. Pins the supportsBatchScan guard. (A null scan provider
        // also resolves to supportsBatchScan=false, but that mapping lives in computeBatchMode's
        // null-guard and is NOT exercised by this static-helper test — see DV-019.)
        Assertions.assertFalse(PluginDrivenScanNode.shouldUseBatchMode(pruned(THRESHOLD), true, false, THRESHOLD));
    }

    @Test
    public void testZeroThresholdDisablesBatch() {
        // num_partitions_in_batch_mode == 0 disables batch mode entirely (legacy contract). Pins the
        // `numPartitionsInBatchMode > 0` guard: with `>= 0` a zero threshold would wrongly batch.
        Assertions.assertFalse(PluginDrivenScanNode.shouldUseBatchMode(pruned(THRESHOLD), true, true, 0));
    }

    @Test
    public void testBelowThresholdDoesNotBatch() {
        // Fewer pruned partitions than the threshold -> synchronous path (small scans need no batching).
        Assertions.assertFalse(
                PluginDrivenScanNode.shouldUseBatchMode(pruned(THRESHOLD - 1), true, true, THRESHOLD));
    }

    @Test
    public void testAtThresholdBatches() {
        // size == threshold is INCLUSIVE (legacy uses >=). Pins the boundary: a `>` mutant fails here.
        Assertions.assertTrue(
                PluginDrivenScanNode.shouldUseBatchMode(pruned(THRESHOLD), true, true, THRESHOLD));
    }

    @Test
    public void testAboveThresholdBatches() {
        // The main success case: a large pruned partition set on a file-bearing, sloted, pruned table.
        Assertions.assertTrue(
                PluginDrivenScanNode.shouldUseBatchMode(pruned(THRESHOLD + 5), true, true, THRESHOLD));
    }
}
