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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * FIX-EXPLAIN-PARTITION-COUNT — guards {@link PluginDrivenScanNode#displayPartitionCounts}, which
 * derives the EXPLAIN {@code partition=N/M} counts (also fed to SQL-block-rule enforcement via
 * {@code getSelectedPartitionNum()}) from the Nereids {@link SelectedPartitions}.
 *
 * <p><b>Why this matters / the bug this pins:</b> the gate is {@code != NOT_PRUNED}, deliberately NOT
 * {@code isPruned}. A partitioned table queried WITHOUT a partition predicate keeps the initial
 * all-partitions selection from {@code ExternalTable.initSelectedPartitions} — {@code isPruned=false}
 * but a full, non-{@code NOT_PRUNED} map ({@code PruneFileScanPartition} only runs under a
 * {@code LogicalFilter}, so a no-WHERE / non-partition-predicate query never flips {@code isPruned}).
 * It must still report {@code partition=total/total} (e.g. {@code SELECT * FROM t} over 2 partitions
 * &rarr; {@code 2/2}). An {@code isPruned} gate regressed this to {@code 0/0}
 * ({@code test_max_compute_partition_prune}'s {@code one_partition_3_all} et al.). The contrast with
 * the connector pushdown gate ({@code resolveRequiredPartitions}, which correctly stays {@code
 * isPruned} — an unpruned scan reads ALL partitions and pushes no restriction) is the load-bearing
 * subtlety: the same {@code SelectedPartitions} maps to DIFFERENT answers for "what to display" vs
 * "what to push down".</p>
 */
public class PluginDrivenScanNodePartitionCountTest {

    private static Map<String, PartitionItem> items(int count) {
        Map<String, PartitionItem> items = new LinkedHashMap<>();
        for (int i = 0; i < count; i++) {
            items.put("pt=" + i, Mockito.mock(PartitionItem.class));
        }
        return items;
    }

    @Test
    public void testNotPrunedSentinelShowsNoCounts() {
        // NOT_PRUNED = non-partitioned / pruning unsupported -> leave the fields at default (0/0), as
        // legacy did (its display gate was `!= NOT_PRUNED`). Returning [0,0] here would be acceptable
        // numerically but null keeps "nothing to show" distinct from a genuine 0-partition selection.
        Assertions.assertNull(PluginDrivenScanNode.displayPartitionCounts(SelectedPartitions.NOT_PRUNED));
    }

    @Test
    public void testNullShowsNoCounts() {
        Assertions.assertNull(PluginDrivenScanNode.displayPartitionCounts(null));
    }

    @Test
    public void testNoPartitionPredicateReportsAllOverAll() {
        // THE regression guard: a partitioned table with NO partition predicate keeps the initial
        // all-partitions selection (isPruned=FALSE, full map). It must report total/total (2/2), NOT
        // 0/0. A mutation reverting the gate to `isPruned` makes this red — exactly the bug that showed
        // `partition=0/0` for `SELECT * FROM one_partition_tb`.
        SelectedPartitions allPartitions = new SelectedPartitions(2, items(2), false);
        Assertions.assertArrayEquals(new long[] {2, 2},
                PluginDrivenScanNode.displayPartitionCounts(allPartitions));
    }

    @Test
    public void testPrunedSubsetReportsSelectedOverTotal() {
        // Pruned to 2 of 5 partitions -> selected=2 (map size), total=5 (totalPartitionNum).
        SelectedPartitions pruned = new SelectedPartitions(5, items(2), true);
        Assertions.assertArrayEquals(new long[] {2, 5},
                PluginDrivenScanNode.displayPartitionCounts(pruned));
    }

    @Test
    public void testPrunedToZeroReportsZeroOverTotal() {
        // Pruned away every partition (e.g. WHERE part=<absent value>) -> 0/total, NOT 0/0. Pins that
        // total comes from totalPartitionNum (kept even when the surviving map is empty), and that this
        // value is produced BEFORE getSplits()'s pruned-to-zero short-circuit so EXPLAIN still shows it.
        SelectedPartitions prunedToZero = new SelectedPartitions(2, Collections.emptyMap(), true);
        Assertions.assertArrayEquals(new long[] {0, 2},
                PluginDrivenScanNode.displayPartitionCounts(prunedToZero));
    }
}
