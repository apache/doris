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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * FIX-PRUNE-PUSHDOWN (P4-T06e / DG-1) — guards {@link PluginDrivenScanNode#resolveRequiredPartitions},
 * the three-state mapping from the Nereids {@code SelectedPartitions} to the partition list pushed
 * down to the connector SPI.
 *
 * <p><b>Why this matters:</b> before this fix the plugin-driven MaxCompute read path dropped the
 * pruned partition set entirely, so the ODPS read session spanned ALL partitions (full-scan
 * perf/memory regression). The fix threads the pruned set through, but the null-vs-empty distinction
 * is load-bearing and easy to get wrong:</p>
 * <ul>
 *   <li>{@code null} = "not pruned, scan all" — must NOT be confused with the short-circuit case,
 *       or every row would be silently dropped;</li>
 *   <li>non-empty list = "scan only these" — must be forwarded, or large tables regress to a full
 *       scan;</li>
 *   <li>empty list = "pruned to zero partitions" — must be distinguishable (non-null) so
 *       {@code getSplits()} can short-circuit with no splits, mirroring legacy
 *       {@code MaxComputeScanNode.getSplits():724-727}.</li>
 * </ul>
 */
public class PluginDrivenScanNodePartitionPruningTest {

    @Test
    public void testNotPrunedScansAllPartitions() {
        // NOT_PRUNED -> null (scan all). Returning [] here would be read as "pruned to zero" and
        // silently drop all rows.
        Assertions.assertNull(
                PluginDrivenScanNode.resolveRequiredPartitions(SelectedPartitions.NOT_PRUNED));
    }

    @Test
    public void testNullSelectionScansAllPartitions() {
        Assertions.assertNull(PluginDrivenScanNode.resolveRequiredPartitions(null));
    }

    @Test
    public void testUnprocessedPruningScansAllPartitions() {
        // isPruned=false with a populated map is still "pruning not processed" -> scan all.
        Map<String, PartitionItem> items = new LinkedHashMap<>();
        items.put("pt=1", Mockito.mock(PartitionItem.class));
        SelectedPartitions notProcessed = new SelectedPartitions(3, items, false);
        Assertions.assertNull(PluginDrivenScanNode.resolveRequiredPartitions(notProcessed));
    }

    @Test
    public void testPrunedSubsetForwardsPartitionNames() {
        // Pruned non-empty set must be forwarded; otherwise the connector reads all partitions.
        Map<String, PartitionItem> items = new LinkedHashMap<>();
        items.put("pt=1", Mockito.mock(PartitionItem.class));
        items.put("pt=2,region=cn", Mockito.mock(PartitionItem.class));
        SelectedPartitions pruned = new SelectedPartitions(5, items, true);

        List<String> result = PluginDrivenScanNode.resolveRequiredPartitions(pruned);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.containsAll(Arrays.asList("pt=1", "pt=2,region=cn")));
    }

    @Test
    public void testPrunedToZeroReturnsEmptyNonNullForShortCircuit() {
        // Pruned to zero partitions -> non-null empty list, distinct from the null "scan all"
        // case, so getSplits() can short-circuit and read nothing.
        SelectedPartitions emptyPruned = new SelectedPartitions(5, Collections.emptyMap(), true);

        List<String> result = PluginDrivenScanNode.resolveRequiredPartitions(emptyPruned);

        Assertions.assertNotNull(result);
        Assertions.assertTrue(result.isEmpty());
    }
}
