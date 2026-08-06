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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner.PartitionPruneResult;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner.PartitionTableType;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

/**
 * Verify fix for the TOCTOU bug in {@link SelectedPartitions}.
 *
 * <p>Before the fix: {@code nameToPartitionItem} was frozen in {@code SelectedPartitions}
 * at T1 but {@code sortedPartitionRanges} was re-read from cache at T2. If the cache
 * changed between T1 and T2 (concurrent ADD/DROP PARTITION), the two snapshots diverged,
 * binary search returned partitions not present in {@code nameToPartitionItem}, and
 * {@code ImmutableMap.copyOf()} threw NPE on the null value.
 *
 * <p>After the fix: {@code sortedPartitionRanges} is built from the same
 * {@code nameToPartitionItems} and frozen inside {@code SelectedPartitions} at T1.
 * The pruning rule reads both from the frozen snapshot so they are always consistent.
 */
public class BinarySearchPartitionInconsistencyTest extends TestWithFeService {
    private final Column partitionColumn = new Column("a", PrimitiveType.INT);
    private final SlotReference slotA = new SlotReference("a", IntegerType.INSTANCE);
    private CascadesContext cascadesContext;

    @Override
    protected void runBeforeAll() throws Exception {
        cascadesContext = createCascadesContext("select * from t1");
    }

    private ListPartitionItem listItem(int value) throws AnalysisException {
        PartitionValue partitionValue = new PartitionValue(String.valueOf(value));
        PartitionKey partitionKey = PartitionKey.createPartitionKey(
                ImmutableList.of(partitionValue), ImmutableList.of(partitionColumn));
        return new ListPartitionItem(ImmutableList.of(partitionKey));
    }

    /**
     * Test that SelectedPartitions correctly freezes sortedPartitionRanges
     * from the same partition items map.
     */
    @Test
    public void testSelectedPartitionsFreezesSortedPartitionRanges() throws AnalysisException {
        Map<String, PartitionItem> nameToPartitionItems = Maps.newHashMapWithExpectedSize(3);
        nameToPartitionItems.put("p1", listItem(1));
        nameToPartitionItems.put("p2", listItem(2));
        nameToPartitionItems.put("p3", listItem(3));

        Optional<SortedPartitionRanges<String>> sortedRanges = Optional.ofNullable(
                SortedPartitionRanges.build(nameToPartitionItems));

        SelectedPartitions sp = new SelectedPartitions(
                nameToPartitionItems.size(), nameToPartitionItems, false, false, sortedRanges);

        // sortedPartitionRanges field is populated
        Assertions.assertTrue(sp.sortedPartitionRanges.isPresent(),
                "sortedPartitionRanges should be present when partitions exist");
        Assertions.assertNotNull(sp.sortedPartitionRanges.get(),
                "sortedPartitionRanges should not be null");
    }

    /**
     * Test that NOT_PRUNED has empty sortedPartitionRanges.
     */
    @Test
    public void testNotPrunedHasEmptySortedPartitionRanges() {
        Assertions.assertFalse(SelectedPartitions.NOT_PRUNED.sortedPartitionRanges.isPresent(),
                "NOT_PRUNED should have empty sortedPartitionRanges");
        Assertions.assertEquals(Optional.empty(), SelectedPartitions.NOT_PRUNED.sortedPartitionRanges,
                "NOT_PRUNED.sortedPartitionRanges should be Optional.empty()");
    }

    /**
     * Test consistent snapshot: when both nameToPartitionItem and sortedPartitionRanges
     * come from the same data, no partition returned by binary search is missing
     * from the map. Simulates the PruneFileScanPartition flow after the fix.
     */
    @Test
    public void testConsistentSnapshotNoMissingPartitions() throws AnalysisException {
        // One consistent snapshot built at T1
        Map<String, PartitionItem> partitionItems = Maps.newHashMapWithExpectedSize(4);
        partitionItems.put("p1", listItem(1));
        partitionItems.put("p2", listItem(2));
        partitionItems.put("p3", listItem(3));
        partitionItems.put("p4", listItem(4));
        partitionItems = ImmutableMap.copyOf(partitionItems);

        SortedPartitionRanges<String> sortedRanges = SortedPartitionRanges.build(partitionItems);
        Assertions.assertNotNull(sortedRanges);

        // predicate hits p4, which DOES exist in both snapshots
        Expression predicate = new EqualTo(slotA, Literal.of(4));

        PartitionPruneResult<String> result = PartitionPruner.pruneWithResult(
                ImmutableList.of(slotA), predicate, partitionItems, cascadesContext,
                PartitionTableType.EXTERNAL, Optional.of(sortedRanges));

        // p4 should be in the result since it exists in the map
        Assertions.assertTrue(result.partitions.contains("p4"),
                "p4 should be returned when it exists in the consistent snapshot");

        // Simulate the PruneFileScanPartition lookup loop. Since both the partition map
        // and sortedPartitionRanges come from the same frozen snapshot, every returned
        // partition must be present in the map — assert the invariant rather than skip.
        Map<String, PartitionItem> selectedPartitionItems = Maps.newHashMap();
        for (String name : result.partitions) {
            PartitionItem item = partitionItems.get(name);
            Assertions.assertNotNull(item,
                    "pruned partition " + name + " must be present in the consistent snapshot");
            selectedPartitionItems.put(name, item);
        }

        // All returned partitions are in the map, none skipped
        Assertions.assertEquals(result.partitions.size(), selectedPartitionItems.size(),
                "all returned partitions should be present in the consistent snapshot");
        Assertions.assertNotNull(selectedPartitionItems.get("p4"),
                "p4 PartitionItem should not be null");
    }

    /**
     * Test: if inconsistent snapshots ever reach the lookup loop (which the fix makes
     * impossible by freezing both from the same snapshot), the invariant check fails
     * loudly instead of silently returning a partial scan over fewer partitions.
     *
     * <p>This mirrors the {@code Preconditions.checkState} in PruneFileScanPartition:
     * a missing partition is treated as an invariant violation, not a partition to skip.
     */
    @Test
    public void testInvariantViolationFailsLoudly() throws AnalysisException {
        // old snapshot (no p4)
        Map<String, PartitionItem> nameToPartitionItem = ImmutableMap.of(
                "p1", listItem(1),
                "p2", listItem(2),
                "p3", listItem(3));

        // new snapshot (has p4) — simulating inconsistent data that the fix prevents,
        // but the invariant check must still surface it rather than silently skip p4
        Map<String, PartitionItem> newPartitions = Maps.newHashMapWithExpectedSize(4);
        newPartitions.put("p1", listItem(1));
        newPartitions.put("p2", listItem(2));
        newPartitions.put("p3", listItem(3));
        newPartitions.put("p4", listItem(4));
        SortedPartitionRanges<String> sortedPartitionRanges = SortedPartitionRanges.build(newPartitions);
        Assertions.assertNotNull(sortedPartitionRanges);

        Expression predicate = new EqualTo(slotA, Literal.of(4));

        PartitionPruneResult<String> result = PartitionPruner.pruneWithResult(
                ImmutableList.of(slotA), predicate, nameToPartitionItem, cascadesContext,
                PartitionTableType.EXTERNAL, Optional.of(sortedPartitionRanges));

        // binary search returns p4 from the new snapshot
        Assertions.assertTrue(result.partitions.contains("p4"),
                "binary search returns p4 from the sortedPartitionRanges");

        // The invariant check (mirroring PruneFileScanPartition) must fail loudly:
        // p4 is in the pruned result but missing from nameToPartitionItem.
        Assertions.assertThrows(IllegalStateException.class, () -> {
            for (String name : result.partitions) {
                PartitionItem item = nameToPartitionItem.get(name);
                if (item == null) {
                    throw new IllegalStateException(
                            "pruned partition " + name + " is missing in the selected partitions snapshot");
                }
            }
        }, "a missing partition must fail the invariant instead of being silently skipped");
    }

    /**
     * Test that with binary search disabled, sequentialFiltering is unaffected.
     */
    @Test
    public void testSequentialFilteringNotAffectedByInconsistentSnapshot() throws AnalysisException {
        Map<String, PartitionItem> nameToPartitionItem = Maps.newHashMapWithExpectedSize(3);
        nameToPartitionItem.put("p1", listItem(1));
        nameToPartitionItem.put("p2", listItem(2));
        nameToPartitionItem.put("p3", listItem(3));
        nameToPartitionItem = ImmutableMap.copyOf(nameToPartitionItem);

        Expression predicate = new EqualTo(slotA, Literal.of(4));

        // binary search disabled -> sequentialFiltering path
        PartitionPruneResult<String> result = PartitionPruner.pruneWithResult(
                ImmutableList.of(slotA), predicate, nameToPartitionItem, cascadesContext,
                PartitionTableType.EXTERNAL, Optional.empty());

        Assertions.assertFalse(result.partitions.contains("p4"),
                "sequential filtering only uses nameToPartitionItem, p4 is never returned");
        Assertions.assertTrue(result.partitions.isEmpty(),
                "no partition matches a=4 in this snapshot");
    }

    /**
     * Test that SelectedPartitions factory method produces consistent state.
     */
    @Test
    public void testNewSelectedPartitionsHasConsistentSortedRanges() throws AnalysisException {
        Map<String, PartitionItem> items = Maps.newHashMapWithExpectedSize(2);
        items.put("p1", listItem(1));
        items.put("p2", listItem(2));

        Optional<SortedPartitionRanges<String>> ranges = Optional.ofNullable(
                SortedPartitionRanges.build(items));

        // The 5-arg constructor freezes both fields from the same snapshot
        SelectedPartitions sp = new SelectedPartitions(items.size(), items, false, false, ranges);

        Assertions.assertTrue(sp.sortedPartitionRanges.isPresent());
        SortedPartitionRanges<String> frozen = sp.sortedPartitionRanges.get();

        // The frozen sortedPartitionRanges contains exactly the same partitions as selectedPartitions
        int partitionCountInSortedRanges = frozen.sortedPartitions.size() + frozen.defaultPartitions.size();
        Assertions.assertEquals(sp.selectedPartitions.size(), partitionCountInSortedRanges,
                "sortedPartitionRanges should cover all partitions in selectedPartitions");
    }
}
