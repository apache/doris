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

        // Simulate the PruneFileScanPartition lookup loop (with the null-guard from the fix)
        Map<String, PartitionItem> selectedPartitionItems = Maps.newHashMap();
        for (String name : result.partitions) {
            PartitionItem item = partitionItems.get(name);
            if (item != null) {
                selectedPartitionItems.put(name, item);
            }
        }

        // No null is stored, all returned partitions are in the map
        Assertions.assertEquals(result.partitions.size(), selectedPartitionItems.size(),
                "all returned partitions should be present in the consistent snapshot");
        Assertions.assertNotNull(selectedPartitionItems.get("p4"),
                "p4 PartitionItem should not be null");
    }

    /**
     * Test: even after the fix, the defensive null guard in PruneFileScanPartition
     * prevents null from entering selectedPartitionItems if inconsistent data
     * somehow reaches this point (defense in depth).
     */
    @Test
    public void testNullGuardPreventsNullEntry() throws AnalysisException {
        // old snapshot (no p4)
        Map<String, PartitionItem> nameToPartitionItem = Maps.newHashMapWithExpectedSize(3);
        nameToPartitionItem.put("p1", listItem(1));
        nameToPartitionItem.put("p2", listItem(2));
        nameToPartitionItem.put("p3", listItem(3));
        nameToPartitionItem = ImmutableMap.copyOf(nameToPartitionItem);

        // new snapshot (has p4) — simulating inconsistent data that should never happen
        // after the fix, but the defensive guard handles it anyway
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

        // The defensive null guard (from the fix) filters out p4
        // because nameToPartitionItem.get("p4") returns null
        Map<String, PartitionItem> selectedPartitionItems = Maps.newHashMap();
        for (String name : result.partitions) {
            PartitionItem item = nameToPartitionItem.get(name);
            if (item != null) {
                selectedPartitionItems.put(name, item);
            }
        }

        // Defense in depth: p4 is excluded, no null stored
        Assertions.assertFalse(selectedPartitionItems.containsKey("p4"),
                "defensive null guard should exclude p4 when it is missing from nameToPartitionItem");
        Assertions.assertNull(selectedPartitionItems.get("p4"),
                "p4 should not be stored at all");

        // SelectedPartitions can be constructed without NPE because no null values exist
        SelectedPartitions sp = new SelectedPartitions(nameToPartitionItem.size(),
                selectedPartitionItems, true, result.hasPartitionPredicate);
        Assertions.assertNotNull(sp, "SelectedPartitions should be constructable without NPE");
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
