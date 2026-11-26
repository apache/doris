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

package org.apache.doris.nereids.stats;

import org.apache.doris.catalog.OlapTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit test of the identity of a scan: which partitions it selects is part of the key, so that
 * {@code PARTITION(p1)} and {@code PARTITION(p2)} of one table cannot share a pinned row count, and the
 * identity survives the text round trip a {@code HBO SET STATISTICS} statement performs.
 */
public class HboScanDescriptorIdentityTest {

    private static HboScanDescriptor descriptor(String table, List<Long> selectedIds, long totalPartitions) {
        return descriptor(table, selectedIds, totalPartitions, Collections.emptyList());
    }

    private static HboScanDescriptor descriptor(String table, List<Long> selectedIds, long totalPartitions,
            List<Long> selectedTabletIds) {
        return new HboScanDescriptor(table, HboScanDescriptor.UNKNOWN, "", 3, 1000,
                HboScanDescriptor.RowsKind.MEASURED, selectedIds.size(), totalPartitions,
                HboScanDescriptor.partitionIdentityOf(selectedIds, totalPartitions),
                HboScanDescriptor.tabletIdentityOf(selectedTabletIds), "");
    }

    /** The same scan of the same table name, read from the table with the given id. */
    private static HboScanDescriptor descriptorOfTable(long tableId) {
        return new HboScanDescriptor("db.t", tableId, "", 3, 1000, HboScanDescriptor.RowsKind.MEASURED,
                0, 0, "", "", "");
    }

    private static HboScanDescriptor rollupDescriptor(String table, long indexId) {
        return new HboScanDescriptor(table, HboScanDescriptor.UNKNOWN, "", 3, 1000,
                HboScanDescriptor.RowsKind.MEASURED, 2, 2, "", "", String.valueOf(indexId));
    }

    @Test
    public void testTheAliasOfAnExternalScanIsPartOfItsIdentity() {
        // an external scan has no data state to describe, so the alias which binds its predicates to the
        // relation is the only identity it can name
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getName()).thenReturn("t");
        HboScanDescriptor aliased = HboScanDescriptor.ofUnreadableState(table, "x");
        Assertions.assertEquals("null.null.t#ra:x", aliased.getShapeIdentity());
        Assertions.assertEquals("", HboScanDescriptor.ofUnreadableState(table).getRelationIdentity());
    }

    @Test
    public void testTheAliasOfARelationSurvivesTheTextForm() {
        HboScanDescriptor aliased = new HboScanDescriptor("db.t", 1000, "t%2Ex", 3, 1000,
                HboScanDescriptor.RowsKind.MEASURED, 0, 0, "", "", "");
        Assertions.assertEquals("db.t#ti:1000#ra:t%2Ex", aliased.getShapeIdentity());
        Assertions.assertEquals("db.t#ti:1000#ra:t%2Ex,v3,r1000", aliased.render());
        // the alias is part of the shape (the fingerprint input) and survives the round trip a pasted
        // struct info performs
        Assertions.assertEquals("t%2Ex",
                HboScanDescriptor.parse(aliased.render()).getRelationIdentity());
    }

    @Test
    public void testATableRecreatedUnderTheSameNameDoesNotShareTheIdentity() {
        // DROP TABLE db.t and CREATE of another db.t with the same row count: the name, the partitions
        // and the version are the same, so only the id of the table tells the two apart - a pinned entry
        // of the first table must not be applied to the second
        HboScanDescriptor first = descriptorOfTable(1000);
        HboScanDescriptor second = descriptorOfTable(2000);
        Assertions.assertEquals("db.t#ti:1000", first.getShapeIdentity());
        Assertions.assertEquals("db.t#ti:2000", second.getShapeIdentity());
        Assertions.assertNotEquals(first.getShapeIdentity(), second.getShapeIdentity());
        Assertions.assertEquals(1000, first.getTableId());
        // the id is part of the shape the fingerprint hashes, and it survives the round trip a pasted
        // struct info performs (the parse of the token keeps it)
        Assertions.assertEquals(first.getShapeIdentity(),
                HboScanDescriptor.parse(first.render()).getShapeIdentity());
        Assertions.assertEquals("db.t#ti:1000,v3,r1000", first.render());
        // a relation whose id is not known (an external table) names no id, as before
        Assertions.assertEquals("db.t", descriptor("db.t", Collections.emptyList(), 0).getShapeIdentity());
    }

    @Test
    public void testAScanOfEveryPartitionNamesNoSelection() {
        HboScanDescriptor descriptor = descriptor("db.t", Arrays.asList(1L, 2L), 2);
        Assertions.assertEquals("", descriptor.getPartitionIdentity());
        Assertions.assertEquals("db.t", descriptor.getShapeIdentity());
        // the token of such a scan is the one it had before the partitions became part of the identity
        Assertions.assertEquals("db.t,v3,r1000", descriptor.render());
    }

    @Test
    public void testTheSelectedPartitionIdsAreSortedAndRendered() {
        HboScanDescriptor descriptor = descriptor("db.t", Arrays.asList(30L, 10L), 5);
        Assertions.assertEquals("10:30", descriptor.getPartitionIdentity());
        Assertions.assertEquals("db.t#pt:10:30", descriptor.getShapeIdentity());
        Assertions.assertEquals("db.t#pt:10:30,v3,r1000,p2/5", descriptor.render());
    }

    @Test
    public void testTwoSelectionsOfTheSameSizeDoNotShareAnIdentity() {
        HboScanDescriptor first = descriptor("db.t", Collections.singletonList(10L), 2);
        HboScanDescriptor second = descriptor("db.t", Collections.singletonList(20L), 2);
        Assertions.assertNotEquals(first.getShapeIdentity(), second.getShapeIdentity());
        // both have one partition of one table: the count alone cannot tell them apart
        Assertions.assertEquals(first.getSelectedPartitions(), second.getSelectedPartitions());
        Assertions.assertEquals(first.getTotalPartitions(), second.getTotalPartitions());
        Assertions.assertEquals(first.getScanRows(), second.getScanRows());
    }

    @Test
    public void testTheOrderOfTheSelectedIdsDoesNotChangeTheIdentity() {
        Assertions.assertEquals(descriptor("db.t", Arrays.asList(10L, 30L), 5).getShapeIdentity(),
                descriptor("db.t", Arrays.asList(30L, 10L), 5).getShapeIdentity());
    }

    @Test
    public void testTheIdentitySurvivesTheTextRoundTrip() {
        HboScanDescriptor parsed = HboScanDescriptor.parse(
                descriptor("db.t", Arrays.asList(30L, 10L), 5).render());
        Assertions.assertEquals("db.t", parsed.getTable());
        Assertions.assertEquals("10:30", parsed.getPartitionIdentity());
        Assertions.assertEquals("db.t#pt:10:30", parsed.getShapeIdentity());
        Assertions.assertEquals(3, parsed.getVisibleVersion());
        Assertions.assertEquals(1000, parsed.getScanRows());
        Assertions.assertEquals(2, parsed.getSelectedPartitions());
        Assertions.assertEquals(5, parsed.getTotalPartitions());
    }

    @Test
    public void testStrippingTheBaselineKeepsTheIdentity() {
        // what HBO SET STATISTICS hashes: if the identity were stripped with the baseline, the sha256 of
        // a pasted struct info would not match the fingerprint printed for the plan
        Assertions.assertEquals("F{b = 1}(S{db.t#pt:10:30})",
                GroupStructInfo.stripScanBaseline("F{b = 1}(S{db.t#pt:10:30,v3,r1000,p2/5})"));
    }

    @Test
    public void testAnExplicitTabletSelectionIsPartOfTheIdentity() {
        // two tablet selections of one partition read different data: the partition count, the total
        // count and the row count are the same for both, so only the tablet ids can tell them apart
        HboScanDescriptor first = descriptor("db.t", Collections.singletonList(10L), 2,
                Collections.singletonList(5001L));
        HboScanDescriptor second = descriptor("db.t", Collections.singletonList(10L), 2,
                Collections.singletonList(5002L));
        Assertions.assertEquals("5001", first.getTabletIdentity());
        Assertions.assertEquals("db.t#pt:10#tb:5001", first.getShapeIdentity());
        Assertions.assertNotEquals(first.getShapeIdentity(), second.getShapeIdentity());
    }

    @Test
    public void testATabletSelectionWithoutPartitionSelection() {
        HboScanDescriptor descriptor = descriptor("db.t", Arrays.asList(1L, 2L), 2,
                Arrays.asList(5002L, 5001L));
        Assertions.assertEquals("", descriptor.getPartitionIdentity());
        Assertions.assertEquals("5001:5002", descriptor.getTabletIdentity());
        Assertions.assertEquals("db.t#tb:5001:5002", descriptor.getShapeIdentity());
    }

    @Test
    public void testBothSelectionsSurviveTheTextRoundTrip() {
        HboScanDescriptor parsed = HboScanDescriptor.parse(
                descriptor("db.t", Collections.singletonList(10L), 2,
                        Arrays.asList(5002L, 5001L)).render());
        Assertions.assertEquals("db.t", parsed.getTable());
        Assertions.assertEquals("10", parsed.getPartitionIdentity());
        Assertions.assertEquals("5001:5002", parsed.getTabletIdentity());
        Assertions.assertEquals("db.t#pt:10#tb:5001:5002", parsed.getShapeIdentity());
        Assertions.assertEquals("F{b = 1}(S{db.t#pt:10#tb:5001:5002})",
                GroupStructInfo.stripScanBaseline("F{b = 1}(S{db.t#pt:10#tb:5001:5002,v3,r1000,p1/2})"));
    }

    @Test
    public void testTheIndexIsPartOfTheIdentityWhenItIsNotTheBaseIndex() {
        // an aggregate rollup holds fewer rows than the base index: an entry recorded for one of them must
        // not be compared with the other, and the whole table query must keep the identity it had
        HboScanDescriptor base = descriptor("db.t", Arrays.asList(1L, 2L), 2, Collections.emptyList());
        Assertions.assertEquals("", base.getIndexIdentity());
        Assertions.assertEquals("db.t", base.getShapeIdentity());
        HboScanDescriptor rollup = rollupDescriptor("db.t", 4001L);
        Assertions.assertEquals("4001", rollup.getIndexIdentity());
        Assertions.assertEquals("db.t#ix:4001", rollup.getShapeIdentity());
        HboScanDescriptor otherRollup = rollupDescriptor("db.t", 4002L);
        Assertions.assertNotEquals(rollup.getShapeIdentity(), otherRollup.getShapeIdentity());
    }

    @Test
    public void testTheIndexSurvivesTheTextRoundTrip() {
        HboScanDescriptor parsed = HboScanDescriptor.parse(
                rollupDescriptor("db.t", 4001L).render());
        Assertions.assertEquals("db.t", parsed.getTable());
        Assertions.assertEquals("4001", parsed.getIndexIdentity());
        Assertions.assertEquals("db.t#ix:4001", parsed.getShapeIdentity());
        Assertions.assertEquals("F{b = 1}(S{db.t#ix:4001})",
                GroupStructInfo.stripScanBaseline("F{b = 1}(S{db.t#ix:4001,v3,r10})"));
    }

    @Test
    public void testAllMarkedPartsSurviveTogether() {
        HboScanDescriptor parsed = HboScanDescriptor.parse(
                "db.t#pt:10:11#tb:5001#ix:4001,v3,r10,p2/2");
        Assertions.assertEquals("db.t", parsed.getTable());
        Assertions.assertEquals("10:11", parsed.getPartitionIdentity());
        Assertions.assertEquals("5001", parsed.getTabletIdentity());
        Assertions.assertEquals("4001", parsed.getIndexIdentity());
        Assertions.assertEquals("db.t#pt:10:11#tb:5001#ix:4001", parsed.getShapeIdentity());
    }
}
