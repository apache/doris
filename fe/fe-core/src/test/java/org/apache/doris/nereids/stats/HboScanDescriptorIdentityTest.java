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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
        return new HboScanDescriptor(table, 3, 1000, HboScanDescriptor.RowsKind.MEASURED,
                selectedIds.size(), totalPartitions,
                HboScanDescriptor.partitionIdentityOf(selectedIds, totalPartitions));
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
}
