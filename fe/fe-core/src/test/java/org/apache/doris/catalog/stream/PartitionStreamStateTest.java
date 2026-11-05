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

package org.apache.doris.catalog.stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The rules a stream read is estimated with. Over-estimating only makes the optimizer plan the
 * read differently; under-estimating lets a large read become the build side of a broadcast join,
 * which is what these tests guard.
 */
public class PartitionStreamStateTest {

    private static final long UNKNOWN = PartitionStreamState.UNKNOWN_ROW_COUNT;

    @Test
    public void testAReadOfAnEmptyRangeReturnsOneRow() {
        // Nothing was committed after the offset. The TSOs say so even when the row counts are
        // stale, which is why they decide the empty case rather than a comparison of the counters.
        Assertions.assertEquals(1, PartitionStreamState.estimateChangelogRead(100, 100, 500, 500, 400, 900));
        Assertions.assertEquals(1, PartitionStreamState.estimateChangelogRead(100, 90, 500, 500, 400, 900));
    }

    @Test
    public void testTheDeltaIsTheDifferenceOfTheCounters() {
        // 300 change rows were added after the offset was advanced.
        Assertions.assertEquals(300, PartitionStreamState.estimateChangelogRead(100, 200, 500, 800, 400, 1200));
    }

    @Test
    public void testWithoutABaselineTheWholeChangelogBoundsTheRead() {
        // No state was recorded for the partition: every row the read may return is still in the
        // changelog, so its row count bounds the read.
        Assertions.assertEquals(800, PartitionStreamState.estimateChangelogRead(100, 200, UNKNOWN, 800, 400, 1200));
    }

    @Test
    public void testCountersGoingBackwardsTakeTheCurrentCount() {
        // The counters moved backwards, for instance because the changelog was reclaimed or the
        // partition was recreated. The rows the read may return are in the changelog now.
        Assertions.assertEquals(400, PartitionStreamState.estimateChangelogRead(100, 200, 500, 400, 300, 900));
        // Taking the absolute difference or the smaller of the differences would under-estimate.
        Assertions.assertTrue(PartitionStreamState.estimateChangelogRead(100, 200, 500, 400, 300, 900) < 500);
    }

    @Test
    public void testUnreportedCountsFallBackToValuesOfTheRightOrderOfMagnitude() {
        // The partition was just written and the backend has not reported it yet: the changelog
        // count is 0 although the TSO says there are changes. A base row can be changed many
        // times, so the base row count is not a strict bound, but it keeps the read from looking
        // empty.
        Assertions.assertEquals(1000, PartitionStreamState.estimateChangelogRead(100, 200, 0, 0, 1000, 5000));
        // Neither count is available: the table row count is the last value of a usable size.
        Assertions.assertEquals(5000, PartitionStreamState.estimateChangelogRead(100, 200, 0, 0, 0, 5000));
        // Nothing to fall back to at all while the range holds changes: a relation too large to be
        // broadcast, not one row, which the planner would treat as a small one.
        Assertions.assertEquals(PartitionStreamState.UNKNOWN_NONEMPTY_ROW_COUNT,
                PartitionStreamState.estimateChangelogRead(100, 200, 0, 0, 0, UNKNOWN));
    }

    @Test
    public void testAKnownNonemptyRangeWithoutCountersIsNotEstimatedAtOneRow() {
        // A load wrote the partition and no counter exists yet: the changelog count is 0, the base
        // count is 0 and the table has no record either. The read returns the whole range, so it
        // must not be reported as a one-row scan -- the optimizer would broadcast it.
        Assertions.assertEquals(PartitionStreamState.UNKNOWN_NONEMPTY_ROW_COUNT,
                PartitionStreamState.estimateChangelogRead(100, 200, 0, 0, 0, UNKNOWN));
        // Had the range been empty, the answer stays one row.
        Assertions.assertEquals(1, PartitionStreamState.estimateChangelogRead(100, 100, 0, 0, 0, UNKNOWN));
    }

    @Test
    public void testAnUnknownRangeIsEstimatedAsIfItHadChanges() {
        // The partition has no offset range in this read, so the empty-range shortcut cannot apply
        // and the estimate must not collapse to one row.
        Assertions.assertEquals(800, PartitionStreamState.estimateChangelogRead(UNKNOWN, UNKNOWN, 500, 800, 400, 1200));
    }

    @Test
    public void testADuplicateKeySnapshotUsesTheLargerOfTheRecordedAndCurrentRows() {
        // An append-only image only grows, so the current rows bound it and adding the change rows
        // would only make the estimate worse.
        Assertions.assertEquals(700, PartitionStreamState.estimateSnapshotRead(true, 500, 700, 300, 1200));
        // The recorded image outgrows the current rows when the recorded one was taken after the
        // partition was truncated, for instance; take the larger.
        Assertions.assertEquals(900, PartitionStreamState.estimateSnapshotRead(true, 900, 700, 300, 1200));
    }

    @Test
    public void testAUniqueKeySnapshotAddsBackTheRowsDeletedAfterTheOffset() {
        // The image holds the rows deleted after the offset as well; the change rows bound how many
        // those can be.
        Assertions.assertEquals(1000, PartitionStreamState.estimateSnapshotRead(false, 800, 700, 300, 1200));
        // Without a delta it still takes the recorded image when that is larger.
        Assertions.assertEquals(800, PartitionStreamState.estimateSnapshotRead(false, 800, 700, UNKNOWN, 1200));
        // Nothing recorded: the current rows are the best available value.
        Assertions.assertEquals(700, PartitionStreamState.estimateSnapshotRead(false, UNKNOWN, 700, UNKNOWN, 1200));
    }

    @Test
    public void testASnapshotOfAnEmptyPartitionIsOneRow() {
        Assertions.assertEquals(1, PartitionStreamState.estimateSnapshotRead(true, 0, 0, UNKNOWN, UNKNOWN));
    }
}
