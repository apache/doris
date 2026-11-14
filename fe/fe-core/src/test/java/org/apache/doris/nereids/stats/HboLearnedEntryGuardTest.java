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
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.statistics.hbo.PlanStatistics;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatisticsEntry;
import org.apache.doris.statistics.hbo.ScanPlanStatistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Unit test of the guard which judges a learned entry by the rows the input tables of its sub tree held.
 * The publish path records one scan statistics per scan which feeds the node, so the guard applies to an
 * entry of a filter (and of a join or an aggregation) as well as to the entry of a scan node: none of
 * them may keep the row count of a run whose inputs have since grown by orders of magnitude.
 */
public class HboLearnedEntryGuardTest {

    @Test
    public void testALearnedEntryOfAFilterIsJudgedByTheScanBelowIt() {
        GroupStructInfo filter = filterOverScanOfT1();
        Assertions.assertEquals(1, filter.getScans().size());
        // the run the entry was measured in read 1000 rows of t1, while the query now describes a scan of
        // no rows at all: the entry describes a state which is gone
        Assertions.assertTrue(HboStatsCalculator.learnedEntryFreshness(entryWithRecordedRows(1000), filter)
                .isStale(), "a learned entry of a filter survives a large change of its input");
        // the sentinel of the publish path (the runtime counter of a scan operator was not read) keeps
        // the entry applicable, like every other number which cannot be compared
        Assertions.assertFalse(HboStatsCalculator.learnedEntryFreshness(entryWithRecordedRows(0), filter)
                .isStale());
        // an entry which was injected without input table statistics (HBO SET LEARNED STATISTICS) has
        // nothing to compare and stays applicable
        Assertions.assertFalse(HboStatsCalculator.learnedEntryFreshness(
                new RecentRunsPlanStatisticsEntry(PlanStatistics.EMPTY, ImmutableList.of()), filter)
                .isStale());
    }

    private static GroupStructInfo filterOverScanOfT1() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        SlotReference slot = (SlotReference) scan.getOutput().get(0);
        return GroupStructInfo.describeGroup(new Memo(null,
                new LogicalFilter<>(ImmutableSet.of(new IsNull(slot)), scan)).getRoot());
    }

    /** A learned entry whose one recorded input table is t1 and whose run read {@code rows} rows of it. */
    private static RecentRunsPlanStatisticsEntry entryWithRecordedRows(long rows) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getName()).thenReturn("t1");
        PhysicalOlapScan scan = Mockito.mock(PhysicalOlapScan.class);
        Mockito.when(scan.getTable()).thenReturn(table);
        ScanPlanStatistics recorded = new ScanPlanStatistics(1, rows, rows, 0, 0, 0, 0, 0, 0, 0, 0, 1,
                scan, ImmutableSet.of(), false, new PartitionInfo(), ImmutableList.of());
        return new RecentRunsPlanStatisticsEntry(PlanStatistics.EMPTY, ImmutableList.of(recorded));
    }
}
