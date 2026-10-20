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
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.statistics.hbo.PlanStatistics;
import org.apache.doris.statistics.hbo.ScanPlanStatistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit test of the alignment between the input tables a learned entry recorded and the scans the current
 * plan binds to the same relation ids: a relation id is a position in the plan, so a query which writes the
 * key with the tables in the other order binds the positions to opposite tables, and its predicates must not
 * be read as the entry's.
 */
public class HboLearnedInputAlignmentTest {

    @Test
    public void testTheRecordedInputsAreAlignedByTableIdentity() {
        Map<RelationId, Long> current = new HashMap<>();
        current.put(new RelationId(0), 10L);
        current.put(new RelationId(1), 20L);

        // the entry was measured with the same binding: it still describes this plan
        Assertions.assertTrue(HboUtils.sameTablesByRelation(current,
                ImmutableList.of(recordedScan(0, 10), recordedScan(1, 20))));
        // the same key reused by a query which binds the two positions to the opposite tables
        Assertions.assertFalse(HboUtils.sameTablesByRelation(current,
                ImmutableList.of(recordedScan(0, 20), recordedScan(1, 10))));
        // a position the collector did not record cannot be compared and does not reject the entry
        Assertions.assertTrue(HboUtils.sameTablesByRelation(current,
                ImmutableList.of(recordedScan(2, 20), recordedScan(0, 10))));
        // a recorded position bound to the same table stays aligned although another is unknown
        Assertions.assertFalse(HboUtils.sameTablesByRelation(current,
                ImmutableList.of(recordedScan(1, 10), recordedScan(2, 20))));
    }

    /** One recorded input table of a learned entry: the relation id and the id of the table it bound. */
    private static PlanStatistics recordedScan(long relationId, long tableId) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(tableId);
        PhysicalOlapScan scan = Mockito.mock(PhysicalOlapScan.class);
        Mockito.when(scan.getRelationId()).thenReturn(new RelationId((int) relationId));
        Mockito.when(scan.getTable()).thenReturn(table);
        return new ScanPlanStatistics(1, 1000, 1000, 0, 0, 0, 0, 0, 0, 0, 0, 1,
                scan, ImmutableSet.of(), false, new PartitionInfo(), ImmutableList.of());
    }
}
