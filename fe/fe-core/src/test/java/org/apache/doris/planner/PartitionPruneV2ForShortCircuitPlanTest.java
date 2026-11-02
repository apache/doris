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

package org.apache.doris.planner;

import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.catalog.Type;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class PartitionPruneV2ForShortCircuitPlanTest {
    private final List<Column> columns = ImmutableList.of(new Column("d", Type.INT));

    private RangePartitionItem range(int lower, int upper) throws Exception {
        PartitionKey low = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue(Integer.toString(lower))), columns);
        PartitionKey high = PartitionKey.createPartitionKey(
                ImmutableList.of(new PartitionValue(Integer.toString(upper))), columns);
        return new RangePartitionItem(Range.closedOpen(low, high));
    }

    @Test
    void testTopologyChangesInvalidateWarmRangeCache() throws Exception {
        RangePartitionInfo info = new RangePartitionInfo(columns);
        PartitionPruneV2ForShortCircuitPlan pruner = new PartitionPruneV2ForShortCircuitPlan();
        info.setItem(1L, false, range(0, 10));
        Assertions.assertTrue(pruner.update(info.getIdToItem(false), 1L));
        Assertions.assertEquals(ImmutableList.of(1L), pruner.prune(new IntLiteral(0), new IntLiteral(0)));
        Assertions.assertTrue(pruner.prune(new IntLiteral(10), new IntLiteral(10)).isEmpty());
        Assertions.assertFalse(pruner.update(info.getIdToItem(false), 1L));

        info.setItem(2L, false, range(10, 20));
        Assertions.assertTrue(pruner.update(info.getIdToItem(false), 2L));
        Assertions.assertEquals(ImmutableList.of(2L), pruner.prune(new IntLiteral(10), new IntLiteral(10)));
        info.dropPartition(1L);
        Assertions.assertTrue(pruner.update(info.getIdToItem(false), 3L));
        Assertions.assertTrue(pruner.prune(new IntLiteral(1), new IntLiteral(1)).isEmpty());

        info.setItem(1L, false, range(0, 10));
        Assertions.assertTrue(pruner.update(info.getIdToItem(false), 4L));
        Assertions.assertEquals(ImmutableList.of(1L), pruner.prune(new IntLiteral(1), new IntLiteral(1)));
        info.setItem(2L, false, range(30, 40));
        Assertions.assertTrue(pruner.update(info.getIdToItem(false), 5L));
        Assertions.assertTrue(pruner.prune(new IntLiteral(11), new IntLiteral(11)).isEmpty());
        Assertions.assertEquals(ImmutableList.of(2L), pruner.prune(new IntLiteral(31), new IntLiteral(31)));

        info.setItem(3L, true, range(20, 30));
        Assertions.assertFalse(pruner.update(info.getIdToItem(false), 5L));
        info.moveFromTempToFormal(3L);
        Assertions.assertTrue(pruner.update(info.getIdToItem(false), 6L));
        Assertions.assertEquals(ImmutableList.of(3L), pruner.prune(new IntLiteral(21), new IntLiteral(21)));
        Assertions.assertFalse(pruner.update(info.getIdToItem(false), 6L));
    }

    @Test
    void testNewPartitionMapWithSameRevisionInvalidatesCache() throws Exception {
        RangePartitionInfo original = new RangePartitionInfo(columns);
        original.setItem(1L, false, range(0, 10));
        RangePartitionInfo restored = new RangePartitionInfo(columns);
        restored.setItem(9L, false, range(0, 10));
        PartitionPruneV2ForShortCircuitPlan pruner = new PartitionPruneV2ForShortCircuitPlan();
        pruner.update(original.getIdToItem(false), 1L);
        Assertions.assertTrue(pruner.update(restored.getIdToItem(false), 1L));
        Assertions.assertEquals(ImmutableList.of(9L), pruner.prune(new IntLiteral(1), new IntLiteral(1)));
    }
}
