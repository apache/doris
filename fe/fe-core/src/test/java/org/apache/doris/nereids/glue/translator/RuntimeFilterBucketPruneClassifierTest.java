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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class RuntimeFilterBucketPruneClassifierTest {
    @Test
    void testSingleColumnHashInSupported() {
        Column distributionColumn = new Column("dist_col", PrimitiveType.INT);
        RuntimeFilterBucketPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.IN, distributionColumn,
                new HashDistributionInfo(8, ImmutableList.of(distributionColumn)));

        Assertions.assertTrue(classification.canPruneBuckets());
    }

    @Test
    void testInOrBloomSupportedAtPlanTime() {
        Column distributionColumn = new Column("dist_col", PrimitiveType.INT);
        RuntimeFilterBucketPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.IN_OR_BLOOM, distributionColumn,
                new HashDistributionInfo(8, ImmutableList.of(distributionColumn)));

        Assertions.assertTrue(classification.canPruneBuckets());
    }

    @Test
    void testBloomRejected() {
        Column distributionColumn = new Column("dist_col", PrimitiveType.INT);
        RuntimeFilterBucketPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.BLOOM, distributionColumn,
                new HashDistributionInfo(8, ImmutableList.of(distributionColumn)));

        Assertions.assertFalse(classification.canPruneBuckets());
        Assertions.assertTrue(classification.getUnsupportedReason().contains("IN"));
    }

    @Test
    void testCompositeHashRejected() {
        Column distributionColumn = new Column("dist_col", PrimitiveType.INT);
        Column secondDistributionColumn = new Column("dist_col_2", PrimitiveType.INT);
        RuntimeFilterBucketPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.IN, distributionColumn,
                new HashDistributionInfo(8,
                        ImmutableList.of(distributionColumn, secondDistributionColumn)));

        Assertions.assertFalse(classification.canPruneBuckets());
        Assertions.assertTrue(classification.getUnsupportedReason().contains("single-column"));
    }

    @Test
    void testNonDistributionTargetRejected() {
        Column distributionColumn = new Column("dist_col", PrimitiveType.INT);
        Column targetColumn = new Column("value_col", PrimitiveType.INT);
        RuntimeFilterBucketPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.IN, targetColumn,
                new HashDistributionInfo(8, ImmutableList.of(distributionColumn)));

        Assertions.assertFalse(classification.canPruneBuckets());
        Assertions.assertTrue(classification.getUnsupportedReason().contains("distribution column"));
    }

    @Test
    void testRandomDistributionRejected() {
        Column targetColumn = new Column("dist_col", PrimitiveType.INT);
        RuntimeFilterBucketPruneClassifier.Classification classification = classify(
                TRuntimeFilterType.IN, targetColumn, new RandomDistributionInfo(8));

        Assertions.assertFalse(classification.canPruneBuckets());
        Assertions.assertTrue(classification.getUnsupportedReason().contains("not HASH"));
    }

    private RuntimeFilterBucketPruneClassifier.Classification classify(
            TRuntimeFilterType filterType, Column targetColumn,
            org.apache.doris.catalog.DistributionInfo distributionInfo) {
        SlotDescriptor slotDescriptor = new SlotDescriptor(new SlotId(1), new TupleId(1));
        slotDescriptor.setColumn(targetColumn);
        slotDescriptor.setType(targetColumn.getType());
        SlotRef targetSlot = new SlotRef(slotDescriptor);

        OlapTable table = Mockito.mock(OlapTable.class);
        Partition partition = Mockito.mock(Partition.class);
        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        Mockito.when(scanNode.getOlapTable()).thenReturn(table);
        Mockito.when(scanNode.getSelectedPartitionIds()).thenReturn(ImmutableList.of(1L));
        Mockito.when(table.getPartition(1L)).thenReturn(partition);
        Mockito.when(partition.getDistributionInfo()).thenReturn(distributionInfo);

        return RuntimeFilterBucketPruneClassifier.classify(filterType, targetSlot, scanNode);
    }
}
