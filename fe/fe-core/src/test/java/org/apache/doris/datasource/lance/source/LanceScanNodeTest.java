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

package org.apache.doris.datasource.lance.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.datasource.lance.LanceTableMetadata;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;

import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LanceScanNodeTest {

    @Test
    public void testFragmentRowsDetermineSplitWeights() throws Exception {
        LanceTableMetadata metadata = new LanceTableMetadata(
                "s3://bucket/table.lance",
                42,
                new Schema(Collections.emptyList()),
                Arrays.asList(
                        new LanceTableMetadata.LanceFragmentInfo(7, 1000, 1000),
                        new LanceTableMetadata.LanceFragmentInfo(11, 250, 250),
                        new LanceTableMetadata.LanceFragmentInfo(13, 0, 0)),
                Collections.emptyMap());
        LanceScanNode node = newNode();
        setMetadata(node, metadata);

        List<Split> splits = node.getSplits(2);

        Assert.assertEquals(3, splits.size());
        assertSplit(splits.get(0), 7, 1000, 100);
        assertSplit(splits.get(1), 11, 1000, 25);
        assertSplit(splits.get(2), 13, 1000, 1);
    }

    @Test
    public void testDeletionHeavyFragmentKeepsPhysicalScanWeight() throws Exception {
        // Both fragments read 1000 physical rows, but one has 990 tombstones so its logical
        // row count is only 10. The pinned BE legacy reader still scans all physical rows, so
        // both fragments must keep the standard weight instead of underweighting the
        // tombstone-heavy one to the minimum.
        LanceTableMetadata metadata = new LanceTableMetadata(
                "s3://bucket/table.lance",
                42,
                new Schema(Collections.emptyList()),
                Arrays.asList(
                        new LanceTableMetadata.LanceFragmentInfo(7, 1000, 1000),
                        new LanceTableMetadata.LanceFragmentInfo(11, 10, 1000)),
                Collections.emptyMap());
        LanceScanNode node = newNode();
        setMetadata(node, metadata);

        List<Split> splits = node.getSplits(2);

        Assert.assertEquals(2, splits.size());
        assertSplit(splits.get(0), 7, 1000, 100);
        assertSplit(splits.get(1), 11, 1000, 100);
    }

    private static LanceScanNode newNode() {
        return new LanceScanNode(
                new PlanNodeId(0),
                new TupleDescriptor(new TupleId(0)),
                false,
                new SessionVariable(),
                ScanContext.EMPTY);
    }

    private static void setMetadata(LanceScanNode node, LanceTableMetadata metadata) throws Exception {
        java.lang.reflect.Field metadataField = LanceScanNode.class.getDeclaredField("plannedMetadata");
        metadataField.setAccessible(true);
        metadataField.set(node, metadata);
    }

    private static void assertSplit(Split split, long fragmentId, long targetRows, long weight) {
        LanceSplit lanceSplit = (LanceSplit) split;
        Assert.assertEquals(fragmentId, lanceSplit.getFragmentId());
        Assert.assertEquals(targetRows, lanceSplit.getTargetSplitSize().longValue());
        Assert.assertEquals(weight, lanceSplit.getSplitWeight().getRawValue());
    }
}
