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

import org.apache.doris.datasource.lance.metadata.LanceFragmentInfo;
import org.apache.doris.datasource.lance.metadata.LancePlannedDataset;
import org.apache.doris.spi.Split;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LanceSplitBuilderTest {

    @Test
    public void testEachSplitCarriesItsOwnDataset() {
        List<Split> splits = LanceSplitBuilder.buildFragmentSplits(Arrays.asList(
                dataset("s3://bucket/a", 7, 0L, 1L), dataset("s3://bucket/b", 9, 0L)), 1);
        Assert.assertEquals(3, splits.size());
        assertSplit(splits.get(0), "s3://bucket/a", 7, 0L);
        assertSplit(splits.get(1), "s3://bucket/a", 7, 1L);
        assertSplit(splits.get(2), "s3://bucket/b", 9, 0L);
    }

    @Test
    public void testDatasetWithoutFragmentsContributesNoSplits() {
        List<Split> splits = LanceSplitBuilder.buildFragmentSplits(Arrays.asList(
                dataset("s3://bucket/a", 1, 0L), dataset("s3://bucket/empty", 2)), 1);
        Assert.assertEquals(1, splits.size());
        assertSplit(splits.get(0), "s3://bucket/a", 1, 0L);
    }

    @Test
    public void testWeightsAreNormalizedAcrossDatasets() {
        List<Split> splits = LanceSplitBuilder.buildFragmentSplits(Arrays.asList(
                weighted("s3://bucket/a", 1, 1000L), weighted("s3://bucket/b", 2, 10L)), 1);
        Assert.assertEquals(2, splits.size());
        Assert.assertEquals(Long.valueOf(1000L), ((LanceSplit) splits.get(0)).targetSplitSize);
        Assert.assertEquals(Long.valueOf(1000L), ((LanceSplit) splits.get(1)).targetSplitSize);
    }

    private static void assertSplit(Split split, String uri, long version, long fragmentId) {
        LanceSplit lanceSplit = (LanceSplit) split;
        Assert.assertEquals(uri, lanceSplit.getDatasetUri());
        Assert.assertEquals(version, lanceSplit.getVersion());
        Assert.assertEquals(Collections.singletonList(fragmentId), lanceSplit.getFragmentIds());
    }

    private static LancePlannedDataset dataset(String uri, long version, long... fragmentIds) {
        List<LanceFragmentInfo> fragments = new ArrayList<>();
        for (long id : fragmentIds) {
            fragments.add(new LanceFragmentInfo(id, 10, 100));
        }
        return new LancePlannedDataset(uri, version, fragments);
    }

    private static LancePlannedDataset weighted(String uri, long version, long physicalRows) {
        return new LancePlannedDataset(uri, version,
                Collections.singletonList(new LanceFragmentInfo(0L, 10, physicalRows)));
    }
}
