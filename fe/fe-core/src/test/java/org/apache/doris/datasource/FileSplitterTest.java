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

package org.apache.doris.datasource;

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.spi.Split;

import org.apache.hadoop.fs.BlockLocation;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class FileSplitterTest {

    private static final long MB = 1024L * 1024L;

    private static final int DEFAULT_INITIAL_SPLITS = 200;

    @Test
    public void testNonSplittableCompressedFileProducesSingleSplit() throws Exception {
        LocationPath loc = LocationPath.of("hdfs://example.com/path/file.gz");
        BlockLocation[] locations = new BlockLocation[]{new BlockLocation(null, new String[]{"h1"}, 0L, 10 * MB)};
        FileSplitter fileSplitter = new FileSplitter(32 * MB, 64 * MB, DEFAULT_INITIAL_SPLITS);
        List<Split> splits = fileSplitter.splitFile(
                loc,
                0L,
                locations,
                10 * MB,
                0L,
                true,
                Collections.emptyList(),
                FileSplit.FileSplitCreator.DEFAULT);
        Assert.assertEquals(1, splits.size());
        Split s = splits.get(0);
        Assert.assertEquals(10 * MB, ((org.apache.doris.datasource.FileSplit) s).getLength());
        // host should be preserved
        Assert.assertArrayEquals(new String[]{"h1"}, ((org.apache.doris.datasource.FileSplit) s).getHosts());
        Assert.assertEquals(DEFAULT_INITIAL_SPLITS - 1, fileSplitter.getRemainingInitialSplitNum());
    }

    @Test
    public void testEmptyBlockLocationsProducesSingleSplitAndNullHosts() throws Exception {
        LocationPath loc = LocationPath.of("hdfs://example.com/path/file");
        BlockLocation[] locations = new BlockLocation[0];
        FileSplitter fileSplitter = new FileSplitter(32 * MB, 64 * MB, DEFAULT_INITIAL_SPLITS);
        List<Split> splits = fileSplitter.splitFile(
                loc,
                0L,
                locations,
                5 * MB,
                0L,
                true,
                Collections.emptyList(),
                FileSplit.FileSplitCreator.DEFAULT);
        Assert.assertEquals(1, splits.size());
        org.apache.doris.datasource.FileSplit s = (org.apache.doris.datasource.FileSplit) splits.get(0);
        Assert.assertEquals(5 * MB, s.getLength());
        // hosts should be empty array when passing null
        Assert.assertNotNull(s.getHosts());
        Assert.assertEquals(0, s.getHosts().length);
    }

    @Test
    public void testSplittableSingleBigBlockProducesExpectedSplitsWithInitialSmallChunks() throws Exception {
        LocationPath loc = LocationPath.of("hdfs://example.com/path/bigfile");
        long length = 200 * MB;
        BlockLocation[] locations = new BlockLocation[]{new BlockLocation(null, new String[]{"h1"}, 0L, length)};
        // set maxInitialSplits to 2 to force the first two splits to be small.
        FileSplitter fileSplitter = new FileSplitter(32 * MB, 64 * MB, 2);
        List<Split> splits = fileSplitter.splitFile(
                loc,
                0L,
                locations,
                length,
                0L,
                true,
                Collections.emptyList(),
                FileSplit.FileSplitCreator.DEFAULT);

        // expect splits sizes: 32MB, 32MB, 64MB, 36MB, 36MB -> sum is 200MB
        long[] expected = new long[]{32 * MB, 32 * MB, 64 * MB, 36 * MB, 36 * MB};
        Assert.assertEquals(expected.length, splits.size());
        long sum = 0L;
        for (int i = 0; i < expected.length; i++) {
            org.apache.doris.datasource.FileSplit s = (org.apache.doris.datasource.FileSplit) splits.get(i);
            Assert.assertEquals(expected[i], s.getLength());
            sum += s.getLength();
            // ensure host preserved
            Assert.assertArrayEquals(new String[]{"h1"}, s.getHosts());
        }
        Assert.assertEquals(length, sum);
        // ensure the initial small-split counter is consumed for the two initial small splits
        Assert.assertEquals(0, fileSplitter.getRemainingInitialSplitNum());
    }

    @Test
    public void testMultiBlockSplitsAndHostPreservation() throws Exception {
        LocationPath loc = LocationPath.of("hdfs://example.com/path/twoblocks");
        long len = 96 * MB;
        BlockLocation[] locations = new BlockLocation[]{
                new BlockLocation(null, new String[]{"h1"}, 0L, 48 * MB),
                new BlockLocation(null, new String[]{"h2"}, 48 * MB, 48 * MB)
        };
        FileSplitter fileSplitter = new FileSplitter(32 * MB, 64 * MB, 0);
        List<Split> splits = fileSplitter.splitFile(
                loc,
                0L,
                locations,
                len,
                0L,
                true,
                Collections.emptyList(),
                FileSplit.FileSplitCreator.DEFAULT);
        Assert.assertEquals(2, splits.size());
        FileSplit s0 = (FileSplit) splits.get(0);
        FileSplit s1 = (FileSplit) splits.get(1);
        Assert.assertEquals(48 * MB, s0.getLength());
        Assert.assertEquals(48 * MB, s1.getLength());
        Assert.assertArrayEquals(new String[]{"h1"}, s0.getHosts());
        Assert.assertArrayEquals(new String[]{"h2"}, s1.getHosts());
    }

    @Test
    public void testZeroLengthBlockIsSkipped() throws Exception {
        LocationPath loc = LocationPath.of("hdfs://example.com/path/zeroblock");
        long length = 10 * MB;
        BlockLocation[] locations = new BlockLocation[]{
                new BlockLocation(null, new String[]{"h1"}, 0L, 0L),
                new BlockLocation(null, new String[]{"h1"}, 0L, 10 * MB)
        };
        FileSplitter fileSplitter = new FileSplitter(32 * MB, 64 * MB, DEFAULT_INITIAL_SPLITS);
        List<Split> splits = fileSplitter.splitFile(
                loc,
                0L,
                locations,
                length,
                0L,
                true,
                Collections.emptyList(),
                FileSplit.FileSplitCreator.DEFAULT);
        Assert.assertEquals(1, splits.size());
        FileSplit s = (FileSplit) splits.get(0);
        Assert.assertEquals(10 * MB, s.getLength());
        Assert.assertArrayEquals(new String[]{"h1"}, s.getHosts());
    }

    @Test
    public void testNonSplittableFlagDecrementsCounter() throws Exception {
        LocationPath loc = LocationPath.of("hdfs://example.com/path/file.gz");
        BlockLocation[] locations = new BlockLocation[]{new BlockLocation(null, new String[]{"h1"}, 0L, 10 * MB)};
        FileSplitter fileSplitter = new FileSplitter(32 * MB, 64 * MB, 2);
        List<Split> splits = fileSplitter.splitFile(
                loc,
                0L,
                locations,
                10 * MB,
                0L,
                false,
                Collections.emptyList(),
                FileSplit.FileSplitCreator.DEFAULT);
        Assert.assertEquals(1, splits.size());
    }

    @Test
    public void testNullRemainingInitialSplitIsAllowed() throws Exception {
        LocationPath loc = LocationPath.of("hdfs://example.com/path/somefile");
        BlockLocation[] locations = new BlockLocation[]{new BlockLocation(null, new String[]{"h1"}, 0L, 10 * MB)};
        FileSplitter fileSplitter = new FileSplitter(32 * MB, 64 * MB, DEFAULT_INITIAL_SPLITS);
        List<Split> splits = fileSplitter.splitFile(
                loc,
                0L,
                locations,
                10 * MB,
                0L,
                true,
                Collections.emptyList(),
                FileSplit.FileSplitCreator.DEFAULT);
        Assert.assertEquals(1, splits.size());
    }

    @Test
    public void testSmallFileNoSplit() throws Exception {
        LocationPath loc = LocationPath.of("hdfs://example.com/path/small");
        BlockLocation[] locations = new BlockLocation[]{new BlockLocation(null, new String[]{"h1"}, 0L, 2 * MB)};
        FileSplitter fileSplitter = new FileSplitter(32 * MB, 64 * MB, DEFAULT_INITIAL_SPLITS);
        List<Split> splits = fileSplitter.splitFile(
                loc,
                0L,
                locations,
                2 * MB,
                0L,
                true,
                Collections.emptyList(),
                FileSplit.FileSplitCreator.DEFAULT);
        Assert.assertEquals(1, splits.size());
        FileSplit s = (FileSplit) splits.get(0);
        Assert.assertEquals(2 * MB, s.getLength());
    }
}
