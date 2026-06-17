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

package org.apache.doris.maxcompute;

import org.junit.Assert;
import org.junit.Test;

public class MaxComputeJniWriterTest {
    @Test
    public void testFindPartialRowRangeFillsRemainingBlock() throws Exception {
        MaxComputeJniWriter.RowRange range = MaxComputeJniWriter.findPartialRowRange(
                0, 4, 60L, 100L, prefixEstimator(10L, 20L, 30L, 40L));

        Assert.assertFalse(range.rotateBeforeWrite);
        Assert.assertEquals(2, range.rowEnd);
        Assert.assertEquals(30L, range.bytes);
    }

    @Test
    public void testFindPartialRowRangeRotatesWhenNoRowFitsNonEmptyBlock() throws Exception {
        MaxComputeJniWriter.RowRange range = MaxComputeJniWriter.findPartialRowRange(
                0, 3, 95L, 100L, prefixEstimator(10L, 20L, 30L));

        Assert.assertTrue(range.rotateBeforeWrite);
    }

    @Test
    public void testFindPartialRowRangeKeepsSingleOversizeFallbackOnEmptyBlock() throws Exception {
        MaxComputeJniWriter.RowRange range = MaxComputeJniWriter.findPartialRowRange(
                0, 3, 0L, 5L, prefixEstimator(10L, 20L, 30L));

        Assert.assertFalse(range.rotateBeforeWrite);
        Assert.assertEquals(1, range.rowEnd);
        Assert.assertEquals(10L, range.bytes);
    }

    @Test
    public void testFindPartialRowRangeUsesRowStartOffset() throws Exception {
        MaxComputeJniWriter.RowRange range = MaxComputeJniWriter.findPartialRowRange(
                1, 4, 50L, 100L, prefixEstimator(999L, 30L, 30L, 50L));

        Assert.assertFalse(range.rotateBeforeWrite);
        Assert.assertEquals(2, range.rowEnd);
        Assert.assertEquals(30L, range.bytes);
    }

    private static MaxComputeJniWriter.RowRangeByteEstimator prefixEstimator(long... rowBytes) {
        return (rowStart, rowEnd) -> {
            long bytes = 0L;
            for (int i = rowStart; i < rowEnd; i++) {
                bytes += rowBytes[i];
            }
            return bytes;
        };
    }
}
