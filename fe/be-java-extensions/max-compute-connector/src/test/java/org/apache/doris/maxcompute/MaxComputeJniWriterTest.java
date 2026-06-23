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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class MaxComputeJniWriterTest {
    @Test
    public void testPrefixBufferBytesMeasuresLeadingRowsWithoutRebuild() {
        try (BufferAllocator allocator = new RootAllocator();
                IntVector vec = new IntVector("c", allocator)) {
            vec.allocateNew(8);
            for (int i = 0; i < 8; i++) {
                vec.set(i, i);
            }
            vec.setValueCount(8);
            try (VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vec))) {
                // The whole-root measurement must match estimateBatchPayloadBytes...
                Assert.assertEquals(MaxComputeJniWriter.estimateBatchPayloadBytes(root),
                        MaxComputeJniWriter.prefixBufferBytes(root, root.getRowCount()));
                // ...and a leading prefix must be strictly smaller, computed from the
                // already-built buffers (no rebuild).
                Assert.assertTrue(MaxComputeJniWriter.prefixBufferBytes(root, 4)
                        < MaxComputeJniWriter.prefixBufferBytes(root, 8));
            }
        }
    }

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

    @Test
    public void testBoundedProbeRowCountBootstrapsWithSingleRow() {
        // No per-row estimate yet: bootstrap by measuring a single row's real Arrow payload,
        // so an oversized input is never copied whole and we never guess a row count.
        int probeRows = MaxComputeJniWriter.boundedProbeRowCount(0L, 64L * 1024 * 1024, 1_000_000);

        Assert.assertEquals(1, probeRows);
    }

    @Test
    public void testBoundedProbeRowCountTargetsOneBlockAfterMeasurement() {
        // 1 KiB/row against a 64 MiB block => ~65536 rows fill one block.
        int probeRows = MaxComputeJniWriter.boundedProbeRowCount(1024L, 64L * 1024 * 1024, 1_000_000);

        Assert.assertEquals(65536, probeRows);
        Assert.assertTrue(probeRows < 1_000_000);
    }

    @Test
    public void testBoundedProbeRowCountReturnsRemainingWhenItFitsCap() {
        // A small input that comfortably fits one block is probed in one shot.
        int probeRows = MaxComputeJniWriter.boundedProbeRowCount(1024L, 64L * 1024 * 1024, 4096);

        Assert.assertEquals(4096, probeRows);
    }

    @Test
    public void testBoundedProbeRowCountProbesSingleRowWhenRowExceedsBlock() {
        // A single row larger than a whole block must still make progress (never 0 rows).
        int probeRows = MaxComputeJniWriter.boundedProbeRowCount(
                128L * 1024 * 1024, 64L * 1024 * 1024, 1_000_000);

        Assert.assertEquals(1, probeRows);
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
