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

package org.apache.doris.connector.cache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.RandomAccess;
import java.util.concurrent.atomic.AtomicInteger;

class ReflectiveObjectSizeEstimatorTest {
    @Test
    void bufferSlicesCountTheWholeBackingArrayAndRejectHiddenHeapPayload() {
        byte[] backing = new byte[1024 * 1024];
        ByteBuffer buffer = ByteBuffer.wrap(backing);
        ByteBuffer slice = buffer.duplicate().limit(1).slice();
        Assertions.assertTrue(ReflectiveObjectSizeEstimator.estimateComplete(slice) >= backing.length);
        for (ByteBuffer readOnly : new ByteBuffer[] {buffer.asReadOnlyBuffer(), slice.asReadOnlyBuffer()}) {
            MetaCacheSizeEstimate estimate = MetaCacheSizeEstimator.estimateSafely("buffer",
                    () -> MetaCacheSizeEstimate.complete(ReflectiveObjectSizeEstimator.estimateComplete(readOnly)));
            Assertions.assertFalse(estimate.isComplete());
        }
        Assertions.assertNull(JvmSizeUtils.byteBufferBackingArray(ByteBuffer.allocateDirect(16)));
    }

    @Test
    void randomAccessContainerTraversalIsBoundedBySampleSize() {
        AtomicInteger reads = new AtomicInteger();
        AbstractList<String> millionElements = new CountingList(reads);

        Assertions.assertTrue(ReflectiveObjectSizeEstimator.estimate(millionElements) > 0L);
        Assertions.assertEquals(5, reads.get());
    }

    @Test
    void typedListSamplingHasBoundedWorkAndIncludesTheTail() {
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger largestIndex = new AtomicInteger();
        AbstractList<String> millionElements = new CountingList(reads);

        long estimated = JvmSizeUtils.sampledListPayload(millionElements, 16, value -> {
            int index = Integer.parseInt(value.substring("value-".length()));
            largestIndex.accumulateAndGet(index, Math::max);
            return 1L;
        });

        Assertions.assertEquals(16, reads.get());
        Assertions.assertEquals(999_999, largestIndex.get());
        Assertions.assertEquals(1_000_000L, estimated);
    }

    @Test
    void completeAdmissionEstimateIncludesAValueOutsideTheFiveElementSample() {
        Map<String, String> skewed = new LinkedHashMap<>();
        for (int i = 0; i < 99; i++) {
            skewed.put("small-" + i, "x");
        }
        String largeTail = "x".repeat(10_000_000);
        skewed.put("large-tail", largeTail);

        long sampled = ReflectiveObjectSizeEstimator.estimate(skewed);
        MetaCacheSizeEstimate complete = MetaCacheSizeEstimators
                .<String, Map<String, String>>reflective().estimate("key", skewed);

        Assertions.assertTrue(complete.isComplete());
        Assertions.assertTrue(complete.getBytes() >= JvmSizeUtils.stringSize(largeTail));
        Assertions.assertTrue(complete.getBytes() > sampled * 100L,
                "weighted admission must not accept the fixed five-element sample as complete");
    }

    @Test
    void completeAdmissionEstimateRejectsGraphsBeyondItsVisitBudget() {
        AtomicInteger reads = new AtomicInteger();
        MetaCacheSizeEstimate estimate = MetaCacheSizeEstimator.estimateSafely(
                "bounded_complete_estimate",
                () -> MetaCacheSizeEstimate.complete(
                        ReflectiveObjectSizeEstimator.estimateComplete(new CountingList(reads))));

        Assertions.assertFalse(estimate.isComplete());
        Assertions.assertTrue(reads.get() <= 10_001,
                "complete fallback must stop when its work budget is exhausted");
    }

    @Test
    void inaccessibleJdkReferenceFieldMakesTheEstimateIncomplete() {
        MetaCacheSizeEstimate estimate = MetaCacheSizeEstimator.estimateSafely(
                "reflection_failure",
                () -> MetaCacheSizeEstimate.complete(
                        ReflectiveObjectSizeEstimator.estimate(URI.create("s3://bucket/path"))));

        Assertions.assertFalse(estimate.isComplete());
        Assertions.assertTrue(estimate.getIncompleteReason().startsWith("reflection_failure:"));
    }

    @Test
    void depthTruncationMakesTheEstimateIncomplete() {
        Node root = new Node();
        Node current = root;
        for (int i = 0; i < 25; i++) {
            current.next = new Node();
            current = current.next;
        }

        MetaCacheSizeEstimate estimate = MetaCacheSizeEstimator.estimateSafely(
                "depth_limit",
                () -> MetaCacheSizeEstimate.complete(ReflectiveObjectSizeEstimator.estimate(root)));

        Assertions.assertFalse(estimate.isComplete());
        Assertions.assertTrue(estimate.getIncompleteReason().startsWith("depth_limit:"));
    }

    private static final class CountingList extends AbstractList<String> implements RandomAccess {
        private final AtomicInteger reads;

        private CountingList(AtomicInteger reads) {
            this.reads = reads;
        }

        @Override
        public String get(int index) {
            reads.incrementAndGet();
            return "value-" + index;
        }

        @Override
        public int size() {
            return 1_000_000;
        }
    }

    private static final class Node {
        private Node next;
    }
}
