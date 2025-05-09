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

package org.apache.doris.common;

import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class UUIDv7GeneratorTest {
    /**
     * Extract timestamp from a UUIDv7.
     * UUIDv7 has the timestamp in the first 48 bits
     */
    private long extractTimestampFromUuid(UUID uuid) {
        long msb = uuid.getMostSignificantBits();
        return (msb >>> 16) & 0xFFFFFFFFFFFL; // 48 bits
    }

    /**
     * Extract random component from UUIDv7 for comparison
     */
    private String extractRandomFromUuid(UUID uuid) {
        return Long.toHexString(uuid.getLeastSignificantBits());
    }

    @Test
    public void testUniqueness() {
        final int numUuids = 10000;
        Set<String> uuidStrings = new HashSet<>();
        UUIDv7Generator generator = UUIDv7Generator.getInstance();

        for (int i = 0; i < numUuids; i++) {
            UUID uuid = generator.nextUUID();
            String uuidStr = uuid.toString();

            // Make sure we haven't seen this UUID before
            Assert.assertFalse("Generated duplicate UUID: " + uuidStr,
                    uuidStrings.contains(uuidStr));
            uuidStrings.add(uuidStr);
        }
    }

    @Test
    public void testMonotonicIncrease() throws InterruptedException {
        final int numUuids = 100;
        UUIDv7Generator generator = UUIDv7Generator.getInstance();

        UUID prevUuid = generator.nextUUID();
        long prevTimestamp = extractTimestampFromUuid(prevUuid);

        // Sleep to ensure timestamp changes
        Thread.sleep(5);

        for (int i = 0; i < numUuids; i++) {
            UUID uuid = generator.nextUUID();
            long timestamp = extractTimestampFromUuid(uuid);

            // Timestamp should be >= previous one
            Assert.assertTrue("UUID timestamp not monotonically increasing",
                    timestamp >= prevTimestamp);

            prevUuid = uuid;
            prevTimestamp = timestamp;
        }
    }

    @Test
    public void testRandomComponent() {
        final int numUuids = 1000;
        List<String> randomParts = new ArrayList<>();
        UUIDv7Generator generator = UUIDv7Generator.getInstance();

        for (int i = 0; i < numUuids; i++) {
            UUID uuid = generator.nextUUID();
            randomParts.add(extractRandomFromUuid(uuid));
        }

        Set<String> uniqueRandoms = new HashSet<>(randomParts);

        Assert.assertEquals("Random component is not unique enough between UUIDs",
                randomParts.size(), uniqueRandoms.size());
    }

    @Test
    public void testParallelGeneration() throws InterruptedException {
        final int numThreads = 8;
        final int uuidsPerThread = 1000;
        final Set<String> allUuids = Collections.newSetFromMap(new ConcurrentHashMap<>());
        final CountDownLatch latch = new CountDownLatch(numThreads);
        final UUIDv7Generator generator = UUIDv7Generator.getInstance();

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (int t = 0; t < numThreads; t++) {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < uuidsPerThread; i++) {
                        UUID uuid = generator.nextUUID();
                        String uuidStr = uuid.toString();

                        Assert.assertFalse("Generated duplicate UUID in parallel: " + uuidStr,
                                allUuids.contains(uuidStr));
                        allUuids.add(uuidStr);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        Assert.assertEquals("Parallel UUID generation produced duplicates",
                numThreads * uuidsPerThread, allUuids.size());
    }

    @Test
    public void testTimestampCorrelation() {
        UUIDv7Generator generator = UUIDv7Generator.getInstance();

        long before = Instant.now().toEpochMilli();
        UUID uuid = generator.nextUUID();
        long after = Instant.now().toEpochMilli();

        long uuidTimestamp = extractTimestampFromUuid(uuid);

        // UUID timestamp should be between 'before' and 'after'
        Assert.assertTrue("UUID timestamp should be >= system time before generation",
                uuidTimestamp >= before);
        Assert.assertTrue("UUID timestamp should be <= system time after generation",
                uuidTimestamp <= after);
    }

    @Test
    public void testVersion() {
        UUIDv7Generator generator = UUIDv7Generator.getInstance();
        UUID uuid = generator.nextUUID();

        // Extract version (should be 7)
        int version = uuid.version();
        Assert.assertEquals("UUID should be version 7", 7, version);

        // Also verify variant
        int variant = uuid.variant();
        Assert.assertEquals("UUID should have RFC 4122 variant", 2, variant);
    }
}
