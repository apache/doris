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

package org.apache.doris.common.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class UUIDUtilTest {
    @Test
    public void testUniqueness() {
        final int numUuids = 10000;
        Set<String> uuidStrings = new HashSet<>();

        for (int i = 0; i < numUuids; i++) {
            UUID uuid = UUIDUtil.genUUID();
            String uuidStr = uuid.toString();

            // Make sure we haven't seen this UUID before
            Assert.assertTrue("Generated duplicate UUID: " + uuidStr,
                    uuidStrings.add(uuidStr));
        }
    }

    @Test
    public void testParallelGeneration() throws InterruptedException {
        final int numThreads = 8;
        final int uuidsPerThread = 1000;
        final Set<String> allUuids = Collections.newSetFromMap(new ConcurrentHashMap<>());
        final CountDownLatch latch = new CountDownLatch(numThreads);

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (int t = 0; t < numThreads; t++) {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < uuidsPerThread; i++) {
                        UUID uuid = UUIDUtil.genUUID();
                        String uuidStr = uuid.toString();

                        Assert.assertTrue("Generated duplicate UUID in parallel: " + uuidStr,
                                allUuids.add(uuidStr));
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
    public void testVersion() {
        UUID uuid = UUIDUtil.genUUID();

        // Extract version (should be 1)
        int version = uuid.version();
        Assert.assertEquals("UUID should be version 4", 4, version);

        // Also verify variant
        int variant = uuid.variant();
        Assert.assertEquals("UUID should have RFC 4122 variant", 2, variant);
    }
}
