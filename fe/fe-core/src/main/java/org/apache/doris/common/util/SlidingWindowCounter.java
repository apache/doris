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

import java.util.concurrent.atomic.AtomicLongArray;

public class SlidingWindowCounter {
    private final int windowSizeInSeconds;
    private final int numberOfBuckets;
    private final AtomicLongArray buckets;
    private final AtomicLongArray bucketTimestamps;

    public SlidingWindowCounter(int windowSizeInSeconds) {
        this.windowSizeInSeconds = windowSizeInSeconds;
        this.numberOfBuckets = windowSizeInSeconds; // Each bucket represents 1 second
        this.buckets = new AtomicLongArray(numberOfBuckets);
        this.bucketTimestamps = new AtomicLongArray(numberOfBuckets);
    }

    private int getCurrentBucketIndex() {
        long currentTime = System.currentTimeMillis() / 1000; // Current time in seconds
        return (int) (currentTime % numberOfBuckets);
    }

    private void updateCurrentBucket() {
        long currentTime = System.currentTimeMillis() / 1000; // Current time in seconds
        int currentBucketIndex = getCurrentBucketIndex();
        long bucketTimestamp = bucketTimestamps.get(currentBucketIndex);

        if (currentTime - bucketTimestamp >= 1) {
            buckets.set(currentBucketIndex, 0);
            bucketTimestamps.set(currentBucketIndex, currentTime);
        }
    }

    public void add(long value) {
        updateCurrentBucket();
        int bucketIndex = getCurrentBucketIndex();
        buckets.addAndGet(bucketIndex, value);
    }

    public long get() {
        updateCurrentBucket();
        long currentTime = System.currentTimeMillis() / 1000; // Current time in seconds
        long count = 0;

        for (int i = 0; i < numberOfBuckets; i++) {
            if (currentTime - bucketTimestamps.get(i) < windowSizeInSeconds) {
                count += buckets.get(i);
            }
        }
        return count;
    }

    public String toString() {
        return String.valueOf(get());
    }
}
