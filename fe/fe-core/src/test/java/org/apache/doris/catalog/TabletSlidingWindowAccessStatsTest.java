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

package org.apache.doris.catalog;

import org.apache.doris.common.Config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;


/**
 * Unit tests for TabletSlidingWindowAccessStatsTest sliding window logic.
 *
 * <p>We use reflection to test the private static inner class SlidingWindowCounter,
 * focusing on time-window counting, bucket expiration, cleanup and cache invalidation.
 */
public class TabletSlidingWindowAccessStatsTest {

    private static class CounterHarness {
        private final Object counter;
        private final Method add;
        private final Method getCount;
        private final Method cleanup;
        private final Method hasRecentActivity;
        private final Method getLastAccessTime;

        CounterHarness(int numBuckets) throws Exception {
            Class<?> clazz = Class.forName(
                    "org.apache.doris.catalog.TabletSlidingWindowAccessStats$SlidingWindowCounter");
            Constructor<?> ctor = clazz.getDeclaredConstructor(int.class);
            ctor.setAccessible(true);
            this.counter = ctor.newInstance(numBuckets);

            this.add = clazz.getDeclaredMethod("add", long.class, long.class, int.class);
            this.add.setAccessible(true);
            this.getCount = clazz.getDeclaredMethod("getCount", long.class, long.class, long.class, int.class);
            this.getCount.setAccessible(true);
            this.cleanup = clazz.getDeclaredMethod("cleanup", long.class, long.class);
            this.cleanup.setAccessible(true);
            this.hasRecentActivity = clazz.getDeclaredMethod("hasRecentActivity", long.class, long.class);
            this.hasRecentActivity.setAccessible(true);
            this.getLastAccessTime = clazz.getDeclaredMethod("getLastAccessTime");
            this.getLastAccessTime.setAccessible(true);
        }

        void add(long currentTimeMs, long bucketSizeMs, int numBuckets) throws Exception {
            add.invoke(counter, currentTimeMs, bucketSizeMs, numBuckets);
        }

        long getCount(long currentTimeMs, long timeWindowMs, long bucketSizeMs, int numBuckets) throws Exception {
            Object v = getCount.invoke(counter, currentTimeMs, timeWindowMs, bucketSizeMs, numBuckets);
            return (long) v;
        }

        void cleanup(long currentTimeMs, long timeWindowMs) throws Exception {
            cleanup.invoke(counter, currentTimeMs, timeWindowMs);
        }

        boolean hasRecentActivity(long currentTimeMs, long timeWindowMs) throws Exception {
            Object v = hasRecentActivity.invoke(counter, currentTimeMs, timeWindowMs);
            return (boolean) v;
        }

        long getLastAccessTime() throws Exception {
            Object v = getLastAccessTime.invoke(counter);
            return (long) v;
        }
    }

    @Test
    public void testAddAndGetCountWithinWindow() throws Exception {
        int numBuckets = 5;
        long bucketSizeMs = 1000L;
        long timeWindowMs = 5000L;
        long base = 10_000L; // avoid 0 (bucket timestamp 0 is treated as "unset" in getCount)

        CounterHarness c = new CounterHarness(numBuckets);
        c.add(base, bucketSizeMs, numBuckets);
        c.add(base + 1000L, bucketSizeMs, numBuckets);
        c.add(base + 2000L, bucketSizeMs, numBuckets);

        Assertions.assertEquals(base + 2000L, c.getLastAccessTime());
        Assertions.assertTrue(c.hasRecentActivity(base + 2000L, timeWindowMs));

        long cnt = c.getCount(base + 2000L, timeWindowMs, bucketSizeMs, numBuckets);
        Assertions.assertEquals(3L, cnt);

        // Cached result (same timestamp) should be consistent
        long cnt2 = c.getCount(base + 2000L, timeWindowMs, bucketSizeMs, numBuckets);
        Assertions.assertEquals(3L, cnt2);

        // add() should invalidate cachedTotalCount
        c.add(base + 2500L, bucketSizeMs, numBuckets);
        long cnt3 = c.getCount(base + 2500L, timeWindowMs, bucketSizeMs, numBuckets);
        Assertions.assertEquals(4L, cnt3);
    }

    @Test
    public void testWindowExpiresOldBuckets() throws Exception {
        int numBuckets = 5;
        long bucketSizeMs = 1000L;
        long timeWindowMs = 5000L;
        long base = 20_000L;

        CounterHarness c = new CounterHarness(numBuckets);
        c.add(base, bucketSizeMs, numBuckets);
        c.add(base + 1000L, bucketSizeMs, numBuckets);
        c.add(base + 2000L, bucketSizeMs, numBuckets);

        // At base + 6000, window start is base + 1000 => count should be 2 (1000 and 2000 buckets)
        long cnt1 = c.getCount(base + 6000L, timeWindowMs, bucketSizeMs, numBuckets);
        Assertions.assertEquals(2L, cnt1);

        // At base + 7000, window start is base + 2000 => count should be 1 (2000 bucket)
        long cnt2 = c.getCount(base + 7000L, timeWindowMs, bucketSizeMs, numBuckets);
        Assertions.assertEquals(1L, cnt2);

        // recent activity should still be true (lastAccessTime=base+2000 within 5000ms of base+7000? exactly 5000ms)
        Assertions.assertTrue(c.hasRecentActivity(base + 7000L, timeWindowMs));
    }

    @Test
    public void testCleanupRemovesExpiredBuckets() throws Exception {
        int numBuckets = 5;
        long bucketSizeMs = 1000L;
        long timeWindowMs = 5000L;
        long base = 30_000L;

        CounterHarness c = new CounterHarness(numBuckets);
        c.add(base, bucketSizeMs, numBuckets);
        c.add(base + 1000L, bucketSizeMs, numBuckets);
        c.add(base + 2000L, bucketSizeMs, numBuckets);

        // cleanup at base + 7000: windowStart=base+2000, so base/base+1000 buckets should be cleared
        c.cleanup(base + 7000L, timeWindowMs);
        long cnt = c.getCount(base + 7000L, timeWindowMs, bucketSizeMs, numBuckets);
        Assertions.assertEquals(1L, cnt);
    }

    @Test
    public void testBucketWrapAroundResetsExpiredBucket() throws Exception {
        int numBuckets = 5;
        long bucketSizeMs = 1000L;
        long timeWindowMs = 5000L;
        long base = 40_000L;

        CounterHarness c = new CounterHarness(numBuckets);
        // bucket index wraps every numBuckets * bucketSizeMs (5s here)
        c.add(base, bucketSizeMs, numBuckets);            // idx = 0
        c.add(base + 5000L, bucketSizeMs, numBuckets);    // idx = 0 again, should reset old bucket

        // With current implementation, the old "base" bucket is overwritten, so only 1 remains.
        long cnt = c.getCount(base + 5000L, timeWindowMs, bucketSizeMs, numBuckets);
        Assertions.assertEquals(1L, cnt);
    }

    @Test
    public void testTimeWindowLessThanBucketSizeStillUsesValidBucketCount() throws Exception {
        long originWindowSecond = Config.active_tablet_sliding_window_time_window_second;
        boolean originEnabled = Config.enable_active_tablet_sliding_window_access_stats;
        try {
            Config.active_tablet_sliding_window_time_window_second = 59L;
            Config.enable_active_tablet_sliding_window_access_stats = false;

            TabletSlidingWindowAccessStats stats = new TabletSlidingWindowAccessStats();
            Field numBucketsField = TabletSlidingWindowAccessStats.class.getDeclaredField("numBuckets");
            numBucketsField.setAccessible(true);
            int numBuckets = (int) numBucketsField.get(stats);
            Assertions.assertEquals(1, numBuckets);

            // No ArithmeticException should be thrown by modulo numBuckets path.
            stats.recordAccess(123L);
        } finally {
            Config.active_tablet_sliding_window_time_window_second = originWindowSecond;
            Config.enable_active_tablet_sliding_window_access_stats = originEnabled;
        }
    }

    @Test
    public void testTopNComparatorOrdersByAccessCountThenLastAccessTimeDesc() throws Exception {
        Field f = TabletSlidingWindowAccessStats.class.getDeclaredField("TOPN_ACTIVE_COMPARATOR");
        f.setAccessible(true);
        @SuppressWarnings("unchecked")
        Comparator<TabletSlidingWindowAccessStats.AccessStatsResult> cmp =
                (Comparator<TabletSlidingWindowAccessStats.AccessStatsResult>) f.get(null);

        List<TabletSlidingWindowAccessStats.AccessStatsResult> results = new ArrayList<>();
        results.add(new TabletSlidingWindowAccessStats.AccessStatsResult(1L, 1L, 200L));
        results.add(new TabletSlidingWindowAccessStats.AccessStatsResult(2L, 10L, 100L));
        results.add(new TabletSlidingWindowAccessStats.AccessStatsResult(3L, 10L, 300L));
        results.add(new TabletSlidingWindowAccessStats.AccessStatsResult(4L, 2L, 400L));

        results.sort(cmp);

        // accessCount desc => 10 first, then 2, then 1
        // tie-breaker lastAccessTime desc => id=3 (300) before id=2 (100)
        Assertions.assertEquals(3L, results.get(0).id);
        Assertions.assertEquals(2L, results.get(1).id);
        Assertions.assertEquals(4L, results.get(2).id);
        Assertions.assertEquals(1L, results.get(3).id);
    }
}
