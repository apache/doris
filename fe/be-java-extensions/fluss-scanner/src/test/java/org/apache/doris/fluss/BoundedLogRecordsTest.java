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

package org.apache.doris.fluss;

import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.TableBucket;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** Cases where Fluss's public poll omits an empty successful server response. */
public class BoundedLogRecordsTest {

    private static final TableBucket BUCKET = new TableBucket(7L, 0);

    @Test
    public void pinnedLakeTailFailsWhenItsStartIsOnlyInTheLake() throws Exception {
        AtomicLong now = new AtomicLong();
        try (BoundedLogRecords records = records(100L, 120L, true,
                offset -> new BoundedLogRecords.ProbeResult(false, 120L), now)) {
            now.set(Duration.ofSeconds(6).toNanos());
            IOException failure = Assertions.assertThrows(IOException.class,
                    () -> records.poll(Duration.ZERO));
            Assertions.assertTrue(failure.getMessage().contains("[100, 120)"), failure.getMessage());
            Assertions.assertTrue(failure.getMessage().contains("local or remote"), failure.getMessage());
        }
    }

    @Test
    public void flussOnlyLakeReadFailsInsteadOfHidingLocallyRetainedRows() throws Exception {
        AtomicLong now = new AtomicLong();
        try (BoundedLogRecords records = records(0L, 120L, true,
                offset -> new BoundedLogRecords.ProbeResult(false, 120L), now)) {
            now.set(Duration.ofSeconds(6).toNanos());
            IOException failure = Assertions.assertThrows(IOException.class,
                    () -> records.poll(Duration.ZERO));
            Assertions.assertTrue(failure.getMessage().contains("offset 0"), failure.getMessage());
        }
    }

    @Test
    public void retainedRemoteSegmentIsNotMisclassifiedAsMissing() throws Exception {
        AtomicLong now = new AtomicLong();
        AtomicInteger probes = new AtomicInteger();
        try (BoundedLogRecords records = records(100L, 120L, true, offset -> {
            probes.incrementAndGet();
            return new BoundedLogRecords.ProbeResult(true, 120L);
        }, now)) {
            now.set(Duration.ofSeconds(6).toNanos());
            Assertions.assertTrue(records.poll(Duration.ZERO).isEmpty());
            Assertions.assertFalse(records.isFinished());
            Assertions.assertEquals(1, probes.get());
            now.set(Duration.ofMinutes(2).plusSeconds(1).toNanos());
            Assertions.assertTrue(records.poll(Duration.ZERO).isEmpty());
            Assertions.assertFalse(records.isFinished());
            Assertions.assertEquals(2, probes.get());
        }
    }

    @Test
    public void oldEarliestSentinelFinishesWhenNonLakeBucketHasExpired() throws Exception {
        AtomicLong now = new AtomicLong();
        try (BoundedLogRecords records = records(LogScanner.EARLIEST_OFFSET, 12L, false,
                offset -> new BoundedLogRecords.ProbeResult(false, 12L), now)) {
            now.set(Duration.ofSeconds(6).toNanos());
            Assertions.assertTrue(records.poll(Duration.ZERO).isEmpty());
            Assertions.assertTrue(records.isFinished());
        }
    }

    private static BoundedLogRecords records(long start, long stop, boolean lakeEnabled,
            BoundedLogRecords.LogRangeProbe probe, AtomicLong now) {
        LogScanner scanner = (LogScanner) Proxy.newProxyInstance(LogScanner.class.getClassLoader(),
                new Class<?>[] {LogScanner.class}, (proxy, method, args) -> {
                    if ("poll".equals(method.getName())) {
                        return ScanRecords.EMPTY;
                    }
                    if ("subscribe".equals(method.getName()) || "close".equals(method.getName())) {
                        return null;
                    }
                    throw new AssertionError("Unexpected LogScanner call: " + method.getName());
                });
        return new BoundedLogRecords(scanner, BUCKET, start, stop, lakeEnabled,
                "db.t", probe, now::get);
    }
}
