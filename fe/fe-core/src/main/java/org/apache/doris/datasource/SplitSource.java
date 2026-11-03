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

import org.apache.doris.common.UserException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * If there are many files, splitting these files into scan ranges will consume a lot of time.
 * Even the simplest queries(e.g. select * from large_table limit 1) can get stuck or crash due to the split process.
 * Furthermore, during the splitting process, the backend did not do anything.
 * It is completely possible to split files whiling scanning data on the ready splits at once.
 * `SplitSource` introduce a lazy and batch mode to provide the file splits. Each `SplitSource` has a unique ID,
 * which is used by backends to call `FrontendServiceImpl#fetchSplitBatch` to fetch splits batch by batch.
 * `SplitSource`s are managed by `SplitSourceManager`, which stores `SplitSource` as a weak reference, and clean
 * the split source when its related scan node is GC.
 */
public class SplitSource {
    private static final AtomicLong UNIQUE_ID_GENERATOR = new AtomicLong(0);
    private static final long WAIT_TIME_OUT = 100; // 100ms

    private final long uniqueId;
    private final Backend backend;
    private final SplitAssignment splitAssignment;
    private final AtomicBoolean isLastBatch;
    private final long maxWaitTime;

    public SplitSource(Backend backend, SplitAssignment splitAssignment, long maxWaitTime) {
        Preconditions.checkArgument(maxWaitTime > 0, "fetch_splits_max_wait_time_ms must be positive");
        this.uniqueId = UNIQUE_ID_GENERATOR.getAndIncrement();
        this.backend = backend;
        this.splitAssignment = splitAssignment;
        this.maxWaitTime = maxWaitTime;
        this.isLastBatch = new AtomicBoolean(false);
        splitAssignment.registerSource(uniqueId);
    }

    /** Close this backend's shared consumer without stopping other backends of the scan. */
    public void close() {
        isLastBatch.set(true);
        splitAssignment.stopBackend(backend.getId());
    }

    public long getUniqueId() {
        return uniqueId;
    }

    /**
     * Get the next batch of file splits. If there's no more split, return empty list.
     */
    public boolean isLastBatch() {
        return isLastBatch.get();
    }

    public synchronized List<TScanRangeLocations> getNextBatch(int maxBatchSize) throws UserException {
        Preconditions.checkArgument(maxBatchSize > 0);
        if (isLastBatch.get()) {
            return Collections.emptyList();
        }
        List<TScanRangeLocations> scanRanges = Lists.newArrayListWithExpectedSize(maxBatchSize);
        long started = System.nanoTime();
        long waitNanos = TimeUnit.MILLISECONDS.toNanos(maxWaitTime);
        while (scanRanges.size() < maxBatchSize) {
            BlockingQueue<TScanRangeLocations> splits = splitAssignment.getAssignedSplits(backend);
            if (splits == null) {
                isLastBatch.set(true);
                break;
            }
            TScanRangeLocations split = splits.poll();
            if (split != null) {
                scanRanges.add(split);
                continue;
            }
            if (!scanRanges.isEmpty()) {
                break;
            }
            long wait = waitNanos - (System.nanoTime() - started);
            if (wait <= 0) {
                break;
            }
            try {
                split = splits.poll(Math.min(wait, TimeUnit.MILLISECONDS.toNanos(WAIT_TIME_OUT)),
                        TimeUnit.NANOSECONDS);
                if (split != null) {
                    scanRanges.add(split);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UserException("Interrupted while waiting for scan splits", e);
            }
        }
        return scanRanges;
    }
}
