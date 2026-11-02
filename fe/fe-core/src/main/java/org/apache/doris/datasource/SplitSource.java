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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
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
    private Iterator<TScanRangeLocations> pendingSplits = Collections.emptyIterator();

    public SplitSource(Backend backend, SplitAssignment splitAssignment, long maxWaitTime) {
        this.uniqueId = UNIQUE_ID_GENERATOR.getAndIncrement();
        this.backend = backend;
        this.splitAssignment = splitAssignment;
        this.maxWaitTime = maxWaitTime;
        this.isLastBatch = new AtomicBoolean(false);
        splitAssignment.registerSource(uniqueId);
    }

    public long getUniqueId() {
        return uniqueId;
    }

    /**
     * Get the next batch of file splits. If there's no more split, return empty list.
     */
    public synchronized List<TScanRangeLocations> getNextBatch(int maxBatchSize) throws UserException {
        Preconditions.checkArgument(maxBatchSize > 0, "Requested split batch size must be positive");
        if (isLastBatch.get()) {
            return Collections.emptyList();
        }
        List<TScanRangeLocations> scanRanges = new ArrayList<>(
                Math.min(maxBatchSize, splitAssignment.getSplitBatchSize()));
        long startTime = System.nanoTime();
        long waitNanos = TimeUnit.MILLISECONDS.toNanos(maxWaitTime);
        while (scanRanges.size() < maxBatchSize) {
            // Check producer errors even while draining a previously fetched batch.
            BlockingQueue<Collection<TScanRangeLocations>> splits = splitAssignment.getAssignedSplits(backend);
            if (splitAssignment.isStop()) {
                pendingSplits = Collections.emptyIterator();
                isLastBatch.set(true);
                break;
            }
            while (pendingSplits.hasNext() && scanRanges.size() < maxBatchSize) {
                scanRanges.add(pendingSplits.next());
            }
            if (scanRanges.size() == maxBatchSize) {
                return scanRanges;
            }
            pendingSplits = Collections.emptyIterator();
            // A finished, empty queue is EOF only after the pending batch has been drained.
            if (splits == null) {
                isLastBatch.set(true);
                break;
            }
            Collection<TScanRangeLocations> batch = splits.poll();
            if (batch != null) {
                splitAssignment.notifyQueueSpace(backend);
                pendingSplits = batch.iterator();
                continue;
            }
            if (!scanRanges.isEmpty()) {
                break;
            }
            long remainingNanos = waitNanos - (System.nanoTime() - startTime);
            if (remainingNanos <= 0) {
                throw new UserException("Timeout. Max wait time(ms): " + maxWaitTime);
            }
            try {
                batch = splits.poll(Math.min(remainingNanos, TimeUnit.MILLISECONDS.toNanos(WAIT_TIME_OUT)),
                        TimeUnit.NANOSECONDS);
                if (batch != null) {
                    splitAssignment.notifyQueueSpace(backend);
                    pendingSplits = batch.iterator();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UserException(e.getMessage(), e);
            }
        }
        return scanRanges;
    }
}
