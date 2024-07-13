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

import com.google.common.collect.Lists;

import java.util.Collection;
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
    public List<TScanRangeLocations> getNextBatch(int maxBatchSize) throws UserException {
        if (isLastBatch.get()) {
            return Collections.emptyList();
        }
        List<TScanRangeLocations> scanRanges = Lists.newArrayListWithExpectedSize(maxBatchSize);
        long startTime = System.currentTimeMillis();
        while (scanRanges.size() < maxBatchSize) {
            BlockingQueue<Collection<TScanRangeLocations>> splits = splitAssignment.getAssignedSplits(backend);
            if (splits == null) {
                isLastBatch.set(true);
                break;
            }
            while (scanRanges.size() < maxBatchSize) {
                try {
                    Collection<TScanRangeLocations> splitCollection = splits.poll(WAIT_TIME_OUT, TimeUnit.MILLISECONDS);
                    if (splitCollection != null) {
                        scanRanges.addAll(splitCollection);
                    }
                    if (!scanRanges.isEmpty() && System.currentTimeMillis() - startTime > maxWaitTime) {
                        return scanRanges;
                    }
                    if (splitCollection == null) {
                        break;
                    }
                } catch (InterruptedException e) {
                    throw new UserException("Failed to get next batch of splits", e);
                }
            }
        }
        return scanRanges;
    }
}
