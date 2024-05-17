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
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TScanRangeLocations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    private final long uniqueId;
    private final SplitToScanRange splitToScanRange;
    private final Backend backend;
    private final Map<String, String> locationProperties;
    private final List<String> pathPartitionKeys;
    private final SplitAssignment splitAssignment;
    private Iterator<Split> splitIterator = null;
    private boolean isLastBatch = false;

    public SplitSource(
            SplitToScanRange splitToScanRange,
            Backend backend,
            Map<String, String> locationProperties,
            SplitAssignment splitAssignment,
            List<String> pathPartitionKeys) {
        this.uniqueId = UNIQUE_ID_GENERATOR.getAndIncrement();
        this.splitToScanRange = splitToScanRange;
        this.backend = backend;
        this.locationProperties = locationProperties;
        this.pathPartitionKeys = pathPartitionKeys;
        this.splitAssignment = splitAssignment;
    }

    public long getUniqueId() {
        return uniqueId;
    }

    /**
     * Get the next batch of file splits. If there's no more split, return empty list.
     */
    public synchronized List<TScanRangeLocations> getNextBatch(int maxBatchSize) throws UserException {
        if (isLastBatch) {
            return Collections.emptyList();
        }
        List<TScanRangeLocations> scanRanges = new ArrayList<>(maxBatchSize);
        for (int i = 0; i < maxBatchSize; i++) {
            if (splitIterator == null || !splitIterator.hasNext()) {
                Collection<Split> splits = splitAssignment.getNextBatch(backend);
                if (splits.isEmpty()) {
                    isLastBatch = true;
                    return scanRanges;
                }
                splitIterator = splits.iterator();
            }
            scanRanges.add(splitToScanRange.getScanRange(
                    backend, locationProperties, splitIterator.next(), pathPartitionKeys));
        }
        return scanRanges;
    }
}
