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

package org.apache.doris.catalog.stream;

import org.apache.doris.catalog.MaterializedIndex;

/** Row-count formulas shared by consumption snapshots and stream reads. */
public final class StreamRowCountEstimator {
    public static final long UNKNOWN_ROW_COUNT = -1L;

    private StreamRowCountEstimator() {
    }

    /** A positive partial estimate is useful even before a complete tablet report. */
    public static long rowCount(MaterializedIndex index) {
        if (index == null) {
            return UNKNOWN_ROW_COUNT;
        }
        long rows = index.getRowCount();
        return rows > 0 || (rows == 0 && index.getRowCountReported()) ? rows : UNKNOWN_ROW_COUNT;
    }

    /** Record scale before changelog scan-type expansion. Only offsets can prove an empty range. */
    public static long estimateChangeRecords(Long start, Long end, long storedChangelog, long storedBase,
            long currentChangelog, long currentBase, long tableFallback) {
        if (start != null && end != null) {
            if (end <= start) {
                return 0;
            }
            long deltaC = storedChangelog >= 0 && currentChangelog >= 0
                    ? currentChangelog - storedChangelog : UNKNOWN_ROW_COUNT;
            long deltaB = storedBase >= 0 && currentBase >= 0
                    ? currentBase - storedBase : UNKNOWN_ROW_COUNT;
            long delta = Math.max(deltaC, deltaB);
            if (delta > 0) {
                return delta;
            }
        }
        if (currentChangelog > 0) {
            return currentChangelog;
        }
        if (currentBase > 0) {
            return currentBase;
        }
        return tableFallback > 0 ? tableFallback : UNKNOWN_ROW_COUNT;
    }

    public static long outputRows(long records, boolean unique, BaseTableStream.StreamScanType type) {
        if (records < 0) {
            return UNKNOWN_ROW_COUNT;
        }
        return unique && type != BaseTableStream.StreamScanType.APPEND_ONLY ? records * 2 : records;
    }

    /** Complete snapshot scale; UNIQUE reconstruction instead estimates its branches separately. */
    public static long estimateSnapshotRead(boolean appendOnly, long storedBaseRowCount,
            long currentBaseRowCount, long changeRecords, long tableRowCount) {
        if (storedBaseRowCount >= 0) {
            return storedBaseRowCount;
        }
        if (currentBaseRowCount >= 0) {
            return !appendOnly && changeRecords >= 0 ? currentBaseRowCount + changeRecords : currentBaseRowCount;
        }
        return tableRowCount;
    }
}
