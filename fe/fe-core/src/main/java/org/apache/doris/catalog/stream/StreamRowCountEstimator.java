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
import org.apache.doris.catalog.stream.BaseTableStream.StreamScanType;

import com.google.common.math.LongMath;

import java.util.function.LongSupplier;

/** Row-count formulas shared by consumption snapshots and stream reads. */
public final class StreamRowCountEstimator {
    public static final long UNKNOWN_ROW_COUNT = -1L;

    record ChangeEstimate(long records, String source) {
    }

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
    static ChangeEstimate estimateChangeRecords(Long start, Long end, long storedChangelog, long storedBase,
            long currentChangelog, long currentBase, boolean mergeOnWrite, StreamScanType scanType,
            LongSupplier tableFallback) {
        if (start != null && end != null) {
            if (end <= start) {
                return new ChangeEstimate(0, "empty-offset-range");
            }
            long changelogEstimate = currentChangelog >= 0 && storedChangelog >= 0
                    ? currentChangelog - storedChangelog : currentChangelog;
            // A known zero delta and a missing baseline with zero current rows have different fallbacks.
            boolean changelogUnavailable = changelogEstimate < 0 || (storedChangelog < 0 && currentChangelog == 0);
            long baseEstimate = UNKNOWN_ROW_COUNT;
            boolean useBaseDecline = mergeOnWrite && changelogUnavailable
                    && (scanType == StreamScanType.DETAIL || scanType == StreamScanType.MIN_DELTA);
            if (storedBase >= 0 && currentBase >= 0) {
                long deltaBase = currentBase - storedBase;
                baseEstimate = useBaseDecline ? Math.abs(deltaBase) : deltaBase;
            }
            if (changelogEstimate > 0 && changelogEstimate >= baseEstimate) {
                return new ChangeEstimate(changelogEstimate, storedChangelog >= 0
                        ? "changelog-delta" : "changelog-index-without-baseline");
            }
            if (baseEstimate > 0) {
                return new ChangeEstimate(baseEstimate, currentBase < storedBase ? "base-decline" : "base-growth");
            }
        }
        if (currentChangelog > 0) {
            return new ChangeEstimate(currentChangelog, "changelog-index");
        }
        if (currentBase > 0) {
            return new ChangeEstimate(currentBase, "base-index");
        }
        long fallback = tableFallback.getAsLong();
        return new ChangeEstimate(fallback > 0 ? fallback : UNKNOWN_ROW_COUNT, "table-fallback");
    }

    public static long outputRows(long records, boolean unique, BaseTableStream.StreamScanType type) {
        if (records < 0) {
            return UNKNOWN_ROW_COUNT;
        }
        return unique && type != StreamScanType.APPEND_ONLY ? LongMath.saturatedMultiply(records, 2) : records;
    }

    /** Complete snapshot scale; UNIQUE reconstruction instead estimates its branches separately. */
    public static long estimateSnapshotRead(boolean appendOnly, long storedBaseRowCount,
            long currentBaseRowCount, long changeRecords, LongSupplier tableRowCount) {
        if (storedBaseRowCount >= 0) {
            return storedBaseRowCount;
        }
        if (currentBaseRowCount >= 0) {
            return !appendOnly && changeRecords >= 0 ? currentBaseRowCount + changeRecords : currentBaseRowCount;
        }
        return tableRowCount.getAsLong();
    }
}
