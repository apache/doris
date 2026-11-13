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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;

import java.lang.ref.WeakReference;
import java.util.Objects;

/** Immutable, FE-local row-count baseline associated with one successful consumption. */
public class StreamRowCountBaseline {
    public static final long UNKNOWN_ROW_COUNT = -1L;

    private final Long baselineOffset;
    private final long changelogRowCount;
    private final long baseRowCount;
    private final WeakReference<Partition> partition;
    private final WeakReference<Object> baseEpoch;
    private final WeakReference<Object> changelogEpoch;
    private final long feEpoch;
    private final long partitionEpoch;

    private StreamRowCountBaseline(Long baselineOffset, long changelogRowCount, long baseRowCount,
            Partition partition, Object baseEpoch, Object changelogEpoch, long feEpoch, long partitionEpoch) {
        this.baselineOffset = baselineOffset;
        this.changelogRowCount = changelogRowCount;
        this.baseRowCount = baseRowCount;
        this.partition = new WeakReference<>(partition);
        this.baseEpoch = new WeakReference<>(baseEpoch);
        this.changelogEpoch = new WeakReference<>(changelogEpoch);
        this.feEpoch = feEpoch;
        this.partitionEpoch = partitionEpoch;
    }

    public static StreamRowCountBaseline capture(OlapTable table, long partitionId, Long offset) {
        Partition partition = table.getPartition(partitionId);
        MaterializedIndex base = partition.getIndex(table.getBaseIndexId());
        MaterializedIndex changelog = table.hasChangelog()
                ? partition.getIndex(table.getChangelogIndexMeta().getIndexId()) : null;
        return new StreamRowCountBaseline(offset, rowCount(changelog), rowCount(base), partition,
                dataEpoch(base), dataEpoch(changelog), Env.getCurrentEnv().getEpoch(), partition.getDataEpoch());
    }

    /** Rebind a creation sample to the offset returned by the existing authoritative capture. */
    public StreamRowCountBaseline atOffset(Long offset) {
        return new StreamRowCountBaseline(offset, changelogRowCount, baseRowCount, partition.get(),
                baseEpoch.get(), changelogEpoch.get(), feEpoch, partitionEpoch);
    }

    public Long getBaselineOffset() {
        return baselineOffset;
    }

    public long getChangelogRowCount() {
        return changelogRowCount;
    }

    public long getBaseRowCount() {
        return baseRowCount;
    }

    public boolean matches(OlapTable table, long partitionId, Long offset) {
        Partition current = table.getPartition(partitionId);
        return current != null && current == partition.get() && Objects.equals(baselineOffset, offset)
                && feEpoch == Env.getCurrentEnv().getEpoch()
                && partitionEpoch == current.getDataEpoch()
                && baseEpoch.get() == dataEpoch(current.getIndex(table.getBaseIndexId()))
                && changelogEpoch.get() == dataEpoch(table.hasChangelog()
                        ? current.getIndex(table.getChangelogIndexMeta().getIndexId()) : null);
    }

    private static Object dataEpoch(MaterializedIndex index) {
        return index == null ? null : index.getDataEpoch();
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
