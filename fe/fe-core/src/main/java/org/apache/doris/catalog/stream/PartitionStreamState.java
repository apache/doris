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

import com.google.gson.annotations.SerializedName;

/**
 * What a table stream recorded for one partition when it last advanced that partition's offset.
 *
 * <p>A changelog read returns the change rows between the stream offset and the end TSO of the
 * read, a number the FE cannot derive from table metadata: the changelog index of a partition
 * holds the change rows of every offset range, so its row count is the size of the whole
 * changelog, not of the range being read. Two counters recorded next to the offset give the
 * estimate a baseline to subtract:
 *
 * <ul>
 *   <li>{@code changelogRowCount} (P): the row count of the partition's changelog index at the
 *       moment the offset advanced; the read estimate is the current row count minus it;</li>
 *   <li>{@code baseRowCount} (Q): the row count of the partition's base index at the same moment;
 *       a snapshot read answers with the image at the offset, and Q is its size.</li>
 * </ul>
 *
 * <p>The rule behind the estimates is to err on the large side: over-estimating a relation only
 * makes the optimizer plan it differently, while under-estimating lets a large relation become
 * the build side of a broadcast join and exhaust memory.
 */
public class PartitionStreamState {
    public static final long UNKNOWN_ROW_COUNT = -1L;

    /**
     * The estimate of a read of a range which is known to hold changes when no counter can size
     * it: the backends have not reported the partition, a load just wrote it, and neither its
     * changelog index nor its base index has a count the frontend can use. Reporting one row there
     * would let the optimizer take a range of unknown size for a small relation, which is the
     * failure this estimate exists to avoid, so the read is reported as a relation too large to be
     * broadcast instead.
     */
    public static final long UNKNOWN_NONEMPTY_ROW_COUNT = 1L << 30;

    @SerializedName("crc")
    private final long changelogRowCount;

    @SerializedName("brc")
    private final long baseRowCount;

    public PartitionStreamState(long changelogRowCount, long baseRowCount) {
        this.changelogRowCount = changelogRowCount;
        this.baseRowCount = baseRowCount;
    }

    public long getChangelogRowCount() {
        return changelogRowCount;
    }

    public long getBaseRowCount() {
        return baseRowCount;
    }

    /**
     * Change rows the read of {@code (startTso, endTso]} may return, or {@link #UNKNOWN_ROW_COUNT}
     * when no counter can tell.
     *
     * <p>A read of an empty range returns nothing no matter what the row counts report: the two
     * TSOs are commit metadata that do not lag behind, unlike the row counts, so they decide the
     * empty case. Otherwise the rows still in the changelog bound the read: a valid read only
     * returns rows which were not reclaimed yet.
     */
    public static long changelogDelta(long startTso, long endTso, long storedChangelogRowCount,
            long currentChangelogRowCount) {
        if (startTso < 0 || endTso < 0) {
            // The range of the read is not known, so the difference of the counters cannot be
            // attributed to it: the read may start before the counters do.
            return UNKNOWN_ROW_COUNT;
        }
        if (endTso <= startTso) {
            return 0;
        }
        if (storedChangelogRowCount >= 0 && currentChangelogRowCount > storedChangelogRowCount) {
            return currentChangelogRowCount - storedChangelogRowCount;
        }
        if (currentChangelogRowCount > 0) {
            // No baseline, or the counters moved backwards (a reclamation, or a partition which
            // was recreated): every row the read may return is in the changelog right now.
            return currentChangelogRowCount;
        }
        return UNKNOWN_ROW_COUNT;
    }

    /**
     * Estimate of a changelog read. See {@link #changelogDelta}. When the row counts cannot bound
     * the read (not reported yet, or the partition was just written and the backend has not
     * reported it), fall back to values which keep the estimate in the right order of magnitude
     * instead of returning one row: a base row can be changed many times, so the base row count
     * is not a strict bound, it only keeps the relation from looking tiny.
     */
    public static long estimateChangelogRead(long startTso, long endTso, long storedChangelogRowCount,
            long currentChangelogRowCount, long currentBaseRowCount, long tableRowCount) {
        long delta = changelogDelta(startTso, endTso, storedChangelogRowCount, currentChangelogRowCount);
        if (delta < 0) {
            delta = Math.max(currentChangelogRowCount, currentBaseRowCount);
            if (delta <= 0) {
                delta = tableRowCount;
            }
            if (delta <= 0 && startTso >= 0 && endTso > startTso) {
                // The TSOs say the range holds changes, but nothing sizes them: a load which just
                // wrote the partition, before any counter exists. One row would say the opposite.
                delta = UNKNOWN_NONEMPTY_ROW_COUNT;
            }
        }
        return Math.max(1, delta);
    }

    /**
     * Estimate of a snapshot read answering with the image at the stream offset.
     *
     * <p>For a duplicate key table the image only grows by appends, so the current base rows are an
     * upper bound and adding the change rows would only make the estimate worse. For a unique or
     * aggregating table a delete after the offset makes the image larger than the current rows,
     * and the rows added back never exceed the change rows of the range.
     */
    public static long estimateSnapshotRead(boolean appendOnly, long storedBaseRowCount,
            long currentBaseRowCount, long changelogDelta, long tableRowCount) {
        long currentRows = Math.max(0, currentBaseRowCount);
        long addedBack = appendOnly || changelogDelta < 0 ? 0 : changelogDelta;
        long estimate = Math.max(storedBaseRowCount, currentRows + addedBack);
        if (estimate <= 0) {
            estimate = tableRowCount;
        }
        return Math.max(1, estimate);
    }
}
