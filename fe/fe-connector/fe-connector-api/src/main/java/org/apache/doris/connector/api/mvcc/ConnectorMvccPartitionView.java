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

package org.apache.doris.connector.api.mvcc;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A connector-supplied, range-aware partition view for the MTMV / partition-aware materialized-view
 * refresh path of a plugin-driven MVCC table.
 *
 * <p>The generic table model ({@code PluginDrivenMvccExternalTable}) builds its partition view from
 * {@link org.apache.doris.connector.api.ConnectorTableOps#listPartitions} by default, which always
 * yields {@code LIST} partitions keyed on a last-modified timestamp. Connectors whose partitions are
 * intrinsically ranges (e.g. iceberg's {@code YEAR}/{@code MONTH}/{@code DAY}/{@code HOUR} time
 * transforms) need {@code RANGE} partition items plus a snapshot-id freshness marker to preserve the
 * pre-SPI behavior. Such a connector returns this view from
 * {@link org.apache.doris.connector.api.ConnectorMetadata#getMvccPartitionView}; the generic model then
 * assembles {@code RangePartitionItem}s from the pre-rendered bounds and selects the snapshot type from
 * {@link #getFreshness()} — all connector-specific math (transform-to-range, partition-evolution overlap
 * merge, snapshot-id resolution) having already happened inside the connector.</p>
 *
 * <p>The view also carries a {@link #getNewestUpdateTimeMillis() newest-update-time} marker: a monotonically
 * non-decreasing table-change timestamp the generic model answers the dictionary auto-refresh probe with
 * (the snapshot id alone is unusable there because it need not be monotonic).</p>
 *
 * <p>A connector that does NOT override {@code getMvccPartitionView} leaves the generic model on its
 * default {@code listPartitions} / LIST / timestamp path (byte-unchanged), so this view is purely
 * additive.</p>
 */
public final class ConnectorMvccPartitionView {

    /** How the generic model should model this table's partitions. */
    public enum Style {
        /** The table is not a valid partitioned related table (e.g. the connector's eligibility gate
         * failed); the generic model treats it as unpartitioned. */
        UNPARTITIONED,
        /** Range partitions: each {@link ConnectorMvccPartition} carries pre-rendered [lower, upper)
         * bounds the generic model turns into a {@code RangePartitionItem}. */
        RANGE
    }

    /** Which per-partition value the generic model compares to decide MTMV staleness. */
    public enum Freshness {
        /** {@code getFreshnessValue()} is a snapshot id; the model wraps it in {@code MTMVSnapshotIdSnapshot}. */
        SNAPSHOT_ID,
        /** {@code getFreshnessValue()} is an epoch-millis timestamp; wrapped in {@code MTMVTimestampSnapshot}. */
        LAST_MODIFIED
    }

    private final Style style;
    private final Freshness freshness;
    private final List<ConnectorMvccPartition> partitions;
    private final long newestUpdateTimeMillis;

    public ConnectorMvccPartitionView(Style style, Freshness freshness,
            List<ConnectorMvccPartition> partitions, long newestUpdateTimeMillis) {
        this.style = Objects.requireNonNull(style, "style");
        this.freshness = Objects.requireNonNull(freshness, "freshness");
        this.partitions = partitions == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(partitions);
        this.newestUpdateTimeMillis = newestUpdateTimeMillis;
    }

    /** Returns an {@code UNPARTITIONED} view (no partitions, newest-update-time {@code 0}); the freshness
     * marker is irrelevant. */
    public static ConnectorMvccPartitionView unpartitioned() {
        return new ConnectorMvccPartitionView(Style.UNPARTITIONED, Freshness.SNAPSHOT_ID,
                Collections.emptyList(), 0L);
    }

    public Style getStyle() {
        return style;
    }

    public Freshness getFreshness() {
        return freshness;
    }

    public List<ConnectorMvccPartition> getPartitions() {
        return partitions;
    }

    /**
     * The table's newest data-update marker, used by the dictionary auto-refresh path
     * ({@code MTMVRelatedTableIf.getNewestUpdateVersionOrTime}). This is a MONOTONICALLY non-decreasing
     * change marker (NOT the snapshot id, which need not be monotonic), so the generic model can answer
     * the dictionary's "is the source newer?" probe without crashing on a smaller-than-previous value.
     *
     * <p><b>Unit is source-defined, NOT guaranteed epoch millis</b> — only its monotonicity is relied upon, never
     * its absolute scale. For iceberg this is the {@code last_updated_at} value from the PARTITIONS metadata table,
     * which iceberg represents in MICROSECONDS; the generic model passes it through verbatim (parity with master,
     * which also compares this raw value without conversion). Do NOT treat it as millis or convert it.
     * {@code 0} when the table has no partitions / is unpartitioned (treated as "unchanged").</p>
     */
    public long getNewestUpdateTimeMillis() {
        return newestUpdateTimeMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorMvccPartitionView)) {
            return false;
        }
        ConnectorMvccPartitionView that = (ConnectorMvccPartitionView) o;
        return style == that.style
                && freshness == that.freshness
                && newestUpdateTimeMillis == that.newestUpdateTimeMillis
                && partitions.equals(that.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(style, freshness, partitions, newestUpdateTimeMillis);
    }

    @Override
    public String toString() {
        return "ConnectorMvccPartitionView{style=" + style
                + ", freshness=" + freshness
                + ", partitions=" + partitions.size()
                + ", newestUpdateTimeMillis=" + newestUpdateTimeMillis + "}";
    }
}
