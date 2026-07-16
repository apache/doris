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

package org.apache.doris.connector.api;

import java.util.Objects;

/**
 * Per-column statistics a connector can serve WITHOUT a table scan (e.g. hive's HMS/spark column stats),
 * feeding the query-planner column-statistics fast path.
 *
 * <p>This carries only RAW facts. The connector does NOT compute the Doris-type-dependent
 * {@code dataSize}/{@code avgSize} — those depend on the column's fixed slot width, which the connector
 * cannot know without importing fe-type. fe-core derives them from {@link #getAvgSizeBytes()} (when the
 * source knows the average value size, e.g. a hive string column's {@code avgColLen}) or the column's slot
 * width otherwise, mirroring the {@link #getTableStatistics}-style raw/derived split. Use {@link #UNKNOWN}
 * when statistics are unavailable.</p>
 */
public final class ConnectorColumnStatistics {

    /** Sentinel indicating no per-column statistics are available. */
    public static final ConnectorColumnStatistics UNKNOWN =
            new ConnectorColumnStatistics(-1, -1, -1, -1);

    private final long rowCount;
    private final long ndv;
    private final long numNulls;
    private final double avgSizeBytes;

    public ConnectorColumnStatistics(long rowCount, long ndv, long numNulls, double avgSizeBytes) {
        this.rowCount = rowCount;
        this.ndv = ndv;
        this.numNulls = numNulls;
        this.avgSizeBytes = avgSizeBytes;
    }

    /** The table row count the stats are relative to (source {@code numRows}), or -1 if unknown. */
    public long getRowCount() {
        return rowCount;
    }

    /** Number of distinct values, or -1 if unknown. */
    public long getNdv() {
        return ndv;
    }

    /** Number of nulls, or -1 if unknown. */
    public long getNumNulls() {
        return numNulls;
    }

    /**
     * Average per-value size in bytes when the source knows it (e.g. a hive string column's {@code avgColLen}),
     * or {@code -1} when it does not — fe-core then falls back to the Doris column's fixed slot width.
     */
    public double getAvgSizeBytes() {
        return avgSizeBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorColumnStatistics)) {
            return false;
        }
        ConnectorColumnStatistics that = (ConnectorColumnStatistics) o;
        return rowCount == that.rowCount
                && ndv == that.ndv
                && numNulls == that.numNulls
                && Double.compare(that.avgSizeBytes, avgSizeBytes) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowCount, ndv, numNulls, avgSizeBytes);
    }

    @Override
    public String toString() {
        return "ConnectorColumnStatistics{rowCount=" + rowCount
                + ", ndv=" + ndv
                + ", numNulls=" + numNulls
                + ", avgSizeBytes=" + avgSizeBytes + "}";
    }
}
