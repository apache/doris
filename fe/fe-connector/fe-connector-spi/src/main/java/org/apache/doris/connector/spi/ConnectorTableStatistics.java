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

package org.apache.doris.connector.spi;

import java.util.Objects;

/**
 * Basic table-level statistics.
 *
 * <p>A connector that has no statistics at all returns {@code Optional.empty()} from
 * {@link ConnectorStatisticsOps#getTableStatistics} — that, not a sentinel instance, is how
 * "unavailable" is expressed. Returning a present value with an individual field set to -1 is
 * still allowed and means "this one number is unknown" (e.g. hive knows the total size but not
 * the row count).</p>
 */
public final class ConnectorTableStatistics {

    private final long rowCount;
    private final long dataSize;

    public ConnectorTableStatistics(long rowCount, long dataSize) {
        this.rowCount = rowCount;
        this.dataSize = dataSize;
    }

    /** Returns the estimated row count, or -1 if unknown. */
    public long getRowCount() {
        return rowCount;
    }

    /** Returns the estimated data size in bytes, or -1 if unknown. */
    public long getDataSize() {
        return dataSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorTableStatistics)) {
            return false;
        }
        ConnectorTableStatistics that = (ConnectorTableStatistics) o;
        return rowCount == that.rowCount && dataSize == that.dataSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowCount, dataSize);
    }

    @Override
    public String toString() {
        return "ConnectorTableStatistics{rowCount=" + rowCount
                + ", dataSize=" + dataSize + "}";
    }
}
