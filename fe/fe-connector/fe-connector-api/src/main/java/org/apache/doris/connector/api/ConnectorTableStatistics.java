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
 * Basic table-level statistics. Use {@link #UNKNOWN} when statistics
 * are unavailable.
 */
public final class ConnectorTableStatistics {

    /** Sentinel value indicating statistics are not available. */
    public static final ConnectorTableStatistics UNKNOWN =
            new ConnectorTableStatistics(-1, -1);

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
