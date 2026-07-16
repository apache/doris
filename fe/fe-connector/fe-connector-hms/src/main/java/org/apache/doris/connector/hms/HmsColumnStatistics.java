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

package org.apache.doris.connector.hms;

import java.util.Objects;

/**
 * Neutral per-column statistics extracted from a hive metastore {@code ColumnStatisticsObj}, hiding the
 * hive-thrift type from callers.
 *
 * <p>{@code avgColLenBytes} is set only for string columns (hive's {@code avgColLen}); it is {@code -1} for
 * every other type, where the consumer uses the column's fixed slot width instead. {@code ndv}/{@code numNulls}
 * are {@code 0} for a stats variant hive records but the legacy reader does not recognize (boolean / binary /
 * timestamp), matching legacy {@code HMSExternalTable.setStatData}.</p>
 */
public final class HmsColumnStatistics {

    private final String columnName;
    private final long ndv;
    private final long numNulls;
    private final double avgColLenBytes;

    public HmsColumnStatistics(String columnName, long ndv, long numNulls, double avgColLenBytes) {
        this.columnName = columnName;
        this.ndv = ndv;
        this.numNulls = numNulls;
        this.avgColLenBytes = avgColLenBytes;
    }

    public String getColumnName() {
        return columnName;
    }

    public long getNdv() {
        return ndv;
    }

    public long getNumNulls() {
        return numNulls;
    }

    /** Average string length in bytes for a string column, or {@code -1} for non-string columns. */
    public double getAvgColLenBytes() {
        return avgColLenBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HmsColumnStatistics)) {
            return false;
        }
        HmsColumnStatistics that = (HmsColumnStatistics) o;
        return ndv == that.ndv
                && numNulls == that.numNulls
                && Double.compare(that.avgColLenBytes, avgColLenBytes) == 0
                && Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, ndv, numNulls, avgColLenBytes);
    }

    @Override
    public String toString() {
        return "HmsColumnStatistics{columnName='" + columnName
                + "', ndv=" + ndv
                + ", numNulls=" + numNulls
                + ", avgColLenBytes=" + avgColLenBytes + "}";
    }
}
