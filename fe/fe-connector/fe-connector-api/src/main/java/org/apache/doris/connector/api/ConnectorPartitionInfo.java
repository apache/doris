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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Describes a single partition in a connector table.
 */
public final class ConnectorPartitionInfo {

    /** Sentinel for "unknown" on the numeric stats fields. */
    public static final long UNKNOWN = -1L;

    private final String partitionName;
    private final Map<String, String> partitionValues;
    private final Map<String, String> properties;
    private final long rowCount;
    private final long sizeBytes;
    private final long lastModifiedMillis;
    private final long fileCount;

    /**
     * Backward-compatible constructor. Numeric stats fields are set to
     * {@link #UNKNOWN}.
     */
    public ConnectorPartitionInfo(String partitionName,
            Map<String, String> partitionValues,
            Map<String, String> properties) {
        this(partitionName, partitionValues, properties,
                UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN);
    }

    public ConnectorPartitionInfo(String partitionName,
            Map<String, String> partitionValues,
            Map<String, String> properties,
            long rowCount, long sizeBytes, long lastModifiedMillis, long fileCount) {
        this.partitionName = Objects.requireNonNull(
                partitionName, "partitionName");
        this.partitionValues = partitionValues == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(partitionValues);
        this.properties = properties == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(properties);
        this.rowCount = rowCount;
        this.sizeBytes = sizeBytes;
        this.lastModifiedMillis = lastModifiedMillis;
        this.fileCount = fileCount;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public Map<String, String> getPartitionValues() {
        return partitionValues;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    /** @return row count, or {@link #UNKNOWN} when not collected. */
    public long getRowCount() {
        return rowCount;
    }

    /** @return on-disk size in bytes, or {@link #UNKNOWN}. */
    public long getSizeBytes() {
        return sizeBytes;
    }

    /** @return last-modified epoch millis, or {@link #UNKNOWN}. */
    public long getLastModifiedMillis() {
        return lastModifiedMillis;
    }

    /** @return number of data files in the partition, or {@link #UNKNOWN}. */
    public long getFileCount() {
        return fileCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorPartitionInfo)) {
            return false;
        }
        ConnectorPartitionInfo that = (ConnectorPartitionInfo) o;
        return rowCount == that.rowCount
                && sizeBytes == that.sizeBytes
                && lastModifiedMillis == that.lastModifiedMillis
                && fileCount == that.fileCount
                && partitionName.equals(that.partitionName)
                && partitionValues.equals(that.partitionValues)
                && properties.equals(that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionName, partitionValues, properties,
                rowCount, sizeBytes, lastModifiedMillis, fileCount);
    }

    @Override
    public String toString() {
        return "ConnectorPartitionInfo{name='" + partitionName
                + "', values=" + partitionValues
                + ", rowCount=" + rowCount
                + ", sizeBytes=" + sizeBytes
                + ", fileCount=" + fileCount + "}";
    }
}
