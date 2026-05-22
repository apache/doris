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

package org.apache.doris.tso;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * TSOTimestamp represents a Timestamp Oracle timestamp with physical time and logical counter.
 *
 * TSO timestamp format (64 bits):
 * 63                                                      18 17                                    0
 * ┌─────────────────────────────────────────────────────────┬──────────────────────────────────────┐
 * |              Physical Time (milliseconds, 46 bits)      │      Logical Counter 18 bits         │
 * └─────────────────────────────────────────────────────────┴──────────────────────────────────────┘
 *
 * Example:
 * Physical time: 1625097600000 (milliseconds, 46 bits)
 * Logical counter: 123 (18 bits)
 * Combined TSO: 123456789012345678
 */
public final class TSOTimestamp implements Writable, Comparable<TSOTimestamp> {

    @SerializedName(value = "physicalTimestamp")
    private long physicalTimestamp = 0L;
    @SerializedName(value = "logicalCounter")
    private long logicalCounter = 0L;

    // Bit width for each field
    private static final int LOGICAL_BITS   = 18;  // Logical counter bits
    private static final int PHYSICAL_BITS  = 46;  // Physical time bits (milliseconds)

    // Starting bit offset for each field (relative to bit0)
    private static final int PHYSICAL_SHIFT = LOGICAL_BITS;  // 18

    // Masks for each field in 64-bit TSO
    private static final long LOGICAL_MASK  = ((1L << LOGICAL_BITS)  - 1L);
    private static final long PHYSICAL_MASK = ((1L << PHYSICAL_BITS) - 1L) << PHYSICAL_SHIFT;

    // Raw masks for bit operations
    private static final long RAW_LOGICAL_MASK  = (1L << LOGICAL_BITS)  - 1L;
    private static final long RAW_PHYSICAL_MASK = (1L << PHYSICAL_BITS) - 1L;

    // Maximum logical counter value
    public static final long MAX_LOGICAL_COUNTER = (1L << LOGICAL_BITS) - 1L;

    /**
     * Constructor with specific physical time and logical counter
     *
     * @param physicalTimestamp Physical time in milliseconds
     * @param logicalCounter Logical counter value
     */
    @VisibleForTesting
    public TSOTimestamp(long physicalTimestamp, long logicalCounter) {
        if (physicalTimestamp < 0 || logicalCounter < 0) {
            throw new IllegalArgumentException("TSO components must be non-negative");
        }
        this.physicalTimestamp = physicalTimestamp;
        this.logicalCounter = logicalCounter;
    }

    public TSOTimestamp(long timestamp) {
        this(extractPhysicalTime(timestamp), extractLogicalCounter(timestamp));
    }

    /**
     * Default constructor initializes with zero values
     */
    @VisibleForTesting
    public TSOTimestamp() {
        this(0L, 0L);
    }

    /**
     * Compose 64-bit TSO timestamp from physical time and logical counter
     *
     * @return 64-bit TSO timestamp
     */
    public long composeTimestamp() {
        return composeTimestamp(physicalTimestamp, logicalCounter);
    }

    /**
     * Extract physical time (milliseconds) from TSO timestamp
     *
     * @param timestamp 64-bit TSO timestamp
     * @return Physical time in milliseconds
     */
    public static long extractPhysicalTime(long timestamp) {
        return (timestamp & PHYSICAL_MASK) >>> PHYSICAL_SHIFT;
    }

    /**
     * Extract logical counter from TSO timestamp
     *
     * @param timestamp 64-bit TSO timestamp
     * @return Logical counter value
     */
    public static long extractLogicalCounter(long timestamp) {
        return (timestamp & LOGICAL_MASK);
    }

    /**
     * Get physical timestamp
     *
     * @return Physical timestamp in milliseconds
     */
    public long getPhysicalTimestamp() {
        return physicalTimestamp;
    }

    /**
     * Set physical timestamp
     *
     * @param physicalTimestamp Physical timestamp in milliseconds
     */
    public void setPhysicalTimestamp(long physicalTimestamp) {
        if (physicalTimestamp < 0) {
            throw new IllegalArgumentException("physicalTimestamp must be non-negative");
        }
        this.physicalTimestamp = physicalTimestamp;
    }

    /**
     * Get logical counter
     *
     * @return Logical counter value
     */
    public long getLogicalCounter() {
        return logicalCounter;
    }

    /**
     * Set logical counter
     *
     * @param logicalCounter Logical counter value
     */
    public void setLogicalCounter(long logicalCounter) {
        if (logicalCounter < 0) {
            throw new IllegalArgumentException("logicalCounter must be non-negative");
        }
        this.logicalCounter = logicalCounter;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this, TSOTimestamp.class);
        Text.writeString(out, json);
    }

    public static TSOTimestamp read(DataInput dataInput) throws IOException {
        String json = Text.readString(dataInput);
        TSOTimestamp tsoTimestamp = GsonUtils.GSON.fromJson(json, TSOTimestamp.class);
        if (tsoTimestamp == null) {
            throw new IOException("failed to deserialize TSOTimestamp from journal/image");
        }
        return tsoTimestamp;
    }

    /**
     * Compose TSO timestamp from physical time and logical counter
     *
     * @param physicalTime   Physical time (milliseconds)
     * @param logicalCounter Logical counter
     * @return 64-bit TSO timestamp
     */
    public static long composeTimestamp(long physicalTime, long logicalCounter) {
        // Prevent overflow by masking to appropriate bit widths
        long physical = physicalTime   & RAW_PHYSICAL_MASK; // Keep only 46 bits
        long logical  = logicalCounter & RAW_LOGICAL_MASK;  // Keep only 18 bits

        // Bitwise assembly: High 46 bits physical time + Low 18 bits logical counter
        return (physical  << PHYSICAL_SHIFT)
            | (logical);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("physical timestamp: ").append(physicalTimestamp);
        sb.append(", logical counter: ").append(logicalCounter);

        return sb.toString();
    }

    public String toJson() {
        return GsonUtils.GSON.toJson(this, TSOTimestamp.class);
    }

    /**
     * Decompose a composed 64-bit TSO into physical and logical parts
     */
    public static TSOTimestamp decompose(long tso) {
        long physical = extractPhysicalTime(tso);
        long logical = extractLogicalCounter(tso);
        return new TSOTimestamp(physical, logical);
    }

    @Override
    public int compareTo(TSOTimestamp other) {
        int cmp = Long.compare(this.physicalTimestamp, other.physicalTimestamp);
        return (cmp != 0) ? cmp : Long.compare(this.logicalCounter, other.logicalCounter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TSOTimestamp)) {
            return false;
        }
        TSOTimestamp t = (TSOTimestamp) o;
        return physicalTimestamp == t.physicalTimestamp
                && logicalCounter == t.logicalCounter;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(physicalTimestamp);
        result = 31 * result + Long.hashCode(logicalCounter);
        return result;
    }
}
