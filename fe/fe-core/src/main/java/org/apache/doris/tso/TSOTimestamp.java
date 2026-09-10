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
import com.google.common.base.Preconditions;
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

    // Sentinel meaning "no upper bound" / "latest" (e.g. FOR VERSION AS OF 9223372036854775807).
    // It is intentionally NOT a real allocated TSO; it sorts above every real TSO so that a
    // right-open predicate {@code x < UNBOUNDED_TSO} still selects all rows.
    public static final long UNBOUNDED_TSO = Long.MAX_VALUE;

    // The largest legal real (allocated) TSO. Real TSOs never reach UNBOUNDED_TSO, which leaves
    // room for nextTso() to compute a successor without overflow.
    public static final long MAX_REAL_TSO = Long.MAX_VALUE - 1;

    // Largest physical millisecond that can be represented by a real TSO.
    public static final long MAX_PHYSICAL_TIMESTAMP = extractPhysicalTime(MAX_REAL_TSO);

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
     * Compose the lower boundary of a physical millisecond.
     *
     * @return 64-bit TSO timestamp with a zero logical counter
     */
    public static long composePhysicalTimestamp(long physicalTimestamp) {
        return composeRealTso(physicalTimestamp, 0L);
    }

    /**
     * The next discrete TSO after a real {@code tso}. TSO values are dense integers, so this converts
     * an inclusive bound into the equivalent right-open (exclusive) bound: {@code x <= tso} is the
     * same row set as {@code x < nextTso(tso)}, and a lower bound that excludes {@code tso} itself is
     * {@code x >= nextTso(tso)}. Callers should use this instead of a bare {@code + 1} so the TSO
     * interval arithmetic stays in one place.
     *
     * <p>This is a pure successor over real TSOs only. The {@link #UNBOUNDED_TSO} sentinel is not a
     * real TSO and must be handled by callers before reaching here; requiring a real input keeps the
     * successor free of overflow and makes the "real TSOs never reach the sentinel" assumption an
     * enforced invariant rather than a comment.
     */
    public static long nextTso(long tso) {
        Preconditions.checkArgument(tso >= 0 && tso <= MAX_REAL_TSO,
                "nextTso expects a real TSO in [0, %s], got %s", MAX_REAL_TSO, tso);
        return tso + 1;
    }

    /**
     * Convert an inclusive stored TSO bound into the half-open (exclusive) bound the scan pushes
     * down, tolerating "no bound" inputs. Returns {@code null} when the input is {@code null} or a
     * negative sentinel (e.g. a partition that never got a real TSO stores -1, meaning no committed
     * change): a {@code null} result tells the caller to leave that bound unset rather than feeding
     * a non-real value into {@link #nextTso}. A real TSO is mapped to its successor.
     */
    public static Long toExclusiveBound(Long storedTso) {
        if (storedTso == null || storedTso < 0) {
            return null;
        }
        return nextTso(storedTso);
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

    /**
     * Compose a real (allocated) TSO from physical time and logical counter, validating that the
     * inputs and the result stay within the legal real-TSO range instead of silently masking. This
     * is the single construction entry for TSOs produced by the generator, so the invariant
     * "a real TSO is non-negative and never reaches {@link #UNBOUNDED_TSO}" is enforced here once
     * rather than assumed at every call site.
     *
     * @throws IllegalArgumentException if the components are out of range or the composed value
     *         would exceed {@link #MAX_REAL_TSO}
     */
    public static long composeRealTso(long physicalTime, long logicalCounter) {
        Preconditions.checkArgument(physicalTime >= 0 && physicalTime <= RAW_PHYSICAL_MASK,
                "physicalTime out of range [0, %s]: %s", RAW_PHYSICAL_MASK, physicalTime);
        Preconditions.checkArgument(logicalCounter >= 0 && logicalCounter <= MAX_LOGICAL_COUNTER,
                "logicalCounter out of range [0, %s]: %s", MAX_LOGICAL_COUNTER, logicalCounter);
        long tso = Math.addExact(
                Math.multiplyExact(physicalTime, 1L << PHYSICAL_SHIFT), logicalCounter);
        Preconditions.checkArgument(tso <= MAX_REAL_TSO,
                "composed TSO exceeds MAX_REAL_TSO (%s): %s", MAX_REAL_TSO, tso);
        return tso;
    }

    public static long extractTimestamp(long tso) {
        //  extract physical time from TSO timestamp by remove Lower 18 bits logical counter bits
        return (tso >> PHYSICAL_SHIFT);
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
