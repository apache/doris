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

package org.apache.doris.connector.api.pushdown;

import java.io.Serializable;
import java.util.Objects;

/**
 * A single range bound (inclusive or exclusive) on a value.
 *
 * <p>A {@code ConnectorRange} describes {@code [low, high]} or any
 * open/half-open variant.  {@code null} endpoints represent unbounded.</p>
 */
public final class ConnectorRange implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The lower bound value (null = unbounded). */
    private final Comparable<?> low;
    private final boolean lowInclusive;
    /** The upper bound value (null = unbounded). */
    private final Comparable<?> high;
    private final boolean highInclusive;

    private ConnectorRange(Comparable<?> low, boolean lowInclusive,
            Comparable<?> high, boolean highInclusive) {
        this.low = low;
        this.lowInclusive = lowInclusive;
        this.high = high;
        this.highInclusive = highInclusive;
    }

    /** Unbounded (matches everything). */
    public static ConnectorRange all() {
        return new ConnectorRange(null, false, null, false);
    }

    /** Exact equality. */
    public static ConnectorRange equal(Comparable<?> value) {
        Objects.requireNonNull(value, "value");
        return new ConnectorRange(value, true, value, true);
    }

    /** Greater-than: {@code (low, +∞)}. */
    public static ConnectorRange greaterThan(Comparable<?> low) {
        return new ConnectorRange(low, false, null, false);
    }

    /** Greater-than-or-equal: {@code [low, +∞)}. */
    public static ConnectorRange greaterThanOrEqual(Comparable<?> low) {
        return new ConnectorRange(low, true, null, false);
    }

    /** Less-than: {@code (-∞, high)}. */
    public static ConnectorRange lessThan(Comparable<?> high) {
        return new ConnectorRange(null, false, high, false);
    }

    /** Less-than-or-equal: {@code (-∞, high]}. */
    public static ConnectorRange lessThanOrEqual(Comparable<?> high) {
        return new ConnectorRange(null, false, high, true);
    }

    /** Arbitrary range. */
    public static ConnectorRange range(Comparable<?> low, boolean lowInclusive,
            Comparable<?> high, boolean highInclusive) {
        return new ConnectorRange(low, lowInclusive, high, highInclusive);
    }

    public Comparable<?> getLow() {
        return low;
    }

    public boolean isLowInclusive() {
        return lowInclusive;
    }

    public boolean isLowUnbounded() {
        return low == null;
    }

    public Comparable<?> getHigh() {
        return high;
    }

    public boolean isHighInclusive() {
        return highInclusive;
    }

    public boolean isHighUnbounded() {
        return high == null;
    }

    public boolean isSingleValue() {
        return low != null && high != null && lowInclusive && highInclusive
                && low.equals(high);
    }

    public boolean isAll() {
        return low == null && high == null;
    }

    @Override
    public String toString() {
        String lo = low == null ? "(-∞" : (lowInclusive ? "[" + low : "(" + low);
        String hi = high == null ? "+∞)" : (highInclusive ? high + "]" : high + ")");
        return lo + ", " + hi;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorRange)) {
            return false;
        }
        ConnectorRange that = (ConnectorRange) o;
        return lowInclusive == that.lowInclusive
                && highInclusive == that.highInclusive
                && Objects.equals(low, that.low)
                && Objects.equals(high, that.high);
    }

    @Override
    public int hashCode() {
        return Objects.hash(low, lowInclusive, high, highInclusive);
    }
}
