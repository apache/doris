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
// This file is copied from
// https://github.com/trinodb/trino/blob/master/core/trino-spi/src/main/java/io/trino/spi/SplitWeight.java
// and modified by Doris

package org.apache.doris.datasource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.errorprone.annotations.DoNotCall;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.function.Function;

public final class SplitWeight {
    private static final long UNIT_VALUE = 100;
    private static final int UNIT_SCALE = 2; // Decimal scale such that (10 ^ UNIT_SCALE) == UNIT_VALUE
    private static final SplitWeight STANDARD_WEIGHT = new SplitWeight(UNIT_VALUE);

    private final long value;

    private SplitWeight(long value) {
        if (value <= 0) {
            throw new IllegalArgumentException("value must be > 0, found: " + value);
        }
        this.value = value;
    }

    /**
     * Produces a {@link SplitWeight} from the raw internal value representation. This method is intended
     * primarily for JSON deserialization, and connectors should use not call this factory method directly
     * to construct {@link SplitWeight} instances. Instead, connectors should use
     * {@link SplitWeight#fromProportion(double)}
     * to avoid breakages that could arise if {@link SplitWeight#UNIT_VALUE} changes in the future.
     */
    @JsonCreator
    @DoNotCall // For JSON serialization only
    public static SplitWeight fromRawValue(long value) {
        return fromRawValueInternal(value);
    }

    /**
     * Produces a {@link SplitWeight} that corresponds to the {@link SplitWeight#standard()} weight
     * proportionally, i.e., a parameter of <code>1.0</code> will be equivalent to the standard weight
     * and a value of <code>0.5</code> will be 1/2 of the standard split weight. Valid arguments
     * must be greater than zero and finite. Connectors should prefer constructing split weights
     * using this factory method rather than passing a raw integer value in case the integer representation
     * of a standard split needs to change in the future.
     *
     * @param weight the proportional weight relative to a standard split, expressed as a double
     * @return a {@link SplitWeight} with a raw value corresponding to the requested proportion
     */
    public static SplitWeight fromProportion(double weight) {
        if (weight <= 0 || !Double.isFinite(weight)) {
            throw new IllegalArgumentException("Invalid weight: " + weight);
        }
        // Must round up to avoid small weights rounding to 0
        return fromRawValueInternal((long) Math.ceil(weight * UNIT_VALUE));
    }

    private static SplitWeight fromRawValueInternal(long value) {
        return value == UNIT_VALUE ? STANDARD_WEIGHT : new SplitWeight(value);
    }

    public static SplitWeight standard() {
        return STANDARD_WEIGHT;
    }

    public static long rawValueForStandardSplitCount(int splitCount) {
        if (splitCount < 0) {
            throw new IllegalArgumentException("splitCount must be >= 0, found: " + splitCount);
        }
        return Math.multiplyExact(splitCount, UNIT_VALUE);
    }

    public static <T> long rawValueSum(Collection<T> collection, Function<T, SplitWeight> getter) {
        long sum = 0;
        for (T item : collection) {
            long value = getter.apply(item).getRawValue();
            sum = Math.addExact(sum, value);
        }
        return sum;
    }

    /**
     * @return The internal integer representation for this weight value
     */
    @JsonValue
    public long getRawValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof SplitWeight)) {
            return false;
        }
        return this.value == ((SplitWeight) other).value;
    }

    @Override
    public String toString() {
        if (value == UNIT_VALUE) {
            return "1";
        }
        return BigDecimal.valueOf(value, -UNIT_SCALE).stripTrailingZeros().toPlainString();
    }
}
