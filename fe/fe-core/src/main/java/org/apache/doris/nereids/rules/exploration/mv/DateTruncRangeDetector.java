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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.types.DateV2Type;

import java.time.YearMonth;
import java.util.Optional;

/**
 * Utility class to detect if a date range covers exactly one complete date_trunc bucket.
 * Supports 'day', 'month', and 'year' granularities.
 */
public class DateTruncRangeDetector {

    /**
     * Detect if [lower, upper] covers exactly one whole date_trunc bucket.
     * Returns the granularity and bucket start if match found.
     */
    public static Optional<BucketInfo> detectWholeBucket(DateLiteral lower, DateLiteral upper) {
        if (lower == null || upper == null) {
            return Optional.empty();
        }

        // Check year first (coarsest granularity)
        if (coversWholeYear(lower, upper)) {
            return Optional.of(new BucketInfo("year", getBucketStart(lower, "year")));
        }

        // Check month
        if (coversWholeMonth(lower, upper)) {
            return Optional.of(new BucketInfo("month", getBucketStart(lower, "month")));
        }

        // Check day
        if (coversWholeDay(lower, upper)) {
            return Optional.of(new BucketInfo("day", getBucketStart(lower, "day")));
        }

        return Optional.empty();
    }

    /**
     * Check if [lower, upper] covers exactly one whole year.
     */
    public static boolean coversWholeYear(DateLiteral lower, DateLiteral upper) {
        if (lower.getYear() != upper.getYear()) {
            return false;
        }
        return lower.getMonth() == 1 && lower.getDay() == 1
                && upper.getMonth() == 12 && upper.getDay() == 31;
    }

    /**
     * Check if [lower, upper] covers exactly one whole month.
     */
    public static boolean coversWholeMonth(DateLiteral lower, DateLiteral upper) {
        if (lower.getYear() != upper.getYear() || lower.getMonth() != upper.getMonth()) {
            return false;
        }
        if (lower.getDay() != 1) {
            return false;
        }
        int lastDayOfMonth = YearMonth.of((int) lower.getYear(), (int) lower.getMonth()).lengthOfMonth();
        return upper.getDay() == lastDayOfMonth;
    }

    /**
     * Check if [lower, upper] covers exactly one whole day.
     */
    public static boolean coversWholeDay(DateLiteral lower, DateLiteral upper) {
        return lower.getYear() == upper.getYear()
                && lower.getMonth() == upper.getMonth()
                && lower.getDay() == upper.getDay();
    }

    /**
     * Get the bucket start for a given granularity.
     */
    public static DateLiteral getBucketStart(DateLiteral anyDateInBucket, String unit) {
        boolean isV2 = anyDateInBucket instanceof DateV2Literal
                || anyDateInBucket.getDataType() instanceof DateV2Type;

        long year = anyDateInBucket.getYear();
        long month = anyDateInBucket.getMonth();
        long day = anyDateInBucket.getDay();

        switch (unit.toLowerCase()) {
            case "year":
                return isV2 ? new DateV2Literal(year, 1, 1) : new DateLiteral(year, 1, 1);
            case "month":
                return isV2 ? new DateV2Literal(year, month, 1) : new DateLiteral(year, month, 1);
            case "day":
                return isV2 ? new DateV2Literal(year, month, day) : new DateLiteral(year, month, day);
            default:
                throw new IllegalArgumentException("Unsupported unit: " + unit);
        }
    }

    /**
     * Container for bucket detection result.
     */
    public static class BucketInfo {
        public final String unit;
        public final DateLiteral bucketStart;

        public BucketInfo(String unit, DateLiteral bucketStart) {
            this.unit = unit;
            this.bucketStart = bucketStart;
        }
    }
}
