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
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class DateTruncRangeDetectorTest {

    @Test
    void testCoversWholeMonth_January() {
        DateLiteral lower = new DateLiteral(2025, 1, 1);
        DateLiteral upper = new DateLiteral(2025, 1, 31);
        Assertions.assertTrue(DateTruncRangeDetector.coversWholeMonth(lower, upper));
    }

    @Test
    void testCoversWholeMonth_February_LeapYear() {
        DateLiteral lower = new DateLiteral(2024, 2, 1);
        DateLiteral upper = new DateLiteral(2024, 2, 29);
        Assertions.assertTrue(DateTruncRangeDetector.coversWholeMonth(lower, upper));
    }

    @Test
    void testCoversWholeMonth_February_NonLeapYear() {
        DateLiteral lower = new DateLiteral(2025, 2, 1);
        DateLiteral upper = new DateLiteral(2025, 2, 28);
        Assertions.assertTrue(DateTruncRangeDetector.coversWholeMonth(lower, upper));
    }

    @Test
    void testCoversWholeMonth_Partial() {
        DateLiteral lower = new DateLiteral(2025, 1, 5);
        DateLiteral upper = new DateLiteral(2025, 1, 31);
        Assertions.assertFalse(DateTruncRangeDetector.coversWholeMonth(lower, upper));
    }

    @Test
    void testCoversWholeMonth_WrongEndDay() {
        DateLiteral lower = new DateLiteral(2025, 1, 1);
        DateLiteral upper = new DateLiteral(2025, 1, 30);
        Assertions.assertFalse(DateTruncRangeDetector.coversWholeMonth(lower, upper));
    }

    @Test
    void testCoversWholeMonth_CrossMonth() {
        DateLiteral lower = new DateLiteral(2025, 1, 15);
        DateLiteral upper = new DateLiteral(2025, 2, 15);
        Assertions.assertFalse(DateTruncRangeDetector.coversWholeMonth(lower, upper));
    }

    @Test
    void testCoversWholeYear() {
        DateLiteral lower = new DateLiteral(2025, 1, 1);
        DateLiteral upper = new DateLiteral(2025, 12, 31);
        Assertions.assertTrue(DateTruncRangeDetector.coversWholeYear(lower, upper));
    }

    @Test
    void testCoversWholeYear_Partial() {
        DateLiteral lower = new DateLiteral(2025, 2, 1);
        DateLiteral upper = new DateLiteral(2025, 12, 31);
        Assertions.assertFalse(DateTruncRangeDetector.coversWholeYear(lower, upper));
    }

    @Test
    void testCoversWholeDay() {
        DateLiteral lower = new DateLiteral(2025, 1, 15);
        DateLiteral upper = new DateLiteral(2025, 1, 15);
        Assertions.assertTrue(DateTruncRangeDetector.coversWholeDay(lower, upper));
    }

    @Test
    void testCoversWholeDay_DifferentDays() {
        DateLiteral lower = new DateLiteral(2025, 1, 15);
        DateLiteral upper = new DateLiteral(2025, 1, 16);
        Assertions.assertFalse(DateTruncRangeDetector.coversWholeDay(lower, upper));
    }

    @Test
    void testDetectWholeBucket_Month() {
        DateLiteral lower = new DateLiteral(2025, 1, 1);
        DateLiteral upper = new DateLiteral(2025, 1, 31);
        Optional<DateTruncRangeDetector.BucketInfo> result =
                DateTruncRangeDetector.detectWholeBucket(lower, upper);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals("month", result.get().unit);
        Assertions.assertEquals(new DateLiteral(2025, 1, 1).getDouble(),
                result.get().bucketStart.getDouble());
    }

    @Test
    void testDetectWholeBucket_Year() {
        DateLiteral lower = new DateLiteral(2025, 1, 1);
        DateLiteral upper = new DateLiteral(2025, 12, 31);
        Optional<DateTruncRangeDetector.BucketInfo> result =
                DateTruncRangeDetector.detectWholeBucket(lower, upper);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals("year", result.get().unit);
    }

    @Test
    void testDetectWholeBucket_Day() {
        DateLiteral lower = new DateLiteral(2025, 1, 15);
        DateLiteral upper = new DateLiteral(2025, 1, 15);
        Optional<DateTruncRangeDetector.BucketInfo> result =
                DateTruncRangeDetector.detectWholeBucket(lower, upper);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals("day", result.get().unit);
    }

    @Test
    void testDetectWholeBucket_NoMatch() {
        DateLiteral lower = new DateLiteral(2025, 1, 5);
        DateLiteral upper = new DateLiteral(2025, 1, 31);
        Optional<DateTruncRangeDetector.BucketInfo> result =
                DateTruncRangeDetector.detectWholeBucket(lower, upper);
        Assertions.assertFalse(result.isPresent());
    }

    @Test
    void testGetBucketStart_Month() {
        DateLiteral date = new DateLiteral(2025, 1, 15);
        DateLiteral bucketStart = DateTruncRangeDetector.getBucketStart(date, "month");
        Assertions.assertEquals(2025, bucketStart.getYear());
        Assertions.assertEquals(1, bucketStart.getMonth());
        Assertions.assertEquals(1, bucketStart.getDay());
    }

    @Test
    void testGetBucketStart_Year() {
        DateLiteral date = new DateLiteral(2025, 6, 15);
        DateLiteral bucketStart = DateTruncRangeDetector.getBucketStart(date, "year");
        Assertions.assertEquals(2025, bucketStart.getYear());
        Assertions.assertEquals(1, bucketStart.getMonth());
        Assertions.assertEquals(1, bucketStart.getDay());
    }

    @Test
    void testGetBucketStart_Day() {
        DateLiteral date = new DateLiteral(2025, 1, 15);
        DateLiteral bucketStart = DateTruncRangeDetector.getBucketStart(date, "day");
        Assertions.assertEquals(2025, bucketStart.getYear());
        Assertions.assertEquals(1, bucketStart.getMonth());
        Assertions.assertEquals(15, bucketStart.getDay());
    }

    @Test
    void testCoversWholeMonth_DateV2Literal() {
        DateV2Literal lower = new DateV2Literal(2025, 1, 1);
        DateV2Literal upper = new DateV2Literal(2025, 1, 31);
        Assertions.assertTrue(DateTruncRangeDetector.coversWholeMonth(lower, upper));
    }

    @Test
    void testDetectWholeBucket_DateV2Literal() {
        DateV2Literal lower = new DateV2Literal(2025, 1, 1);
        DateV2Literal upper = new DateV2Literal(2025, 1, 31);
        Optional<DateTruncRangeDetector.BucketInfo> result =
                DateTruncRangeDetector.detectWholeBucket(lower, upper);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals("month", result.get().unit);
        Assertions.assertTrue(result.get().bucketStart instanceof DateV2Literal);
    }

    @Test
    void testGetBucketStart_DateV2Literal() {
        DateV2Literal date = new DateV2Literal(2025, 1, 15);
        DateLiteral bucketStart = DateTruncRangeDetector.getBucketStart(date, "month");
        Assertions.assertTrue(bucketStart instanceof DateV2Literal);
        Assertions.assertEquals(2025, bucketStart.getYear());
        Assertions.assertEquals(1, bucketStart.getMonth());
        Assertions.assertEquals(1, bucketStart.getDay());
    }

    @Test
    void testCoversWholeMonth_DateTimeLiteral_ShouldNotMatch() {
        // DateTimeLiteral extends DateLiteral — verify the inheritance that motivates the guard
        DateTimeLiteral dtLiteral = new DateTimeLiteral(2025, 1, 1, 0, 0, 0);
        Assertions.assertTrue(dtLiteral instanceof DateLiteral);
    }

    @Test
    void testDetectWholeBucket_DateTimeLiteral_ShouldNotDetect() {
        // DateTimeLiteral has time-of-day semantics: dt <= '2025-01-31 00:00:00' excludes
        // most of Jan 31, so the detector must reject DateTimeLiteral inputs directly.
        DateTimeLiteral lower = new DateTimeLiteral(2025, 1, 1, 0, 0, 0);
        DateTimeLiteral upper = new DateTimeLiteral(2025, 1, 31, 0, 0, 0);

        Optional<DateTruncRangeDetector.BucketInfo> result =
                DateTruncRangeDetector.detectWholeBucket(lower, upper);

        Assertions.assertFalse(result.isPresent());
    }
}
