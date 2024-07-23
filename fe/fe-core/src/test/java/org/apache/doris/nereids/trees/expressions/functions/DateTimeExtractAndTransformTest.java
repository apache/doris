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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.trees.expressions.functions.executable.DateTimeExtractAndTransform;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

class DateTimeExtractAndTransformTest {
    @Test
    void testSpecialDateWeeks() {
        // test week/yearweek for 0000-01-01/02
        LocalDateTime time1 = LocalDateTime.of(0, 1, 1, 0, 0, 0, 0);
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time1, 0));
        Assertions.assertEquals(new TinyIntLiteral((byte) 0), DateTimeExtractAndTransform.week(time1, 1));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time1, 2));
        Assertions.assertEquals(new TinyIntLiteral((byte) 52), DateTimeExtractAndTransform.week(time1, 3));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time1, 4));
        Assertions.assertEquals(new TinyIntLiteral((byte) 0), DateTimeExtractAndTransform.week(time1, 5));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time1, 6));
        Assertions.assertEquals(new TinyIntLiteral((byte) 52), DateTimeExtractAndTransform.week(time1, 7));

        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time1, 0));
        Assertions.assertEquals(new TinyIntLiteral((byte) 0), DateTimeExtractAndTransform.yearWeek(time1, 1));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time1, 2));
        Assertions.assertEquals(new TinyIntLiteral((byte) 52), DateTimeExtractAndTransform.yearWeek(time1, 3));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time1, 4));
        Assertions.assertEquals(new TinyIntLiteral((byte) 0), DateTimeExtractAndTransform.yearWeek(time1, 5));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time1, 6));
        Assertions.assertEquals(new TinyIntLiteral((byte) 52), DateTimeExtractAndTransform.yearWeek(time1, 7));

        LocalDateTime time2 = LocalDateTime.of(0, 1, 2, 0, 0, 0, 0);
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time2, 0));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time2, 1));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time2, 2));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time2, 3));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time2, 4));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time2, 5));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time2, 6));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.week(time2, 7));

        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time2, 0));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time2, 1));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time2, 2));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time2, 3));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time2, 4));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time2, 5));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time2, 6));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.yearWeek(time2, 7));
    }

    @Test
    void testSpecialDates() {
        DateTimeV2Literal dt1 = new DateTimeV2Literal(0, 1, 1, 0, 0, 0);
        DateTimeV2Literal dt2 = new DateTimeV2Literal(0, 1, 2, 0, 0, 0);
        DateTimeV2Literal dt3 = new DateTimeV2Literal(0, 1, 3, 0, 0, 0);
        DateTimeV2Literal dt4 = new DateTimeV2Literal(0, 1, 4, 0, 0, 0);
        DateTimeV2Literal dt5 = new DateTimeV2Literal(0, 1, 5, 0, 0, 0);
        DateTimeV2Literal dt6 = new DateTimeV2Literal(0, 1, 6, 0, 0, 0);
        DateTimeV2Literal dt7 = new DateTimeV2Literal(0, 1, 7, 0, 0, 0);
        DateTimeV2Literal dt8 = new DateTimeV2Literal(0, 1, 8, 0, 0, 0);
        DateTimeV2Literal dtx1 = new DateTimeV2Literal(0, 2, 27, 0, 0, 0);
        DateTimeV2Literal dtx2 = new DateTimeV2Literal(0, 2, 28, 0, 0, 0);
        DateTimeV2Literal dtx3 = new DateTimeV2Literal(0, 3, 1, 0, 0, 0);
        DateTimeV2Literal dtx4 = new DateTimeV2Literal(0, 3, 2, 0, 0, 0);

        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.dayOfWeek(dt1));
        Assertions.assertEquals(new TinyIntLiteral((byte) 2), DateTimeExtractAndTransform.dayOfWeek(dt2));
        Assertions.assertEquals(new TinyIntLiteral((byte) 3), DateTimeExtractAndTransform.dayOfWeek(dt3));
        Assertions.assertEquals(new TinyIntLiteral((byte) 4), DateTimeExtractAndTransform.dayOfWeek(dt4));
        Assertions.assertEquals(new TinyIntLiteral((byte) 5), DateTimeExtractAndTransform.dayOfWeek(dt5));
        Assertions.assertEquals(new TinyIntLiteral((byte) 6), DateTimeExtractAndTransform.dayOfWeek(dt6));
        Assertions.assertEquals(new TinyIntLiteral((byte) 7), DateTimeExtractAndTransform.dayOfWeek(dt7));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.dayOfWeek(dt8));
        Assertions.assertEquals(new TinyIntLiteral((byte) 2), DateTimeExtractAndTransform.dayOfWeek(dtx1));
        Assertions.assertEquals(new TinyIntLiteral((byte) 3), DateTimeExtractAndTransform.dayOfWeek(dtx2));
        Assertions.assertEquals(new TinyIntLiteral((byte) 4), DateTimeExtractAndTransform.dayOfWeek(dtx3));
        Assertions.assertEquals(new TinyIntLiteral((byte) 5), DateTimeExtractAndTransform.dayOfWeek(dtx4));

        Assertions.assertEquals(new SmallIntLiteral((short) 1), DateTimeExtractAndTransform.dayOfYear(dt1));
        Assertions.assertEquals(new SmallIntLiteral((short) 2), DateTimeExtractAndTransform.dayOfYear(dt2));
        Assertions.assertEquals(new SmallIntLiteral((short) 3), DateTimeExtractAndTransform.dayOfYear(dt3));
        Assertions.assertEquals(new SmallIntLiteral((short) 4), DateTimeExtractAndTransform.dayOfYear(dt4));
        Assertions.assertEquals(new SmallIntLiteral((short) 5), DateTimeExtractAndTransform.dayOfYear(dt5));
        Assertions.assertEquals(new SmallIntLiteral((short) 6), DateTimeExtractAndTransform.dayOfYear(dt6));
        Assertions.assertEquals(new SmallIntLiteral((short) 7), DateTimeExtractAndTransform.dayOfYear(dt7));
        Assertions.assertEquals(new SmallIntLiteral((short) 8), DateTimeExtractAndTransform.dayOfYear(dt8));
        Assertions.assertEquals(new SmallIntLiteral((short) 58), DateTimeExtractAndTransform.dayOfYear(dtx1));
        Assertions.assertEquals(new SmallIntLiteral((short) 59), DateTimeExtractAndTransform.dayOfYear(dtx2));
        Assertions.assertEquals(new SmallIntLiteral((short) 60), DateTimeExtractAndTransform.dayOfYear(dtx3));
        Assertions.assertEquals(new SmallIntLiteral((short) 61), DateTimeExtractAndTransform.dayOfYear(dtx4));

        Assertions.assertEquals(new TinyIntLiteral((byte) 52), DateTimeExtractAndTransform.weekOfYear(dt1));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.weekOfYear(dt2));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.weekOfYear(dt3));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.weekOfYear(dt4));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.weekOfYear(dt5));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.weekOfYear(dt6));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.weekOfYear(dt7));
        Assertions.assertEquals(new TinyIntLiteral((byte) 1), DateTimeExtractAndTransform.weekOfYear(dt8));
        Assertions.assertEquals(new TinyIntLiteral((byte) 9), DateTimeExtractAndTransform.weekOfYear(dtx1));
        Assertions.assertEquals(new TinyIntLiteral((byte) 9), DateTimeExtractAndTransform.weekOfYear(dtx2));
        Assertions.assertEquals(new TinyIntLiteral((byte) 9), DateTimeExtractAndTransform.weekOfYear(dtx3));
        Assertions.assertEquals(new TinyIntLiteral((byte) 9), DateTimeExtractAndTransform.weekOfYear(dtx4));
    }
}
